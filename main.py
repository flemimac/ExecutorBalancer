from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import asyncio
from datetime import datetime
import pandas as pd
import io

from database import get_db, init_db
from models import Request, Executor
from distribution import DistributionEngine

app = FastAPI(
    title="Request Distribution System",
    description="Система равномерного распределения заявок между исполнителями",
    version="1.0.0"
)


class RequestCreate(BaseModel):
    parameters: Dict[str, Any]


class RequestBulkCreate(BaseModel):
    requests: List[Dict[str, Any]]  


class ExecutorCreate(BaseModel):
    name: str
    parameters: Optional[Dict[str, Any]] = {}


class ExecutorUpdate(BaseModel):
    name: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class RequestResponse(BaseModel):
    id: int
    parameters: Dict[str, Any]
    status: str
    assigned_to: Optional[int]
    assigned_at: Optional[datetime]
    created_at: datetime


class ExecutorResponse(BaseModel):
    id: int
    name: str
    parameters: Dict[str, Any]
    total_assigned: int
    is_active: bool
    created_at: datetime


@app.on_event("startup")
async def startup_event():
    """Инициализация базы данных при запуске"""
    await init_db()


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def serve_dashboard():
    """Возвращаем главную страницу"""
    return FileResponse("static/index.html")



@app.post("/requests/", response_model=RequestResponse)
async def create_request(
    request_data: RequestCreate,
    session: AsyncSession = Depends(get_db)
):
    """Создать одну заявку"""
    db_request = Request(
        parameters=request_data.parameters,
        status="pending"
    )
    session.add(db_request)
    await session.commit()
    await session.refresh(db_request)
    
    return db_request


@app.post("/requests/bulk/", response_model=Dict[str, Any])
async def create_requests_bulk(
    request_data: RequestBulkCreate,
    session: AsyncSession = Depends(get_db)
):
    """Создать множество заявок одним запросом"""
    db_requests = [
        Request(parameters=params, status="pending")
        for params in request_data.requests
    ]
    
    session.add_all(db_requests)
    await session.commit()
    
    return {
        "message": f"Создано {len(db_requests)} заявок",
        "count": len(db_requests)
    }


@app.post("/requests/{request_id}/complete")
async def complete_request(
    request_id: int,
    session: AsyncSession = Depends(get_db)
):
    """Завершить обработку заявки"""
    result = await session.execute(
        select(Request).where(Request.id == request_id)
    )
    request = result.scalar_one_or_none()
    
    if not request:
        raise HTTPException(status_code=404, detail="Заявка не найдена")
    
    request.status = "completed"
    await session.commit()
    
    return {"message": "Заявка завершена", "request_id": request_id}


@app.get("/requests/{request_id}", response_model=RequestResponse)
async def get_request(
    request_id: int,
    session: AsyncSession = Depends(get_db)
):
    """Получить информацию о заявке"""
    result = await session.execute(
        select(Request).where(Request.id == request_id)
    )
    request = result.scalar_one_or_none()
    
    if not request:
        raise HTTPException(status_code=404, detail="Заявка не найдена")
    
    return request


@app.get("/requests/recent/{limit}", response_model=List[RequestResponse])
async def get_recent_requests(
    limit: int = 20,
    session: AsyncSession = Depends(get_db)
):
    """Получить последние заявки"""
    result = await session.execute(
        select(Request)
        .order_by(Request.id.desc())
        .limit(limit)
    )
    requests = result.scalars().all()
    return list(requests)



@app.post("/executors/", response_model=ExecutorResponse)
async def create_executor(
    executor_data: ExecutorCreate,
    session: AsyncSession = Depends(get_db)
):
    """Создать нового исполнителя (можно в любое время)"""
    
    result = await session.execute(
        select(Executor).where(Executor.name == executor_data.name)
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        raise HTTPException(
            status_code=400, 
            detail="Исполнитель с таким именем уже существует"
        )
    
    db_executor = Executor(
        name=executor_data.name,
        parameters=executor_data.parameters or {}
    )
    session.add(db_executor)
    await session.commit()
    await session.refresh(db_executor)
    
    return db_executor


@app.get("/executors/", response_model=List[ExecutorResponse])
async def list_executors(
    session: AsyncSession = Depends(get_db)
):
    """Получить список всех исполнителей"""
    result = await session.execute(select(Executor))
    executors = result.scalars().all()
    return list(executors)


@app.get("/executors/{executor_id}", response_model=ExecutorResponse)
async def get_executor(
    executor_id: int,
    session: AsyncSession = Depends(get_db)
):
    """Получить информацию об исполнителе"""
    result = await session.execute(
        select(Executor).where(Executor.id == executor_id)
    )
    executor = result.scalar_one_or_none()
    
    if not executor:
        raise HTTPException(status_code=404, detail="Исполнитель не найден")
    
    return executor


@app.patch("/executors/{executor_id}", response_model=ExecutorResponse)
async def update_executor(
    executor_id: int,
    executor_data: ExecutorUpdate,
    session: AsyncSession = Depends(get_db)
):
    """Обновить параметры исполнителя"""
    result = await session.execute(
        select(Executor).where(Executor.id == executor_id)
    )
    executor = result.scalar_one_or_none()
    
    if not executor:
        raise HTTPException(status_code=404, detail="Исполнитель не найден")
    
    if executor_data.name is not None:
        executor.name = executor_data.name
    if executor_data.parameters is not None:
        executor.parameters = executor_data.parameters
    if executor_data.is_active is not None:
        executor.is_active = executor_data.is_active
    
    await session.commit()
    await session.refresh(executor)
    
    return executor



@app.post("/executors/{executor_id}/get-next-request", response_model=Optional[RequestResponse])
async def get_next_request_for_executor(
    executor_id: int,
    session: AsyncSession = Depends(get_db)
):
    """
    Получить следующую заявку для исполнителя
    Вернет None, если заявок нет
    """
    request = await DistributionEngine.get_next_request(session, executor_id)
    return request



@app.get("/stats")
async def get_statistics(
    session: AsyncSession = Depends(get_db)
):
    """Получить статистику распределения"""
    stats = await DistributionEngine.get_distribution_stats(session)
    return stats


@app.post("/upload/excel")
async def upload_excel(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db)
):
    """Загрузить заявки из Excel файла"""
    try:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="Только файлы .xlsx и .xls")
        
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        
        if 'parameters' not in df.columns:
            requests_data = []
            for _, row in df.iterrows():
                params = row.to_dict()
                requests_data.append(params)
        else:
            requests_data = df['parameters'].tolist()
        
        created = 0
        for params in requests_data:
            if isinstance(params, dict):
                db_request = Request(
                    parameters=params,
                    status="pending"
                )
                session.add(db_request)
                created += 1
        
        await session.commit()
        
        return {
            "message": f"Успешно загружено {created} заявок из Excel",
            "count": created
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки файла: {str(e)}")


@app.get("/")
async def root():
    """Главная страница с документацией"""
    return {
        "message": "Request Distribution System API",
        "docs": "/docs",
        "version": "1.0.0"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

