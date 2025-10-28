from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import asyncio
from datetime import datetime
import pandas as pd
import io

from database import get_db, init_db
from models import Request, Executor
from distribution import DistributionEngine
import re
from datetime import datetime as dt

def detect_data_type(value: str) -> str:
    """Автоматически определяет тип данных по значению"""
    if not value or not isinstance(value, str):
        return "unknown"
    
    value = value.strip()
    
    # Проверка на дату (различные форматы)
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        r'^\d{2}\.\d{2}\.\d{4}$',  # DD.MM.YYYY
        r'^\d{2}/\d{2}/\d{4}$',   # DD/MM/YYYY
        r'^\d{4}\.\d{2}\.\d{2}$',  # YYYY.MM.DD
    ]
    
    for pattern in date_patterns:
        if re.match(pattern, value):
            return "date"
    
    if re.match(r'^-?\d+$', value):
        return "integer"
    
    if re.match(r'^-?\d+\.\d+$', value):
        return "float"
    
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.svg']
    if any(value.lower().endswith(ext) for ext in image_extensions):
        return "raster"
    
    if re.match(r'^[а-яёА-ЯЁa-zA-Z\s]+$', value):
        return "text"
    
    if len(value) > 0:
        return "string"
    
    return "unknown"


def match_parameter_values(executor_param: str, request_param: str) -> bool:
    """Сравнивает параметры исполнителя и заявки с учетом типов данных"""
    if not executor_param or not request_param:
        return False
    
    # Определяем типы данных
    executor_type = detect_data_type(executor_param)
    request_type = detect_data_type(request_param)
    
    # Если типы не совпадают, проверяем совместимость
    if executor_type != request_type:
        # Числовые типы совместимы между собой
        if {executor_type, request_type}.issubset({"integer", "float"}):
            try:
                float(executor_param)
                float(request_param)
                return abs(float(executor_param) - float(request_param)) < 0.001
            except ValueError:
                return False
        
        # Текстовые типы совместимы
        if {executor_type, request_type}.issubset({"text", "string"}):
            return executor_param.lower() == request_param.lower()
        
        # Растровые данные совместимы
        if {executor_type, request_type}.issubset({"raster"}):
            return executor_param.lower() == request_param.lower()
        
        return False
    
    # Если типы совпадают, сравниваем значения
    if executor_type == "integer":
        return int(executor_param) == int(request_param)
    elif executor_type == "float":
        return abs(float(executor_param) - float(request_param)) < 0.001
    elif executor_type == "date":
        return executor_param == request_param
    else:  # text, string, raster
        return executor_param.lower() == request_param.lower()


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
    parameters: Optional[Dict[str, Any]] = None


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

class RequestComplete(BaseModel):
    result: str = "Completed"

class BatchCompleteRequest(BaseModel):
    request_ids: List[int]
    result: str = "Batch completed"


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
    
    # Используем переданные параметры или пустой словарь
    parameters = executor_data.parameters or {}
    
    db_executor = Executor(
        name=executor_data.name,
        parameters=parameters
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


@app.delete("/executors/{executor_id}")
async def delete_executor(
    executor_id: int,
    session: AsyncSession = Depends(get_db)
):
    """Удалить исполнителя"""
    result = await session.execute(
        select(Executor).where(Executor.id == executor_id)
    )
    executor = result.scalar_one_or_none()
    
    if not executor:
        raise HTTPException(status_code=404, detail="Исполнитель не найден")
    
    await session.delete(executor)
    await session.commit()
    
    return {"success": True, "message": "Executor deleted"}


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


@app.delete("/requests/clear")
async def clear_all_requests(session: AsyncSession = Depends(get_db)):
    """Удалить все заявки"""
    try:
        # Удаляем все заявки
        await session.execute(delete(Request))
        await session.commit()
        
        return {"message": "Все заявки удалены", "count": 0}
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка удаления заявок: {str(e)}")


@app.post("/executors/{executor_id}/get-batch-requests")
async def get_batch_requests(
    executor_id: int,
    batch_size: int = 5,
    grouping_param: str = "city",  # city, data_type, или custom
    db: AsyncSession = Depends(get_db)
):
    """Получить батч заявок для исполнителя с группировкой по параметру"""
    try:
        # Получаем исполнителя
        result = await db.execute(
            select(Executor).where(Executor.id == executor_id, Executor.is_active == True)
        )
        executor = result.scalar_one_or_none()
        
        if not executor:
            raise HTTPException(status_code=404, detail="Исполнитель не найден или неактивен")
        
        # Получаем параметры исполнителя
        executor_params = executor.parameters or {}
        executor_city = executor_params.get("city")
        executor_data_type = executor_params.get("data_type")
        
        # Строим запрос для получения заявок
        query = select(Request).where(Request.status == "pending")
        
        # Пока что убираем фильтрацию по параметрам для упрощения
        # В будущем можно добавить более сложную логику фильтрации
        
        # Простая группировка по ID
        query = query.order_by(Request.id)
        
        # Ограничиваем количество заявок
        query = query.limit(batch_size)
        
        result = await db.execute(query)
        requests = result.scalars().all()
        
        if not requests:
            return {"requests": [], "batch_size": 0, "message": "Нет доступных заявок для батча"}
        
        # Назначаем заявки исполнителю
        assigned_requests = []
        for request in requests:
            request.status = "assigned"
            request.assigned_to = executor_id
            request.assigned_at = datetime.utcnow()
            executor.total_assigned += 1
            assigned_requests.append(request)
        
        await db.commit()
        
        # Обновляем объекты после коммита
        for request in assigned_requests:
            await db.refresh(request)
        
        return {
            "requests": [
                {
                    "id": req.id,
                    "parameters": req.parameters,
                    "status": req.status,
                    "assigned_to": req.assigned_to,
                    "assigned_at": req.assigned_at,
                    "created_at": req.created_at
                }
                for req in assigned_requests
            ],
            "batch_size": len(assigned_requests),
            "executor_id": executor_id,
            "grouping_param": grouping_param,
            "message": f"Назначено {len(assigned_requests)} заявок батчем"
        }
        
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка получения батча заявок: {str(e)}")


@app.post("/requests/batch-complete")
async def batch_complete_requests(
    request: BatchCompleteRequest,
    db: AsyncSession = Depends(get_db)
):
    """Завершить несколько заявок батчем"""
    try:
        # Получаем заявки
        result_query = await db.execute(
            select(Request).where(
                Request.id.in_(request.request_ids),
                Request.status == "assigned"
            )
        )
        requests = result_query.scalars().all()
        
        if not requests:
            return {"message": "Нет заявок для завершения", "completed": 0}
        
        # Завершаем заявки
        completed_count = 0
        for req in requests:
            req.status = "completed"
            req.completed_at = datetime.utcnow()
            req.result = request.result
            completed_count += 1
        
        await db.commit()
        
        return {
            "message": f"Завершено {completed_count} заявок батчем",
            "completed": completed_count,
            "request_ids": [req.id for req in requests]
        }
        
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка завершения батча заявок: {str(e)}")


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

