from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import JSON
from sqlalchemy.sql import cast
from typing import List, Optional, Dict, Any
from models import Executor, Request
from datetime import datetime
import json
import re


def detect_data_type(value: str) -> str:
    """Автоматически определяет тип данных по значению"""
    if not value or not isinstance(value, str):
        return "unknown"
    
    value = value.strip()
    
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
    
    executor_type = detect_data_type(executor_param)
    request_type = detect_data_type(request_param)
    
    if executor_type != request_type:
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
        
        if {executor_type, request_type}.issubset({"raster"}):
            return executor_param.lower() == request_param.lower()
        
        return False
    
    if executor_type == "integer":
        return int(executor_param) == int(request_param)
    elif executor_type == "float":
        return abs(float(executor_param) - float(request_param)) < 0.001
    elif executor_type == "date":
        return executor_param == request_param
    else:  # text, string, raster
        return executor_param.lower() == request_param.lower()


class DistributionEngine:
    
    @staticmethod
    async def get_next_request(
        session: AsyncSession, 
        executor_id: int
    ) -> Optional[Request]:

        result = await session.execute(
            select(Executor).where(Executor.id == executor_id, Executor.is_active == True)
        )
        executor = result.scalar_one_or_none()
        
        if not executor:
            return None
        
        executor_params = executor.parameters or {}
        
        request = None
        
        if executor_params:
            all_requests = await session.execute(
                select(Request).where(Request.status == "pending")
            )
            all_requests = all_requests.scalars().all()
            
            matching_requests = []
            for req in all_requests:
                if req.parameters and 'parameters' in req.parameters:
                    req_params = req.parameters['parameters']
                    match = True
                    
                    for param_name, param_value in executor_params.items():
                        if param_name in req_params:
                            if not match_parameter_values(str(param_value), str(req_params[param_name])):
                                match = False
                                break
                        else:
                            match = False
                            break
                    
                    if match:
                        matching_requests.append(req)
            
            if matching_requests:
                request = matching_requests[0] 
            else:
                request = None
        
        if not request:
            result = await session.execute(
                select(Request).where(Request.status == "pending").limit(1)
            )
            request = result.scalar_one_or_none()
        
        if request:
            request.status = "assigned"
            request.assigned_to = executor_id
            request.assigned_at = datetime.utcnow()
            
            executor.total_assigned += 1
            
            await session.commit()
            await session.refresh(request)
        
        return request
    
    @staticmethod
    async def get_optimal_executor(
        session: AsyncSession
    ) -> Optional[Executor]:
        result = await session.execute(
            select(Executor)
            .where(Executor.is_active == True)
            .order_by(Executor.total_assigned.asc())
            .limit(1)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def auto_distribute_batch(
        session: AsyncSession,
        request_ids: List[int]
    ) -> int:      
        distributed_count = 0
        
        for request_id in request_ids:
            result = await session.execute(
                select(Request).where(Request.id == request_id)
            )
            request = result.scalar_one_or_none()
            
            if not request or request.status != "pending":
                continue
            
            executor = await DistributionEngine.get_optimal_executor(session)
            
            if not executor:
                break 
            
            request.status = "assigned"
            request.assigned_to = executor.id
            request.assigned_at = datetime.utcnow()
            executor.total_assigned += 1
            
            distributed_count += 1
        
        await session.commit()
        return distributed_count
    
    @staticmethod
    async def get_distribution_stats(
        session: AsyncSession
    ) -> Dict[str, Any]:
        total_requests = await session.scalar(
            select(func.count(Request.id))
        )
        
        unassigned_requests = await session.scalar(
            select(func.count(Request.id)).where(Request.assigned_to.is_(None))
        )
        
        assigned_requests = await session.scalar(
            select(func.count(Request.id)).where(Request.assigned_to.isnot(None))
        )
        
        result = await session.execute(
            select(Executor)
            .where(Executor.is_active == True)
            .order_by(Executor.total_assigned.desc())
        )
        executors = result.scalars().all()
        
        executor_stats = []
        for executor in executors:
            result = await session.execute(
                select(func.count(Request.id))
                .where(Request.assigned_to == executor.id)
            )
            actual_count = result.scalar() or 0
            
            executor_stats.append({
                "id": executor.id,
                "name": executor.name,
                "total_assigned": executor.total_assigned,
                "actual_count": actual_count,
                "parameters": executor.parameters
            })
        
        return {
            "total_requests": total_requests,
            "unassigned_requests": unassigned_requests,
            "assigned_requests": assigned_requests,
            "active_executors": len(executor_stats),
            "executor_stats": executor_stats
        }

