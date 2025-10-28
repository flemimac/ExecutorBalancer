from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import JSON
from sqlalchemy.sql import cast
from typing import List, Optional, Dict, Any
from models import Executor, Request
from datetime import datetime
import json


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
        
        # Получаем параметры исполнителя
        executor_params = executor.parameters or {}
        executor_city = executor_params.get("city")
        executor_data_type = executor_params.get("data_type")
        
        # Инициализируем переменную request
        request = None
        
        # Если у исполнителя есть параметры, фильтруем заявки
        if executor_city or executor_data_type:
            # Получаем все заявки и фильтруем в Python
            all_requests = await session.execute(
                select(Request).where(Request.status == "pending")
            )
            all_requests = all_requests.scalars().all()
            
            # Фильтруем заявки по параметрам
            matching_requests = []
            for req in all_requests:
                if req.parameters and 'parameters' in req.parameters:
                    req_params = req.parameters['parameters']
                    match = True
                    
                    if executor_city and req_params.get('city') != executor_city:
                        match = False
                    
                    if executor_data_type and req_params.get('data_type') != executor_data_type:
                        match = False
                    
                    if match:
                        matching_requests.append(req)
            
            if matching_requests:
                request = matching_requests[0]  # Берем первую подходящую
            else:
                request = None
        
        # Если не нашли подходящую заявку, берем любую
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
        
        pending_requests = await session.scalar(
            select(func.count(Request.id)).where(Request.status == "pending")
        )
        
        assigned_requests = await session.scalar(
            select(func.count(Request.id)).where(Request.status == "assigned")
        )
        
        completed_requests = await session.scalar(
            select(func.count(Request.id)).where(Request.status == "completed")
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
                .where(Request.status.in_(["assigned", "completed"]))
            )
            actual_count = result.scalar() or 0
            
            executor_stats.append({
                "id": executor.id,
                "name": executor.name,
                "total_assigned": executor.total_assigned,
                "actual_count": actual_count,
                "parameters": executor.parameters
            })
        
        if executor_stats and assigned_requests > 0:
            # Фильтруем исполнителей, которые реально работали (обработали хотя бы 1 заявку)
            working_executors = [stats for stats in executor_stats if stats["actual_count"] > 0]
            
            if len(working_executors) >= 2:  # Нужно минимум 2 работающих исполнителя для сравнения
                # Используем actual_count (текущие заявки) для расчета равномерности
                loads = [stats["actual_count"] for stats in working_executors]
                avg_load = sum(loads) / len(loads)
                
                if avg_load > 0:
                    # Рассчитываем максимальное отклонение от среднего
                    max_deviation = max(abs(load - avg_load) for load in loads)
                    
                    # Процент погрешности как максимальное отклонение от среднего
                    error_percent = (max_deviation / avg_load * 100)
                else:
                    error_percent = 0
            else:
                # Если работающих исполнителей меньше 2, погрешность = 0
                error_percent = 0
        else:
            error_percent = 0
        
        return {
            "total_requests": total_requests,
            "pending_requests": pending_requests,
            "assigned_requests": assigned_requests,
            "completed_requests": completed_requests,
            "active_executors": len(executor_stats),
            "executor_stats": executor_stats,
            "distribution_error_percent": round(error_percent, 2)
        }

