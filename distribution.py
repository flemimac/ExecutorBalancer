from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional, Dict, Any
from models import Executor, Request
from datetime import datetime
import math


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
        
        result = await session.execute(
            select(Request)
            .where(Request.status == "pending")
            .order_by(Request.id)
            .limit(1)
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
            avg_load = assigned_requests / len(executor_stats) if len(executor_stats) > 0 else 0
            if avg_load > 0:
                max_deviation = max(
                    abs(stats["actual_count"] - avg_load) 
                    for stats in executor_stats
                )
                error_percent = (max_deviation / avg_load * 100)
            else:
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

