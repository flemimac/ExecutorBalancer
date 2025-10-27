from sqlalchemy import Column, Integer, String, JSON, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from typing import Optional, Dict, Any

Base = declarative_base()


class Request(Base):
    """Модель заявки"""
    __tablename__ = "requests"
    
    id = Column(Integer, primary_key=True, index=True)
    parameters = Column(JSON)  # Параметры заявки
    status = Column(String, default="pending")  # pending, assigned, completed
    assigned_to = Column(Integer, ForeignKey("executors.id"), nullable=True)
    assigned_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Индексы для быстрого поиска
    __table_args__ = (
        Index('idx_status', 'status'),
        Index('idx_assigned_to', 'assigned_to'),
    )


class Executor(Base):
    """Модель исполнителя"""
    __tablename__ = "executors"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    parameters = Column(JSON, default={})  # Параметры исполнителя
    total_assigned = Column(Integer, default=0)  # Всего заявок назначено
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Связь с заявками
    requests = relationship("Request", back_populates="executor")


# Добавляем обратную связь
Request.executor = relationship("Executor", back_populates="requests")

