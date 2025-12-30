__version__ = "0.1.0"

# 导出核心组件
from .models import Event, EventStatus
from .base import DBEvent, EventContext, TransactionSession, publish_event
from .repo import EventRepository
from .queue import EventQueue
from .listener import EventListener
from .routing import EventRouter
from .worker import EventWorker, BaseWorker
from .pool import EventWorkerPool
from .system import EventSystem
from .config import (
    DatabaseConfig,
    EngineConfig,
    EventSystemConfig,
    Settings,
)
from .db import DatabaseSessionManager

__all__ = [
    # 模型
    "Event",
    "EventStatus",
    # 基础类
    "DBEvent",
    "EventContext",
    "TransactionSession",
    "publish_event",
    # 仓储
    "EventRepository",
    # 队列
    "EventQueue",
    # 监听器
    "EventListener",
    # 路由
    "EventRouter",
    # Worker
    "BaseWorker",
    "EventWorker",
    "EventWorkerPool",
    # 系统
    "EventSystem",
    # DB
    "DatabaseConfig",
    "EngineConfig",
    "DatabaseSessionManager",
    # Settings
    "EventSystemConfig",
    "Settings",
]


def main() -> None:
    print("pgoutbox is a PostgreSQL-backed transactional outbox processor.")
    print(f"Version: {__version__}")
