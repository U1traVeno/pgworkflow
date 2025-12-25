"""
pg-event-bus - 基于 PostgreSQL 的事件总线系统

一个轻量级、高性能的事件系统，使用 PostgreSQL 作为唯一的事实来源。
支持事件发布、订阅、延迟执行和并发处理。
"""

__version__ = "0.1.0"

# 导出核心组件
from .models import Event, EventStatus, EventSource
from .base import DBEvent, BaseEventHandler, publish_event
from .repo import EventRepository
from .queue import EventQueue
from .listener import EventListener, create_listener_connection
from .dispatcher import EventDispatcher, get_event_dispatcher
from .worker import EventWorker, BaseWorker
from .pool import EventWorkerPool
from .system import EventSystem, get_event_system, start_event_system

__all__ = [
    # 模型
    "Event",
    "EventStatus",
    "EventSource",
    # 基础类
    "DBEvent",
    "BaseEventHandler",
    "publish_event",
    # 仓储
    "EventRepository",
    # 队列
    "EventQueue",
    # 监听器
    "EventListener",
    "create_listener_connection",
    # 分发器
    "EventDispatcher",
    "get_event_dispatcher",
    # Worker
    "BaseWorker",
    "EventWorker",
    "EventWorkerPool",
    # 系统
    "EventSystem",
    "get_event_system",
    "start_event_system",
]


def main() -> None:
    print("pg-event-bus - PostgreSQL Event Bus System")
    print(f"Version: {__version__}")
