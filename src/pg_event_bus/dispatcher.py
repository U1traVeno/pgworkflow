"""NOTIFY 事件的事件分发器。"""

from __future__ import annotations
import logging
from functools import lru_cache
from typing import Optional, List

from sqlalchemy.ext.asyncio import AsyncSession

from .base import BaseEventHandler, DBEvent

logger = logging.getLogger(__name__)


class EventDispatcher(BaseEventHandler):
    """数据库事件的主事件分发器。

    根据事件类型前缀将事件路由到特定领域的处理器。

    树状结构示例:
    EventDispatcher
    ├── template → TemplateEventHandler
    │   └── version → VersionEventHandler
    └── simulation → SimulationEventHandler
    """

    def __init__(self, sub_handlers: Optional[List[BaseEventHandler]] = None) -> None:
        """初始化事件分发器并注册领域处理器。

        Args:
            sub_handlers: 子处理器列表。会自动从各个 handler 的 event_prefix 属性读取前缀。
        """
        super().__init__(sub_handlers)

    async def handle(self, session: AsyncSession, event: DBEvent) -> None:
        """处理与任何已注册的子处理器都不匹配的事件。

        Args:
            session: 数据库会话
            event: 数据库事件
        """
        logger.warning(f"未处理的事件类型: {event['type']}")


@lru_cache(maxsize=1)
def get_event_dispatcher() -> EventDispatcher:
    """获取单例 EventDispatcher 实例。

    使用 @lru_cache 确保在应用程序生命周期内只创建一个实例，
    避免每次事件时都进行昂贵的重新实例化。

    Returns:
        EventDispatcher 单例实例
    """
    return EventDispatcher()
