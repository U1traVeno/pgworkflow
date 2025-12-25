"""PostgreSQL NOTIFY/LISTEN 的基础事件系统。"""

from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Event, EventSource


class DBEvent(Dict[str, Any]):
    """数据库事件负载结构。

    继承自 Dict 以在 Python 3.8 中支持 TypedDict 的行为。

    Attributes:
        type: 事件类型标识符 (例如 'template.version.created')
        payload: 事件特定数据
    """

    def __init__(self, type: str, payload: Dict[str, Any]) -> None:
        super().__init__(type=type, payload=payload)

    @property
    def event_type(self) -> str:
        """获取事件类型"""
        return self["type"]

    @property
    def event_payload(self) -> Dict[str, Any]:
        """获取事件负载"""
        return self["payload"]


class BaseEventHandler(ABC):
    """数据库事件处理器的基类。

    提供基于树的事件分发：每个处理器可以根据事件类型前缀
    进一步将事件分发给子处理器。

    Attributes:
        event_prefix: 该处理器处理的事件前缀。子类应该重写此属性。
                     EventDispatcher 不需要设置此属性。
    """

    event_prefix: Optional[str] = None

    def __init__(self, sub_handlers: Optional[List["BaseEventHandler"]] = None) -> None:
        """初始化事件处理器并注册子处理器。

        Args:
            sub_handlers: 子处理器列表。会自动从各个 handler 的 event_prefix 属性读取前缀。
        """
        self._sub_handlers: Dict[str, BaseEventHandler] = {}
        if sub_handlers:
            for handler in sub_handlers:
                if handler.event_prefix is None:
                    raise ValueError(
                        f"Handler {handler.__class__.__name__} must define event_prefix attribute"
                    )
                self.register_sub_handler(handler.event_prefix, handler)

    def register_sub_handler(
        self, event_prefix: str, handler: BaseEventHandler
    ) -> None:
        """注册用于匹配前缀的事件的子处理器。

        Args:
            event_prefix: 事件类型前缀 (例如 'template' 匹配 'template.*')
            handler: 处理匹配事件的子处理器实例
        """
        self._sub_handlers[event_prefix] = handler

    async def process_event(self, session: AsyncSession, event: DBEvent) -> None:
        """通过分发给子处理器或直接处理来处理事件。

        此方法:
        1. 检查是否有子处理器可以处理该事件
        2. 如果有，则分发给子处理器（剥离已匹配的前缀）
        3. 如果没有，则调用 handle 方法

        Args:
            session: 数据库会话
            event: 要处理的数据库事件
        """
        # 尝试分发给子处理器
        for prefix, handler in self._sub_handlers.items():
            if self._match_event_type(event["type"], prefix):
                # 剥离已匹配的前缀，创建新的事件传递给子处理器
                remaining_type = self._strip_prefix(event["type"], prefix)
                sub_event = DBEvent(
                    type=remaining_type,
                    payload=event["payload"],
                )
                await handler.process_event(session, sub_event)
                return

        # 没有匹配的子处理器，自己处理
        await self.handle(session, event)

    def _match_event_type(self, event_type: str, prefix: str) -> bool:
        """检查事件类型是否匹配前缀。

        Args:
            event_type: 完整的事件类型 (例如 'template.version.created')
            prefix: 要匹配的前缀 (例如 'template')

        Returns:
            如果事件类型以前缀开头并后跟点号，则返回 True
        """
        return event_type.startswith(f"{prefix}.")

    def _strip_prefix(self, event_type: str, prefix: str) -> str:
        """从事件类型中剥离已匹配的前缀。

        Args:
            event_type: 完整的事件类型 (例如 'template.version.created')
            prefix: 要剥离的前缀 (例如 'template')

        Returns:
            剥离前缀后的事件类型 (例如 'version.created')
        """
        return event_type[len(prefix) + 1 :]  # +1 for the dot

    @abstractmethod
    async def handle(self, session: AsyncSession, event: DBEvent) -> None:
        """使用特定的业务逻辑处理事件。

        子类必须实现此方法来处理事件。

        Args:
            session: 数据库会话
            event: 要处理的数据库事件
        """
        pass


async def publish_event(
    session: AsyncSession,
    event_type: str,
    payload: Dict[str, Any],
    source: EventSource,
    channel: str,
    run_at: Optional[datetime] = None,
) -> Event:
    """发布事件到数据库并通过 PostgreSQL NOTIFY 通知。

    Args:
        session: 数据库会话
        event_type: 事件类型标识符 (例如 'template.version.created')
        payload: 事件特定数据（应该保持薄，只包含必要的 ID）
        source: 事件来源
        channel: PostgreSQL NOTIFY 频道名称
        run_at: 延迟执行时间，None 表示立即执行

    Returns:
        创建的事件对象

    Example:
        >>> async with session.begin():
        ...     event = await publish_event(
        ...         session,
        ...         "simulation.created",
        ...         {"simulation_id": 123},
        ...         EventSource.INTERNAL,
        ...         "events",
        ...     )
    """
    # 1. 创建事件记录
    event = Event(
        type=event_type,
        payload=payload,
        source=source,
        run_at=run_at,
    )
    session.add(event)
    await session.flush()  # 获取 ID

    # 2. 发送 NOTIFY（提示有新事件）
    # NOTIFY 只是提示信号，不携带完整事件数据
    await session.execute(
        text(f"NOTIFY {channel}, :event_id"),
        {"event_id": str(event.id)},
    )

    return event
