"""PostgreSQL NOTIFY/LISTEN 的基础事件系统。"""

from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Event


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


class TransactionSession:
    """事件处理函数可用的受限 Session 包装器（白名单模式）。

    设计目标：
    - 暴露“事务内工作集”所需的一整套常用能力（ORM + 查询 + 只读状态）
    - 显式禁止任何事务边界与会话生命周期控制，确保 dispatcher 拥有唯一控制权
    - 提供 `unsafe` 作为明确的逃生门（不推荐）
    """

    __slots__ = ("_session",)

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ===== 允许的方法（ORM / Unit of Work） =====

    def add(self, instance: Any) -> None:
        self._session.add(instance)

    def add_all(self, instances: Any) -> None:
        self._session.add_all(instances)

    async def delete(self, instance: Any) -> None:
        await self._session.delete(instance)

    async def merge(self, instance: Any, **kwargs: Any) -> Any:
        return await self._session.merge(instance, **kwargs)

    async def flush(self, objects: Optional[Any] = None) -> None:
        await self._session.flush(objects)

    async def refresh(self, instance: Any, **kwargs: Any) -> None:
        await self._session.refresh(instance, **kwargs)

    def expire(self, instance: Any, attribute_names: Optional[Any] = None) -> None:
        self._session.expire(instance, attribute_names=attribute_names)

    def expire_all(self) -> None:
        self._session.expire_all()

    # ===== 允许的方法（查询） =====

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        return await self._session.execute(*args, **kwargs)

    async def scalar(self, *args: Any, **kwargs: Any) -> Any:
        return await self._session.scalar(*args, **kwargs)

    async def scalars(self, *args: Any, **kwargs: Any) -> Any:
        return await self._session.scalars(*args, **kwargs)

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        return await self._session.get(*args, **kwargs)

    # ===== 允许的方法（只读状态） =====

    @property
    def is_active(self) -> bool:
        return bool(self._session.is_active)

    def in_transaction(self) -> bool:
        return bool(self._session.in_transaction())

    @property
    def info(self) -> Any:
        return self._session.info

    # ===== 明确禁止（破坏 dispatcher 控制权） =====

    def begin(self) -> Any:
        raise RuntimeError("Transaction is managed by pgoutbox")

    def begin_nested(self) -> Any:
        raise RuntimeError("Transaction is managed by pgoutbox")

    async def commit(self) -> None:
        raise RuntimeError("Transaction is managed by pgoutbox")

    async def rollback(self) -> None:
        raise RuntimeError("Transaction is managed by pgoutbox")

    async def close(self) -> None:
        raise RuntimeError("Session lifecycle is managed by pgoutbox")

    async def invalidate(self) -> None:
        raise RuntimeError("Session lifecycle is managed by pgoutbox")

    async def connection(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Connection access is managed by pgoutbox")

    def get_bind(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Connection access is managed by pgoutbox")

    # ===== 明确的逃生门 =====

    @property
    def unsafe(self) -> AsyncSession:
        """Raw AsyncSession access.

        Using this means you give up pgoutbox transaction guarantees.
        """
        return self._session


class EventContext:
    """事件处理上下文。

    Attributes:
        session: 受限的 TransactionSession；当 dispatcher 未创建 DB 会话时为 None
    """

    def __init__(self, session: Optional[TransactionSession]) -> None:
        self.session = session

    @property
    def has_db(self) -> bool:
        return self.session is not None


async def publish_event(
    session: AsyncSession,
    event_type: str,
    payload: Dict[str, Any],
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
