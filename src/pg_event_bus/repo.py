# -*- coding: utf-8 -*-
"""
Event Repository - 事件数据访问层
"""

from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Event, EventStatus

logger = logging.getLogger(__name__)


class EventRepository:
    """事件仓储层，负责事件的数据库操作"""

    async def get_event_by_id(
        self, session: AsyncSession, event_id: int
    ) -> Optional[Event]:
        """根据 ID 获取事件

        Args:
            session: 数据库会话
            event_id: 事件 ID

        Returns:
            事件对象或 None
        """
        result = await session.execute(select(Event).where(Event.id == event_id))
        return result.scalar_one_or_none()

    async def mark_processing(self, session: AsyncSession, event_id: int) -> bool:
        """标记事件为处理中

        Args:
            session: 数据库会话
            event_id: 事件 ID

        Returns:
            是否成功标记
        """
        result = await session.execute(
            update(Event)
            .where(Event.id == event_id)
            .values(status=EventStatus.PROCESSING)
            .returning(Event.id)
        )
        return result.scalar_one_or_none() is not None

    async def mark_completed(self, session: AsyncSession, event_id: int) -> bool:
        """标记事件为已完成

        Args:
            session: 数据库会话
            event_id: 事件 ID

        Returns:
            是否成功标记
        """
        result = await session.execute(
            update(Event)
            .where(Event.id == event_id)
            .values(
                status=EventStatus.COMPLETED,
                processed_at=datetime.now(timezone.utc),
            )
            .returning(Event.id)
        )
        return result.scalar_one_or_none() is not None

    async def mark_failed(
        self,
        session: AsyncSession,
        event_id: int,
        error_message: str,
        increment_retry: bool = True,
    ) -> bool:
        """标记事件为失败

        Args:
            session: 数据库会话
            event_id: 事件 ID
            error_message: 错误信息
            increment_retry: 是否增加重试计数

        Returns:
            是否成功标记
        """
        values: Dict[str, Any] = {
            "status": EventStatus.FAILED,
            "error_message": error_message,
        }

        if increment_retry:
            # 使用 SQL 表达式增加 retry_count
            values["retry_count"] = Event.retry_count + 1

        result = await session.execute(
            update(Event)
            .where(Event.id == event_id)
            .values(**values)
            .returning(Event.id)
        )
        return result.scalar_one_or_none() is not None

    async def mark_retrying(self, session: AsyncSession, event_id: int) -> bool:
        """标记事件为重试中

        Args:
            session: 数据库会话
            event_id: 事件 ID

        Returns:
            是否成功标记
        """
        result = await session.execute(
            update(Event)
            .where(Event.id == event_id)
            .values(
                status=EventStatus.RETRYING,
                retry_count=Event.retry_count + 1,
            )
            .returning(Event.id)
        )
        return result.scalar_one_or_none() is not None

    async def claim_events(
        self,
        session: AsyncSession,
        batch_size: int = 10,
    ) -> List[Event]:
        """原子性地 claim 待处理事件（并发安全）

        使用 UPDATE ... RETURNING 保证 SELECT 和 UPDATE 是原子的。
        这是唯一正确的并发安全做法。

        Args:
            session: 数据库会话
            batch_size: 一次获取的事件数量

        Returns:
            已 claim 的事件列表
        """
        # 使用 SQLAlchemy CTE 确保 LIMIT 和 FOR UPDATE SKIP LOCKED 正确执行
        subquery = (
            select(Event.id)
            .where(
                (Event.status == EventStatus.PENDING)
                & ((Event.run_at.is_(None)) | (Event.run_at <= func.now()))
            )
            .order_by(func.coalesce(Event.run_at, Event.created_at))
            .limit(batch_size)
            .with_for_update(skip_locked=True)
            .cte("claimed")
        )

        stmt = (
            update(Event)
            .where(Event.id.in_(select(subquery.c.id)))
            .values(status=EventStatus.PROCESSING)
            .returning(Event)
        )

        result = await session.execute(stmt)
        events = list(result.scalars().all())

        if events:
            logger.debug(f"Claimed {len(events)} events: {[e.id for e in events]}")

        # UPDATE...RETURNING 不保证顺序，需要手动排序
        # 按照 COALESCE(run_at, created_at) 排序，与查询逻辑一致
        events.sort(key=lambda e: e.run_at if e.run_at else e.created_at)

        return events
