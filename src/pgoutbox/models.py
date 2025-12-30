# -*- coding: utf-8 -*-
"""
Event 数据库模型 - 用于事件持久化

注意: 这是一个独立的库，需要用户自行配置 SQLAlchemy Base 和数据库连接
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import DateTime, String, Text, Enum, JSON, Index, Integer
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.declarative import declarative_base

# 由用户提供 Base，这里提供一个默认的
Base = declarative_base()


class EventStatus(str, enum.Enum):
    """事件处理状态"""

    PENDING = "pending"  # 等待处理
    PROCESSING = "processing"  # 处理中
    COMPLETED = "completed"  # 已完成
    FAILED = "failed"  # 处理失败
    RETRYING = "retrying"  # 重试中


class Event(Base):
    """事件持久化模型"""

    __tablename__ = "events"

    # 主键
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 时间戳
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        comment="创建时间",
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
        comment="更新时间",
    )

    # 事件类型和来源
    type: Mapped[str] = mapped_column(
        String(255),
        index=True,
        nullable=False,
        comment="事件类型，如 'template.version.created'",
    )

    # 事件负载
    payload: Mapped[Dict[str, Any]] = mapped_column(
        JSON, nullable=False, comment="事件负载数据"
    )

    # 事件状态
    status: Mapped[EventStatus] = mapped_column(
        Enum(EventStatus),
        default=EventStatus.PENDING,
        index=True,
        nullable=False,
        comment="事件处理状态",
    )

    # 处理信息
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="事件处理完成时间"
    )
    retry_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, comment="重试次数"
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, comment="错误信息（如果处理失败）"
    )

    # 延迟执行
    run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="事件执行时间（NULL 表示立即执行）",
    )

    # 索引
    __table_args__ = (
        # 按状态和创建时间查询（用于处理待处理事件）
        Index("ix_events_status_created_at", "status", "created_at"),
        # 按类型和状态查询
        Index("ix_events_type_status", "type", "status"),
        # 按状态、延迟执行时间和创建时间查询（用于处理延迟事件）
        Index("ix_events_status_run_at_created_at", "status", "run_at", "created_at"),
    )
