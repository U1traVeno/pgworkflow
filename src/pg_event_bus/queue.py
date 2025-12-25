# -*- coding: utf-8 -*-
"""
EventQueue - 基于数据库的事件队列抽象

设计原则(三件事的身份定义):
====================

1. event 表 = 唯一的队列(Source of Truth)
   - 任何事件必须先存在于 event 表
   - 事件的状态由 DB 中的 status 字段决定
   - 这是系统中唯一的"事实"

2. asyncio.Queue = 内存缓冲区/预取缓存
   - 只存储已从 DB claim 的 event_id
   - 目的是减少频繁查表,提高性能
   - 不是队列,是已 claim 事件的引用列表

3. PostgreSQL NOTIFY = 唤醒信号/触发器
   - 仅用于通知"DB 可能有新事件"
   - 不携带事件数据,只是提示
   - 不会直接导致 Worker 处理事件

核心流程:
=========

触发 prefetch 的两种情况:
  1. 收到 NOTIFY -> notify() -> maybe_prefetch()
  2. queue 低水位 -> maybe_prefetch()

prefetch 做的事(唯一查表入口):
  1. 调用 repo.claim_events() 批量获取事件
  2. 将 event_id 放入 asyncio.Queue
  3. 将完整 event 放入 cache
  4. claim_events 使用 FOR UPDATE SKIP LOCKED 保证并发安全

Worker 的使用方式(极度简单):
  event = await queue.get()  # 从 cache 获取完整事件
  await dispatcher.process(event)
  await repo.mark_completed(event.id)

禁止的行为:
===========
❌ Worker 自己查 event 表
❌ NOTIFY payload 直接进 queue
❌ asyncio.Queue 里放完整 event
❌ put_nowait(event) - 事件不是"推入"队列的
"""

from __future__ import annotations
import asyncio
import logging
from typing import Dict, Optional, Callable

from sqlalchemy.ext.asyncio import AsyncSession

from .base import DBEvent
from .repo import EventRepository

logger = logging.getLogger(__name__)


class EventQueue:
    """基于数据库的事件队列抽象

    职责:
    ----
    1. 响应 NOTIFY,决定何时从 DB prefetch 事件
    2. 维护已 claim 事件的内存缓存
    3. 向 Worker 提供简单的 get() 接口

    不负责:
    ------
    - 不负责创建事件(由 Dispatcher 负责)
    - 不负责处理事件(由 Worker 负责)
    - 不负责事件状态变更(由 EventRepository 负责)
    """

    def __init__(
        self,
        event_repo: EventRepository,
        session_factory: Callable[[], AsyncSession],
        maxsize: int = 1000,
        batch_size: int = 10,
        low_watermark: int = 5,
    ) -> None:
        """初始化事件队列

        Args:
            event_repo: 事件仓储
            session_factory: 创建数据库会话的工厂函数
            maxsize: 内存缓冲区最大容量
            batch_size: 每次 prefetch 的事件数量
            low_watermark: 触发 prefetch 的低水位阈值
        """
        self.event_repo = event_repo
        self.session_factory = session_factory
        self.batch_size = batch_size
        self.low_watermark = low_watermark

        # 内存缓冲区:只存储已 claim 的 event_id
        self._queue: asyncio.Queue[int] = asyncio.Queue(maxsize=maxsize)

        # 缓存:存储完整的事件对象,避免重复查表
        self._cache: Dict[int, DBEvent] = {}

        # Prefetch 锁:防止多个协程同时触发 prefetch
        self._prefetch_lock = asyncio.Lock()

    def notify(self) -> None:
        """收到 NOTIFY 信号的回调

        这个方法会被 PostgreSQL NOTIFY 监听器调用。
        它的唯一职责是:检查是否需要 prefetch,如果需要就触发。

        注意:
        ----
        - NOTIFY 不携带事件数据,只是"提示信号"
        - 不会立即查表,只是标记"可能需要 prefetch"
        - 实际的 prefetch 由 maybe_prefetch() 完成
        """
        logger.debug("收到 NOTIFY 信号")
        # 创建一个 task 来执行 maybe_prefetch,不阻塞当前调用
        asyncio.create_task(self.maybe_prefetch())

    async def maybe_prefetch(self) -> None:
        """根据需要从 DB prefetch 事件(唯一查表入口)

        触发条件:
        --------
        1. 收到 NOTIFY 信号
        2. Worker 调用 get() 时发现队列低于水位

        工作流程:
        --------
        1. 检查队列是否低于水位,如果不是则跳过
        2. 使用锁保证只有一个协程执行 prefetch
        3. 调用 repo.claim_events() 批量获取事件
        4. 将 event_id 放入 asyncio.Queue
        5. 将完整 event 放入 cache

        并发安全:
        --------
        - claim_events 使用 FOR UPDATE SKIP LOCKED
        - _prefetch_lock 防止多个协程同时 prefetch
        - 即使多个 Worker 同时调用也是安全的
        """
        # 快速检查:如果队列足够就不用 prefetch
        if self._queue.qsize() >= self.low_watermark:
            logger.debug(
                f"队列水位足够 ({self._queue.qsize()}/{self.low_watermark}),跳过 prefetch"
            )
            return

        # 获取锁,防止多个协程同时 prefetch
        async with self._prefetch_lock:
            # 双重检查:可能在等锁期间其他协程已经 prefetch 了
            if self._queue.qsize() >= self.low_watermark:
                logger.debug("其他协程已完成 prefetch,跳过")
                return

            # 从 DB claim 事件
            session = self.session_factory()
            try:
                events = await self.event_repo.claim_events(
                    session, batch_size=self.batch_size
                )

                if not events:
                    logger.debug("DB 中没有待处理事件")
                    return

                # 放入队列和缓存
                for event in events:
                    try:
                        # 转换为 DBEvent 格式
                        db_event = DBEvent(
                            type=event.type,
                            payload={
                                "event_id": event.id,
                                **event.payload,
                            },
                        )

                        # 放入内存队列
                        self._queue.put_nowait(event.id)

                        # 放入缓存
                        self._cache[event.id] = db_event

                    except asyncio.QueueFull:
                        logger.warning(
                            f"队列已满,事件 {event.id} 将在下次 prefetch 时获取"
                        )
                        break

                # 提交事务,释放行锁
                await session.commit()

                logger.info(
                    f"Prefetch 完成: 获取 {len(events)} 个事件, "
                    f"队列水位 {self._queue.qsize()}/{self._queue.maxsize}"
                )
            finally:
                await session.close()

    async def get(self) -> DBEvent:
        """阻塞地从队列获取一个事件

        工作流程:
        --------
        1. 检查队列水位,如果低于阈值则触发 prefetch
        2. 从 asyncio.Queue 获取 event_id
        3. 从 cache 获取完整事件对象

        Returns:
            完整的事件对象(从 cache 中获取)

        注意:
        ----
        - 不会直接查 DB,事件已在 prefetch 时加载到 cache
        - 如果 cache miss,说明有 bug,会返回错误事件
        """
        # 检查水位,可能触发 prefetch
        if self._queue.qsize() < self.low_watermark:
            logger.debug(
                f"队列低水位 ({self._queue.qsize()}/{self.low_watermark}),触发 prefetch"
            )
            asyncio.create_task(self.maybe_prefetch())

        # 从队列获取 event_id(阻塞)
        event_id = await self._queue.get()

        # 从 cache 获取完整事件
        if event_id in self._cache:
            event = self._cache.pop(event_id)
            logger.debug(f"从 cache 获取事件 {event_id}")
            return event
        else:
            # 这不应该发生,说明 cache 管理有问题
            logger.error(f"严重错误: event_id {event_id} 在队列中但不在 cache 中")
            return DBEvent(
                type="system.error",
                payload={
                    "event_id": event_id,
                    "error": "Cache miss - this is a bug",
                },
            )

    async def try_get(self) -> Optional[DBEvent]:
        """非阻塞地尝试从队列获取一个事件

        Returns:
            完整的事件对象,如果队列为空则返回 None
        """
        try:
            event_id = self._queue.get_nowait()

            # 从 cache 获取
            if event_id in self._cache:
                return self._cache.pop(event_id)
            else:
                logger.error(f"严重错误: event_id {event_id} 在队列中但不在 cache 中")
                return DBEvent(
                    type="system.error",
                    payload={
                        "event_id": event_id,
                        "error": "Cache miss - this is a bug",
                    },
                )
        except asyncio.QueueEmpty:
            return None

    def qsize(self) -> int:
        """获取队列大小

        Returns:
            队列中待处理的事件数量
        """
        return self._queue.qsize()

    def empty(self) -> bool:
        """检查队列是否为空

        Returns:
            队列是否为空
        """
        return self._queue.empty()

    def full(self) -> bool:
        """检查队列是否已满

        Returns:
            队列是否已满
        """
        return self._queue.full()

    def task_done(self) -> None:
        """标记一个事件已处理完毕

        Worker 处理完事件后应调用此方法，以便 join() 能够正确等待。
        """
        self._queue.task_done()

    async def join(self) -> None:
        """等待队列中的所有事件被处理完毕

        阻塞直到队列中的所有事件都被 get() 获取并且对应的 task_done() 被调用。
        """
        await self._queue.join()
