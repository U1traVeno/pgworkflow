"""事件系统集成，统一管理 EventListener 和 WorkerPool。"""

from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.schema import CreateSchema

from .listener import EventListener, create_listener_connection
from .pool import EventWorkerPool
from .queue import EventQueue
from .repo import EventRepository
from .routing import EventRouter
from .config import Settings
from .db import DatabaseSessionManager

logger = logging.getLogger(__name__)


class EventSystem:
    """事件系统，集成 EventListener 和 WorkerPool。

    架构：
    1. EventListener 监听 PostgreSQL NOTIFY，将事件 ID 放入队列
    2. EventQueue 使用 EventRepository 从数据库加载完整事件数据
    3. WorkerPool 管理多个 Worker 从队列并发消费事件
    4. 每个 Worker 获取数据库会话并使用 EventRouter 处理事件

    使用示例：
        router = EventRouter()
        settings = Settings(
            database={
                "host": "localhost",
                "port": 5432,
                "user": "user",
                "password": "password",
                "database": "mydb",
                "application_name": "pgebus",
            },
            event_system={
                "n_workers": 5,
            },
        )
        event_system = EventSystem(
            router=router,
            settings=settings,
        )
        await event_system.start()

        # ... 应用运行 ...

        await event_system.stop()
    """

    def __init__(
        self,
        router: EventRouter,
        settings: Settings,
    ) -> None:
        """初始化事件系统。

        Args:
            router: 事件路由器
            settings: 配置（包含 database 与 event_system）
        """
        self.router = router
        self.settings = settings
        self.db = settings.database

        self.channel = settings.event_system.channel
        self.n_workers = settings.event_system.n_workers
        self.queue_maxsize = settings.event_system.queue_maxsize
        self.max_retries = settings.event_system.max_retries
        self.poll_interval = settings.event_system.poll_interval

        # pending 轮询（兜底触发 maybe_prefetch，避免无 NOTIFY 时事件“卡死”）
        self.pending_poll_enabled = settings.event_system.pending_poll_enabled
        self.pending_poll_interval_seconds = (
            settings.event_system.pending_poll_interval_seconds
        )

        # 定时清理配置（按 processed_at 清理终态事件）
        self.cleanup_enabled = settings.event_system.cleanup_enabled
        self.cleanup_retention_seconds = settings.event_system.cleanup_retention_seconds
        self.cleanup_interval_seconds = settings.event_system.cleanup_interval_seconds
        self.cleanup_batch_size = settings.event_system.cleanup_batch_size

        # 内部创建 engine/session（使用 session_manager 的 async with 管理生命周期）
        self.session_manager = DatabaseSessionManager(self.db)

        # 创建事件仓储
        self.event_repo = EventRepository()

        # 创建事件队列（基于数据库）
        self.event_queue = EventQueue(
            event_repo=self.event_repo,
            session_manager=self.session_manager,
            maxsize=self.queue_maxsize,
        )

        # 组件（延迟初始化）
        self.listener: Optional[EventListener] = None
        self.worker_pool: Optional[EventWorkerPool] = None
        self._connection = None

        # house-keeping task
        self._pending_poll_task: Optional[asyncio.Task[None]] = None
        self._cleanup_task: Optional[asyncio.Task[None]] = None

    async def _cleanup_loop(self) -> None:
        try:
            # 避免启动瞬间抢占资源
            await asyncio.sleep(self.cleanup_interval_seconds)

            while True:
                if not self.cleanup_enabled or self.cleanup_retention_seconds <= 0:
                    await asyncio.sleep(self.cleanup_interval_seconds)
                    continue

                cutoff = datetime.now(timezone.utc) - timedelta(
                    seconds=self.cleanup_retention_seconds
                )

                try:
                    async with self.session_manager.session() as session:
                        deleted = await self.event_repo.purge_processed_events(
                            session,
                            older_than=cutoff,
                            batch_size=self.cleanup_batch_size,
                        )
                        await session.commit()

                    if deleted > 0:
                        logger.info(
                            "事件定时清理完成: deleted=%s cutoff=%s",
                            deleted,
                            cutoff.isoformat(),
                        )
                except Exception as exc:
                    logger.error("事件定时清理失败: %s", exc)

                await asyncio.sleep(self.cleanup_interval_seconds)
        except asyncio.CancelledError:
            return

    async def _pending_poll_loop(self) -> None:
        try:
            while True:
                try:
                    await self.event_queue.maybe_prefetch()
                except Exception as exc:
                    logger.error("事件 pending 轮询 prefetch 失败: %s", exc)

                await asyncio.sleep(self.pending_poll_interval_seconds)
        except asyncio.CancelledError:
            return

    async def start(self) -> None:
        """启动事件系统（EventListener 和 WorkerPool）。"""
        logger.info("启动事件系统...")

        # 确保默认 schema 存在（与业务表隔离）
        async with self.session_manager.session() as session:
            await session.execute(CreateSchema(self.db.schema_name, if_not_exists=True))
            await session.commit()

        # 创建 asyncpg 连接
        self._connection = await create_listener_connection(self.db)

        # 创建并启动 EventListener
        self.listener = EventListener(
            connection=self._connection,
            event_queue=self.event_queue,
            channel=self.channel,
        )
        await self.listener.start()

        # 创建并启动 WorkerPool
        self.worker_pool = EventWorkerPool(
            event_queue=self.event_queue,
            event_repo=self.event_repo,
            router=self.router,
            session_manager=self.session_manager,
            n_workers=self.n_workers,
            max_retries=self.max_retries,
            poll_interval=self.poll_interval,
        )
        await self.worker_pool.start()

        # 启动 pending 轮询任务（兜底 NOTIFY 丢失/启动堆积）
        if self.pending_poll_enabled:
            if self._pending_poll_task is None or self._pending_poll_task.done():
                self._pending_poll_task = asyncio.create_task(
                    self._pending_poll_loop(),
                    name="event-system-pending-poll",
                )
                logger.info(
                    "事件 pending 轮询已启动: interval=%ss",
                    self.pending_poll_interval_seconds,
                )

        # 启动定时清理任务（按 processed_at 清理终态事件）
        if self.cleanup_enabled and self.cleanup_retention_seconds > 0:
            if self._cleanup_task is None or self._cleanup_task.done():
                self._cleanup_task = asyncio.create_task(
                    self._cleanup_loop(),
                    name="event-system-cleanup",
                )
                logger.info(
                    "事件定时清理已启动: retention=%ss interval=%ss batch=%s",
                    self.cleanup_retention_seconds,
                    self.cleanup_interval_seconds,
                    self.cleanup_batch_size,
                )

        logger.info(
            f"事件系统已启动: "
            f"Workers={self.n_workers}, "
            f"Queue Max Size={self.queue_maxsize or '无限'}"
        )

    async def stop(
        self, wait_for_completion: bool = True, timeout: Optional[float] = 30.0
    ) -> None:
        """停止事件系统。

        Args:
            wait_for_completion: 是否等待队列中的事件处理完毕
            timeout: 等待超时时间（秒），None 表示无限等待
        """
        logger.info("停止事件系统...")

        # 先停止 pending 轮询，避免关闭期间继续打 DB
        if self._pending_poll_task is not None:
            self._pending_poll_task.cancel()
            try:
                await self._pending_poll_task
            except asyncio.CancelledError:
                pass
            finally:
                self._pending_poll_task = None

        # 先停止定时清理，减少关闭期间的 DB 压力
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            finally:
                self._cleanup_task = None

        # 先停止 listener，不再接收新事件
        if self.listener:
            await self.listener.stop()

        # 等待队列中的事件处理完毕（可选）
        if wait_for_completion and self.worker_pool:
            logger.info("等待队列中的事件处理完毕...")
            await self.worker_pool.wait_until_empty(timeout=timeout)

        # 停止 worker pool
        if self.worker_pool:
            await self.worker_pool.stop()

        # 关闭内部 engine
        await self.session_manager.close()

        logger.info("事件系统已停止")

    def get_queue_size(self) -> int:
        """获取当前队列大小。

        Returns:
            队列中待处理的事件数量
        """
        return self.event_queue.qsize()

    def get_worker_count(self) -> int:
        """获取 Worker 数量。

        Returns:
            当前运行的 Worker 数量
        """
        if self.worker_pool:
            return self.worker_pool.get_worker_count()
        return 0
