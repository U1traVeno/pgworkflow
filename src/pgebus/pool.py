"""Worker Pool 管理事件处理 Worker。"""

from __future__ import annotations
import asyncio
import logging
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from .routing import EventRouter
    from .queue import EventQueue
    from .repo import EventRepository
    from .worker import EventWorker
    from .db import DatabaseSessionManager

logger = logging.getLogger(__name__)


class EventWorkerPool:
    """Worker Pool 管理多个事件处理 Worker。

    负责创建、启动和停止固定数量的 Worker，
    每个 Worker 从同一个队列中消费事件。
    """

    def __init__(
        self,
        event_queue: EventQueue,
        event_repo: EventRepository,
        router: EventRouter,
        session_manager: DatabaseSessionManager,
        n_workers: int = 5,
        max_retries: int = 3,
        poll_interval: float = 1.0,
    ) -> None:
        """初始化 Worker Pool。

        Args:
            event_queue: 共享的事件队列
            event_repo: 事件仓储
            router: 事件路由器
            session_manager: 数据库会话管理器（提供 async with session_manager.session()）
            n_workers: Worker 数量
            max_retries: 每个 Worker 的最大重试次数
            poll_interval: 队列为空时的轮询间隔（秒）
        """
        self.event_queue = event_queue
        self.event_repo = event_repo
        self.router = router
        self.session_manager = session_manager
        self.n_workers = n_workers
        self.max_retries = max_retries
        self.poll_interval = poll_interval

        self.workers: List[EventWorker] = []
        self.worker_tasks: List[asyncio.Task[None]] = []
        self.is_running = False

    async def start(self) -> None:
        """启动所有 Worker。"""
        if self.is_running:
            logger.warning("Worker Pool 已经在运行")
            return

        logger.info(f"启动 Worker Pool，Worker 数量: {self.n_workers}")

        # 导入 EventWorker（延迟导入避免循环）
        from .worker import EventWorker

        # 创建 Worker 实例
        self.workers = [
            EventWorker(
                event_queue=self.event_queue,
                event_repo=self.event_repo,
                router=self.router,
                session_manager=self.session_manager,
                worker_id=i,
                max_retries=self.max_retries,
                poll_interval=self.poll_interval,
            )
            for i in range(self.n_workers)
        ]

        # 启动每个 Worker 作为后台任务
        self.worker_tasks = [
            asyncio.create_task(worker.run(), name=f"event-worker-{worker.worker_id}")
            for worker in self.workers
        ]

        self.is_running = True
        logger.info(f"Worker Pool 已启动，{self.n_workers} 个 Worker 正在运行")

    async def stop(self) -> None:
        """停止所有 Worker。"""
        if not self.is_running:
            logger.warning("Worker Pool 未在运行")
            return

        logger.info("停止 Worker Pool...")

        # 停止所有 Worker
        for worker in self.workers:
            worker.running = False

        # 等待所有 Worker 任务完成
        if self.worker_tasks:
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)

        self.worker_tasks.clear()
        self.workers.clear()
        self.is_running = False

        logger.info("Worker Pool 已停止")

    async def wait_until_empty(self, timeout: Optional[float] = None) -> bool:
        """等待队列为空并且所有任务处理完毕。

        Args:
            timeout: 超时时间（秒），None 表示无限等待

        Returns:
            如果队列清空则返回 True，超时则返回 False
        """
        try:
            await asyncio.wait_for(self.event_queue.join(), timeout=timeout)
            logger.info("所有事件已处理完毕")
            return True
        except asyncio.TimeoutError:
            logger.warning(f"等待队列清空超时 ({timeout}s)")
            return False

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
        return len(self.workers)
