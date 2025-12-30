"""事件处理 Worker，从队列消费事件并处理。"""

from __future__ import annotations
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, List, Optional, TypeVar

from .base import DBEvent, EventContext, TransactionSession

if TYPE_CHECKING:
    from .queue import EventQueue
    from .repo import EventRepository
    from .routing import EventRouter
    from .db import DatabaseSessionManager

logger = logging.getLogger(__name__)

# 定义任务类型变量
T = TypeVar("T")


class BaseWorker(ABC, Generic[T]):
    """基础 Worker，提供任务拉取、处理和错误处理的通用框架。"""

    def __init__(
        self,
        max_retries: int = 3,
        poll_interval: float = 1.0,
    ) -> None:
        """初始化 Worker。

        Args:
            max_retries: 最大重试次数
            poll_interval: 队列为空时的轮询间隔（秒）
        """
        self.max_retries = max_retries
        self.poll_interval = poll_interval
        self.running = False

    async def run(self) -> None:
        """运行 Worker 主循环。"""
        self.running = True
        logger.info("Worker 开始运行")

        while self.running:
            try:
                # 拉取任务
                tasks = await self.fetch_tasks(limit=1)

                if not tasks:
                    # 队列为空，等待一会儿
                    await asyncio.sleep(self.poll_interval)
                    continue

                # 处理任务
                for task in tasks:
                    await self._process_task(task)

            except Exception as e:
                logger.error(f"Worker 主循环出错: {e}", exc_info=True)
                await asyncio.sleep(self.poll_interval)

        logger.info("Worker 已停止")

    async def _process_task(self, task: T) -> None:
        """处理单个任务，包含重试逻辑。

        Args:
            task: 要处理的任务
        """
        retry_count = 0
        last_error: Optional[Exception] = None

        while retry_count <= self.max_retries:
            try:
                await self.handle(task)
                await self.ack(task)
                return  # 成功，退出重试循环
            except Exception as e:
                retry_count += 1
                last_error = e
                logger.warning(
                    f"处理任务失败 (重试 {retry_count}/{self.max_retries}): {e}"
                )

                if retry_count <= self.max_retries:
                    # 等待一会儿再重试
                    await asyncio.sleep(min(retry_count * 2, 30))

        # 重试次数耗尽
        if last_error:
            await self.fail(task, last_error)

    @abstractmethod
    async def fetch_tasks(self, limit: int) -> List[T]:
        """拉取任务。

        Args:
            limit: 最大拉取数量

        Returns:
            任务列表
        """
        pass

    @abstractmethod
    async def handle(self, task: T) -> None:
        """处理任务。

        Args:
            task: 要处理的任务
        """
        pass

    @abstractmethod
    async def fail(self, task: T, exc: Exception) -> None:
        """处理失败的任务。

        Args:
            task: 失败的任务
            exc: 异常信息
        """
        pass

    @abstractmethod
    async def ack(self, task: T) -> None:
        """确认任务完成。

        Args:
            task: 已完成的任务
        """
        pass


class EventWorker(BaseWorker[DBEvent]):
    """事件处理 Worker，从队列拉取事件并处理。

    继承 BaseWorker 实现事件的拉取、处理和错误处理。
    使用 EventQueue 从数据库加载完整的事件数据。
    """

    def __init__(
        self,
        event_queue: EventQueue,
        event_repo: EventRepository,
        router: EventRouter,
        session_manager: DatabaseSessionManager,
        worker_id: int,
        max_retries: int = 3,
        poll_interval: float = 1.0,
    ) -> None:
        """初始化事件 Worker。

        Args:
            event_queue: 事件队列
            event_repo: 事件仓储
            router: 事件路由器
            session_manager: 数据库会话管理器（提供 async with session_manager.session()）
            worker_id: Worker 的唯一标识符
            max_retries: 最大重试次数
            poll_interval: 队列为空时的轮询间隔（秒）
        """
        super().__init__(max_retries=max_retries, poll_interval=poll_interval)
        self.event_queue = event_queue
        self.event_repo = event_repo
        self.router = router
        self.session_manager = session_manager
        self.worker_id = worker_id

    async def fetch_tasks(self, limit: int) -> List[DBEvent]:
        """从队列中拉取任务。

        使用 EventQueue.try_get() 非阻塞地获取事件，
        事件会从数据库加载完整数据。

        Args:
            limit: 最大拉取数量（这里始终返回 0 或 1 个事件）

        Returns:
            包含 0 或 1 个事件的列表
        """
        # 使用 try_get() 非阻塞地获取事件（会从数据库加载）
        event = await self.event_queue.try_get()

        if event is None:
            # 队列为空，返回空列表，BaseWorker 会 sleep
            return []

        logger.debug(f"Worker {self.worker_id} 从队列获取事件: {event['type']}")
        return [event]

    async def handle(self, task: DBEvent) -> None:
        """处理单个事件。

        获取数据库会话，调用 dispatcher 处理事件，然后提交。
        处理前标记为 processing，成功后标记为 completed。

        Args:
            task: 要处理的事件
        """
        event_id = task["payload"].get("event_id")
        logger.debug(
            f"Worker {self.worker_id} 开始处理事件: {task['type']} (ID: {event_id})"
        )

        handlers = self.router.get_handlers(task["type"])
        needs_tx = any(h.transactional for h in handlers)

        async with self.session_manager.session() as session:
            # 标记事件为处理中
            if event_id:
                await self.event_repo.mark_processing(session, event_id)
                await session.commit()

        # 调用 handlers（同事件执行全部 handler；事务由 dispatcher 决定）
        if handlers:
            if needs_tx:
                async with self.session_manager.session() as session:
                    async with session.begin():
                        ctx = EventContext(session=TransactionSession(session))
                        payload = task["payload"]
                        for h in handlers:
                            await h.endpoint(ctx, payload)
            else:
                ctx = EventContext(session=None)
                payload = task["payload"]
                for h in handlers:
                    await h.endpoint(ctx, payload)

        async with self.session_manager.session() as session:
            # 标记事件为已完成
            if event_id:
                await self.event_repo.mark_completed(session, event_id)

            # 提交事务
            await session.commit()

            logger.debug(
                f"Worker {self.worker_id} 成功处理事件: {task['type']} (ID: {event_id})"
            )

    async def fail(self, task: DBEvent, exc: Exception) -> None:
        """处理失败的事件。

        记录错误日志，将事件标记为失败状态并保存错误信息。

        Args:
            task: 失败的事件
            exc: 异常信息
        """
        event_id = task["payload"].get("event_id")
        error_message = str(exc)

        logger.error(
            f"Worker {self.worker_id} 处理事件失败: {task['type']} (ID: {event_id}), "
            f"重试次数已达上限, 错误: {error_message}",
            exc_info=True,
        )

        # 标记事件为失败
        if event_id:
            async with self.session_manager.session() as session:
                await self.event_repo.mark_failed(
                    session, event_id, error_message, increment_retry=True
                )
                await session.commit()

    async def ack(self, task: DBEvent) -> None:
        """确认任务完成。

        Args:
            task: 已完成的事件
        """
        logger.debug(f"Worker {self.worker_id} 确认事件完成: {task['type']}")
        # 通知队列该事件已处理完成
        self.event_queue.task_done()
