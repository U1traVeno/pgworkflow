"""PostgreSQL NOTIFY/LISTEN 事件监听器。

重构后的架构：
1. EventListener 仅负责监听 PostgreSQL NOTIFY 并触发 prefetch
2. 不再解析 payload 或直接处理事件
3. NOTIFY 只是"提示信号"，实际的事件获取由 EventQueue.maybe_prefetch() 完成
"""

from __future__ import annotations
import logging
from typing import TYPE_CHECKING

import asyncpg  # type: ignore

if TYPE_CHECKING:
    from .queue import EventQueue

logger = logging.getLogger(__name__)


async def create_listener_connection(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> asyncpg.Connection:
    """创建用于 LISTEN/NOTIFY 的专用 asyncpg 连接。

    这个连接独立于 SQLAlchemy 的连接池，
    因为 SQLAlchemy 对 LISTEN/NOTIFY 的支持不好。

    Args:
        host: 数据库主机
        port: 数据库端口
        user: 数据库用户
        password: 数据库密码
        database: 数据库名称

    Returns:
        asyncpg.Connection: 用于事件监听的专用连接
    """
    dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    connection: asyncpg.Connection = await asyncpg.connect(dsn)  # type: ignore
    logger.debug("为事件监听器创建了专用的 asyncpg 连接")
    return connection  # type: ignore


class EventListener:
    """PostgreSQL NOTIFY/LISTEN 事件监听器。

    重构后的架构：仅负责监听并将事件 ID 放入 EventQueue，不做任何 I/O 或数据库操作。
    事件处理由 WorkerPool 中的 Worker 并发消费。
    """

    def __init__(
        self,
        connection: asyncpg.Connection,  # type: ignore
        event_queue: EventQueue,
        channel: str = "events",
    ) -> None:
        """使用连接和事件队列初始化事件监听器。

        Args:
            connection: 用于 LISTEN/NOTIFY 的专用 asyncpg 连接
            event_queue: EventQueue 实例，接收事件 ID
            channel: PostgreSQL NOTIFY 频道名称
        """
        self.connection = connection
        self.event_queue = event_queue
        self.channel = channel
        self.is_running = False

    async def start(self) -> None:
        """开始监听数据库事件。"""
        if self.is_running:
            logger.warning("事件监听器已经在运行")
            return

        try:
            # 设置通知回调
            await self.connection.add_listener(  # type: ignore
                self.channel,
                self._handle_notification,
            )

            self.is_running = True
            logger.info(f"事件监听器已在频道 '{self.channel}' 上启动")

        except Exception as e:
            logger.error(f"启动事件监听器失败: {e}", exc_info=True)
            raise

    async def stop(self) -> None:
        """停止监听数据库事件。"""
        if not self.is_running:
            logger.warning("事件监听器未在运行")
            return

        try:
            await self.connection.remove_listener(  # type: ignore
                self.channel,
                self._handle_notification,
            )
            await self.connection.close()  # type: ignore

            self.is_running = False
            logger.info("事件监听器已停止")

        except Exception as e:
            logger.error(f"停止事件监听器时出错: {e}", exc_info=True)

    def _handle_notification(
        self,
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """处理来自 PostgreSQL 的传入通知。

        这是 asyncpg 要求的同步回调，不能 await。
        只负责触发队列的 prefetch 机制，不做任何 I/O 操作。

        Args:
            connection: 数据库连接
            pid: 通知后端的进程 ID
            channel: 频道名称
            payload: JSON 负载字符串

        注意:
        ----
        NOTIFY 只是"提示信号",不携带完整事件数据。
        实际的事件加载由 EventQueue.maybe_prefetch() 完成。
        """
        try:
            logger.debug(f"收到 NOTIFY from channel '{channel}' (pid={pid})")

            # 触发 prefetch(非阻塞)
            # notify() 会在内部创建 task 执行 maybe_prefetch()
            self.event_queue.notify()

            logger.debug(f"已触发 prefetch, 当前队列大小: {self.event_queue.qsize()}")

        except Exception as e:
            logger.error(f"处理通知时出错: {e}", exc_info=True)
