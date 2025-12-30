"""事件路由模块。"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Any

from .base import DBEvent, EventContext

# 定义 Endpoint 类型别名
Endpoint = Callable[[EventContext, Dict[str, Any]], Awaitable[Any]]


@dataclass(frozen=True)
class HandlerSpec:
    path: str
    endpoint: Endpoint
    transactional: bool = False


class EventRouter:
    """事件路由器。

    类似于 FastAPI 的 APIRouter，用于注册和管理事件处理函数。
    """

    def __init__(self, prefix: str = "") -> None:
        """初始化 EventRouter。

        Args:
            prefix: 路由前缀。
        """
        self.prefix = prefix
        self.event_handlers: List[HandlerSpec] = []

    def add_event_route(
        self, path: str, endpoint: Endpoint, transactional: bool
    ) -> None:
        """添加事件路由。

        Args:
            path: 事件路径（相对于当前 router 的 prefix）
            endpoint: 处理函数
            transactional: 是否要求 dispatcher 管理的事务
        """
        self.event_handlers.append(
            HandlerSpec(path=path, endpoint=endpoint, transactional=transactional)
        )

    def on(
        self, path: str, *, transactional: bool = False
    ) -> Callable[[Endpoint], Endpoint]:
        """注册事件处理函数的装饰器。

        Args:
            path: 事件路径
            transactional: 是否要求运行在 dispatcher 管理的事务中

        Returns:
            装饰器函数
        """

        def decorator(endpoint: Endpoint) -> Endpoint:
            self.add_event_route(path, endpoint, transactional)
            return endpoint

        return decorator

    def include_router(self, router: "EventRouter", prefix: str = "") -> None:
        """包含另一个路由器。

        将子路由器的所有路由添加到当前路由器中。
        路径计算规则：prefix (参数) + router.prefix (子路由) + path (路由)

        Args:
            router: 要包含的 EventRouter 实例
            prefix: 包含时的额外前缀
        """
        for spec in router.event_handlers:
            parts: List[str] = []
            if prefix:
                parts.append(prefix)
            if router.prefix:
                parts.append(router.prefix)
            if spec.path:
                parts.append(spec.path)

            new_path = ".".join(parts)
            self.add_event_route(new_path, spec.endpoint, spec.transactional)

    def get_handlers(self, event_name: str) -> List[HandlerSpec]:
        """获取匹配事件名称的所有 handler（精确匹配）。

        Args:
            event_name: 事件名称（通常是 DBEvent["type"]）

        Returns:
            匹配的 handler 列表（按注册顺序）
        """
        return [spec for spec in self.event_handlers if spec.path == event_name]

    async def call_handlers(self, ctx: EventContext, event: DBEvent) -> bool:
        """在给定上下文下调用匹配的 handlers。

        注意：此方法不负责创建 session/事务；事务边界由 dispatcher 决定。
        """
        handlers = self.get_handlers(event["type"])
        if not handlers:
            return False

        payload = event["payload"]
        for spec in handlers:
            await spec.endpoint(ctx, payload)
        return True
