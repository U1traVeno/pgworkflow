"""数据库配置与会话管理。

目标:
- 提供类似 tmp/paramer 的数据库配置项
- 由 pgebus 内部创建 SQLAlchemy AsyncEngine
- 对外暴露一个 session_manager，使用 async with 获取会话

注意:
- 这里不引入 pydantic 以避免额外依赖。
- 事件表的建表/迁移仍由使用方负责。
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine


JsonSerializer = Callable[[Any], str]
JsonDeserializer = Callable[[str], Any]


@dataclass(frozen=True)
class EngineConfig:
    """SQLAlchemy Engine 配置。"""

    pool_size: int = 5
    max_overflow: int = 2
    pool_timeout: int = 120
    pool_pre_ping: bool = True
    pool_recycle: int = 3600

    # 透传给 create_async_engine(connect_args=...)
    connect_args: Dict[str, Any] = field(default_factory=lambda: {})

    # 可选 JSON 序列化器（默认使用标准库 json）
    json_serializer: Optional[JsonSerializer] = None
    json_deserializer: Optional[JsonDeserializer] = None

    def engine_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_pre_ping": self.pool_pre_ping,
            "pool_recycle": self.pool_recycle,
        }

        if self.connect_args:
            kwargs["connect_args"] = dict(self.connect_args)

        def _default_json_serializer(value: Any) -> str:
            return json.dumps(value, ensure_ascii=False)

        kwargs["json_serializer"] = self.json_serializer or _default_json_serializer
        kwargs["json_deserializer"] = self.json_deserializer or json.loads

        return kwargs


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL 数据库配置。"""

    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"
    application_name: Optional[str] = None

    engine: EngineConfig = field(default_factory=EngineConfig)

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def asyncpg_dsn(self) -> str:
        # LISTEN/NOTIFY 专用连接使用 asyncpg 原生 DSN
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class DatabaseSessionManager:
    """SQLAlchemy AsyncEngine + AsyncSession 工厂。"""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config

        self._engine: Optional[AsyncEngine] = None
        self._sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None

        # 默认立即打开；close() 后也可按需重新打开
        self.ensure_open()

    @property
    def closed(self) -> bool:
        return self._engine is None or self._sessionmaker is None

    def _build_engine_kwargs(self) -> Dict[str, Any]:
        config = self._config
        engine_kwargs = config.engine.engine_kwargs()

        if config.application_name:
            connect_args = dict(engine_kwargs.get("connect_args", {}))
            server_settings = dict(connect_args.get("server_settings", {}))
            server_settings.setdefault("application_name", config.application_name)
            connect_args["server_settings"] = server_settings
            engine_kwargs["connect_args"] = connect_args

        return engine_kwargs

    def ensure_open(self) -> None:
        if not self.closed:
            return

        config = self._config
        engine_kwargs = self._build_engine_kwargs()

        self._engine = create_async_engine(
            config.sqlalchemy_url,
            **engine_kwargs,
        )
        self._sessionmaker = async_sessionmaker(
            self._engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    @property
    def engine(self) -> AsyncEngine:
        self.ensure_open()
        if self._engine is None:
            raise RuntimeError("DatabaseSessionManager failed to open")
        return self._engine

    def new_session(self) -> AsyncSession:
        """创建一个新的 AsyncSession（不带上下文管理）。"""
        self.ensure_open()
        if self._sessionmaker is None:
            raise RuntimeError("DatabaseSessionManager failed to open")
        return self._sessionmaker()

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """获取一个受控的 AsyncSession。

        用法:
            async with session_manager.session() as session:
                ...

        语义:
        - 正常退出: 不自动 commit（由调用方决定何时 commit）
        - 异常退出: 自动 rollback
        - 总是 close
        """
        session = self.new_session()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def close(self) -> None:
        if self._engine is None:
            return
        try:
            await self._engine.dispose()
        finally:
            self._engine = None
            self._sessionmaker = None
