"""数据库配置与会话管理。

目标:
- 提供数据库配置项（由 pydantic-settings 统一管理）
- 由 pgoutbox 内部创建 SQLAlchemy AsyncEngine
- 对外暴露一个 session_manager，使用 async with 获取会话
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from .config import DatabaseConfig


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

        server_settings_updates: Dict[str, str] = {}
        if config.application_name:
            server_settings_updates["application_name"] = config.application_name
        if config.search_path:
            server_settings_updates["search_path"] = config.search_path

        if server_settings_updates:
            connect_args = dict(engine_kwargs.get("connect_args", {}))
            server_settings = dict(connect_args.get("server_settings", {}))

            for key, value in server_settings_updates.items():
                server_settings.setdefault(key, value)

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
