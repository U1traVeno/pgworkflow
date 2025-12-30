from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Type

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import (
    BaseSettings,
    InitSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
    YamlConfigSettingsSource,
)

JsonSerializer = Callable[[Any], str]
JsonDeserializer = Callable[[str], Any]


class EngineConfig(BaseModel):
    """SQLAlchemy Engine 配置。"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    pool_size: int = 5
    max_overflow: int = 2
    pool_timeout: int = 120
    pool_pre_ping: bool = True
    pool_recycle: int = 3600

    # 透传给 create_async_engine(connect_args=...)
    connect_args: Dict[str, Any] = Field(default_factory=dict)

    # 可选 JSON 序列化器（默认使用标准库 json）
    json_serializer: Optional[JsonSerializer] = Field(default=None, repr=False)
    json_deserializer: Optional[JsonDeserializer] = Field(default=None, repr=False)

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


class DatabaseConfig(BaseModel):
    """PostgreSQL 数据库配置。"""

    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"
    application_name: Optional[str] = None

    # 默认使用独立 schema 与业务表隔离
    model_config = ConfigDict(populate_by_name=True)
    schema_name: str = Field(default="pgoutbox", alias="schema")

    engine: EngineConfig = Field(default_factory=EngineConfig)

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

    @property
    def search_path(self) -> Optional[str]:
        """PostgreSQL search_path。

        默认优先在独立 schema 中解析（并保留 public 作为兜底）。
        """
        return f"{self.schema_name},public"


class EventSystemConfig(BaseModel):
    """事件系统配置（LISTEN/NOTIFY + Worker Pool）。"""

    channel: str = "events"

    n_workers: int = Field(
        default=5,
        ge=1,
        le=100,
        description="并发处理事件的 Worker 数量。根据负载和 CPU 核心数调整。",
    )
    queue_maxsize: int = Field(
        default=1000,
        ge=0,
        description="事件队列最大容量。0 表示无限制（不推荐）。队列满时会丢弃新事件。",
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="事件处理失败后的最大重试次数。",
    )
    poll_interval: float = Field(
        default=1.0,
        ge=0.1,
        le=60.0,
        description="Worker 在队列为空时的轮询间隔（秒）。",
    )

    pending_poll_enabled: bool = Field(
        default=True,
        description="是否启用轮询触发队列 prefetch（兜底 NOTIFY 丢失/启动堆积）。",
    )
    pending_poll_interval_seconds: float = Field(
        default=5.0,
        ge=1.0,
        description="轮询间隔（秒），默认 5 秒。",
    )

    shutdown_wait_timeout: float = Field(
        default=30.0,
        ge=0.0,
        description="优雅关闭时等待队列清空的超时时间（秒）。0 表示不等待。",
    )
    shutdown_wait_for_completion: bool = Field(
        default=True,
        description="优雅关闭时是否等待队列中的事件处理完毕。",
    )

    cleanup_enabled: bool = Field(
        default=False,
        description="是否启用定时清理已终态事件(COMPLETED/FAILED)。",
    )
    cleanup_retention_seconds: int = Field(
        default=7 * 24 * 3600,
        ge=0,
        description=(
            "保留时长（秒）。仅清理 processed_at 早于 now-retention 的事件；0 表示不清理。"
        ),
    )
    cleanup_interval_seconds: float = Field(
        default=3600.0,
        ge=1.0,
        description="清理任务执行间隔（秒）。",
    )
    cleanup_batch_size: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="每轮清理的最大删除条数（分批删除）。",
    )


class EnvVarFileConfigSettingsSource(InitSettingsSource):
    """
    一个从环境变量中指定的文件加载配置的源。
    它会根据文件扩展名自动选择 TOML 或 YAML 解析器。
    """

    def __init__(
        self,
        settings_cls: Type[BaseSettings],
        env_var: str = "PGOUTBOX_CONFIG_FILE",
        env_file_encoding: Optional[str] = None,
    ):
        """
        Args:
            settings_cls: The settings class.
            env_var: The name of the environment variable to read the file path from.
            env_file_encoding: The encoding to use for YAML files.
        """
        self.env_var = env_var
        self.file_path_str = os.getenv(env_var)
        self.encoding = env_file_encoding

        file_data: Dict[str, Any] = {}

        if not self.file_path_str:
            super().__init__(settings_cls, file_data)
            return

        if not Path(self.file_path_str).expanduser().is_absolute():
            raise ValueError(
                f"Environment variable '{self.env_var}' must point to an absolute path"
            )

        file_path = Path(self.file_path_str)
        if not file_path.exists():
            print(f"警告: 环境变量 '{self.env_var}' 指向的文件 '{file_path}' 不存在。")

        # 根据文件扩展名，复用现有的源逻辑
        suffix = file_path.suffix.lower()
        if suffix == ".toml":
            # 内部创建一个 TomlConfigSettingsSource 实例来加载文件
            file_data = TomlConfigSettingsSource(
                settings_cls, toml_file=file_path
            ).toml_data
        elif suffix in (".yaml", ".yml"):
            # 内部创建一个 YamlConfigSettingsSource 实例来加载文件
            file_data = YamlConfigSettingsSource(
                settings_cls, yaml_file=file_path, yaml_file_encoding=self.encoding
            ).yaml_data
        else:
            print(f"警告: 不支持的文件类型 '{suffix}'。已忽略。")

        # 调用 InitSettingsSource 的 __init__，传入从文件中加载的数据
        super().__init__(settings_cls, file_data)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(env_var={self.env_var}, file_path={self.file_path_str!r})"


class Settings(BaseSettings):
    """pgoutbox 配置（仅包含 DB 与事件系统）。"""

    model_config = SettingsConfigDict(
        toml_file=Path("pgoutbox.toml"),
        yaml_file=Path("pgoutbox.yaml") or Path("pgoutbox.yml"),
        yaml_file_encoding="utf-8",
        env_prefix="PGOUTBOX_",
        env_nested_delimiter="__",
    )

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    event_system: EventSystemConfig = Field(default_factory=EventSystemConfig)

    @classmethod
    def settings_customise_sources(
        cls: Type["Settings"],
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        定义不同配置源的优先级。
        参见: https://docs.pydantic.dev/latest/concepts/pydantic_settings/#customise-settings-sources
        """
        return (
            init_settings,
            # Custom Source
            EnvVarFileConfigSettingsSource(settings_cls),
            env_settings,
            dotenv_settings,
            # https://docs.pydantic.dev/latest/concepts/pydantic_settings/#other-settings-source
            TomlConfigSettingsSource(settings_cls),
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )
