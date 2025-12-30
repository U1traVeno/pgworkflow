# pgoutbox

[**English**](README.md) | [简体中文](README.zh-CN.md)

基于 PostgreSQL 的工作流 / Outbox 引擎  
用于处理**不可回滚的外部操作**

---

## pgoutbox 是什么

**pgoutbox 不是 Event Bus，也不是消息队列。**

pgoutbox 是一个**基于 PostgreSQL 的工作流 / outbox 执行引擎**，用于解决下面这一类问题：

- 系统需要调用 **不可回滚的外部系统**
- 失败不能“算了重试”
- 状态必须**可追溯、可审计、可恢复**
- PostgreSQL 已经是系统的事实来源（Source of Truth）

如果你想要的是发布 / 订阅、广播、流式处理 ——  
**请不要使用 pgoutbox。**

---

## 核心思想

pgoutbox 只坚持三件事：

> **PostgreSQL 是唯一可信状态源**  
> **外部副作用永远不被视为事务的一部分**  
> **数据库状态绝不能“说谎”**

它解决的问题不是“如何更快地发消息”，而是：

- 这件事**到底做没做**
- 做到哪一步了
- 失败是**什么时候、因为什么**
- 现在系统**该不该继续、还是需要人工介入**

---

## 特性

- ✅ **PostgreSQL 作为唯一事实来源**
  - 所有操作 / 工作流步骤都持久化在数据库中
  - 没有隐藏状态

- ✅ **Outbox 风格执行模型**
  - 先提交数据库意图
  - 再由 worker 执行外部副作用

- ✅ **明确的失败语义**
  - 失败是状态，而不是异常日志
  - 不存在“可能已经处理过”的灰色状态

- ✅ **并发安全**
  - 基于 `SELECT ... FOR UPDATE SKIP LOCKED`
  - 单数据库内保证不重复执行

- ✅ **延迟执行**
  - 通过 `run_at` 支持定时 / 延迟操作

- ✅ **自动重试**
  - 内置重试与尝试次数记录

- ✅ **基于 Router 的结构化组织**
  - 类 FastAPI 的路由模型（`prefix` / `include_router`）
  - 适合大型但结构清晰的工作流

- ✅ **完整类型标注**
  - 全量类型注解（PEP 561）

---

## pgoutbox 不是什么

pgoutbox **不能、也不打算替代**：

- RabbitMQ
- Redis Streams
- Kafka
- NATS
- 各类云消息服务

它**不适合**：

- 高吞吐事件流
- 大规模 fan-out / pub-sub
- 跨数据中心消息分发
- “尽力而为”的通知型事件

如果你关心的是吞吐量、广播能力或消费者解耦 ——  
**请使用真正的消息队列。**

---

## 什么时候适合用 pgoutbox

你很可能适合 pgoutbox，如果：

- 系统是**单体或轻分布式**
- 你**完全控制 PostgreSQL**
- 存在**昂贵或不可回滚的外部操作**
- 重复执行会带来真实成本
- 你需要**查看、审计、回放**执行过程
- 相比吞吐量，你更在乎正确性

---

## 什么时候不该用

如果符合以下情况，请不要使用 pgoutbox：

- 系统高度微服务化
- 需要跨地域水平扩展
- 存在突发高并发 / 大 fan-out
- 事件只是“通知信号”
- 你期望 exactly-once（外部副作用不存在真正的 exactly-once）

---

## API 概览

pgoutbox 有意采用类似 FastAPI 的心智模型：

- 使用 `WorkflowRouter` 定义工作流
- 使用 `@router.on("...")` 注册步骤
- 通过 `include_router(...)` 组合结构
- 使用 `WorkflowSystem.start()` 启动 worker
- 通过 `publish_operation(...)` 记录执行意图

---

## 快速开始

```python
import asyncio
from datetime import datetime, timedelta, timezone

from pgoutbox import (
    WorkflowRouter,
    WorkflowSystem,
    Settings,
    DatabaseSessionManager,
    publish_operation,
)

router = WorkflowRouter()


@router.on("demo.hello", transactional=False)
async def handle_demo(ctx, payload):
    # payload 是已持久化的操作数据
    # 除非有 handler 声明 transactional=True，否则 ctx.session 为 None
    print("got:", payload)


async def main() -> None:
    settings = Settings(
        database={
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "postgres",
            "database": "postgres",
            "application_name": "pgoutbox",
            "schema": "pgoutbox",
        },
        workflow_system={
            "channel": "workflow",
            "n_workers": 5,
        },
    )

    system = WorkflowSystem(router=router, settings=settings)
    await system.start()

    # 在业务代码中记录“执行意图”
    sm = DatabaseSessionManager(settings.database)
    async with sm.session() as session:
        await publish_operation(
            session,
            operation_type="demo.hello",
            payload={"msg": "hi"},
            channel=settings.workflow_system.channel,
        )
        # 注意：publish_operation 不会自动 commit
        await session.commit()

    # 延迟执行
    async with sm.session() as session:
        await publish_operation(
            session,
            operation_type="demo.hello",
            payload={"msg": "later"},
            channel=settings.workflow_system.channel,
            run_at=datetime.now(timezone.utc) + timedelta(seconds=10),
        )
        await session.commit()

    # ... 应用继续运行 ...

    await system.stop()
    await sm.close()


asyncio.run(main())
```

---

## 事务语义（重要）

- `transactional=True` 的含义是：
  - **该 handler 需要运行在 dispatcher 管理的数据库事务中**

- 它 **不意味着**：
  - 自动提交
  - 每个 handler 一个事务

规则：

- 每个 operation **最多只会开启一个事务**
- 只有在确实需要时才会开启事务
- handler 内 **禁止调用**：
  - `commit`
  - `rollback`
  - `close`
  - `begin`
  - `begin_nested`

如果你非常清楚自己在做什么，可以使用 unsafe ：

```python
ctx.session.unsafe
```

一旦使用，意味着你放弃 pgoutbox 提供的事务保证。

---

## 一致性模型（请务必阅读）

pgoutbox **不会、也无法**让外部副作用具备事务性。

它只保证：

- 数据库不会在没有明确记录的情况下推进状态
- 提交失败不会导致工作流“假完成”
- 每一次尝试都是可观察、可审计的

外部系统可能成功或失败，  
pgoutbox 的责任是：**数据库永远不说谎。**

---

## 依赖

- Python 3.8+
- SQLAlchemy 2.0+（async）
- asyncpg
- PostgreSQL 14+

---

## 许可证

MIT

---

## 贡献

欢迎对以下方向感兴趣的开发者参与讨论或贡献：

- 基于数据库的任务执行与状态记录
- 不可回滚外部操作的可靠调度
- 简单状态机建模
- 执行失败后的重试与人工恢复

本项目仍处于探索阶段，更适合作为学习、实验或中小规模系统的基础组件。
