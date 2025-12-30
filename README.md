# pgoutbox

[**English**](README.md) | [简体中文](README.zh-CN.md)

PostgreSQL-backed workflow / outbox engine for irreversible operations

---

## What pgoutbox Is

**pgoutbox is NOT an event bus.**

pgoutbox is a **database-backed workflow and outbox engine** designed for systems that:

- Must interact with **irreversible external side effects**
- Require **strong auditability and traceability**
- Treat failures as **first-class, inspectable states**
- Already rely on PostgreSQL as the system of record

If you are looking for pub/sub, streaming, or fan-out messaging — this is not it.

---

## Core Idea

pgoutbox embraces a simple but strict model:

> **PostgreSQL is the single source of truth.  
> External side effects are never considered transactional.  
> The database must never lie about what has (or has not) happened.**

It helps you answer questions like:

- *Has this operation actually completed?*
- *Did we attempt the external call?*
- *If it failed, why — and where should we resume?*

---

## Features

- ✅ **PostgreSQL as the Single Source of Truth**
  - Events / operations are stored durably in Postgres
  - No hidden state in brokers or memory

- ✅ **Outbox-style Execution Model**
  - Database state is committed **before** any external side effects
  - Side effects are executed by workers based on persisted intent

- ✅ **Strong Failure Semantics**
  - Failures are recorded, not swallowed
  - No “maybe processed” states

- ✅ **Concurrency Safety**
  - `SELECT ... FOR UPDATE SKIP LOCKED` based dispatch
  - No duplicate execution within a single database

- ✅ **Delayed Execution**
  - Schedule operations via `run_at`

- ✅ **Automatic Retry**
  - Built-in retry with attempt tracking

- ✅ **Router-based Handler Composition**
  - FastAPI-style router API (`prefix`, `include_router`)
  - Designed for large but structured workflows

- ✅ **Fully Typed**
  - Complete type annotations (PEP 561)

---

## What pgoutbox Is NOT

pgoutbox does **not** replace:

- RabbitMQ
- Redis Streams
- Kafka
- NATS
- Cloud-native message brokers

It is **not suitable** for:

- High-throughput event streaming
- Large fan-out pub/sub workloads
- Cross-datacenter or internet-scale messaging
- Best-effort or lossy signaling

If you need throughput, fan-out, or decoupled consumers, use a real message broker.

---

## When pgoutbox Makes Sense

You are likely a good fit if:

- Your system is mostly **local or monolithic**
- You **control the PostgreSQL instance**
- You perform **expensive or irreversible external operations**
- Duplicate execution would cause **real cost**
- You need to **inspect, audit, or replay** operations
- You care more about correctness than throughput

---

## When You Should Not Use It

You probably should not use pgoutbox if:

- Your system is microservice-heavy
- You need cross-region horizontal scaling
- You expect burst traffic or massive fan-out
- You treat events as best-effort signals
- You want exactly-once delivery semantics (no system can give you that for external side effects)

---

## API Overview

pgoutbox intentionally mimics FastAPI's mental model:

- Define workflows via `WorkflowRouter`
- Register steps via `@router.on("...")`
- Compose routers with `include_router(...)`
- Run workers via `WorkflowSystem.start()`
- Record intent via `publish_operation(...)`

---

## Quickstart

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
    # payload is the persisted operation payload
    # ctx.session is None unless transactional=True is required
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

    # Record intent from your application code
    sm = DatabaseSessionManager(settings.database)
    async with sm.session() as session:
        await publish_operation(
            session,
            operation_type="demo.hello",
            payload={"msg": "hi"},
            channel=settings.workflow_system.channel,
        )
        # IMPORTANT: publish_operation does NOT commit
        await session.commit()

    # Delayed execution
    async with sm.session() as session:
        await publish_operation(
            session,
            operation_type="demo.hello",
            payload={"msg": "later"},
            channel=settings.workflow_system.channel,
            run_at=datetime.now(timezone.utc) + timedelta(seconds=10),
        )
        await session.commit()

    # ... your app runs ...

    await system.stop()
    await sm.close()


asyncio.run(main())
```

---

## Transaction Semantics

- `transactional=True` means:
  - “This handler requires a dispatcher-managed database transaction.”

- It does **NOT** mean:
  - Auto-commit
  - One transaction per handler

Rules:

- At most **one transaction per operation**
- Transaction is opened **only if required**
- Handlers **cannot**:
  - `commit`
  - `rollback`
  - `close`
  - `begin`
  - `begin_nested`

An explicit escape hatch exists:

```python
ctx.session.unsafe
```

Using it means you give up pgoutbox’s guarantees.

---

## Consistency Model (Important)

pgoutbox does **not** attempt to make external side effects transactional.

Instead, it guarantees:

- The database never marks an operation as completed unless it explicitly records that fact
- Failed commits do not advance workflow state
- Every attempt is observable and auditable

External systems may succeed or fail independently.  
pgoutbox ensures your database never lies about it.

---

## Dependencies

- Python 3.8+
- SQLAlchemy 2.0+ (async)
- asyncpg
- PostgreSQL 14+

---

## License

MIT

---

## Contributing

Contributions and discussions are welcome if you are interested in topics such as:

- Managing task execution and state using a relational database
- Coordinating non-reversible external side effects
- Simple state machine modeling
- Retry and manual recovery after execution failures

This project is still in an exploratory stage.
It is intended primarily for learning, experimentation, and use in small-to-medium scale systems, rather than as a production-grade workflow or saga framework.
