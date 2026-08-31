![logo](resources/logo.png)

# FabMQ

FabMQ is a Python SDK and CLI for using CockroachDB as a durable message queue.

The project is built around a simple principle: the Python package contains
queue behavior, while CockroachDB remains the source of truth for topics,
messages, ordering, consumer progress, and retention. There is no separate
broker process or daemon; producers and consumers talk directly to CockroachDB
over SQL.

## Design

This repository also hosts the [CockroachDB-backed message queue design paper](resources/CockroachDB-MQ-Design.md).

A related blog post is available to introduce the design and project:
[A CockroachDB-Backed Message Queue Between `SKIP LOCKED` and Kafka](https://dev.to/cockroachlabs/a-cockroachdb-backed-message-queue-between-skip-locked-and-kafka-15l8).

## Installation

FabMQ is not published to PyPI yet. Install it directly from GitHub:

```bash
pip install git+https://github.com/fabiog1901/fabmq.git
```

## CLI Example

```bash
export DATABASE_URL="postgresql://user:password@localhost:26257/mq?sslmode=disable"

fabmq init
fabmq topic create payments
fabmq produce --topic payments '{"account_id":"123","amount":100}' --json
fabmq consume --topic payments --consumer-group accounting --batch-size 10 --limit 10
```

## SDK Admin Example

```python
import psycopg

from fabmq import MQ

with psycopg.connect(
    "postgresql://user:password@localhost:26257/mq?sslmode=disable"
) as conn:
    mq = MQ(conn)

    mq.init_schema()
    mq.create_topic("payments")
```

## SDK Producer Example

```python
import psycopg

from fabmq import MQ

with psycopg.connect(
    "postgresql://user:password@localhost:26257/mq?sslmode=disable"
) as conn:
    mq = MQ(conn)

    job_id = mq.enqueue(
        topic="payments",
        payload={"account_id": "123", "amount": 100},
    )
```

## SDK Listener Example

```python
import time

import psycopg

from fabmq import MQ

with psycopg.connect(
    "postgresql://user:password@localhost:26257/mq?sslmode=disable",
    autocommit=True,
) as conn:
    mq = MQ(conn)

    while True:
        with mq.consume(
            topic="payments",
            consumer_group="accounting",
            bucket=42,
            batch_size=10,
        ) as jobs:
            for job in jobs:
                print(job.id, job.payload)

        if not jobs:
            time.sleep(1.0)
```

The database schema and SQL functions must be installed before using the CLI or
SDK. The installer uses `sql/001_initial.sql` as the schema source.
