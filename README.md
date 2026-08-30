# FabMQ

FabMQ is a Python SDK and CLI for using CockroachDB as a durable message queue.

The project is built around a simple principle: the Python package contains
queue behavior, while CockroachDB remains the source of truth for topics,
messages, ordering, consumer progress, and retention. There is no separate
broker process or daemon; producers and consumers talk directly to CockroachDB
over SQL.

## Installation

FabMQ is not published to PyPI yet. Install it directly from GitHub:

```bash
pip install git+https://github.com/fabiog1901/fabmq.git
```

## CLI Example

```bash
export DATABASE_URL="postgresql://user:password@localhost:26257/mq?sslmode=disable"

fabmq topic create payments
fabmq produce --topic payments '{"account_id":"123","amount":100}' --json
fabmq consume --topic payments --consumer-group accounting --batch-size 10 --limit 10
```

## SDK Example

```python
from fabmq import MQ

mq = MQ("postgresql://user:password@localhost:26257/mq?sslmode=disable")

mq.create_topic("payments")

job_id = mq.enqueue(
    topic="payments",
    payload={"account_id": "123", "amount": 100},
)

with mq.consume(
    topic="payments",
    consumer_group="accounting",
    bucket=42,
    batch_size=10,
) as jobs:
    for job in jobs:
        print(job.id, job.payload)
```

The database schema and SQL functions must be installed before using the CLI or
SDK. The current prototype schema lives in `mq/mq.sql`.
