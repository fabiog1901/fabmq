# CockroachDB MQ Python SDK / CLI — Design Specification

## 1. Overview

The project provides a **stateless Python SDK and CLI that turns CockroachDB into a distributed message queue**.

CockroachDB is the system of record for all durable state, including:

* Topics
* Messages
* Message ordering
* Consumer groups
* Consumer high-water marks
* Consumer ownership and leases, if implemented
* Retention configuration
* Retry/DLQ state, if implemented
* Product/schema metadata

The Python package provides the application-facing interface and operational tooling. It contains behavior, but no durable state.

The core design principle is:

> **The SDK contains behavior; CockroachDB contains truth.**

Users may interact with the queue through either:

1. The Python SDK.
2. The CLI.
3. SQL directly.

The SQL interface remains a first-class interface rather than an implementation that requires a permanently running MQ service.

---

## 2. Goals

The product should provide:

* Durable message queuing using CockroachDB.
* Ordered delivery within a bucket.
* Multiple independent topics.
* Multiple independent consumer groups.
* Stateless producers and consumers.
* Append-only message and consumer-progress storage where practical.
* Automatic retention through Row-Level TTL.
* Efficient producer micro-batching.
* Simple Python APIs for producing and consuming.
* A CLI for administration and diagnostics.
* Transactional enqueue alongside application database operations.
* Direct SQL access for advanced users.
* No additional server or daemon required.

The SDK and CLI should hide implementation details such as:

* Bucket calculation.
* Sequence allocation.
* Bucket locking.
* HWM management.
* Transaction bookkeeping.
* Consumer ownership.
* Retries.
* Batch management.

---

## 3. Non-Goals

The initial implementation should not attempt to reproduce every Kafka feature.

In particular, the first version does not need:

* Kafka protocol compatibility.
* A standalone broker service.
* HTTP/gRPC APIs.
* Cross-database messaging.
* Complex stream processing.
* Exactly-once external side effects.
* Dynamic repartitioning of existing topics.
* Persistent client-side state.

The architecture should allow these capabilities to be added later without redesigning the fundamental storage model.

---

## 4. High-Level Architecture

```text
                    Applications
                         |
              +----------+----------+
              |                     |
          Python SDK              CLI
              |                     |
              +----------+----------+
                         |
                         | SQL
                         |
                  CockroachDB
                         |
          +--------------+--------------+
          |              |              |
        topics           mq             hwm
          |              |              |
     configuration    messages      consumer progress
                         |
                    Row-Level TTL
```

There is deliberately no intermediary MQ server:

```text
Application -> CockroachDB
```

rather than:

```text
Application -> MQ Service -> CockroachDB
```

This keeps the product stateless and avoids introducing another distributed service.

---

## 5. Python Package

Tentative package name:

```text
fabmq
```

Suggested initial structure:

```text
fabmq/
    __init__.py
    client.py
    producer.py
    consumer.py
    topic.py
    admin.py
    models.py
    exceptions.py
    connection.py
    migrations.py
    cli.py

    sql/
        001_initial.sql
        002_*.sql
```

The exact module layout is not important initially, but database access, producer behavior, consumer behavior, and CLI behavior should remain logically separated.

---

## 6. Core Client

The main SDK entry point should be a client object.

Example:

```python
from fabmq import MQ

mq = MQ(
    "postgresql://user:password@host:26257/database"
)
```

Alternatively:

```python
mq = MQ(connection=conn)
```

The client should support both:

* SDK-managed database connections.
* Caller-provided CockroachDB connections.

This distinction is important for transactional enqueue.

---

## 7. Database Initialization

The CLI should install the complete MQ schema.

Example:

```bash
fabmq init --url "$DATABASE_URL"
```

Initialization should create:

* Required tables.
* Primary and secondary indexes.
* Row-Level TTL configuration.
* SQL functions.
* Placeholder rows.
* Metadata/schema version information.
* Required constraints.

Initialization must be idempotent where practical.

A schema metadata table should track installed versions.

For example:

```sql
CREATE TABLE fabmq_meta (
    key STRING PRIMARY KEY,
    value STRING NOT NULL
);
```

Possible values:

```text
schema_version   1
product_version  0.1.0
```

The CLI should eventually support:

```bash
fabmq upgrade
```

to apply database migrations.

---

## 8. Topics

A topic represents an independent logical queue.

Example SDK API:

```python
mq.create_topic("payments")
```

CLI:

```bash
fabmq topic create payments
```

SQL:

```sql
SELECT create_topic('payments');
```

Other operations:

```python
mq.list_topics()
mq.delete_topic("payments")
mq.describe_topic("payments")
```

CLI equivalents:

```bash
fabmq topic list
fabmq topic describe payments
fabmq topic delete payments
```

A topic must exist before messages can be enqueued.

---

## 9. Buckets

Messages within a topic are divided across buckets.

The initial implementation uses:

```text
256 buckets
```

with bucket IDs:

```text
0 .. 255
```

Buckets serve a role similar to Kafka partitions.

They provide:

* Independent ordered message streams.
* Producer parallelism.
* Consumer parallelism.
* Distribution across CockroachDB ranges.
* Potential geo-placement.

Ordering is guaranteed **within a bucket**, not across the entire topic.

---

## 10. Placeholder Rows

Each topic contains one permanent placeholder message per bucket:

```text
seq_id = 0
```

Therefore, creating a topic creates 256 rows:

```text
(topic, bucket=0,   seq_id=0)
(topic, bucket=1,   seq_id=0)
...
(topic, bucket=255, seq_id=0)
```

These rows serve two purposes:

1. They establish the existence of each topic/bucket.
2. They provide the exclusive lock used to serialize producers.

Placeholder rows must never expire through Row-Level TTL.

They are not consumer-visible messages.

---

## 11. Producer Ordering

A producer determines a bucket using a deterministic routing key.

The default implementation hashes the payload:

```text
bucket = CRC32(payload) % 256
```

An application may eventually specify another routing key.

For example:

```python
mq.enqueue(
    topic="payments",
    payload=payload,
    key=account_id,
)
```

This could provide FIFO ordering for all messages sharing the same `account_id`.

---

## 12. Producer Serialization

CockroachDB tables are ordered indexes rather than append-only log files.

To emulate appending, the producer:

1. Determines the bucket.
2. Acquires an exclusive lock on `(topic, bucket, seq_id=0)`.
3. Finds the highest existing `seq_id`.
4. Calculates `next_seq_id = last_seq_id + 1`.
5. Inserts the message.
6. Commits.
7. Releases the lock.

Conceptually:

```text
lock bucket placeholder
        |
        v
find current tail
        |
        v
tail + 1
        |
        v
insert message
        |
        v
commit
```

The exclusive lock serializes producers at:

```text
(topic, bucket)
```

Different buckets remain independent and can accept messages concurrently.

---

## 13. Producer SQL API

The primary low-level producer API is:

```sql
enqueue_job(topic, payload)
```

Conceptually:

```sql
SELECT enqueue_job(
    'payments',
    '{"account_id":"123","amount":100}'
);
```

It returns the generated `job_id`.

Applications should normally use the SDK, but direct SQL remains supported.

---

## 14. Python Producer API

Basic API:

```python
job_id = mq.enqueue(
    topic="payments",
    payload={
        "account_id": "123",
        "amount": 100,
    },
)
```

The SDK is responsible for serialization of Python objects into the message payload format.

The API should also allow strings/bytes where appropriate.

---

## 15. Transactional Enqueue

Transactional enqueue is an important differentiating feature.

An application must be able to use an existing CockroachDB transaction:

```python
with conn.transaction():

    update_business_state(conn)

    mq.enqueue(
        topic="payments",
        payload=event,
        connection=conn,
    )
```

The application mutation and queue insertion then belong to the same CockroachDB transaction.

The outcome is:

```text
business mutation + enqueue
```

either both commit or both roll back.

This effectively provides transactional-outbox semantics without requiring a separate outbox-to-broker replication process.

The SDK must therefore never force creation of its own transaction when a caller supplies an existing transactional connection.

---

## 16. Producer Batch API

The SDK should support explicit batching:

```python
job_ids = mq.enqueue_batch(
    topic="payments",
    payloads=[
        payload1,
        payload2,
        payload3,
    ],
)
```

A batch is intentionally routed to **one bucket**.

The initial routing algorithm should calculate the bucket from the first payload:

```text
bucket = CRC32(payloads[0]) % 256
```

All payloads in the batch inherit that bucket.

This is intentionally different from independently hashing every message.

---

## 17. Batch Sequence Allocation

Suppose the current bucket tail is:

```text
seq_id = 100
```

and the producer submits five messages.

They receive:

```text
payload 1 -> 101
payload 2 -> 102
payload 3 -> 103
payload 4 -> 104
payload 5 -> 105
```

SQL can implement this using:

```sql
unnest(payloads) WITH ORDINALITY
```

and:

```text
next_seq_id = current_tail + ordinality
```

The entire batch performs:

```text
1 exclusive lock
1 tail lookup
N message inserts
1 transaction commit
```

rather than:

```text
N exclusive locks
N tail lookups
N commits
```

This is the primary producer micro-batching optimization.

---

## 18. Automatic Producer Micro-Batching

A later SDK optimization may transparently accumulate messages for a short interval.

Configuration might look like:

```python
mq = MQ(
    url,
    linger_ms=5,
    producer_batch_size=500,
)
```

Applications continue calling:

```python
mq.enqueue(...)
```

while the SDK internally groups compatible messages into batches.

Possible behavior:

```text
enqueue()
enqueue()
enqueue()
enqueue()
    |
    | 5 ms
    v
group messages
    |
    v
batch enqueue
```

This feature belongs primarily in the SDK, not the database schema.

It must remain optional because batching changes enqueue latency.

---

## 19. Consumer Groups

Consumers belong to a named consumer group.

Example:

```text
topic          payments
consumer_group accounting
```

Different consumer groups consume the same topic independently.

For example:

```text
payments
    |
    +-- accounting
    +-- analytics
    +-- fraud-detection
```

Each group maintains its own progress.

---

## 20. Consumer Position

Consumer progress is stored in `hwm`.

The logical key is:

```text
(topic, consumer_group, bucket)
```

Each committed HWM record indicates the highest successfully processed `seq_id`.

The HWM table is append-only.

The latest HWM can be obtained using the highest sequence number for the combination.

The SDK should hide this mechanism completely from applications.

---

## 21. Consumer Ownership Contract

At any given time:

> At most one active consumer may process a particular `(topic, consumer_group, bucket)` combination.

For example:

```text
payments / accounting

worker A -> buckets 0-63
worker B -> buckets 64-127
worker C -> buckets 128-191
worker D -> buckets 192-255
```

Another consumer group may independently consume those same buckets:

```text
payments / analytics

worker E -> buckets 0-255
```

The exclusivity rule applies only within the same:

```text
(topic, consumer_group, bucket)
```

---

## 22. Initial Consumer Assignment

The first implementation may use explicit bucket assignment.

Example:

```python
consumer = mq.consumer(
    topic="payments",
    consumer_group="accounting",
    buckets=range(0, 64),
)
```

This keeps the first implementation simple.

The SDK must not allow two workers to intentionally claim the same bucket within the same group unless an explicit unsafe/advanced mode is introduced.

---

## 23. Consumer API

The consumer API should hide:

* HWM lookup.
* Sequence IDs.
* Transaction management.
* HWM insertion.
* Batch acknowledgement.

The desired API is:

```python
with mq.consume(
    topic="payments",
    consumer_group="accounting",
    bucket=42,
    batch_size=100,
) as jobs:

    for job in jobs:
        process(job)
```

The application should only need to understand:

```python
job.id
job.payload
```

Internal fields such as `seq_id` should not normally be exposed.

---

## 24. Consumer Context Semantics

The context manager provides acknowledgement semantics.

On entry:

```text
determine current HWM
fetch next messages
return batch
```

On successful exit:

```text
insert new HWM
commit
```

On exceptional exit:

```text
do not advance HWM
rollback / abandon acknowledgement
```

The application contract is therefore:

> If the context exits normally, the batch is acknowledged.
> If the context exits with an exception, the batch is not acknowledged.

Example:

```python
with mq.consume(...) as jobs:
    for job in jobs:
        process(job)
```

If `process(job)` raises an exception, the HWM must not advance beyond that batch.

---

## 25. Consumer Transaction Strategy

The simplest initial implementation may keep a CockroachDB transaction open while the application processes the batch:

```text
BEGIN

read HWM
read messages

<application processes messages>

insert HWM

COMMIT
```

This provides simple semantics but creates potentially long-running transactions.

This is acceptable for an initial implementation if message processing is expected to be short.

The SDK should document this behavior.

Long-running job support should eventually use a lease/claim mechanism rather than keeping a SQL transaction open throughout arbitrary application processing.

---

## 26. Consumer Iterator API

For long-running workers, an iterator-style interface should eventually be provided.

For example:

```python
consumer = mq.consumer(
    topic="payments",
    consumer_group="accounting",
)

for batch in consumer:
    with batch:
        for job in batch:
            process(job)
```

The consumer object can manage:

* Bucket ownership.
* Polling.
* Empty queues.
* Backoff.
* Reconnection.
* Batch sizing.
* Graceful shutdown.

The batch context continues to control acknowledgement.

---

## 27. Automatic Consumer Coordination

A later enterprise feature should automatically distribute buckets among consumers.

Applications would then use:

```python
consumer = mq.consumer(
    topic="payments",
    consumer_group="accounting",
)
```

without specifying buckets.

If four workers join, the SDK distributes the buckets.

If one worker disappears, its buckets are reassigned.

All coordination state must remain in CockroachDB.

No external coordinator may be required.

---

## 28. Consumer Leases

Automatic coordination should use leases stored in CockroachDB.

Conceptual state:

```text
topic
consumer_group
bucket
consumer_id
generation
lease_expiration
```

A consumer periodically renews its lease.

If the lease expires, another consumer may acquire the bucket.

---

## 29. Fencing

Lease ownership should include a monotonically increasing generation/fencing token.

Example:

```text
bucket 42

worker A -> generation 18
worker B -> generation 19
```

Once worker B acquires generation 19, worker A must no longer be able to commit an HWM using generation 18.

This protects against network partitions and paused/stale workers.

The fencing invariant should be enforced by CockroachDB rather than relying solely on SDK correctness.

---

## 30. Retry Behavior

The SDK should own CockroachDB transaction retry handling.

Applications should not need to understand CockroachDB retry errors for ordinary queue operations.

Operations such as:

```python
mq.enqueue(...)
mq.create_topic(...)
```

should retry safely where the operation is internally idempotent or transactionally restartable.

Consumer processing requires special handling because arbitrary application code cannot safely be automatically executed twice.

The SDK must never silently rerun user processing code unless the API contract explicitly allows it.

---

## 31. Dead-Letter Queues

A future feature should support dead-letter topics.

Example:

```python
consumer = mq.consumer(
    topic="payments",
    consumer_group="billing",
    max_attempts=5,
    dead_letter_topic="payments.dlq",
)
```

After repeated failures, the message can be routed to the DLQ.

DLQ metadata should include:

```text
original_topic
original_job_id
original_bucket
consumer_group
attempt_count
first_failure_at
last_failure_at
error
payload
```

Where possible, DLQ insertion and HWM advancement should occur atomically.

---

## 32. Retry Policies

Future consumer configuration may support:

```python
retry=ExponentialBackoff(
    initial=1,
    maximum=300,
    attempts=10,
)
```

Potential policies:

* Immediate retry.
* Fixed delay.
* Exponential backoff.
* Maximum attempts.
* DLQ after exhaustion.

Delayed delivery must be designed carefully because it can conflict with strict sequential bucket processing.

A later design should explicitly define whether a failed message blocks subsequent messages in its bucket.

---

## 33. Retention

Message cleanup is handled by CockroachDB Row-Level TTL.

Placeholder rows with:

```text
seq_id = 0
```

must never expire.

Eventually, retention should be configurable per topic.

Example:

```bash
fabmq topic create payments --retention 7d
fabmq topic create notifications --retention 24h
```

Topic configuration should be stored in CockroachDB.

---

## 34. Observability

The product should expose MQ-level metrics rather than requiring users to derive everything from raw SQL.

Useful metrics include:

* Producer messages/sec.
* Consumer messages/sec.
* Message count.
* Consumer lag.
* Time lag.
* Oldest unprocessed message.
* Producer latency.
* Processing latency.
* Retry rate.
* DLQ rate.
* Bucket skew.
* Bucket lock wait time.
* Consumer lease status.

---

## 35. Consumer Lag

For every:

```text
(topic, consumer_group, bucket)
```

the system can compare:

```text
producer tail
consumer HWM
```

to derive:

```text
lag = producer_tail - consumer_hwm
```

CLI example:

```bash
fabmq lag payments
```

Possible output:

```text
GROUP       BUCKET   HEAD    HWM     LAG
accounting  0        9123    9101    22
accounting  1        8821    7400    1421
analytics   0        9123    9123    0
```

Time-based lag should also be supported because message-count lag alone may be misleading.

---

## 36. Status CLI

Operational commands should include:

```bash
fabmq status
fabmq topic list
fabmq topic describe payments
fabmq group list payments
fabmq group describe payments accounting
fabmq lag payments
```

The CLI is a presentation layer over CockroachDB state.

It must not maintain its own persistent state.

---

## 37. Pause and Resume

A future operational feature should support:

```bash
fabmq topic pause payments
fabmq topic resume payments

fabmq group pause payments accounting
fabmq group resume payments accounting
```

Topic producer pause and consumer-group pause should have distinct semantics.

For example:

```text
topic producer pause
    -> reject new messages

consumer group pause
    -> continue accepting messages
    -> stop delivery to that consumer group
```

Pause state belongs in CockroachDB.

---

## 38. Graceful Consumer Drain

Consumers should eventually support:

```python
consumer.drain()
```

Drain means:

1. Stop acquiring new batches.
2. Finish the current batch.
3. Commit its HWM.
4. Release owned buckets.
5. Exit.

This allows fast reassignment during deployments and shutdowns.

---

## 39. Prefetch

The SDK may eventually prefetch messages while processing the current batch.

Conceptually:

```text
DB:    [fetch N]          [fetch N+1]          [fetch N+2]
APP:            [process N]          [process N+1]
```

Acknowledgement must remain ordered.

Prefetching must never allow the HWM to advance beyond a batch that has not successfully completed.

---

## 40. Adaptive Consumer Batching

The SDK may support:

```python
batch_size="auto"
```

It can dynamically adjust batch size using:

* Current lag.
* Processing latency.
* Database latency.
* Average payload size.
* Consumer throughput.

For example:

```text
1 -> 10 -> 50 -> 250 -> 1000
```

Large batches favor throughput.

Small batches favor latency.

This behavior belongs in the SDK.

---

## 41. Backpressure

Future topic configuration may define limits such as:

```text
maximum queue depth
maximum retained bytes
maximum consumer lag
maximum oldest-message age
```

Possible producer behavior when limits are reached:

```text
REJECT
THROTTLE
WARN
```

Backpressure may eventually be tied to specific critical consumer groups rather than every consumer group.

---

## 42. Geo-Awareness

Buckets can potentially be mapped to CockroachDB placement/locality configuration.

The SDK could expose locality-aware consumer assignment so workers preferentially consume buckets whose leaseholders are nearby.

This should be considered an optimization rather than part of the fundamental queue correctness model.

---

## 43. Statelessness Requirement

The Python application must not store durable queue state locally.

Do not persist:

* HWM state.
* Bucket ownership.
* Topic configuration.
* Retry state.
* Consumer membership.
* Schema version.
* DLQ state.

Any in-memory caching must be reconstructable from CockroachDB after restart.

The following should always be safe:

```text
kill SDK process
start new SDK process
connect to CockroachDB
resume operation
```

Local filesystem loss must not affect queue correctness.

---

## 44. Direct SQL Access

The SDK must not prevent advanced users from interacting with the database directly.

Examples:

```sql
SELECT * FROM list_topics();
```

```sql
SELECT enqueue_job(
    'payments',
    '{"id":123}'
);
```

Operators should also be able to inspect:

```sql
SELECT *
FROM mq
WHERE topic = 'payments'
ORDER BY bucket, seq_id;
```

The SQL schema should therefore remain understandable and documented.

However, operations that affect correctness should use the defined database API/protocol rather than arbitrary direct mutations.

In particular, direct message insertion that bypasses producer bucket locking must be considered unsupported.

---

## 45. Public API Philosophy

The SDK should expose intent rather than database mechanics.

Good:

```python
mq.enqueue(...)
mq.enqueue_batch(...)
mq.create_topic(...)
mq.consumer(...)
```

Avoid requiring applications to understand:

```text
seq_id
HWM
SELECT FOR UPDATE
bucket placeholder rows
lease generations
CockroachDB retry protocol
```

Those are implementation details.

---

## 46. Error Model

Define package-specific exceptions.

For example:

```python
class MQError(Exception):
    pass


class TopicNotFound(MQError):
    pass


class TopicAlreadyExists(MQError):
    pass


class ConsumerOwnershipLost(MQError):
    pass


class QueuePaused(MQError):
    pass


class InvalidPayload(MQError):
    pass
```

Do not leak raw database exceptions for expected product-level conditions.

Unexpected CockroachDB errors may retain their original exception as the Python exception cause.

---

## 47. Connection Management

The SDK should support:

```python
MQ(url)
```

where the SDK owns a connection pool.

It should also support:

```python
MQ(connection=existing_connection)
```

or operation-level connection injection:

```python
mq.enqueue(
    topic,
    payload,
    connection=conn,
)
```

Connection ownership must be explicit.

The SDK must never close a connection supplied by the application.

---

## 48. CLI Connection Configuration

The CLI should accept connection information through standard mechanisms.

For example:

```bash
fabmq --url "$DATABASE_URL" topic list
```

and/or:

```bash
export FABMQ_URL="postgresql://..."
fabmq topic list
```

Connection configuration is runtime configuration, not durable MQ state.

---

## 49. Data Models

Suggested Python models:

```python
@dataclass(frozen=True)
class Job:
    id: int
    payload: Any
```

Potential internal representation:

```python
@dataclass(frozen=True)
class _QueueRecord:
    job_id: int
    topic: str
    bucket: int
    seq_id: int
    payload: Any
    created_at: datetime
```

Public APIs should expose only fields applications actually need.

Operational/admin APIs may expose richer metadata.

---

## 50. Threading and Async Support

Initial implementation should favor correctness and a simple synchronous API.

For example:

```python
with mq.consume(...) as jobs:
    ...
```

Async support can later mirror the same semantics:

```python
async with mq.consume_async(...) as jobs:
    ...
```

Avoid designing separate conceptual APIs for sync and async operation.

---

## 51. Performance Principles

The implementation should optimize around the actual queue architecture:

### Producer

Minimize time holding the bucket sentinel lock.

Do not perform unrelated application work after acquiring it.

Prefer batch insertion where possible.

### Consumer

Use ordered primary-key access:

```text
(topic, bucket, seq_id)
```

Avoid scans across unrelated buckets.

Cache HWM in memory during normal operation when safe, but treat CockroachDB as authoritative after restart.

### Administration

Administrative queries should not interfere significantly with producer/consumer paths.

---

## 52. Core Correctness Invariants

Codex should treat the following as non-negotiable invariants.

### Invariant 1 — Ordered bucket append

For a given:

```text
(topic, bucket)
```

committed message sequence IDs increase monotonically.

### Invariant 2 — Single producer allocation authority

All producers must acquire the bucket placeholder lock before deriving the next sequence ID.

### Invariant 3 — Atomic allocation

Sequence allocation and message insertion belong to the same transaction.

### Invariant 4 — Placeholder permanence

`seq_id = 0` rows must never be removed by TTL or normal queue operations.

### Invariant 5 — Consumer progress

HWM advances only after successful processing according to the consumer acknowledgement contract.

### Invariant 6 — Exclusive consumer ownership

At most one active consumer may process:

```text
(topic, consumer_group, bucket)
```

when ownership management is enabled.

### Invariant 7 — Stateless clients

Loss of a Python process must not result in loss of durable MQ state.

### Invariant 8 — Transactional enqueue

When the caller supplies an existing transaction, enqueue must participate in that transaction rather than creating or committing an independent transaction.

---

## 53. Testing Strategy

Tests should include both unit and integration tests.

CockroachDB integration tests are essential because locking, transactions, retries, TTL, and concurrency cannot be adequately validated with mocks.

Critical tests include:

* Create topic.
* Duplicate topic creation.
* Delete topic.
* List topics.
* Single enqueue.
* Concurrent enqueue into different buckets.
* Heavy concurrent enqueue into the same bucket.
* Producer rollback while holding the bucket lock.
* Verify no duplicate sequence IDs.
* Verify committed sequence ordering.
* Batch enqueue.
* Batch rollback.
* Placeholder survival.
* TTL expiration of normal messages.
* Consumer initial HWM.
* Consumer successful acknowledgement.
* Consumer exception without acknowledgement.
* Multiple consumer groups.
* Consumer restart from HWM.
* Consumer ownership conflict.
* Lease expiration and takeover.
* Fencing stale consumers.
* Transactional enqueue with application data.
* Transaction rollback containing enqueue.
* CockroachDB transaction retry behavior.

Concurrency tests should intentionally create races rather than merely test sequential execution.

---

## 54. Benchmarking

Create a benchmark suite early.

Producer benchmarks:

```text
single producer
multiple producers
same bucket
uniform buckets
skewed buckets
single-message enqueue
batch sizes 10 / 50 / 100 / 500 / 1000
```

Consumer benchmarks:

```text
batch size
consumer count
bucket count
consumer lag
payload size
```

Measure:

```text
messages/sec
p50 latency
p95 latency
p99 latency
bucket lock wait time
transaction retries
CPU
storage growth
TTL overhead
```

The batch producer optimization should be benchmarked against individual enqueue operations.

---

## 55. Initial CLI Scope

Version 1 should stay small.

Suggested commands:

```bash
fabmq init
fabmq upgrade

fabmq topic create <topic>
fabmq topic delete <topic>
fabmq topic list
fabmq topic describe <topic>

fabmq produce <topic> <payload>

fabmq status
fabmq lag <topic>
```

Do not build a large CLI before the SDK semantics are stable.

The CLI should mostly call the same Python API exposed to applications.

There should not be separate CLI-specific implementations of queue behavior.

---

## 56. Initial SDK Scope

Version 1 should prioritize:

```python
MQ(...)
```

```python
mq.init()
```

```python
mq.create_topic(...)
mq.delete_topic(...)
mq.list_topics()
```

```python
mq.enqueue(...)
```

```python
mq.enqueue_batch(...)
```

```python
with mq.consume(...) as jobs:
    ...
```

```python
mq.get_topic_stats(...)
mq.get_consumer_lag(...)
```

Everything else can evolve from this foundation.

---

## 57. Suggested Implementation Phases

### Phase 1 — Database Bootstrap

Implement:

* Python package skeleton.
* Connection management.
* Schema installation.
* Schema versioning.
* Topic functions.
* `mq`.
* `hwm`.
* TTL configuration.
* Existing enqueue UDF.

Success criterion:

```bash
fabmq init
fabmq topic create test
fabmq topic list
```

works against a fresh CockroachDB database.

### Phase 2 — Producer

Implement:

* `mq.enqueue()`.
* Payload serialization.
* Existing SQL enqueue function integration.
* Error mapping.
* Transaction injection.

Validate same-bucket concurrency heavily.

### Phase 3 — Producer Batching

Implement:

* `enqueue_batch()`.
* Single-bucket batch routing.
* `WITH ORDINALITY` sequence assignment.
* Batch transaction semantics.

Benchmark against individual enqueue.

### Phase 4 — Basic Consumer

Implement:

```python
with mq.consume(...) as jobs:
    ...
```

with explicit bucket assignment.

Implement:

* HWM initialization.
* Message retrieval.
* HWM insertion.
* Context success/failure semantics.

### Phase 5 — Operational CLI

Implement:

* Status.
* Topic describe.
* Consumer-group visibility.
* Lag reporting.
* Queue depth.
* Bucket statistics.

### Phase 6 — Consumer Coordination

Implement:

* Consumer registration.
* Leases.
* Heartbeats.
* Bucket assignment.
* Fencing.
* Rebalancing.
* Graceful drain.

### Phase 7 — Enterprise Processing Features

Implement as justified by demand:

* Retry policies.
* DLQ.
* Pause/resume.
* Backpressure.
* Prefetch.
* Adaptive batching.
* Automatic producer micro-batching.

---

## 58. Design Principle for Future Features

When deciding whether functionality belongs in CockroachDB or Python, use this rule:

> **If incorrect client behavior could violate queue correctness, enforce the invariant in CockroachDB. If the feature primarily improves ergonomics, coordination strategy, observability, or performance, implement it in the SDK.**

Examples:

```text
CockroachDB                         SDK
--------------------------------   --------------------------------
message durability                 context manager
producer ordering                  producer batching
bucket locking                     micro-batching
HWM durability                     retry/backoff policy
lease/fencing state                heartbeat loop
TTL                                bucket assignment algorithm
transactional enqueue              graceful drain
                                  prefetch
                                  adaptive batch sizing
                                  metrics export
```

This separation should remain a core architectural principle.

---

## 59. Product Definition

The resulting product should be describable as:

> **A stateless Python SDK and CLI that provides a distributed message queue backed entirely by CockroachDB, with ordered partitioned delivery, consumer groups, transactional enqueue, batching, durable consumer progress, and SQL-native observability without requiring a separate message-broker service.**

The database provides durability, transactional correctness, ordering, retention, and distributed coordination.

The SDK provides the developer experience and operational behavior.

Neither the SDK nor CLI owns durable state.

---

## 60. Codex Implementation Guidance

When implementing this specification:

1. Keep the first version small.
2. Do not introduce a daemon or HTTP service.
3. Do not create local persistent state.
4. Keep SQL as a first-class supported interface.
5. Reuse the same implementation for the SDK and CLI.
6. Prefer CockroachDB transactions for correctness rather than application-side synchronization.
7. Do not automatically retry arbitrary user processing code.
8. Make transaction ownership explicit.
9. Write concurrency tests before optimizing producer behavior.
10. Benchmark single-message enqueue before and after batch enqueue.
11. Preserve the `(topic, bucket, seq_id=0)` placeholder locking protocol.
12. Treat all correctness invariants in this document as requirements, not suggestions.

A useful next step for Codex is to implement **Phases 1-3 first**, including concurrency tests and the batch-enqueue benchmark, before introducing the consumer coordination layer.
