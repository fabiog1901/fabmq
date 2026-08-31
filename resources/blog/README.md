> Disclaimer: While this text was prepared with AI assistance, the content has
> been vetted and approved by the author.

# A CockroachDB-Backed Message Queue Between `SKIP LOCKED` and Kafka

## Synopsys

This post explores a CockroachDB-backed message queue design for teams that
need more structure and throughput than a simple database queue, but do not yet
need a dedicated streaming system like Kafka.

## Problem Statement

Applications often need a **queue** before they need a full streaming platform.

A user signs up, a payment clears, an order changes state, a report needs to be
generated, a webhook needs to be delivered. The application wants to record the
business fact durably, then _hand some work to another process_.

CockroachDB is a distributed SQL database designed for resilient, horizontally
scalable transactional workloads. It gives application developers a familiar SQL
interface while keeping data consistent across a distributed system.

Message queue systems, on the other hand, are built to decouple producers and consumers:
one part of the system publishes work or events, and another part processes them
later.

Both are useful. The hard part is deciding where the boundary should be.

## The Common Queue Decision

Many teams start with a queue table, often called **outbox** or **events** table, in their application database.
A typical pattern is below, known in the industry as **SFUSL**, as in `SELECT FOR UPDATE SKIP LOCKED`:

```sql
BEGIN;

  SELECT id, payload
  FROM outbox
  WHERE status = 'pending'
  ORDER BY created_at
  LIMIT 1
  FOR UPDATE SKIP LOCKED;

  -- do something with that row/job...

  UPDATE outbox
  SET status = 'done'
  WHERE id = $1;

COMMIT;
```

This style is attractive because it keeps the system small. The application data
and the queue live in the same database, so business changes and queue changes
can participate in the same transaction.

But this design has limits. A busy queue table can become a point of contention.
Rows are selected, locked, updated, and deleted. Workers may repeatedly scan for
available work. As throughput grows, the table starts behaving less like a
simple queue and more like an accidental broker.
Many papers and blog posts have explored how to turn a relational table into a queue, and they often arrive at the same conclusion: beyond a certain throughput, this pattern can run into scalability and efficiency limits.

At the other end of the spectrum are dedicated message systems such as Apache
Kafka. Kafka is excellent when you need a distributed event streaming platform:
durable topics, consumer groups, high throughput, replay, and broad ecosystem
support.

The tradeoff is that Kafka and the database are separate systems. Once the
system of record and the queue are split, atomicity gets harder.

For example:

```text
1. Update order status in the database.
2. Publish "order updated" to Kafka.
```

What happens if step 1 commits and step 2 fails? What happens if step 2 succeeds
but the application never observes the acknowledgement? These problems are
solvable, but they usually require extra machinery: an outbox table, CDC,
idempotent consumers, deduplication keys, retries, and operational monitoring
across multiple systems.

That may be the right architecture, _but it is not free_.

## The Middle Space

The CockroachDB-backed MQ [design](../CockroachDB-MQ-Design.md) presented in this blog explores a middle path.

It is meant for cases where a simple queue table with `SELECT FOR UPDATE SKIP
LOCKED` is starting to look too small, but a dedicated MQ or streaming system
still feels too heavy.

The design is inspired by Kafka's partitioned log model, but implemented with
CockroachDB tables:

- `mq` stores messages.
- `hwm` stores consumer high-water marks.
- Messages are divided into 256 buckets.
- Ordering is guaranteed within a bucket.
- Consumer groups maintain independent progress.
- Old rows are cleaned up with CockroachDB Row-Level TTL.

The queue avoids the usual "pending jobs" table pattern. Workers do not scan for
available rows and then lock them with `SKIP LOCKED`. Instead, each topic is
split into ordered bucket streams. Producers hash a payload to a bucket, lock
that bucket's sentinel row, find the next sequence number, and insert the
message at that position.

Conceptually, the producer path looks like this:

```text
Producer sends payload to enqueue
  |
  v
calculate the bucket using a function - hash(payload) modulo 256
  |
  v
lock sentinel row for that bucket
  |
  v
get next seq_id for that bucket
  |
  v
insert message(s) at the end of that bucket
```

That means producer contention is scoped to a single `(topic, bucket)`, not the
entire queue. Different buckets can accept messages independently, and
CockroachDB can distribute those key ranges across the cluster.

Consumers work the same way. A consumer does not ask the database to find any
available job anywhere in the queue. It reads from a known `(topic, bucket)`
after the last committed high-water mark for its consumer group:

```text
Consumer
  |
  v
load last seq_id from hwm
  |
  v
BEGIN
  |
  v
read messages after seq_id
  |
  v
process each job
  |
  v
advance hwm
  |
  v
COMMIT
```

This turns queue access into ordered key lookups rather than broad scans. The
consumer already knows the topic, bucket, and last sequence number. It can ask
for the next rows in that ordered stream.

The design is also exclusively append-only. Producers insert message rows. Consumers
insert high-water-mark rows. Normal processing does not update message rows from
`pending` to `running` to `done`, so the system avoids a large amount of MVCC
churn that update-heavy queue tables can create. Old message and HWM rows are
eventually removed by Row-Level TTL.

The result is a database-backed queue that can scale along several dimensions:

- Producers spread naturally across buckets.
- Consumers can be assigned different buckets.
- Consumer groups progress independently.
- Reads are targeted by ordered keys instead of discovery scans.
- Contention is localized to bucket-level sequence allocation.
- Queue state remains transactionally available inside CockroachDB.

The core idea is:

> CockroachDB stores the durable truth; queue behavior is implemented through
> SQL and application-level consumers.

This keeps the queue design close to the database's strengths: ordered keys,
transactions, range distribution, and TTL cleanup.

## Atomicity Where It Matters

The important property is that message processing and high-water-mark movement
can be part of the same CockroachDB transaction.

For small batches, the consumer flow is:

```text
BEGIN
  read next messages for topic/bucket after last_seq_id
  process each message
  insert new hwm row
COMMIT
```

If the transaction commits cleanly, the high-water mark advances. If processing
fails and the transaction rolls back, the high-water mark does not advance.

The application still owns the business logic. That is intentional. A long
running worker can keep polling in application code:

```text
while running:
  read next batch for assigned bucket
  if rows were returned:
    process rows and advance hwm in one transaction
  else:
    sleep briefly
```

The application decides how to poll, back off, handle signals, retry failures,
and distribute buckets across workers.

## SQL Is the Interface

The SQL interface is not an implementation detail. It is a first-class
interface.

Create a topic directly in SQL:

```sql
SELECT create_topic('payments');
```

List topics:

```sql
SELECT list_topics();
```

Produce one message:

```sql
SELECT enqueue_job(
    'payments',
    '{"account_id":"123","amount":100}'
);
```

Produce a batch:

```sql
SELECT enqueue_jobs(
    'payments',
    ARRAY[
        '{"account_id":"123","amount":100}',
        '{"account_id":"456","amount":250}'
    ]
);
```

A consumer can also be written directly in SQL. The important part is that
processing and high-water-mark advancement belong to the same transaction. If
the transaction rolls back, the HWM row is not inserted, and the consumer will
read the same messages again on the next attempt.

```sql
-- Done once when the worker starts, or cached by the application.
SELECT COALESCE(max(last_seq_id), 0) AS last_seq_id
FROM hwm
WHERE bucket = 42
  AND topic = 'payments'
  AND consumer_group = 'accounting';

BEGIN;

    SELECT payload
    FROM mq
    WHERE bucket = 42
        AND topic = 'payments'
        AND seq_id > ?  -- this is the last_seq_id retrieved earlier
    ORDER BY seq_id ASC
    LIMIT 10;

    -- The application processes every row returned above.
    -- If processing fails, rollback instead of advancing hwm.

    INSERT INTO hwm (bucket, topic, consumer_group, last_seq_id)
    VALUES (
        42,
        'payments',
        'accounting',
        ? -- this is the highest/last seq_id from the earlier select
    );

COMMIT;
```

## Performance Notes

This blog will not provide full test replication steps, and instead it invites readers to test on their own.

Below is a snippet of the summary results taken from the tests conducted by Cockroach Labs Professional Services team.

CockroachDB v26.2.5 · paired 10 h runs on identically specified clusters.
Medians over the `[start+120m, end−10m]` window.

| metric (median) | outbox-256 | mq | mq advantage |
| --- | --- | --- | --- |
| p99 SQL service latency | 11.861 ms | 5.643 ms | 2.10x |
| CPU efficiency | 6.351 ms/stmt | 3.515 ms/stmt | 1.81x |
| IO efficiency | 2.481 IOPS/stmt | 1.565 IOPS/stmt | 1.59x |
| throughput | 3,999 stmt/s | 6,302 stmt/s | 1.58x |

![box plot](box-plot.png)

Below chart shows the SQL p99 service latency over the duration of the test.
The path clearly highlights the stability of the MQ design over SFUSL.

![p99](sql-p99.png)

## FabMQ as a Convenience Layer

FabMQ is a small CLI and Python SDK for managing and using this design. It can
initialize the schema, create topics, produce messages, and consume messages,
but it is not a broker. There is no daemon sitting between the application and
the database.

The SQL interface remains first-class. FabMQ is simply a convenience layer over
the same database objects and functions shown above.

The CLI can initialize the schema and interact with topics:

```bash
export DATABASE_URL="postgresql://user:password@localhost:26257/mq?sslmode=disable"

fabmq init
fabmq topic create payments
fabmq produce --topic payments '{"account_id":"123","amount":100}' --json
fabmq consume --topic payments --consumer-group accounting --batch-size 10
```

For interactive use, the producer can behave a bit like the Kafka console
producer:

```bash
fabmq produce --topic payments --live --no-job-id
> {"account_id":"123","amount":100}
> {"account_id":"456","amount":250}
```

And the consumer can continuously print payloads:

```bash
fabmq consume \
  --topic payments \
  --consumer-group accounting \
  --live \
  --payload-only
```

The SDK is deliberately thinner. It expects the application to provide a
`psycopg.Connection`, because production applications usually already manage
their own connections or pools.

```python
import psycopg

from fabmq import MQ

with psycopg.connect(
    "postgresql://user:password@localhost:26257/mq?sslmode=disable",
    autocommit=True,
) as conn:
    mq = MQ(conn)
    mq.create_topic("payments")
    mq.enqueue("payments", {"account_id": "123", "amount": 100})
```

For consumers, the SDK provides the acknowledgement-safe primitive. The
application owns the polling loop:

```python
import time

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
                process(job)

        if not jobs:
            time.sleep(1.0)
```

## Where This Fits

This design is not a substitute for Kafka.

Kafka remains the better fit when you need a full event streaming platform,
large-scale replay, long retention, stream processing, ecosystem integrations,
or independently operated messaging infrastructure.

The CockroachDB-backed MQ design fits a narrower space:

- You already use CockroachDB as the system of record.
- You want queue semantics without introducing a broker.
- You need stronger throughput than a simple locked queue table can comfortably
  provide.
- You care about keeping business state, enqueued work, and consumer progress
  inside the database transaction model.
- You are comfortable assigning buckets to workers explicitly.

In that space, a CockroachDB-backed MQ can be a useful middle layer: more
structured than a queue table, lighter than Kafka, and still deeply integrated
with the database that already holds the truth.

## References

- [CockroachDB product overview](https://www.cockroachlabs.com/product/overview/)
- [FOR UPDADE](https://docs.cockroachlabs.com/docs/stable/select-for-update)
- [CockroachDB Row-Level TTL explained](https://docs.cockroachlabs.com/docs/stable/row-level-ttl)
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)
- [CockroachDB-backed message queue design](https://github.com/fabiog1901/fabmq/blob/main/resources/CockroachDB-MQ-Design.md)
- [FabMQ](https://github.com/fabiog1901/fabmq)
