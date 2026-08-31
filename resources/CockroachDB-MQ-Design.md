# CockroachDB-Backed Message Queue Design

This design aims to reduce MVCC bloat by using an append-only model: rows are inserted, but never updated.

Expired rows are removed through CockroachDB Row-Level TTL.

The overall architecture is inspired by Apache Kafka.

## Core Tables

The implementation uses two tables:

* `mq`, which stores messages
* `hwm`, which stores consumer high-water marks

These tables work together to provide ordered message delivery and consumer progress tracking.

Both tables are partitioned by `bucket`, an integer between `0` and `255`, inclusive. Each bucket is assigned to its own range.

Partitions can optionally be configured with zone constraints so that groups of buckets are colocated in specific regions. For example, approximately 80 buckets could be placed in one region, 80 in another, and the remaining buckets in a third region.

## Topics

Topics must be created before jobs can be inserted.

The following functions are available for topic management:

```sql
create_topic(topic_name)

delete_topic(topic_name)

list_topics()
```

Creating a topic initializes the placeholder rows required for bucket-level locking and sequence allocation.

## Producers

Kafka can efficiently append records to the end of a log file. CockroachDB, however, stores rows in sorted indexes, so there is no direct equivalent of appending to the end of a table.

To emulate append behavior, the producer:

1. Finds the highest sequence number for the selected bucket.
2. Adds `1` to that value.
3. Inserts the new row using the resulting sequence number.

To prevent concurrent producers from selecting the same sequence number, each producer must first acquire an exclusive lock for the target bucket. This serializes inserts at the bucket level.

Producer clients do not need to implement this logic directly. They enqueue messages through the following functions:

```sql
SELECT enqueue_job('topic_name', 'payload');
SELECT enqueue_jobs('topic_name', ARRAY['p1', 'p2']); 
```

The function hashes the payload using CRC32 and calculates the bucket as:

```text
crc32(payload) modulo 256
```

This assignment is deterministic.

A different implementation could hash another stable field, such as `account_id`, when strict FIFO ordering is required for that field.

## Consumer Processing

Consumers can be assigned to one or more buckets.

When a consumer starts, it retrieves the latest `last_seq_id` from the `hwm` table. This lookup is performed once, and the consumer keeps the value in memory.

The consumer then enters the following loop:

1. Begin a transaction.
2. Fetch the next job, or batch of jobs, after `last_seq_id`.
3. Process each job.
4. Insert a new row into `hwm` containing the sequence number of the last successfully processed job.
5. Commit the transaction.
6. Update the in-memory `last_seq_id`.

The message lookup uses the ordered key for the topic and bucket. It does not require scanning unrelated MVCC history.

The design does not update queue or high-water-mark rows and does not use `SELECT FOR UPDATE SKIP LOCKED`. Consumer progress is represented through inserts into `hwm`.

If processing fails before the transaction commits, the high-water mark does not advance. When the consumer restarts, it resumes from the last successfully committed position.

With a batch size of one, this provides exactly-once processing when the message’s effects and the high-water-mark insert are committed atomically within the same CockroachDB transaction. External, non-transactional side effects still require idempotency or deduplication.

## Consumer Groups

Consumers must provide a `consumer_group` name.

This allows multiple consumer groups to read from the same topic independently and progress at their own pace.

The `hwm` table tracks the latest `last_seq_id` for each combination of:

* bucket
* topic
* consumer group

Each consumer group therefore maintains its own position within every bucket of a topic.

## Setup

Create the objects listed in `queue.ddl`.

Adjust partitions and zone configurations as needed.

## Example flow

### Create topic

A topic must be created first.

```sql
SELECT create_topic('payments');
```

### Enqueuing a job

Users should not insert into the tables directly.
Instead, they should use the provided `enqueue_job()` function.

```sql
SELECT enqueue_job('{"task": "process_payout", "account_id": "account_12345"}', 'payments') AS job_id;
```

Result:

```text
        job_id
-----------------------
  3963167672086036487
```

I simulate a producer using dbworkload, see [`producer.py`](tools/producer.py).

```bash
$ dbworkload run \
  -w producer.py \
  --uri 'postgres://fabio:fabio@localhost:26257/mq?sslmode=require' \
  -c 4 --args '{"payload_size": 10, "topic": "b"}'
```

```sql
select * from mq limit 10;
--   bucket | seq_id | job_id |             payload              |          created_at
-- ---------+--------+--------+----------------------------------+--------------------------------
--        0 |      1 |      1 | SIK3A9Kw0prsCveRDLG5IA0dHh1Nnfm8 | 2026-08-03 15:06:32.753184+00
--        0 |      2 |      2 | R4muGlFgMIiRqvTK7aBHmQBicTxpAWgw | 2026-08-03 15:06:36.999736+00
--        0 |      3 |      3 | 2CAlWCL86Zid3ryJrXfrWadTWXIQvNvq | 2026-08-03 15:06:40.625497+00
--        0 |      4 |      4 | XiLuJrgcLtfHlzmovKxrl0pmnYPJdaaT | 2026-08-03 15:06:41.492759+00
--        0 |      5 |      5 | IYdVsJp9kKIOnpcrCcmGw77rkEx0fktV | 2026-08-03 15:06:46.955281+00
--        0 |      6 |      6 | NyuYngPLh6kQwonIHPDyrfnbBQAc2EkV | 2026-08-03 15:06:53.960731+00
--        0 |      7 |      7 | QoXXWFRkXYcjfZO5OCEQhOayBqujaZwR | 2026-08-03 15:06:57.622538+00
--        0 |      8 |      8 | LZd2A6FEcojXuwzNDBBXZ80XdvIpjWN6 | 2026-08-03 15:07:00.364523+00
--        0 |      9 |      9 | fa72VIl6a2SDkaOXkn6Cauyt9JJvi6JL | 2026-08-03 15:07:01.844448+00
--        0 |     10 |     10 | mbkhzYtbefWJKLO29BBkbXoFchR8GEdZ | 2026-08-03 15:07:03.457065+00
-- (10 rows)
```

### Polling a job

A consumer is identified by a **consumer group**.

The `hwm` table stores one independent position for every combination of:

* bucket
* topic
* consumer group

Each consumer group therefore consumes a topic independently of every other consumer group.

Within a consumer group, bucket ownership is exclusive:
at any given time, exactly one consumer instance must own a particular `(bucket, topic, consumer_group)` combination.

Two workers must never process the same bucket for the same consumer group simultaneously.

For example:

```text
Topic: payments

Consumer Group: accounting
    Worker A -> buckets 0-63
    Worker B -> buckets 64-127
    Worker C -> buckets 128-191
    Worker D -> buckets 192-255

Consumer Group: analytics
    Worker E -> buckets 0-255
```

The `accounting` and `analytics` consumer groups each maintain their own high-water marks and therefore consume the same topic independently.

However, within the accounting group, bucket 42 must be owned by exactly one worker.

This ownership guarantees:

* FIFO processing within a bucket.
* A single writer to the high-water mark for each `(bucket, topic, consumer_group)`.
* No duplicate processing caused by concurrent consumers advancing the same offset.

Sample client implementation in file [`consumer.py`](tools/consumer.py).
