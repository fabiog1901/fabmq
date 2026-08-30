"""
dbworkload consumer class.

Example:

dbworkload run \
  -w consumer.py \
  --uri 'postgres://fabio:fabio@localhost:26257/mq?sslmode=require' \
  -c 8 \
  --args '{"topic": "payments", "consumer_group": "cg1", "buckets": "0-79"}'
"""

import time
from typing import Any

import psycopg
from psycopg.rows import dict_row


def parse_buckets(value: Any) -> list[int]:
    if isinstance(value, int):
        buckets = [value]
    elif isinstance(value, list):
        buckets = [int(bucket) for bucket in value]
    elif isinstance(value, str):
        buckets = []
        for part in value.split(","):
            part = part.strip()
            if not part:
                continue

            if "-" in part:
                start, end = part.split("-", 1)
                buckets.extend(range(int(start), int(end) + 1))
            else:
                buckets.append(int(part))
    else:
        raise ValueError("bucket or buckets must be an int, list, or range string")

    if not buckets:
        raise ValueError("at least one bucket is required")

    invalid_buckets = [bucket for bucket in buckets if bucket < 0 or bucket > 255]
    if invalid_buckets:
        raise ValueError("buckets must be between 0 and 255")

    return list(dict.fromkeys(buckets))


class Consumer:
    def __init__(self, args: dict):
        self.topic: str | None = args.get("topic")
        self.consumer_group: str | None = args.get("consumer_group")
        self.batch_size: int = int(args.get("batch_size", 1))
        self.think_time: float = float(args.get("think_time", 0)) / 1000
        self.process_time: float = float(args.get("process_time", 0)) / 1000

        bucket_arg = args.get("bucket", args.get("buckets"))
        self.buckets = parse_buckets(bucket_arg)

        if not self.topic:
            raise ValueError("topic is a mandatory field")

        if not self.consumer_group:
            raise ValueError("consumer_group is a mandatory field")

        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        self.worker_buckets: list[int] = []
        self.last_seq_ids: dict[int, int] = {}
        self.next_bucket_index: int = 0

    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: int):
        """Assign this dbworkload worker its buckets and load their HWMs."""

        # spread buckets evenly among all worker threads
        self.worker_buckets = self.buckets[id::total_thread_count]

        if not self.worker_buckets:
            raise ValueError(
                "consumer concurrency cannot exceed the selected bucket count "
                f"({total_thread_count=} bucket_count={len(self.buckets)})"
            )

        self.last_seq_ids = {
            bucket: self.load_last_seq_id(conn, bucket)
            for bucket in self.worker_buckets
        }

        print(
            f"id={id}, topic={self.topic}, consumer_group={self.consumer_group}, "
            f"buckets={self.worker_buckets}, last_seq_ids={self.last_seq_ids}"
        )

    def loop(self):
        return [self.txn_poll_bucket, self.__think__]

    def __think__(self, conn: psycopg.Connection):
        time.sleep(self.think_time)

    def load_last_seq_id(self, conn: psycopg.Connection, bucket: int) -> int:
        """Fetch one bucket's latest high-water mark."""
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT last_seq_id
                FROM hwm
                WHERE (bucket, topic, consumer_group) = (%s, %s, %s)
                ORDER BY last_seq_id DESC
                LIMIT 1
                """,
                (
                    bucket,
                    self.topic,
                    self.consumer_group,
                ),
            )
            row = cur.fetchone()

        if row and row["last_seq_id"] is not None:
            return row["last_seq_id"]

        return 0

    def process_job(self, job: dict) -> None:
        """Placeholder for the real job execution logic."""
        # print(f"Job -> {self.topic}, {job['bucket']}, {job['seq_id']}")

        if self.process_time:
            time.sleep(self.process_time)

    def txn_poll_bucket(self, conn: psycopg.Connection):
        """Poll one bucket, process a batch, and advance the high-water mark."""
        bucket = self.worker_buckets[self.next_bucket_index]
        self.next_bucket_index = (self.next_bucket_index + 1) % len(self.worker_buckets)

        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                jobs = cur.execute(
                    """
                    SELECT bucket, seq_id, payload
                    FROM mq
                    WHERE (bucket, topic) = (%s, %s)
                      AND seq_id > %s
                    ORDER BY seq_id ASC
                    LIMIT %s
                    """,
                    (
                        bucket,
                        self.topic,
                        self.last_seq_ids[bucket],
                        self.batch_size,
                    ),
                ).fetchall()

                if not jobs:
                    return

                for job in jobs:
                    self.process_job(job)

                self.last_seq_ids[bucket] = jobs[-1]["seq_id"]

                cur.execute(
                    """
                    INSERT INTO hwm (bucket, topic, consumer_group, last_seq_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        bucket,
                        self.topic,
                        self.consumer_group,
                        self.last_seq_ids[bucket],
                    ),
                )
