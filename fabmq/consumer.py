from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from fabmq.models import Job


def parse_buckets(value: str | int | list[int] | None) -> list[int]:
    if value is None:
        buckets = list(range(256))
    elif isinstance(value, int):
        buckets = [value]
    elif isinstance(value, list):
        buckets = [int(bucket) for bucket in value]
    else:
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

    buckets = list(dict.fromkeys(buckets))
    if not buckets:
        raise ValueError("at least one bucket is required")

    invalid_buckets = [bucket for bucket in buckets if bucket < 0 or bucket > 255]
    if invalid_buckets:
        raise ValueError("buckets must be between 0 and 255")

    return buckets


def deserialize_payload(payload: str) -> Any:
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload


class Consumer:
    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    @contextmanager
    def consume(
        self,
        topic: str,
        consumer_group: str,
        bucket: int,
        batch_size: int = 1,
    ) -> Iterator[list[Job]]:
        with self._consume(
            self.connection,
            topic,
            consumer_group,
            bucket,
            batch_size,
        ) as jobs:
            yield jobs

    @contextmanager
    def _consume(
        self,
        conn: psycopg.Connection[Any],
        topic: str,
        consumer_group: str,
        bucket: int,
        batch_size: int,
    ) -> Iterator[list[Job]]:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT last_seq_id
                    FROM hwm
                    WHERE (bucket, topic, consumer_group) = (%s, %s, %s)
                    ORDER BY last_seq_id DESC
                    LIMIT 1
                    """,
                    (bucket, topic, consumer_group),
                )
                row = cur.fetchone()
                last_seq_id = int(row["last_seq_id"]) if row else 0

                cur.execute(
                    """
                    SELECT bucket, topic, seq_id, job_id, payload
                    FROM mq
                    WHERE (bucket, topic) = (%s, %s)
                      AND seq_id > %s
                    ORDER BY seq_id ASC
                    LIMIT %s
                    """,
                    (bucket, topic, last_seq_id, batch_size),
                )
                jobs = [
                    Job(
                        id=int(job["job_id"]),
                        topic=str(job["topic"]),
                        bucket=int(job["bucket"]),
                        seq_id=int(job["seq_id"]),
                        payload=deserialize_payload(str(job["payload"])),
                    )
                    for job in cur.fetchall()
                ]

                yield jobs

                if jobs:
                    cur.execute(
                        """
                        INSERT INTO hwm (
                            bucket,
                            topic,
                            consumer_group,
                            last_seq_id
                        )
                        VALUES (%s, %s, %s, %s)
                        """,
                        (bucket, topic, consumer_group, jobs[-1].seq_id),
                    )

    def consume_once(
        self,
        topic: str,
        consumer_group: str,
        buckets: list[int],
        batch_size: int = 1,
        limit: int | None = None,
    ) -> list[Job]:
        consumed: list[Job] = []

        for bucket in buckets:
            remaining = None if limit is None else limit - len(consumed)
            if remaining is not None and remaining <= 0:
                break

            size = batch_size if remaining is None else min(batch_size, remaining)
            with self.consume(topic, consumer_group, bucket, size) as jobs:
                consumed.extend(jobs)

        return consumed
