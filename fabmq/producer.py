from __future__ import annotations

import json
from typing import Any

import psycopg

from fabmq.connection import connection_scope


def serialize_payload(payload: Any) -> str:
    if isinstance(payload, bytes):
        return payload.decode()

    if isinstance(payload, str):
        return payload

    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


class Producer:
    def __init__(
        self,
        url: str | None = None,
        connection: psycopg.Connection[Any] | None = None,
    ) -> None:
        self.url = url
        self.connection = connection

    def enqueue(self, topic: str, payload: Any) -> int:
        serialized = serialize_payload(payload)

        with connection_scope(self.url, self.connection) as (conn, should_commit):
            with conn.cursor() as cur:
                cur.execute("SELECT enqueue_job(%s, %s)", (topic, serialized))
                row = cur.fetchone()
            if should_commit:
                conn.commit()

        return int(row[0])

    def enqueue_batch(self, topic: str, payloads: list[Any]) -> list[int]:
        serialized = [serialize_payload(payload) for payload in payloads]

        with connection_scope(self.url, self.connection) as (conn, should_commit):
            with conn.cursor() as cur:
                cur.execute("SELECT enqueue_jobs(%s, %s)", (topic, serialized))
                row = cur.fetchone()
            if should_commit:
                conn.commit()

        return [int(row[0])]
