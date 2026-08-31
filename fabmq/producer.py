from __future__ import annotations

import json
from typing import Any

import psycopg


def serialize_payload(payload: Any) -> str:
    if isinstance(payload, bytes):
        return payload.decode()

    if isinstance(payload, str):
        return payload

    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


class Producer:
    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    def enqueue(self, topic: str, payload: Any) -> int:
        serialized = serialize_payload(payload)
        return self._enqueue(self.connection, topic, serialized)

    def enqueue_batch(self, topic: str, payloads: list[Any]) -> list[int]:
        serialized = [serialize_payload(payload) for payload in payloads]
        return self._enqueue_batch(self.connection, topic, serialized)

    def _enqueue(
        self,
        conn: psycopg.Connection[Any],
        topic: str,
        payload: str,
    ) -> int:
        with conn.cursor() as cur:
            cur.execute("SELECT enqueue_job(%s, %s)", (topic, payload))
            row = cur.fetchone()
        return int(row[0])

    def _enqueue_batch(
        self,
        conn: psycopg.Connection[Any],
        topic: str,
        payloads: list[str],
    ) -> list[int]:
        with conn.cursor() as cur:
            cur.execute("SELECT enqueue_jobs(%s, %s)", (topic, payloads))
            row = cur.fetchone()

        return [int(row[0])]
