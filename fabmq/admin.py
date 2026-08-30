from __future__ import annotations

from typing import Any

import psycopg

from fabmq.connection import connection_scope


class Admin:
    def __init__(
        self,
        url: str | None = None,
        connection: psycopg.Connection[Any] | None = None,
    ) -> None:
        self.url = url
        self.connection = connection

    def create_topic(self, name: str) -> str:
        with connection_scope(self.url, self.connection) as (conn, should_commit):
            with conn.cursor() as cur:
                cur.execute("SELECT create_topic(%s)", (name,))
                row = cur.fetchone()
            if should_commit:
                conn.commit()
        return str(row[0])

    def delete_topic(self, name: str) -> str:
        with connection_scope(self.url, self.connection) as (conn, should_commit):
            with conn.cursor() as cur:
                cur.execute("SELECT delete_topic(%s)", (name,))
                row = cur.fetchone()
            if should_commit:
                conn.commit()
        return str(row[0])

    def list_topics(self) -> list[str]:
        with connection_scope(self.url, self.connection) as (conn, _):
            with conn.cursor() as cur:
                cur.execute("SELECT topic FROM list_topics()")
                rows = cur.fetchall()
        return [str(row[0]) for row in rows]
