from __future__ import annotations

from typing import Any

import psycopg

from fabmq.exceptions import SchemaError
from fabmq.migrations import (
    OWNED_TABLES,
    REMOVE_SCHEMA_SQL,
    SCHEMA_VERSION,
    load_initial_schema_sql,
)


class Admin:
    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    def init_schema(self, drop: bool = False) -> str:
        return self._init_schema(self.connection, drop)

    def remove_schema(self) -> None:
        self._remove_schema(self.connection)

    def status(self) -> dict[str, Any]:
        return self._status(self.connection)

    def create_topic(self, name: str) -> str:
        return self._create_topic(self.connection, name)

    def delete_topic(self, name: str) -> str:
        return self._delete_topic(self.connection, name)

    def list_topics(self) -> list[str]:
        return self._list_topics(self.connection)

    def _init_schema(self, conn: psycopg.Connection[Any], drop: bool) -> str:
        with conn.cursor() as cur:
            if drop:
                cur.execute(REMOVE_SCHEMA_SQL)
            else:
                if not self._should_apply_init(cur):
                    return SCHEMA_VERSION

            cur.execute(load_initial_schema_sql())

        return SCHEMA_VERSION

    def _remove_schema(self, conn: psycopg.Connection[Any]) -> None:
        with conn.cursor() as cur:
            cur.execute(REMOVE_SCHEMA_SQL)

    def _status(self, conn: psycopg.Connection[Any]) -> dict[str, Any]:
        with conn.cursor() as cur:
            existing_tables = self._existing_tables(cur)
            existing_functions = self._existing_functions(cur)
            required_tables = set(OWNED_TABLES)
            required_functions = {
                "create_topic",
                "delete_topic",
                "enqueue_job",
                "enqueue_jobs",
                "list_topics",
            }
            missing_tables = sorted(required_tables.difference(existing_tables))
            missing_functions = sorted(
                required_functions.difference(existing_functions)
            )
            initialized = not missing_tables and not missing_functions

            metadata = {}
            if "fabmq_meta" in existing_tables:
                cur.execute("SELECT key, value FROM fabmq_meta")
                metadata = {str(key): str(value) for key, value in cur.fetchall()}

            topic_count = None
            if initialized:
                cur.execute(
                    """
                    SELECT count(DISTINCT topic)
                    FROM mq
                    WHERE seq_id = 0
                    """
                )
                row = cur.fetchone()
                topic_count = int(row[0]) if row else 0

        return {
            "initialized": initialized,
            "partial": bool(existing_tables or existing_functions) and not initialized,
            "schema_version": metadata.get("schema_version"),
            "product_version": metadata.get("product_version"),
            "topic_count": topic_count if initialized else None,
            "missing_tables": missing_tables,
            "missing_functions": missing_functions,
        }

    def _create_topic(self, conn: psycopg.Connection[Any], name: str) -> str:
        with conn.cursor() as cur:
            cur.execute("SELECT create_topic(%s)", (name,))
            row = cur.fetchone()
        return str(row[0])

    def _delete_topic(self, conn: psycopg.Connection[Any], name: str) -> str:
        with conn.cursor() as cur:
            cur.execute("SELECT delete_topic(%s)", (name,))
            row = cur.fetchone()
        return str(row[0])

    def _list_topics(self, conn: psycopg.Connection[Any]) -> list[str]:
        with conn.cursor() as cur:
            cur.execute("SELECT topic FROM list_topics()")
            rows = cur.fetchall()
        return [str(row[0]) for row in rows]

    def _existing_tables(self, cur: psycopg.Cursor[Any]) -> set[str]:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name IN ('fabmq_meta', 'mq', 'hwm')
            """
        )
        return {str(row[0]) for row in cur.fetchall()}

    def _existing_functions(self, cur: psycopg.Cursor[Any]) -> set[str]:
        cur.execute(
            """
            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_schema = current_schema()
              AND routine_name IN (
                  'create_topic',
                  'delete_topic',
                  'enqueue_job',
                  'enqueue_jobs',
                  'list_topics'
              )
            """
        )
        return {str(row[0]) for row in cur.fetchall()}

    def _should_apply_init(self, cur: psycopg.Cursor[Any]) -> bool:
        existing_tables = self._existing_tables(cur)

        has_queue_tables = bool(existing_tables.intersection({"mq", "hwm"}))
        has_metadata = "fabmq_meta" in existing_tables

        if has_metadata:
            return False

        if has_queue_tables and not has_metadata:
            names = ", ".join(sorted(existing_tables.intersection(OWNED_TABLES)))
            raise SchemaError(
                "refusing to initialize over existing table(s) without "
                f"fabmq_meta: {names}. Use --drop --yes only if these are "
                "FabMQ objects you want to recreate."
            )

        return True
