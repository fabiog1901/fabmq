from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg

from fabmq.exceptions import ConfigurationError

DEFAULT_URL_ENV_VARS = ("FABMQ_DATABASE_URL", "DATABASE_URL")


def resolve_url(url: str | None) -> str:
    if url:
        return url

    for env_var in DEFAULT_URL_ENV_VARS:
        value = os.getenv(env_var)
        if value:
            return value

    env_names = " or ".join(DEFAULT_URL_ENV_VARS)
    raise ConfigurationError(f"database URL required; pass --url or set {env_names}")


@contextmanager
def managed_connection(
    url: str | None = None,
    connection: psycopg.Connection[Any] | None = None,
) -> Iterator[psycopg.Connection[Any]]:
    if connection is not None:
        yield connection
        return

    with psycopg.connect(resolve_url(url)) as conn:
        yield conn


@contextmanager
def connection_scope(
    url: str | None = None,
    connection: psycopg.Connection[Any] | None = None,
) -> Iterator[tuple[psycopg.Connection[Any], bool]]:
    if connection is not None:
        yield connection, False
        return

    with psycopg.connect(resolve_url(url)) as conn:
        yield conn, True
