from __future__ import annotations

import os
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


def connect(url: str | None = None) -> psycopg.Connection[Any]:
    return psycopg.connect(resolve_url(url), autocommit=True)
