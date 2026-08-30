from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Job:
    id: int
    topic: str
    bucket: int
    seq_id: int
    payload: Any
