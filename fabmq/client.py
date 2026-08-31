from __future__ import annotations

from typing import Any

import psycopg

from fabmq.admin import Admin
from fabmq.consumer import Consumer
from fabmq.producer import Producer
from fabmq.topic import TopicManager


class MQ:
    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection
        self.admin = Admin(connection)
        self.topics = TopicManager(self.admin)
        self.producer = Producer(connection)
        self.consumer_client = Consumer(connection)

    def init_schema(self, drop: bool = False) -> str:
        return self.admin.init_schema(drop=drop)

    def remove_schema(self) -> None:
        self.admin.remove_schema()

    def status(self) -> dict[str, Any]:
        return self.admin.status()

    def create_topic(self, name: str) -> str:
        return self.admin.create_topic(name)

    def delete_topic(self, name: str) -> str:
        return self.admin.delete_topic(name)

    def list_topics(self) -> list[str]:
        return self.admin.list_topics()

    def enqueue(self, topic: str, payload: Any) -> int:
        return self.producer.enqueue(topic, payload)

    def enqueue_batch(self, topic: str, payloads: list[Any]) -> list[int]:
        return self.producer.enqueue_batch(topic, payloads)

    def consume(self, *args: Any, **kwargs: Any) -> Any:
        return self.consumer_client.consume(*args, **kwargs)

    def consume_once(self, *args: Any, **kwargs: Any) -> list[Any]:
        return self.consumer_client.consume_once(*args, **kwargs)
