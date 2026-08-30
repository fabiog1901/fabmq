from __future__ import annotations

from typing import Any

import psycopg

from fabmq.admin import Admin
from fabmq.consumer import Consumer
from fabmq.producer import Producer
from fabmq.topic import TopicManager


class MQ:
    def __init__(
        self,
        url: str | None = None,
        connection: psycopg.Connection[Any] | None = None,
    ) -> None:
        self.url = url
        self.connection = connection
        self.admin = Admin(url=url, connection=connection)
        self.topics = TopicManager(self.admin)
        self.producer = Producer(url=url, connection=connection)
        self.consumer_client = Consumer(url=url, connection=connection)

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
