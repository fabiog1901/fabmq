from __future__ import annotations

from fabmq.admin import Admin


class TopicManager:
    def __init__(self, admin: Admin) -> None:
        self.admin = admin

    def create(self, name: str) -> str:
        return self.admin.create_topic(name)

    def delete(self, name: str) -> str:
        return self.admin.delete_topic(name)

    def list(self) -> list[str]:
        return self.admin.list_topics()
