"""
dbworkload producer class.

Example:

dbworkload run \
  -w producer.py \
  --uri 'postgres://fabio:fabio@localhost:26257/mq?sslmode=require' \
  -c 4 \
  --args '{"topic": "payments", "payload_size": 32, "batch_size": 10}'
"""

import random
import time

import psycopg


class Producer:
    def __init__(self, args: dict):
        self.payload_size: int = int(args.get("payload_size", 32))
        self.batch_size: int = int(args.get("batch_size", 1))
        self.think_time: float = float(args.get("think_time", 0)) / 1000
        self.topic: str = args.get("topic", None)

        if self.topic is None:
            raise ValueError("topic is a mandatory field")

        if self.payload_size < 1:
            raise ValueError("payload_size must be at least 1")

        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        self.rng = random.Random(None)
        self.tbl = bytes.maketrans(
            bytearray(range(256)),
            bytearray(
                [ord(b"a") + b % 26 for b in range(113)]
                + [ord(b"0") + b % 10 for b in range(30)]
                + [ord(b"A") + b % 26 for b in range(113)]
            ),
        )

    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: int):
        with conn.cursor() as cur:
            print(
                f"My thread ID is {id}. The total thread count is {total_thread_count}."
            )
            
    def loop(self):
        if self.think_time:
            return [self.txn_enqueue_jobs, self.__think__]
        return [self.txn_enqueue_jobs]

    def random_str(self, size: int):
        return (
            self.rng.getrandbits(8 * size)
            .to_bytes(size, "big")
            .translate(self.tbl)
            .decode()
        )

    def __think__(self, conn: psycopg.Connection):
        time.sleep(self.think_time)

    def txn_enqueue_jobs(self, conn: psycopg.Connection):
        """Generate payload strings and enqueue them as one batch."""
        payloads = [
            self.random_str(
                self.rng.randint(
                    int(self.payload_size * 0.8),
                    int(self.payload_size * 1.2),
                )
            )
            for _ in range(self.batch_size)
        ]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT job_id, seq_id
                FROM enqueue_jobs(%s, %s);
                """,
                (
                    self.topic,
                    payloads,
                ),
            ).fetchall()
