from pathlib import Path

SCHEMA_VERSION = "1"
PRODUCT_VERSION = "0.1.0"

OWNED_TABLES = ("fabmq_meta", "mq", "hwm")
INITIAL_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "001_initial.sql"

REMOVE_SCHEMA_SQL = """
DROP FUNCTION IF EXISTS enqueue_job(STRING, STRING);
DROP FUNCTION IF EXISTS enqueue_jobs(STRING, STRING[]);
DROP FUNCTION IF EXISTS list_topics();
DROP FUNCTION IF EXISTS delete_topic(STRING);
DROP FUNCTION IF EXISTS create_topic(STRING);
DROP TABLE IF EXISTS hwm;
DROP TABLE IF EXISTS mq;
DROP TABLE IF EXISTS fabmq_meta;
"""


def load_initial_schema_sql() -> str:
    return INITIAL_SCHEMA_PATH.read_text(encoding="utf-8")
