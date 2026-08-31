from __future__ import annotations

import json
import sys
import time
from typing import Annotated, Any

import psycopg
import typer

from fabmq.client import MQ
from fabmq.consumer import parse_buckets
from fabmq.exceptions import FabMQError

app = typer.Typer(no_args_is_help=True, help="CockroachDB-backed message queue.")
topic_app = typer.Typer(no_args_is_help=True, help="Manage topics.")
app.add_typer(topic_app, name="topic")


UrlOption = Annotated[
    str | None,
    typer.Option(
        "--url",
        "-u",
        envvar=["FABMQ_DATABASE_URL", "DATABASE_URL"],
        help="CockroachDB connection URL.",
    ),
]


def _mq(url: str | None) -> MQ:
    return MQ(url=url)


def _echo_json(value: Any) -> None:
    typer.echo(json.dumps(value, separators=(",", ":"), sort_keys=True))


def _handle_error(error: Exception) -> None:
    typer.secho(str(error), fg=typer.colors.RED, err=True)
    raise typer.Exit(1)


def _parse_payload(payload: str, json_payload: bool) -> Any:
    return json.loads(payload) if json_payload else payload


def _echo_payload(payload: Any) -> None:
    if isinstance(payload, str):
        typer.echo(payload)
    else:
        _echo_json(payload)


def _echo_job(job: Any, payload_only: bool) -> None:
    if payload_only:
        _echo_payload(job.payload)
        return

    _echo_json(
        {
            "bucket": job.bucket,
            "job_id": job.id,
            "payload": job.payload,
            "seq_id": job.seq_id,
            "topic": job.topic,
        }
    )


def _echo_produced(topic: str, job_id: int, show_job_id: bool) -> None:
    if show_job_id:
        _echo_json({"job_id": job_id, "topic": topic})


@app.command("init")
def init_schema(
    url: UrlOption = None,
    drop: Annotated[
        bool,
        typer.Option(
            "--drop",
            help="Drop existing FabMQ objects before initializing.",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip destructive confirmation prompts."),
    ] = False,
) -> None:
    if drop and not yes:
        typer.confirm(
            "Drop existing FabMQ tables, functions, and metadata first?",
            abort=True,
        )

    try:
        schema_version = _mq(url).init_schema(drop=drop)
    except (FabMQError, ValueError, psycopg.Error) as error:
        _handle_error(error)

    _echo_json({"schema_version": schema_version, "status": "initialized"})


@app.command("remove")
def remove_schema(
    url: UrlOption = None,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip destructive confirmation prompt."),
    ] = False,
) -> None:
    if not yes:
        typer.confirm(
            "Drop all FabMQ tables, functions, and metadata from this database?",
            abort=True,
        )

    try:
        _mq(url).remove_schema()
    except (FabMQError, ValueError, psycopg.Error) as error:
        _handle_error(error)

    _echo_json({"status": "removed"})


@app.command("status")
def status(url: UrlOption = None) -> None:
    try:
        schema_status = _mq(url).status()
    except (FabMQError, ValueError, psycopg.Error) as error:
        _handle_error(error)

    _echo_json(schema_status)


@topic_app.command("create")
def create_topic(
    name: Annotated[str, typer.Argument(help="Topic name.")],
    url: UrlOption = None,
) -> None:
    try:
        topic = _mq(url).create_topic(name)
    except (FabMQError, ValueError, psycopg.Error) as error:
        _handle_error(error)

    typer.echo(topic)


@topic_app.command("delete")
def delete_topic(
    name: Annotated[str, typer.Argument(help="Topic name.")],
    url: UrlOption = None,
) -> None:
    try:
        topic = _mq(url).delete_topic(name)
    except (FabMQError, ValueError, psycopg.Error) as error:
        _handle_error(error)

    typer.echo(topic)


@topic_app.command("list")
def list_topics(url: UrlOption = None) -> None:
    try:
        topics = _mq(url).list_topics()
    except (FabMQError, ValueError, psycopg.Error) as error:
        _handle_error(error)

    for topic in topics:
        typer.echo(topic)


@app.command()
def produce(
    topic: Annotated[
        str,
        typer.Option("--topic", "-t", help="Topic name."),
    ],
    payload: Annotated[str | None, typer.Argument(help="Message payload.")] = None,
    url: UrlOption = None,
    json_payload: Annotated[
        bool,
        typer.Option("--json", help="Parse payload as JSON before producing."),
    ] = False,
    live: Annotated[
        bool,
        typer.Option(
            "--live",
            help="Read one message per line from stdin until Ctrl+C.",
        ),
    ] = False,
    show_job_id: Annotated[
        bool,
        typer.Option(
            "--show-job-id/--no-job-id",
            help="Print producer acknowledgement with the generated job id.",
        ),
    ] = True,
) -> None:
    try:
        mq = _mq(url)

        if live:
            try:
                while True:
                    if sys.stdin.isatty():
                        typer.echo("> ", nl=False, err=True)

                    line = sys.stdin.readline()
                    if not line:
                        break

                    message = _parse_payload(line.rstrip("\n"), json_payload)
                    job_id = mq.enqueue(topic, message)
                    _echo_produced(topic, job_id, show_job_id)
            except KeyboardInterrupt:
                if sys.stdin.isatty():
                    typer.echo(err=True)
                raise typer.Exit(0) from None
            return

        if payload is None:
            raise ValueError("payload is required unless --live is enabled")

        message = _parse_payload(payload, json_payload)
        job_id = mq.enqueue(topic, message)
    except (FabMQError, ValueError, json.JSONDecodeError, psycopg.Error) as error:
        _handle_error(error)

    _echo_produced(topic, job_id, show_job_id)


@app.command()
def consume(
    topic: Annotated[
        str,
        typer.Option("--topic", "-t", help="Topic name."),
    ],
    consumer_group: Annotated[
        str,
        typer.Option("--consumer-group", "-c", help="Consumer group name."),
    ],
    url: UrlOption = None,
    buckets: Annotated[
        str,
        typer.Option(
            "--buckets",
            "-b",
            help="Buckets to poll, for example 0, 0-31, or 0-31,64.",
        ),
    ] = "0-255",
    batch_size: Annotated[
        int,
        typer.Option("--batch-size", "-n", min=1, help="Messages per bucket."),
    ] = 1,
    limit: Annotated[
        int | None,
        typer.Option("--limit", "-l", min=1, help="Maximum messages to consume."),
    ] = None,
    live: Annotated[
        bool,
        typer.Option("--live", help="Continuously print messages until Ctrl+C."),
    ] = False,
    payload_only: Annotated[
        bool,
        typer.Option("--payload-only", help="Print only message payloads."),
    ] = False,
    poll_interval: Annotated[
        float,
        typer.Option(
            "--poll-interval",
            min=0.0,
            help="Seconds to wait between empty live polls.",
        ),
    ] = 1.0,
) -> None:
    try:
        mq = _mq(url)
        parsed_buckets = parse_buckets(buckets)

        if live:
            try:
                while True:
                    found_jobs = False
                    for bucket in parsed_buckets:
                        with mq.consume(
                            topic=topic,
                            consumer_group=consumer_group,
                            bucket=bucket,
                            batch_size=batch_size,
                        ) as jobs:
                            for job in jobs:
                                found_jobs = True
                                _echo_job(job, payload_only)

                    if not found_jobs and poll_interval:
                        time.sleep(poll_interval)
            except KeyboardInterrupt:
                raise typer.Exit(0) from None
            return

        consumed = 0
        for bucket in parsed_buckets:
            remaining = None if limit is None else limit - consumed
            if remaining is not None and remaining <= 0:
                break

            size = batch_size if remaining is None else min(batch_size, remaining)
            with mq.consume(
                topic=topic,
                consumer_group=consumer_group,
                bucket=bucket,
                batch_size=size,
            ) as jobs:
                for job in jobs:
                    _echo_job(job, payload_only)
                    consumed += 1
    except (FabMQError, ValueError, psycopg.Error) as error:
        _handle_error(error)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
