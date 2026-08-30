from __future__ import annotations

import json
from typing import Annotated, Any

import typer

from fabmq.client import MQ
from fabmq.consumer import parse_buckets
from fabmq.exceptions import FabMQError
from fabmq.producer import serialize_payload

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


@topic_app.command("create")
def create_topic(
    name: Annotated[str, typer.Argument(help="Topic name.")],
    url: UrlOption = None,
) -> None:
    try:
        topic = _mq(url).create_topic(name)
    except (FabMQError, ValueError, Exception) as error:
        _handle_error(error)

    typer.echo(topic)


@topic_app.command("delete")
def delete_topic(
    name: Annotated[str, typer.Argument(help="Topic name.")],
    url: UrlOption = None,
) -> None:
    try:
        topic = _mq(url).delete_topic(name)
    except (FabMQError, ValueError, Exception) as error:
        _handle_error(error)

    typer.echo(topic)


@topic_app.command("list")
def list_topics(url: UrlOption = None) -> None:
    try:
        topics = _mq(url).list_topics()
    except (FabMQError, ValueError, Exception) as error:
        _handle_error(error)

    for topic in topics:
        typer.echo(topic)


@app.command()
def produce(
    topic: Annotated[str, typer.Argument(help="Topic name.")],
    payload: Annotated[str, typer.Argument(help="Message payload.")],
    url: UrlOption = None,
    json_payload: Annotated[
        bool,
        typer.Option("--json", help="Parse payload as JSON before producing."),
    ] = False,
) -> None:
    try:
        message = json.loads(payload) if json_payload else payload
        job_id = _mq(url).enqueue(topic, message)
    except (FabMQError, ValueError, json.JSONDecodeError, Exception) as error:
        _handle_error(error)

    _echo_json({"job_id": job_id, "topic": topic})


@app.command()
def consume(
    topic: Annotated[str, typer.Argument(help="Topic name.")],
    consumer_group: Annotated[str, typer.Argument(help="Consumer group name.")],
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
) -> None:
    try:
        jobs = _mq(url).consume_once(
            topic=topic,
            consumer_group=consumer_group,
            buckets=parse_buckets(buckets),
            batch_size=batch_size,
            limit=limit,
        )
    except (FabMQError, ValueError, Exception) as error:
        _handle_error(error)

    for job in jobs:
        _echo_json(
            {
                "bucket": job.bucket,
                "job_id": job.id,
                "payload": job.payload,
                "seq_id": job.seq_id,
                "topic": job.topic,
            }
        )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
