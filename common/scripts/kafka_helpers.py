import json
import os
import time
from threading import Event
from typing import Optional

from confluent_kafka import KafkaError, Producer

producer = Producer(
    {
        "bootstrap.servers": os.environ["KAFKA_BROKERS"],
        "enable.idempotence": True,
    }
)


def to_header(key: str, value: str) -> tuple[str, bytes]:
    """
    Converts a key/value pair of type str into a key/value pair of
    type str and bytes for writing to Kafka as a header.

    Returns: tuple[str, bytes] - A tuple representing the key as a str
    and the value as bytes.
    """
    return (key, value.encode("utf-8") if value is not None else b"")


def generate_dlq_headers(
    msg_key: str,
    error_type: str,
    error_message: str,
    issues: Optional[list[dict[str, str | int]]] = [],
) -> list[tuple[str, bytes]]:
    headers = [
        to_header("message.key", msg_key),
        to_header("error.type", error_type),
        to_header("error.message", error_message),
    ]

    if issues:
        encoded_issues = json.dumps(issues).encode("utf-8")
        headers.append(("data.quality.issues", encoded_issues))

    return headers


def send_to_topic(
    message: bytes,
    topic: str,
    headers: Optional[list[tuple[str, str]]] = [],
    timeout: float = 10.0,
) -> None:
    done = Event()
    result: dict[str, Optional[KafkaError]] = {"error": None}

    def on_delivery(err, _msg):
        result["error"] = err
        done.set()

    producer.produce(
        topic=topic,
        value=message,
        headers=headers,
        callback=on_delivery,
    )

    # poll until delivered or timed out
    start = time.monotonic()
    while not done.is_set():
        elapsed = time.monotonic() - start
        if elapsed >= timeout:
            # let the producer process any final callbacks quickly
            producer.poll(0)
            raise TimeoutError(f"Kafka delivery timed out after {timeout:.2f}s (topic='{topic}').")
        producer.poll(0.05)

    if result["error"] is not None:
        raise result["error"]
