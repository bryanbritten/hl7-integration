import logging
import os
import random
import threading
import time

from confluent_kafka import Producer
from dotenv import load_dotenv
from prometheus_client import start_http_server

from hl7_helpers import (
    MESSAGE_REGISTRY,
    manually_extract_msh_segment,
    parse_msh_segment,
)
from hl7_segment_generators import generate_segments
from metrics import messages_sent_total

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

WRITE_TOPIC = os.environ["PRODUCER_WRITE_TOPIC"]
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
MESSAGE_TYPES = [
    "ADT_A01",
    "ADT_A03",
]

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKERS,
        "acks": "1",
        "linger.ms": 10,
        "retries": 3,
        "compression.type": "lz4",
    }
)

# simple background poller to service delivery callbacks
_stop = threading.Event()


def _poller():
    while not _stop.is_set():
        producer.poll(0.1)
    producer.flush(5)


threading.Thread(target=_poller, daemon=True).start()


def shutdown_producer():
    _stop.set()


def build_message(message_type: str) -> bytes:
    """
    Builds a complete message with the required segments.

    Returns: bytes - The message encoded in UTF-8 if the message type is supported, else `None`.
    """

    schema = MESSAGE_REGISTRY.get(message_type)
    if schema is None:
        return b""

    segments = generate_segments(schema["segments"], message_type)
    return segments


def send_message(message: bytes, message_type: str) -> None:
    """
    Sends the HL7 message to the Kafka service.

    message: bytes - The HL7 message to send.
    """

    def on_delivery(err, msg):
        if msg and not err:
            messages_sent_total.labels(message_type=message_type).inc()

    while True:
        # because the message can have errors intentionally introduced,
        # parsing with the hl7apy library may fail. But the MSH segment
        # will always be present, so we can confidently extract it and
        # then parse it.
        msh = manually_extract_msh_segment(message.decode("utf-8"), cr="\r")
        msh = parse_msh_segment(msh)
        key = msh.msh_10.to_er7() or "some-random-key"
        try:
            producer.produce(
                topic=WRITE_TOPIC,
                value=message,
                key=key,
                headers=[("hl7.message_type", message_type.encode("utf-8"))],
                callback=on_delivery,
            )
            break
        except BufferError:
            producer.poll(0.1)
            time.sleep(0.05)


def main():
    while True:
        message_type = random.choice(MESSAGE_TYPES)
        message = build_message(message_type)
        if message:
            send_message(message, message_type)
        else:
            logger.error(f"Failed to build message of message type {message_type}")
        delay = random.expovariate(5)
        time.sleep(delay)


if __name__ == "__main__":
    start_http_server(8000)
    logger.info(f"Producer starting. Brokers: {KAFKA_BROKERS} Topic: {WRITE_TOPIC}")
    main()
