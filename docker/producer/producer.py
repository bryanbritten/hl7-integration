import logging
import random
import socket
import time
from typing import Any

from hl7_helpers import MESSAGE_REGISTRY
from hl7_segment_generators import generate_segments
from metrics import (
    messages_sent_total,
    messages_unsent_total,
)
from prometheus_client import start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

CONSUMER_HOST = "consumer"
CONSUMER_PORT = 2575
START = b"\x0b"  # VT
END = b"\x1c"  # FS
CR = b"\x0d"  # CR
MESSAGE_TYPES = [
    "ADT_A01",
    "ADT_A03",
]


def build_message(message_type: str) -> bytes | None:
    """
    Builds a complete message with the required segments.

    Returns: bytes - The message encoded in UTF-8 if the message type is supported, else `None`.
    """

    schema = MESSAGE_REGISTRY.get(message_type)
    if schema is None:
        logger.error(f"Unsupported message type identified: {message_type}")
        return None

    segments = generate_segments(schema["segments"], message_type)
    return segments


def send_message(message: bytes, message_type: str) -> bytes | None:
    """
    Sends the HL7 message to the consumer service.

    message: bytes - The HL7 message to send.
    """

    try:
        with socket.create_connection(
            (CONSUMER_HOST, CONSUMER_PORT), timeout=10
        ) as sock:
            sock.sendall(START + message + END + CR)
            ack = sock.recv(4096)

            if ack:
                logger.info("Message successfully sent.")
                messages_sent_total.labels(message_type=message_type).inc()
            else:
                logger.error("Message filed to send.")
                messages_unsent_total.labels(message_type=message_type).inc()
    # In a production environment, specific exceptions should be caught
    except Exception as e:
        logger.exception(f"Unexpected exception: {str(e)}")
        return None


def main():
    while True:
        message_type = random.choice(MESSAGE_TYPES)
        message = build_message(message_type)
        if message:
            send_message(message, message_type)
        else:
            logger.error(f"Failed to build message of message type {message_type}")
        time.sleep(random.uniform(1, 5))


if __name__ == "__main__":
    start_http_server(8000)
    main()
