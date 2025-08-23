import logging
import os
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from dotenv import load_dotenv
from hl7_helpers import get_msh_segment, manually_extract_msh_segment
from hl7apy.exceptions import ParserError
from s3_helpers import MINIO_BRONZE_BUCKET, write_data_to_s3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

producer = Producer(
    {
        "bootstrap.servers": os.environ["KAFKA_BROKERS"],
        "acks": 1,
    }
)


def build_ack(
    msh_segment: str,
    separator: str,
    acknowledgement_code: str,
    message_control_id: str,
) -> bytes:
    START = b"\x0b"
    END = b"\x1c"
    CR = b"\x0d"

    msa_segment = separator.join(
        [
            "MSA",
            acknowledgement_code,
            message_control_id,
        ]
    ).encode("utf-8")

    return START + msh_segment.encode("utf-8") + CR + msa_segment + END + CR


def send_to_topic(message: bytes, topic: str) -> None:
    producer.produce(
        topic=topic,
        value=message,
    )
    producer.flush()


def handle_error(
    e: Exception,
    message: Optional[bytes] = None,
    msh_segment: Optional[str] = None,
    message_control_id: Optional[str] = None,
) -> None:
    # if there is no message then there was an error receiving the message; no need for ACK
    if message is None:
        logger.exception(f"Consumed message was empty: {str(e)}")
    # if the message_control_id is None then the message either failed to parse
    # or had a malformed MSH header. Either way, no way to automatically send ACK.
    elif message_control_id is None:
        logger.exception(f"Failed to parse the MSH header: {str(e)}")
        send_to_topic(message, "DLQ")
    # if the message was received and the MSH header was parsed, then something
    # unidentified occurred.
    elif msh_segment is not None:
        logger.exception(f"Unidentified error consuming message: {str(e)}")
        ack = build_ack(
            msh_segment=msh_segment,
            separator=separator,  # type: ignore
            acknowledgement_code="AE",
            message_control_id=message_control_id,
        )
        send_to_topic(ack, "ACKS")
        send_to_topic(message, "DLQ")
    else:
        logger.exception(f"Unexpected error: {str(e)}")
        send_to_topic(message, "DLQ")


def process_message(message: bytes) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    try:
        msh_segment = get_msh_segment(message)
        separator = msh_segment.msh_1.to_er7()
        message_type = msh_segment.msh_9.msh_9_1.to_er7()
        trigger_event = msh_segment.msh_9.msh_9_2.to_er7()
        message_control_id = msh_segment.msh_10.to_er7()
    except ParserError:
        logger.warning(
            "Failed to automatically extract MSH segment, so doing it manually"
        )
        msh_segment = manually_extract_msh_segment(message.decode("utf-8"))
        separator = msh_segment[3]
        fields = msh_segment.split(separator)
        message_type, trigger_event, _ = fields[8].split("^")
        message_control_id = fields[9]
    if not isinstance(msh_segment, str):
        msh_segment = msh_segment.to_er7()

    key = f"unprocessed/{message_type}/{trigger_event}/{timestamp}.hl7"
    write_data_to_s3(
        bucket=MINIO_BRONZE_BUCKET,
        key=key,
        body=message,
    )

    ack = build_ack(
        msh_segment=msh_segment,
        separator=separator,
        acknowledgement_code="AA",
        message_control_id=message_control_id,
    )
    send_to_topic(ack, "ACKS")
