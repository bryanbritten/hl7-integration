import logging
import os
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from dotenv import load_dotenv
from hl7apy.exceptions import ParserError

from hl7_helpers import (
    generate_empty_msh_segment,
    get_msh_segment,
    manually_extract_msh_segment,
)
from metrics import hl7_acks_total, message_failures_total
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


def send_to_topic(message: bytes, topic: str) -> None:
    producer.produce(
        topic=topic,
        value=message,
    )
    producer.flush()


def build_ack(
    acknowledgement_code: str,
    separator: str,
    msh_segment: str,
    message_control_id: str,
    error_message: Optional[str] = None,
) -> bytes:
    START = b"\x0b"
    END = b"\x1c"
    CR = b"\x0d"

    msa_segment = separator.join(
        # error_message is the only element in the list that will ever be None
        # so HL7 structure compliance is guaranteed.
        filter(None, ["MSA", acknowledgement_code, message_control_id, error_message])
    ).encode("utf-8")

    return START + msh_segment.encode("utf-8") + CR + msa_segment + END + CR


def handle_error(
    e: Exception,
    message: bytes,
    separator: Optional[str] = None,
    msh_segment: Optional[str] = None,
    message_control_id: Optional[str] = None,
) -> None:
    # if the MSH segment is None then the hl7apy parser and the custom extraction failed.
    if msh_segment is None:
        logger.exception(f"Failed to extract the MSH header: {str(e)}")
        error_message = "Failed to extract MSH segment."
        message_failures_total.labels(reason="Missing MSH Segment", service="consumer").inc()
    # if the message_control_id is None then one of the following two things happened:
    #   1. The message was successfully parsed by the `hl7apy` library and the MSH-10
    #      field was empty.
    #   2. The MSH segment was extracted via custom logic and either the MSH segment is
    #      malformed or the MSH-10 field is missing.
    elif message_control_id is None:
        logger.exception(f"Failed to extract MSH-10: {str(e)}")
        error_message = "MSH-10 is required but was not found."
        message_failures_total.labels(reason="Missing Field: MSH-10", service="consumer").inc()
    # if the message was received, the MSH header was parsed, and MSH-10 was not empty,
    # then something unidentified occurred.
    else:
        logger.exception(f"Unexpected error: {str(e)}")
        error_message = "The message could not be processed."
        message_failures_total.labels(reason="Unknown", service="consumer").inc()

    ack = build_ack(
        "AE",
        separator or "|",
        msh_segment or generate_empty_msh_segment(),
        message_control_id or "<ERROR>",
        error_message or None,
    )

    # ACK messages are sent only for demonstrative purposes.
    # In production, an ACK would likely not be sent automatically at this point.
    # The message would be manually reviewed so that internal changes could be made
    # if appropriate and the message could be reprocessed. An ACK would be sent after
    # the message was reprocessed.
    send_to_topic(ack, "ACKS")
    hl7_acks_total.labels(status="AE").inc()
    send_to_topic(message, "DLQ")


def process_message(message: bytes) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    # initialize variables used in building the ACK message
    msh_segment = separator = message_control_id = None

    try:
        msh_segment = get_msh_segment(message)
        separator = msh_segment.msh_1.to_er7()
        message_type = msh_segment.msh_9.msh_9_1.to_er7()
        trigger_event = msh_segment.msh_9.msh_9_2.to_er7()
        message_control_id = msh_segment.msh_10.to_er7()

        if not message_control_id or not message_type or not trigger_event:
            message_control_id = message_control_id or None
            raise ValueError("No valid MSH-9 and/or MSH-10 field was found.")

        msh_segment = msh_segment.to_er7()
    except ParserError:
        try:
            logger.warning("Failed to automatically extract MSH segment, attempting it manually")

            msh_segment = manually_extract_msh_segment(message.decode("utf-8"))
            if len(msh_segment) < 4:
                msh_segment = None
                raise ParserError("No valid MSH segment found.")

            separator = msh_segment[3]
            fields = msh_segment.split(separator)
            if len(fields) < 10:
                message_control_id = None
                raise ValueError("No valid MSH-9 and/or MSH-10 field was found.")

            message_type, trigger_event, _ = fields[8].split("^")
            message_control_id = fields[9]
        except (ParserError, ValueError) as e:
            handle_error(e, message, separator, msh_segment, message_control_id)
            return
    except ValueError as e:
        handle_error(e, message, separator, msh_segment, message_control_id)
        return

    key = f"unprocessed/{message_type}/{trigger_event}/{timestamp}.hl7"
    write_data_to_s3(
        bucket=MINIO_BRONZE_BUCKET,
        key=key,
        body=message,
    )

    ack = build_ack(
        "AA",
        msh_segment=msh_segment,
        separator=separator,
        message_control_id=message_control_id,
    )
    send_to_topic(ack, "ACKS")
    hl7_acks_total.labels(status="AA").inc()
