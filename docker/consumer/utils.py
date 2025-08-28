import json
import logging
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from hl7apy.core import Message
from hl7apy.exceptions import ParserError

from common.helpers.hl7 import (
    HL7Parser,
    HL7Validator,
    build_ack,
)
from common.helpers.kafka import to_header, write_to_topic
from common.helpers.s3 import write_data_to_s3
from common.metrics.counters import hl7_acks_total, messages_accepted_total
from common.metrics.labels import (
    REASON_INVALID_CARDINALITY,
    REASON_INVALID_SEGMENTS,
    REASON_MISSING_MSH10,
    REASON_MISSING_SEGMENTS,
    REASON_PARSING_ERROR,
    REASON_UNKNOWN,
)
from common.metrics.utils import record_validation_failures
from common.registries import HL7_SCHEMA_REGISTRY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()


def handle_error(
    e: Exception,
    raw_message: bytes,
    parsed_message: Optional[Message],
    message_type: str,
    message_control_id: Optional[str],
    dlq_topic: str,
    ack_topic: str,
) -> None:
    if isinstance(e, ParserError):
        failures = {REASON_PARSING_ERROR: True}
    else:
        failures = {REASON_UNKNOWN: True}

    error_message = list(failures.keys())[0]

    headers = [
        to_header("error.stage", "ingest"),
        to_header("error.message", f"{error_message}: {e}"),
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", "hl7.consumer"),
    ]
    write_to_topic(raw_message, dlq_topic, headers)

    # if the message failed parsing, the MSH segment can't be automatically extracted
    # in this scenario no ACK is automatically sent and the message requires manual review
    if parsed_message:
        ack = build_ack("AE", parsed_message, error_message)
        write_to_topic(ack, ack_topic)
        hl7_acks_total.labels(status="AE").inc()

    record_validation_failures(message_type, message_control_id, failures)


def handle_failures(
    failures: dict[str, bool],
    raw_message: bytes,
    parsed_message: Message,
    message_type: str,
    message_control_id: Optional[str],
    dlq_topic: str,
    ack_topic: str,
) -> None:
    failure_str = json.dumps(failures)

    headers = [
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", "hl7.qa"),
        to_header("issues", failure_str),
    ]
    write_to_topic(raw_message, dlq_topic, headers)

    ack = build_ack("AE", parsed_message, failure_str)
    write_to_topic(ack, ack_topic)

    record_validation_failures(message_type, message_control_id, failures)


def handle_success(
    raw_message: bytes,
    parsed_message: Message,
    message_type: str,
    write_topic: str,
    write_bucket: str,
    ack_topic: str,
) -> None:
    # write raw message to S3 for future processing if needed
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    key = f"{message_type}/{timestamp}.hl7"
    write_data_to_s3(
        bucket=write_bucket,
        key=key,
        body=raw_message,
    )

    # see README.md to understand why an ACK is sent in this pipeline
    ack = build_ack("AA", parsed_message)
    write_to_topic(ack, ack_topic)
    hl7_acks_total.labels(status="AA").inc()

    # write to Kafka for further processing
    headers = [
        to_header("hl7.message.type", message_type),
        to_header("consumer.group", "hl7.consumers"),
        to_header("hl7.message.s3key", key),
    ]
    write_to_topic(raw_message, write_topic, headers)

    messages_accepted_total.labels(message_type).inc()


def process_message(
    message: bytes,
    message_type: str,
    write_topic: str,
    write_bucket: str,
    ack_topic: str,
    dlq_topic: str,
) -> None:
    # initialize variables used in sending an ACK message
    parsed_message = None
    message_control_id = None
    # initialize validation metrics
    missing_segments = []
    invalid_segments = []
    violating_segments = []

    try:
        parser = HL7Parser()
        parsed_message = parser.parse(message)

        message_control_id = parsed_message.msh.msh_10.value if parsed_message.msh.msh_10 else None
        if not message_control_id:
            failures = {REASON_MISSING_MSH10: True}
            handle_failures(
                failures,
                message,
                parsed_message,
                message_type,
                message_control_id,
                dlq_topic,
                ack_topic,
            )
            return

        validator = HL7Validator(parsed_message, message_type, HL7_SCHEMA_REGISTRY)
        missing_segments = validator.has_required_segments()
        invalid_segments = validator.all_segments_are_valid()
        violating_segments = validator.segment_cardinality_is_valid()

        if missing_segments or invalid_segments or violating_segments:
            failures = {
                REASON_MISSING_SEGMENTS: True if missing_segments else False,
                REASON_INVALID_SEGMENTS: True if invalid_segments else False,
                REASON_INVALID_CARDINALITY: True if violating_segments else False,
            }
            handle_failures(
                failures,
                message,
                parsed_message,
                message_type,
                message_control_id,
                dlq_topic,
                ack_topic,
            )
            return

        handle_success(message, parsed_message, message_type, write_topic, write_bucket, ack_topic)
    except Exception as e:
        handle_error(
            e, message, parsed_message, message_type, message_control_id, dlq_topic, ack_topic
        )
