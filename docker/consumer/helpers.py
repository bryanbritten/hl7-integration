import json
import logging
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from hl7apy.core import Message
from hl7apy.exceptions import ParserError
from kafka_helpers import generate_dlq_headers, write_to_topic

from hl7_helpers import (
    HL7Parser,
    HL7Validator,
    ValidationError,
)
from metrics import hl7_acks_total, message_failures_total
from s3_helpers import MINIO_BRONZE_BUCKET, write_data_to_s3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()


def build_ack(
    acknowledgement_code: str,
    message: Message,
    error_message: Optional[str] = None,
) -> bytes:
    START = b"\x0b"
    END = b"\x1c"
    CR = b"\x0d"

    separator = message.msh.msh_1.to_er7()  # type: ignore
    message_control_id = message.msh.msh_10.to_er7()  # type: ignore
    msh_segment = message.msh.to_er7()

    msa_segment = separator.join(
        # error_message is the only element in the list that will ever be None
        # so HL7 structure compliance is guaranteed.
        filter(None, ["MSA", acknowledgement_code, message_control_id, error_message])
    ).encode("utf-8")

    return START + msh_segment.encode("utf-8") + CR + msa_segment + END + CR


def handle_error(
    e: Exception,
    raw_message: bytes,
    parsed_message: Optional[Message],
    ack_topic: str,
    dlq_topic: str,
) -> None:
    """
    This method is responsible for identifying the type of error that occurred and
    sending an ACK message back with information about what went wrong as well as
    writing to the Kafka Dead Letter Queue for manual review.

    The errors are handled in the order in which they are most likely to occur.
    """

    if isinstance(e, ParserError):
        error_message = f"Failed to parse message: {e}"
    elif isinstance(e, ValidationError):
        error_message = f"Message failed validation: {e}"
    else:
        error_message = f"Unexpected error occurred: {e}"

    dlq_headers = generate_dlq_headers(
        error_stage="ingestion",
        error_message=error_message,
    )

    # if the message failed parsing, automatically sending an ACK doesn't make sense
    # because there's no Message Control ID to match
    if parsed_message:
        ack = build_ack("AE", parsed_message, error_message)

        # ACK messages are sent only for demonstrative purposes.
        # In production, an ACK would likely not be sent automatically at this point.
        # The message would be manually reviewed so that internal changes could be made
        # if appropriate and the message could be reprocessed. An ACK would be sent after
        # the message was reprocessed.
        write_to_topic(ack, ack_topic)
        hl7_acks_total.labels(status="AE").inc()

    write_to_topic(raw_message, dlq_topic, dlq_headers)


def record_validation_metrics(
    message_type: str,
    message_control_id: Optional[str] = None,
    missing_segments: Optional[list[str]] = [],
    invalid_segments: Optional[list[str]] = [],
    violating_segments: Optional[list[str]] = [],
) -> None:
    if missing_segments:
        details = "Missing Required Segments"
    elif invalid_segments:
        details = "Invalid Segments"
    elif violating_segments:
        details = "Invalid Segment Cardinality"
    elif message_control_id is None:
        details = "Missing MSH-10 Field"
    else:
        details = "Unexpected Error"

    message_failures_total.labels(
        type="validation",
        details=details,
        stage="ingestion",
        message_type=message_type,
    )


def process_message(
    message: bytes,
    message_type: str,
    write_topic: str,
    ack_topic: str,
    dlq_topic: str,
) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
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

        message_control_id = parsed_message.msh.msh_10.value
        if not message_control_id:
            raise ValidationError("Failed to find valid MSH-10 value")

        validator = HL7Validator(parsed_message, message_type)
        missing_segments = validator.get_missing_required_segments()
        invalid_segments = validator.get_invalid_segments()
        violating_segments = validator.get_segment_cardinality_violations()

        if missing_segments or invalid_segments or violating_segments:
            bad_segments = {
                "missing": ",".join(missing_segments),
                "invalid": ",".join(invalid_segments),
                "violating": ",".join(violating_segments),
            }
            raise ValidationError(
                f"Message contains erroneous segments: {json.dumps(bad_segments)}"
            )
    except ParserError as pe:
        handle_error(
            e=pe,
            raw_message=message,
            parsed_message=parsed_message,
            ack_topic=ack_topic,
            dlq_topic=dlq_topic,
        )
        message_failures_total.labels(
            type="parsing",
            details="Failed to Parse Message",
            stage="ingestion",
            message_type=message_type,
        ).inc()
        return
    except ValidationError as ve:
        handle_error(
            e=ve,
            raw_message=message,
            parsed_message=parsed_message,
            ack_topic=ack_topic,
            dlq_topic=dlq_topic,
        )
        record_validation_metrics(
            message_type,
            message_control_id,
            missing_segments,
            invalid_segments,
            violating_segments,
        )
        return
    except Exception as e:
        handle_error(
            e=e,
            raw_message=message,
            parsed_message=parsed_message,
            ack_topic=ack_topic,
            dlq_topic=dlq_topic,
        )
        return

    ack = build_ack("AA", parsed_message)
    write_to_topic(ack, ack_topic)
    hl7_acks_total.labels(status="AA").inc()
    write_to_topic(message, write_topic)

    # write raw message to S3 for future processing if needed
    key = f"{message_type}/{timestamp}.hl7"
    write_data_to_s3(
        bucket=MINIO_BRONZE_BUCKET,
        key=key,
        body=message,
    )
