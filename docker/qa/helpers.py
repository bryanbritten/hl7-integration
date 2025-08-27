from datetime import datetime, timezone

from hl7apy.exceptions import ParserError

from common.helpers.hl7 import HL7Checker, HL7Parser, ValidationError
from common.helpers.kafka import to_header, write_to_topic
from common.metrics import message_failures_total
from common.rules import RULE_REGISTRY


def handle_error(**kwargs):
    pass


def record_validation_metrics(*args):
    pass


def process_message(
    message: bytes,
    message_type: str,
    write_topic: str,
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

        checker = HL7Checker(parsed_message, message_type, RULE_REGISTRY)

    except ParserError as pe:
        handle_error(
            e=pe,
            raw_message=message,
            parsed_message=parsed_message,
            dlq_topic=dlq_topic,
        )
        message_failures_total.labels(
            type="parsing",
            details="Failed to Parse Message",
            stage="qa",
            message_type=message_type,
        ).inc()
        return
    except ValidationError as ve:
        handle_error(
            e=ve,
            raw_message=message,
            parsed_message=parsed_message,
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
            dlq_topic=dlq_topic,
        )
        return

    headers = [
        to_header("hl7.message.type", message_type),
        to_header("consumer.group", "hl7-qa"),
    ]
    write_to_topic(message, write_topic, headers)
