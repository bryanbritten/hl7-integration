import json
import logging
import time
from typing import Any

from prometheus_client import Counter, start_http_server
from quality_assurance import ADTA01QualityChecker
from s3_helpers import (
    MINIO_BRONZE_BUCKET,
    MINIO_DEADLETTER_BUCKET,
    MINIO_SILVER_BUCKET,
    POLL_INTERVAL,
    get_message_from_s3,
    move_message_to_processed,
    write_data_to_s3,
)
from validation import HL7Validator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

messages_failed_validation_total = Counter(
    "messages_failed_validated_total",
    "Total number of received HL7 messages that failed validation.",
    ["message_type"],
)
messages_failed_quality_checks_total = Counter(
    "messages_failed_quality_checks_total",
    "Total number of received HL7 messages that failed data quality checks.",
    ["message_type"],
)
messages_failed_parsing_total = Counter(
    "messages_failed_parsing_total",
    "Total number of Hl7 messages that failed to parse correctly.",
    ["message_tyep"],
)
messages_passed_total = Counter(
    "messages_passed_total",
    "Total number of recieved HL7 messages that passed validation and all data quality checks",
    ["message_type"],
)


def main() -> None:
    while True:
        key, message = get_message_from_s3(MINIO_BRONZE_BUCKET)
        if not message:
            logger.info(
                f"Failed to find new messages. Checking again in {POLL_INTERVAL} seconds."
            )
            time.sleep(POLL_INTERVAL)
            continue

        validator = HL7Validator(message)
        checker = ADTA01QualityChecker(validator.parsed_message)
        issues = checker.run_all_checks()

        message_type = validator.parsed_message.msh.msh_9.msh_9_1.to_er7() or "UNK"
        trigger_event = validator.parsed_message.msh.msh_9.msh_9_2.to_er7() or "UNK"
        message_structure = (
            validator.parsed_message.msh.msh_9.msh_9_3.to_er7() or "UNK_UNK"
        )

        if not validator.message_is_valid():
            deadletter_key = key.replace(
                f"unprocessed/{message_type}/{trigger_event}",
                f"{message_type}/{trigger_event}/messages/",
            )
            write_data_to_s3(
                bucket=MINIO_DEADLETTER_BUCKET,
                key=deadletter_key,
                body=message,
            )

            if not validator.has_required_segments():
                logger.error(f"Message does not contain required segments: {key}")
                messages_failed_validation_total.labels(
                    message_type=message_structure
                ).inc()
            elif not validator.all_segments_are_valid():
                logger.error(f"Message contains invalid segment(s): {key}")
                messages_failed_validation_total.labels(
                    message_type=message_structure
                ).inc()
            elif not validator.segment_cardinality_is_valid():
                logger.error(f"Message contains invalid repeated segments: {key}")
                messages_failed_validation_total.labels(
                    message_type=message_structure
                ).inc()
        elif len(issues) > 0:
            issues_key = key.replace(
                f"unprocessed/{message_type}/{trigger_event}",
                f"{message_type}/{trigger_event}/issues",
            ).replace(".hl7", "-issues.json")

            logger.error(
                f"Message failed data quality checks. See {issues_key} for details"
            )

            messages_failed_quality_checks_total.labels(
                message_type=message_structure
            ).inc()

            write_data_to_s3(
                bucket=MINIO_DEADLETTER_BUCKET,
                key=issues_key,
                body=json.dumps(issues).encode("utf-8"),
                content_type="application/json",
            )
        else:
            write_data_to_s3(
                bucket=MINIO_SILVER_BUCKET,
                key=key,
                body=message,
            )
            logger.info("Message successfully passed validation and quality checks")
            messages_passed_total.labels(message_type=message_structure).inc()

        # regardless of outcome, move the message into the processed directory in the Bronze bucket
        move_message_to_processed(
            bucket=MINIO_BRONZE_BUCKET,
            source_key=key,
            destination_key=key.replace("unprocessed", "processed"),  # type: ignore
        )


if __name__ == "__main__":
    start_http_server(8000)
    main()
