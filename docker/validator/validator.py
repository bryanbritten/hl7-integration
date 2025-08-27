import logging
import time

from kafka_helpers import generate_dlq_headers, send_to_topic
from prometheus_client import start_http_server
from quality_assurance import ADTA01QualityChecker
from s3_helpers import (
    MINIO_BRONZE_BUCKET,
    MINIO_SILVER_BUCKET,
    POLL_INTERVAL,
    get_message_from_s3,
    move_message_to_processed,
    write_data_to_s3,
)
from validation import HL7Validator

from metrics import message_failures_total, messages_passed_total

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    while True:
        key, message = get_message_from_s3(MINIO_BRONZE_BUCKET)
        if not message:
            logger.info(f"Failed to find new messages. Checking again in {POLL_INTERVAL} seconds.")
            time.sleep(POLL_INTERVAL)
            continue

        validator = HL7Validator(message)
        checker = ADTA01QualityChecker(validator.parsed_message)
        issues = checker.run_all_checks()

        if validator.parsed_message.msh.msh_9:
            message_structure = validator.parsed_message.msh.msh_9.msh_9_3.to_er7()
        else:
            message_structure = "UNK_UNK"

        if not validator.message_is_valid():
            if not validator.has_required_segments():
                error_message = "Message does not contain required segments"
                message_failures_total.labels(
                    reason="Missing Required Segment", element="Unknown", service="validation"
                )
            elif not validator.all_segments_are_valid():
                error_message = "Message contains invalid segment(s)"
                message_failures_total.labels(
                    reason="Invalid Segment", element="Unknown", service="validation"
                )
            elif not validator.segment_cardinality_is_valid():
                error_message = "Message contains invalid repeated segments"
                message_failures_total.labels(
                    reason="Invalid Segment Cardinality", element="Unknown", service="validation"
                )
            else:
                error_message = "Message validation failed for an unknown reason"
                message_failures_total.labels(
                    reason="Unknown", element="Unknown", service="validation"
                )

            logger.error(f"{error_message}: {key}")
            headers = generate_dlq_headers(
                msg_key=key,
                error_type="validation",
                error_message=error_message,
            )
            send_to_topic(message, "DLQ", headers)
        elif len(issues) > 0:
            error_message = "Message failed data quality checks"
            message_failures_total.labels(
                reason="Failed Data Quality Checks", element="Unknown", service="validation"
            ).inc()

            logger.error(error_message)
            headers = generate_dlq_headers(
                msg_key=key,
                error_type="quality",
                error_message=error_message,
                issues=issues,
            )
            send_to_topic(message, "DLQ", headers)
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
