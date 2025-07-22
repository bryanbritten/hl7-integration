import json
import time

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
            time.sleep(POLL_INTERVAL)
            continue

        validator = HL7Validator(message)
        checker = ADTA01QualityChecker(validator.parsed_message)
        issues = checker.run_all_checks()
        if not validator.message_is_valid() or len(issues) > 0:
            write_data_to_s3(
                bucket=MINIO_DEADLETTER_BUCKET,
                key=key.replace("unprocessed/adt/a01/", "adt/a01/messages/"),  # type: ignore
                body=message,
            )

            if not validator.msh_segment_is_valid():
                messages_failed_validation_total.labels(message_type="ADT_A01").inc()
            elif not validator.pid_segment_is_valid():
                messages_failed_validation_total.labels(message_type="ADT_A01").inc()
            elif not validator.evn_segment_is_valid():
                messages_failed_validation_total.labels(message_type="ADT_A01").inc()
            elif not validator.pv1_segment_is_valid():
                messages_failed_validation_total.labels(message_type="ADT_A01").inc()
            elif len(issues) > 0:
                issues_file_name = key.replace(  # type: ignore
                    "unprocessed/adt/a01/", "adt/a01/issues/"
                ).replace(".hl7", "-issues.json")
                write_data_to_s3(
                    bucket=MINIO_DEADLETTER_BUCKET,
                    key=issues_file_name,
                    body=json.dumps(issues).encode("utf-8"),
                    content_type="application/json",
                )
                messages_failed_quality_checks_total.labels(
                    message_type="ADT_A01"
                ).inc()
            else:
                messages_failed_parsing_total.labels(message_type="ADT_A01").inc()
        else:
            write_data_to_s3(
                bucket=MINIO_SILVER_BUCKET,
                key=key,
                body=message,
            )
            messages_passed_total.labels(message_type="ADT_A01").inc()
        move_message_to_processed(
            bucket=MINIO_BRONZE_BUCKET,
            source_key=key,
            destination_key=key.replace("unprocessed", "processed"),  # type: ignore
        )


if __name__ == "__main__":
    start_http_server(8000)
    main()
