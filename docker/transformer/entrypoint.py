import json
import time

import boto3
from botocore.config import Config
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
                print(f"MSH segment is invalid in message {key}.")
            elif not validator.pid_segment_is_valid():
                print(f"PID segment is invalid in message {key}.")
            elif not validator.evn_segment_is_valid():
                print(f"EVN segment is invalid in message {key}.")
            elif not validator.pv1_segment_is_valid():
                print(f"PV1 segment is invalid in message {key}.")
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
                print(f"Message {key} failed data quality checks.")
            else:
                print(f"Message {key} failed to parse.")
        else:
            write_data_to_s3(
                bucket=MINIO_SILVER_BUCKET,
                key=key,
                body=message,
            )
        move_message_to_processed(
            bucket=MINIO_BRONZE_BUCKET,
            source_key=key,
            destination_key=key.replace("unprocessed", "processed"),  # type: ignore
        )


if __name__ == "__main__":
    main()
