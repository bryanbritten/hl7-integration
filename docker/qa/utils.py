import json

from common.helpers.hl7 import HL7Checker, HL7Parser
from common.helpers.kafka import to_header, write_to_topic
from common.registries import RULE_REGISTRY
from common.rules.types import RuleResult


def handle_error(
    e: Exception,
    message: bytes,
    message_type: str,
    dlq_topic: str,
) -> None:
    headers = [
        to_header("error.stage", "qa"),
        to_header("error.message", f"Unexpected error occurred: {e}"),
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", "hl7-qa"),
    ]
    write_to_topic(message, dlq_topic, headers)


def handle_failures(
    failures: list[RuleResult], message: bytes, message_type: str, dlq_topic: str
) -> None:
    quality_issues = {failure.rule: failure.issues for failure in failures}
    headers = [
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", "hl7-qa"),
        to_header("issues", json.dumps(quality_issues)),
    ]
    write_to_topic(message, dlq_topic, headers)


def handle_success(message: bytes, message_type: str, write_topic: str) -> None:
    headers = [
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", "hl7-qa"),
    ]
    write_to_topic(message, write_topic, headers)


def record_failures(*args):
    pass


def process_message(
    message: bytes,
    message_type: str,
    write_topic: str,
    dlq_topic: str,
) -> None:
    parsed_message = None

    try:
        parser = HL7Parser()
        parsed_message = parser.parse(message)

        checker = HL7Checker(parsed_message, message_type, RULE_REGISTRY)
        results = checker.run_all_rules()

        failures = [result for result in results if result.stamp == "Fail"]
        if failures:
            handle_failures(failures, message, message_type, dlq_topic)
            return

        handle_success(message, message_type, write_topic)
    except Exception as e:
        handle_error(
            e=e,
            message=message,
            message_type=message_type,
            dlq_topic=dlq_topic,
        )
        return
