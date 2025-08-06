from prometheus_client import Counter

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
