from prometheus_client import Counter, Gauge, Histogram

messages_sent_total = Counter(
    "messages_sent_total", "Total number of HL7 messages sent", ["message_type"]
)

messages_received_total = Counter(
    "messages_received_total",
    "Total number of HL7 messages received by the Consumer service.",
    ["message_type"],
)

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

messages_fhir_conversion_attempts = Counter(
    "messages_fhir_conversion_attempts",
    "Total number of HL7 messages sent to the FHIR Converter API.",
    ["message_type"],
)
messages_fhir_conversion_successes = Counter(
    "messages_fhir_conversion_successes",
    "Total number of HL7 messages successfully converted to the FHIR format.",
    ["message_type"],
)
