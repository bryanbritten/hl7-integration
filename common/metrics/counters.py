from prometheus_client import Counter

messages_sent_total = Counter(
    "messages_sent_total", "Total number of HL7 messages sent", ["message_type"]
)

messages_received_total = Counter(
    "messages_received_total",
    "Total number of HL7 messages received/processed by the Consumer service.",
    ["message_type"],
)

messages_accepted_total = Counter(
    "messages_accepted_total",
    "Total number of HL7 messages that cleared the ingestion process in the Consumer service.",
    ["message_type"],
)

hl7_acks_total = Counter(
    "hl7_acks_total",
    "Total number of HL7 ACK messages sent by the Consumer service.",
    ["status"],
)

validation_failed_total = Counter(
    "validation_failed_total",
    "Total number of HL7 messages that failed the ingestion process.",
    ["message_type"],
)

validation_error_total = Counter(
    "validation_error_total",
    "Count of valiation errors by reason (buckets are not mutually exclusive)",
    ["details", "stage", "message_type"],
)

messages_passed_total = Counter(
    "messages_passed_total",
    "Total number of recieved HL7 messages that passed validation and all data quality checks",
    ["message_type"],
)

fhir_conversion_attempted_total = Counter(
    "fhir_conversion_attempted_total",
    "Count of FHIR conversion attempts",
    ["message_type"],
)

fhir_conversion_error_total = Counter(
    "fhir_conversion_error_total",
    "Count of FHIR conversion failures by reason (buckets are not mutually exclusive)",
    ["details", "message_type"],
)

fhir_conversion_failed_total = Counter(
    "fhir_conversion_failed_total",
    "Count of HL7 messages that failed FHIR conversion",
    ["message_type"],
)
