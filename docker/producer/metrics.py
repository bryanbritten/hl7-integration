from prometheus_client import Counter

messages_sent_total = Counter(
    "messages_sent_total", "Total number of HL7 messages sent", ["message_type"]
)

messages_unsent_total = Counter(
    "messages_unsent_total",
    "Total number of HL7 messages that were not sent",
    ["message_type"],
)
