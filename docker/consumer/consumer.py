import logging
import os
import time

from confluent_kafka import Consumer, KafkaError, TopicPartition
from dotenv import load_dotenv
from helpers import process_message
from prometheus_client import start_http_server

from metrics import messages_received_total

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
TOPIC = "HL7"
DLQ = "DLQ"


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "hl7-consumers",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    while True:
        msg = consumer.poll(timeout=10.0)
        logger.info("Message received")

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Consumer error: {msg.error()}")
                continue

        message = msg.value()
        message_type = next(
            (v.decode("utf-8") for k, v in msg.headers() if k == "hl7.message_type"),
            None,
        )
        messages_received_total.labels(message_type=message_type).inc()

        try:
            process_message(message)
            consumer.commit(message=msg)
        except Exception as e:
            logger.error(f"Processing failed. Not committing offset. Details: {e}")

            # "rewind" the partition so that it sticks on the broken message
            # this will cause this consumer to freeze on that message causing
            # an alert so that it can be fixed.
            # TODO: Determine what metric makes sense here
            tp = TopicPartition(msg.topic(), msg.partition(), msg.offset())
            consumer.seek(tp)
            time.sleep(1.0)
            continue


if __name__ == "__main__":
    start_http_server(8000)
    logger.info(f"Consumer service starting. Brokers: {KAFKA_BROKERS} Topic: {TOPIC}")
    main()
