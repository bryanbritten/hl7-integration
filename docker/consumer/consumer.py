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
READ_TOPIC = os.environ["CONSUMER_READ_TOPIC"]
WRITE_TOPIC = os.environ["CONSUMER_WRITE_TOPIC"]
ACK_TOPIC = os.environ["ACK_TOPIC"]
DLQ_TOPIC = os.environ["DLQ_TOPIC"]


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "hl7-consumers",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([READ_TOPIC])

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
            "ADT_A01",  # hl7.message_type will always be populated, so this default never triggers
        )
        messages_received_total.labels(message_type=message_type).inc()

        try:
            process_message(
                message,
                message_type,
                write_topic=WRITE_TOPIC,
                ack_topic=ACK_TOPIC,
                dlq_topic=DLQ_TOPIC,
            )
            consumer.commit(message=msg)
        except Exception as e:
            logger.exception(f"Processing failed. Not committing offset. Details: {e}")

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
    logger.info(f"Consumer service starting. Brokers: {KAFKA_BROKERS} Topic: {READ_TOPIC}")
    main()
