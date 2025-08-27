FROM python:3.11-slim

WORKDIR /consumer

COPY docker/consumer /consumer
COPY common /consumer/common

RUN mkdir /messages
RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir boto3 confluent-kafka hl7apy prometheus-client python-dotenv

CMD ["python", "consumer.py"]