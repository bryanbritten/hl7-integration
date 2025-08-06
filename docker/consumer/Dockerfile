FROM python:3.11-slim
WORKDIR /consumer
COPY docker/consumer/consumer.py /consumer/consumer.py
COPY docker/consumer/metrics.py /consumer/metrics.py
COPY common/scripts/s3_helpers.py /consumer/s3_helpers.py
COPY common/scripts/hl7_helpers.py /consumer/hl7_helpers.py
RUN mkdir /messages
RUN pip install --no-cache-dir boto3 hl7apy prometheus-client
EXPOSE 2575 8000
CMD ["python", "consumer.py"]