FROM python:3.11-slim
WORKDIR /consumer
COPY docker/consumer/consumer.py /consumer/consumer.py
COPY common/scripts/s3_helpers.py /consumer/s3_helpers.py
COPY common/scripts/hl7_helpers.py /consumer/hl7_helpers.py
RUN mkdir /messages
RUN pip install --no-cache-dir prometheus-client boto3
EXPOSE 2575 8000
CMD ["python", "consumer.py"]