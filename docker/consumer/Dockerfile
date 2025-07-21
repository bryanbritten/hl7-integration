FROM python:3.11-slim
WORKDIR /consumer
COPY docker/consumer/consumer.py /consumer/consumer.py
COPY common/s3_helpers.py /consumer/s3_helpers.py
RUN mkdir /messages
RUN pip install --no-cache-dir boto3
EXPOSE 2575 8000
CMD ["python", "consumer.py"]