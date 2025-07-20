FROM python:3.11-slim
WORKDIR /consumer
COPY consumer.py /consumer/consumer.py
RUN mkdir /messages
RUN pip install --no-cache-dir boto3
EXPOSE 2575
CMD ["python", "consumer.py"]