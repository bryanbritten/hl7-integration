FROM python:3.11-slim
WORKDIR /producer
COPY producer.py /producer/producer.py
COPY hl7_generators.py /producer/hl7_generators.py
COPY helpers.py /producer/helpers.py
RUN pip install --no-cache-dir faker
CMD ["python", "producer.py"]