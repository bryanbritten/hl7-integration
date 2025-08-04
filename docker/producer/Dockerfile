FROM python:3.11-slim
WORKDIR /producer
COPY docker/producer/producer.py /producer/producer.py
COPY docker/producer/fake_data_generators.py /producer/fake_data_generators.py
COPY docker/producer/hl7_segment_generators.py /producer/hl7_segment_generators.py
COPY common/scripts/hl7_helpers.py /producer/hl7_helpers.py
RUN pip install --no-cache-dir faker hl7apy prometheus-client
EXPOSE 8000
CMD ["python", "producer.py"]