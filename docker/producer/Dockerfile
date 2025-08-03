FROM python:3.11-slim
WORKDIR /producer
COPY producer.py /producer/producer.py
COPY fake_data_generators.py /producer/fake_data_generators.py
COPY hl7_segment_generators.py /producer/hl7_segment_generators.py
COPY common/scripts/hl7_helpers.py /producer/hl7_helpers.py
RUN pip install --no-cache-dir prometheus-client faker
EXPOSE 8000
CMD ["python", "producer.py"]