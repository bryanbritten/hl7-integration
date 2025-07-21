FROM python:3.11-slim
WORKDIR /fhir-transformer
COPY docker/fhir-transformer/fhir-transformer.py /fhir-transformer/fhir-transformer.py
COPY common/s3_helpers.py /fhir-transformer/s3_helpers.py
RUN pip install --no-cache-dir boto3 requests
CMD [ "python", "fhir-transformer.py" ]