FROM python:3.11-slim
WORKDIR /fhir-transformer
COPY fhir-transformer.py /fhir-transformer/fhir-transformer.py
RUN pip install --no-cache-dir boto3 requests
CMD [ "python", "fhir-transformer.py" ]