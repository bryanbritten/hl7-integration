FROM python:3.11-slim
WORKDIR /transformer
COPY validation.py /transformer/validation.py
COPY quality_assurance.py /transformer/quality_assurance.py
COPY entrypoint.py /transformer/entrypoint.py
RUN pip install --no-cache-dir hl7apy boto3
CMD [ "python", "entrypoint.py" ]