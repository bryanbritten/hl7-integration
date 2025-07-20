FROM python:3.11-slim
WORKDIR /transformer
COPY validation.py /transformer/validation.py
COPY quality_checks.py /transformer/quality_checks.py
COPY entrypoint.py /transformer/entrypoint.py
RUN pip install --no-cache-dir hl7apy boto3
CMD [ "python", "entrypoint.py" ]