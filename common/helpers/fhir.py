import requests
from requests import Response


def convert_hl7_to_fhir(message: bytes, message_type: str, fhir_url: str) -> Response:
    URL = fhir_url
    data = {
        "InputDataFormat": "Hl7v2",
        "RootTemplateName": message_type,
        "InputDataString": message.decode("utf-8"),
    }
    response = requests.post(URL, json=data)
    return response
