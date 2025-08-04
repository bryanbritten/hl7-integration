from hl7apy.core import Segment
from hl7apy.parser import parse_message, parse_segment

MESSAGE_REGISTRY = {
    "ADT_A01": {
        "segments": [
            {
                "name": "Message Segment Header",
                "identifier": "MSH",
                "required": True,
                "repeatable": False,
            },
            {
                "name": "Event Type",
                "identifier": "EVN",
                "required": True,
                "repeatable": False,
            },
            {
                "name": "Patient Identifier",
                "identifier": "PID",
                "required": True,
                "repeatable": False,
            },
            {
                "name": "Patient Demographic",
                "identifier": "PD1",
                "required": False,
                "repeatable": False,
            },
            {
                "name": "Next of Kin",
                "identifier": "NK1",
                "required": False,
                "repeatable": True,
            },
            {
                "name": "Patient Visit",
                "identifier": "PV1",
                "required": True,
                "repeatable": False,
            },
            {
                "name": "Patient Visit - Additional Information",
                "identifier": "PV2",
                "required": False,
                "repeatable": False,
            },
            {
                "name": "Disability Segment",
                "identifier": "DB1",
                "required": False,
                "repeatable": True,
            },
            {
                "name": "Observation Segment",
                "identifier": "OBX",
                "required": False,
                "repeatable": True,
            },
            {
                "name": "Patient Allergy Information",
                "identifier": "AL1",
                "required": False,
                "repeatable": True,
            },
            {
                "name": "Diagnosis",
                "identifier": "DG1",
                "required": False,
                "repeatable": True,
            },
            {
                "name": "Diagnosis Related Group",
                "identifier": "DRG",
                "required": False,
                "repeatable": False,
            },
            {
                "name": "Procedure",
                "identifier": "ADT_A01_PROCEDURE",
                "required": False,
                "repeatable": True,
                "segments": [
                    {
                        "name": "Procedures",
                        "identifier": "PR1",
                        "required": True,
                        "repeatable": False,
                    },
                    {
                        "name": "Role",
                        "identifier": "ROL",
                        "required": False,
                        "repeatable": True,
                    },
                ],
            },
            {
                "name": "Guarantor",
                "identifier": "GT1",
                "required": False,
                "repeatable": True,
            },
            {
                "name": "Insurance",
                "identifier": "ADT_A01_INSURANCE",
                "required": False,
                "repeatable": True,
                "segments": [
                    {
                        "name": "Insurance",
                        "identifier": "IN1",
                        "required": True,
                        "repeatable": False,
                    },
                    {
                        "name": "Insurance - Additional Information",
                        "identifier": "IN2",
                        "required": False,
                        "repeatable": False,
                    },
                    {
                        "name": "Insurance - Additional Information - Certification",
                        "identifier": "IN3",
                        "required": False,
                        "repeatable": False,
                    },
                ],
            },
            {
                "name": "Accident",
                "identifier": "ACC",
                "required": False,
                "repeatable": False,
            },
            {
                "name": "UB82 Data",
                "identifier": "UB1",
                "required": False,
                "repeatable": False,
            },
            {
                "name": "UB92 Data",
                "identifier": "UB2",
                "required": False,
                "repeatable": False,
            },
        ]
    },
    "ADT_A03": {
        "segments": [
            {
                "name": "Message Segment Header",
                "identifier": "MSH",
                "required": True,
                "repeatable": False,
            },
            {
                "name": "Event Type",
                "identifier": "EVN",
                "required": True,
                "repeatable": False,
            },
            {
                "name": "Patient Identification",
                "identifier": "PID",
                "required": True,
                "repeatable": False,
            },
            {
                "name": "Patient Demographic",
                "identifier": "PD1",
                "required": False,
                "repeatable": False,
            },
            {
                "name": "Patient Visit",
                "identifier": "PV1",
                "required": True,
                "repeatable": False,
            },
            {
                "name": "Patient Visit - Additional Information",
                "identifier": "PV2",
                "required": False,
                "repeatable": False,
            },
            {
                "name": "Disability Segment",
                "identifier": "DB1",
                "required": False,
                "repeatable": True,
            },
            {
                "name": "Diagnosis",
                "identifier": "DG1",
                "required": False,
                "repeatable": True,
            },
            {
                "name": "Diagnosis Related Group",
                "identifier": "DRG",
                "required": False,
                "repeatable": False,
            },
            {
                "name": "Procedure",
                "identifier": "ADT_A03_PROCEDURE",
                "required": False,
                "repeatable": True,
                "segments": [
                    {
                        "name": "Procedures",
                        "identifier": "PR1",
                        "required": True,
                        "repeatable": False,
                    },
                    {
                        "name": "Role",
                        "identifier": "ROL",
                        "required": False,
                        "repeatable": True,
                    },
                ],
            },
            {
                "name": "Observation Segment",
                "identifier": "OBX",
                "required": False,
                "repeatable": True,
            },
        ],
    },
}


def get_msh_segment(message: bytes) -> Segment:
    msg = parse_message(message.decode("utf-8"))
    return parse_segment(msg.msh.to_er7())
