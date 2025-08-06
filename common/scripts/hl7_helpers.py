from hl7apy.core import Segment
from hl7apy.parser import parse_message, parse_segment


def segment(
    identifier: str,
    name: str,
    required: bool = False,
    repeatable: bool = False,
) -> dict[str, str | bool]:
    return {
        "identifier": identifier,
        "name": name,
        "required": required,
        "repeatable": repeatable,
    }


MESSAGE_REGISTRY = {
    "ADT_A01": {
        "segments": [
            segment("MSH", "Message Segment Header", required=True),
            segment("EVN", "Event Type", required=True),
            segment("PID", "Patient Identifier", required=True),
            segment("PD1", "Patient Demographic"),
            segment("NK1", "Next of Kin", repeatable=True),
            segment("PV1", "Patient Visit", required=True),
            segment("PV2", "Patient Visit - Additional Information"),
            segment("DB1", "Disability Segment", repeatable=True),
            segment("OBX", "Observation Segment", repeatable=True),
            segment("AL1", "Patient Allergy Information", repeatable=True),
            segment("DG1", "Diagnosis", repeatable=True),
            segment("DRG", "Diagnosis Related Group"),
            {
                "name": "Procedure",
                "identifier": "ADT_A01_PROCEDURE",
                "required": False,
                "repeatable": True,
                "segments": [
                    segment("PR1", "Procedures", required=True),
                    segment("ROL", "Role", repeatable=True),
                ],
            },
            segment("GT1", "Guarantor", repeatable=True),
            {
                "name": "Insurance",
                "identifier": "ADT_A01_INSURANCE",
                "required": False,
                "repeatable": True,
                "segments": [
                    segment("IN1", "Insurance", required=True),
                    segment("IN2", "Insurance - Additional Information"),
                    segment(
                        "IN3", "Insurance - Additional Information - Certification"
                    ),
                ],
            },
            segment("ACC", "Accident"),
            segment("UB1", "UB82 Data"),
            segment("UB2", "UB92 Data"),
        ]
    },
    "ADT_A03": {
        "segments": [
            segment("MSH", "Message Segment Header", required=True),
            segment("EVN", "Event Type", required=True),
            segment("PID", "Patient Identifier", required=True),
            segment("PD1", "Patient Demographic"),
            segment("PV1", "Patient Visit", required=True),
            segment("PV2", "Patient Visit - Additional Information"),
            segment("DB1", "Disability Segment", repeatable=True),
            segment("DG1", "Diagnosis", repeatable=True),
            segment("DRG", "Diagnosis Related Group"),
            {
                "name": "Procedure",
                "identifier": "ADT_A03_PROCEDURE",
                "required": False,
                "repeatable": True,
                "segments": [
                    segment("PR1", "Procedures", required=True),
                    segment("ROL", "Role", repeatable=True),
                ],
            },
            segment("OBX", "Observation Segment", repeatable=True),
        ],
    },
}


def get_msh_segment(message: bytes) -> Segment:
    msg = parse_message(message.decode("utf-8"))
    return parse_segment(msg.msh.to_er7())
