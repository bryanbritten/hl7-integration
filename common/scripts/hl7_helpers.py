from hl7apy.core import Message, Segment
from hl7apy.exceptions import ParserError
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
                "identifier": "ADT_A01_PROCEDURE",
                "name": "Procedure",
                "required": False,
                "repeatable": True,
                "segments": [
                    segment("PR1", "Procedures", required=True),
                    segment("ROL", "Role", repeatable=True),
                ],
            },
            segment("GT1", "Guarantor", repeatable=True),
            {
                "identifier": "ADT_A01_INSURANCE",
                "name": "Insurance",
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
                "identifier": "ADT_A03_PROCEDURE",
                "name": "Procedure",
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
    "ORU_R01": {
        "segments": [
            segment("MSH", "Message Segment Header", required=True),
            segment("SFT", "Software Segment", repeatable=True),
            {
                "identifier": "ORU_R01_PATIENT_RESULT",
                "name": "Patient Result",
                "required": True,
                "repeatable": True,
                "segments": [
                    {
                        "identifier": "ORU_R01_PATIENT",
                        "name": "Patient",
                        "required": False,
                        "repeatable": False,
                        "segments": [
                            segment("PID", "Patient Identification", required=True),
                            segment("PD1", "Patient Additional Demographic"),
                            segment("NTE", "Notes and Comments", repeatable=True),
                            segment("NK1", "Next of Kin", repeatable=True),
                            {
                                "identifier": "ORU_R01_VISIT",
                                "name": "Visit",
                                "required": False,
                                "repeatable": False,
                                "segments": [
                                    segment("PV1", "Patient Visit", required=True),
                                    segment(
                                        "PV2", "Patient Visit - Additional Information"
                                    ),
                                ],
                            },
                        ],
                    },
                    {
                        "identifier": "ORU_R01_ORDER_OBSERVATION",
                        "name": "Order Observation",
                        "required": True,
                        "repeatable": True,
                        "segments": [
                            segment("ORC", "Common Order"),
                            segment("OBR", "Observation Request", required=True),
                            segment("NTE", "Notes and Comments", repeatable=True),
                            {
                                "identifier": "ORU_R01_TIMING_QUANTITY",
                                "name": "Timing Quantity",
                                "required": False,
                                "repeatable": True,
                                "segments": [
                                    segment("TQ1", "Timing/Quantity", required=True),
                                    segment(
                                        "TQ2",
                                        "Timing/Quantity Relationship",
                                        repeatable=True,
                                    ),
                                ],
                            },
                            segment("CTD", "Contact Data"),
                            {
                                "identifier": "ORU_R01_OBSERVATION",
                                "name": "Observation",
                                "required": False,
                                "repeatable": True,
                                "segments": [
                                    segment(
                                        "OBX", "Observation Segment", required=True
                                    ),
                                    segment(
                                        "NTE", "Notes and Comments", repeatable=True
                                    ),
                                ],
                            },
                            segment("FT1", "Financial Transaction", repeatable=True),
                            segment(
                                "CTI", "Clinical Trial Identification", repeatable=True
                            ),
                            {
                                "identifier": "ORU_R01_SPECIMEN",
                                "name": "Specimen",
                                "required": False,
                                "repeatable": True,
                                "segments": [
                                    segment("SPM", "Specimen", required=True),
                                    segment(
                                        "OBX", "Observation Segment", repeatable=True
                                    ),
                                ],
                            },
                        ],
                    },
                ],
            },
            segment("DSC", "Continuation Pointer"),
        ]
    },
}


def get_msh_segment(message: bytes) -> Segment:
    msg = parse_message(message.decode("utf-8"))
    return parse_segment(msg.msh.to_er7())


def parse_msh_segment(segment: str) -> Segment:
    return parse_segment(segment)


def manually_extract_msh_segment(message: str, cr: str = "\r") -> str:
    segments = message.split(cr)
    msh_candidates = [segment for segment in segments if segment.startswith("MSH")]
    if not any(msh_candidates):
        raise ParserError
    most_likely_candidate = sorted(msh_candidates, key=len, reverse=True)[0]
    return most_likely_candidate


def generate_empty_msh_segment(trigger_event: str) -> str:
    m = Message("ADT_A01")
    m.msh.msh_10 = f"ACK^{trigger_event}"
    return m.msh.to_er7()
