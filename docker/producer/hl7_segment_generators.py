import logging
import random
from typing import Any

from faker import Faker

from fake_data_generators import (
    generate_admission_type,
    generate_admit_source,
    generate_ambulatory_status,
    generate_doctor_name,
    generate_gender,
    generate_hospital_service,
    generate_patient_address,
    generate_patient_birth_date,
    generate_patient_class,
    generate_patient_marital_status,
    generate_patient_name,
    generate_patient_phone_number,
    generate_patient_race,
    generate_patient_ssn,
    generate_random_date_time,
    generate_uuid,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

faker = Faker()

SEPARATOR = "|"
DELIMITERS = "^~\\&"
CR = b"\x0d"


def generate_msh_segment(message_type: str) -> bytes:
    """
    Generates an MSH segment for HL7 messages.

    message_type: str - The type of HL7 message (e.g., "ADT^A01^ADT_A01").
    Returns: bytes - The MSH segment encoded in UTF-8.
    """

    # fmt: off
    return SEPARATOR.join(
        [
            "MSH",                          # MSH-0
            DELIMITERS,                     # MSH-2
            "PythonHL7Producer",            # MSH-3
            "Bryan's Lab",                  # MSH-4
            "PythonHL7Consumer",            # MSH-5
            "Bryan's Ingestion Company",    # MSH-6
            generate_random_date_time(),    # MSH-7
            "",                             # MSH-8
            message_type,                   # MSH-9
            generate_uuid(),                # MSH-10
            "T",                            # MSH-11
            "2.5",                          # MSH-12
        ]
    ).encode("utf-8")
    # fmt: on


def generate_pid_segment() -> bytes:
    """
    Generates a PID segment for HL7 messages.

    Returns: bytes - The PID segment encoded in UTF-8.
    """

    # fmt: off
    return SEPARATOR.join(
        [
            "PID",                              # PID-0
            "1",                                # PID-1
            "",                                 # PID-2
            generate_uuid(),                    # PID-3
            "",                                 # PID-4
            generate_patient_name(),            # PID-5
            faker.last_name(),                  # PID-6
            generate_patient_birth_date(),      # PID-7
            generate_gender(),                  # PID-8
            "",                                 # PID-9
            generate_patient_race(),            # PID-10
            generate_patient_address(),         # PID-11
            "",                                 # PID-12
            generate_patient_phone_number(),    # PID-13
            "",                                 # PID-14
            "",                                 # PID-15
            generate_patient_marital_status(),  # PID-16
            "",                                 # PID-17
            "",                                 # PID-18
            generate_patient_ssn(),             # PID-19
        ]
    ).encode("utf-8")
    # fmt: on


def generate_evn_segment(trigger_event: str) -> bytes:
    """
    Generates an EVN segment for HL7 messages.

    trigger_event: str - The type of event trigger (e.g., "A01").
    Returns: bytes - The EVN segment encoded in UTF-8.
    """

    # fmt: off
    return SEPARATOR.join(
        [
            "EVN",                          # EVN-0
            trigger_event,                  # EVN-1
            generate_random_date_time(),    # EVN-2
        ]
    ).encode("utf-8")
    # fmt: on


def generate_pv1_segment() -> bytes:
    """
    Generates a PV1 segment for HL7 messages.

    set_id: int - Used to identify the transaction.
    Returns: bytes - The PV segment encoded in UTF-8.
    """

    # fmt: off
    return SEPARATOR.join(
        [
            "PV1",                                  # PV1-0
            "0001",                                 # PV1-1
            generate_patient_class(),               # PV1-2
            "",                                     # PV1-3
            generate_admission_type(),              # PV1-4
            "",                                     # PV1-5
            "",                                     # PV1-6
            generate_doctor_name(),                 # PV1-7
            generate_doctor_name(),                 # PV1-8
            generate_doctor_name(),                 # PV1-9
            generate_hospital_service(),            # PV1-10
            "",                                     # PV1-11
            "",                                     # PV1-12
            "R" if random.random() < 0.05 else "",  # PV1-13
            generate_admit_source(),                # PV1-14
            generate_ambulatory_status(),           # PV1-15
            "",                                     # PV1-16
            generate_doctor_name(),                 # PV1-17
            "^" * 26,                               # PV1-18 - PV1-43
            generate_random_date_time(),            # PV1-44
            generate_random_date_time(),            # PV1-45
        ]
    ).encode("utf-8")
    # fmt: on


def generate_segment(
    segment_type: str, message_type: str, trigger_event: str, message_structure: str
) -> bytes:
    match segment_type:
        case "MSH":
            component_sep = DELIMITERS[0]
            message_type = component_sep.join([message_type, trigger_event, message_structure])
            return generate_msh_segment(message_type)
        case "EVN":
            return generate_evn_segment(trigger_event)
        case "PID":
            return generate_pid_segment()
        case "PV1":
            return generate_pv1_segment()
        case _:
            # too common to log right now
            return b""


def generate_segments(
    segments: list[dict[str, Any]],
    message_type: str,
) -> bytes:
    message_structure = message_type
    message_type, trigger_event = message_type.split("_")

    generated_segments = []

    for segment in segments:
        is_group = "segments" in segment
        identifier = segment["identifier"]
        required = segment["required"]
        repeatable = segment["repeatable"]

        if required and repeatable:
            count = random.randint(1, 3)
        elif required:
            count = 1
        elif repeatable:
            count = random.randint(0, 3)
        else:
            count = 1 if random.random() < 0.5 else 0

        for _ in range(count):
            if is_group:
                multi_segment_result = generate_segments(segment["segments"], message_structure)
                if multi_segment_result:
                    generated_segments.append(multi_segment_result)
            else:
                segment_type = identifier
                single_segment_result = generate_segment(
                    segment_type, message_type, trigger_event, message_structure
                )
                if single_segment_result:
                    generated_segments.append(single_segment_result)

    return CR.join(generated_segments)
