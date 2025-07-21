import random

from faker import Faker
from helpers import (
    generate_patient_name,
    generate_patient_birth_date,
    generate_patient_race,
    generate_patient_address,
    generate_patient_phone_number,
    generate_patient_marital_status,
    generate_patient_ssn,
    generate_patient_class,
    generate_admission_type,
    generate_doctor_name,
    generate_hospital_service,
    generate_admit_source,
    generate_ambulatory_status,
    generate_random_date_time,
    generate_gender,
)

faker = Faker()

SEPARATOR: str = "|"


def build_msh_segment(message_type: str) -> bytes:
    """
    Builds the MSH segment for HL7 messages. Faker is used to generate a random
    date and time as well as a random UUID for the message control ID.

    message_type: str - The type of HL7 message (e.g., "ADT^A01").
    Returns: bytes - The MSH segment encoded in UTF-8.
    """

    msh_0 = "MSH"
    msh_1 = "|"
    msh_2 = "^~\\&"
    msh_3 = "PythonHL7Producer"
    msh_4 = "Bryan's Lab"
    msh_5 = "PythonHL7Consumer"
    msh_6 = "Bryan's Ingestion Company"
    msh_7 = generate_random_date_time()
    msh_8 = ""
    msh_9 = message_type
    msh_10 = faker.uuid4()
    msh_11 = "T"
    msh_12 = "2.5"

    return msh_1.join(
        [
            msh_0,
            msh_2,
            msh_3,
            msh_4,
            msh_5,
            msh_6,
            msh_7,
            msh_8,
            msh_9,
            msh_10,
            msh_11,
            msh_12,
        ]
    ).encode("utf-8")


def build_pid_segment() -> bytes:
    """
    Builds the PID segment for HL7 messages.

    separator: str - The field separator to use in the PID segment.
    Returns: bytes - The PID segment encoded in UTF-8.
    """

    pid_0 = "PID"
    pid_1 = "1"
    pid_2 = ""
    pid_3 = faker.uuid4()
    pid_4 = ""
    pid_5 = generate_patient_name()
    pid_6 = faker.last_name()
    pid_7 = generate_patient_birth_date()
    pid_8 = generate_gender()
    pid_9 = ""
    pid_10 = generate_patient_race()
    pid_11 = generate_patient_address()
    pid_12 = ""
    pid_13 = generate_patient_phone_number()
    pid_14 = ""
    pid_15 = ""
    pid_16 = generate_patient_marital_status()
    pid_17 = ""
    pid_18 = ""
    pid_19 = generate_patient_ssn()

    return SEPARATOR.join(
        [
            pid_0,
            pid_1,
            pid_2,
            pid_3,
            pid_4,
            pid_5,
            pid_6,
            pid_7,
            pid_8,
            pid_9,
            pid_10,
            pid_11,
            pid_12,
            pid_13,
            pid_14,
            pid_15,
            pid_16,
            pid_17,
            pid_18,
            pid_19,
        ]
    ).encode("utf-8")


def build_evn_segment(event_type: str = "A01") -> bytes:
    """
    Builds the EVN segment for HL7 messages.

    event_type: str - The type of event (e.g., "A01").
    separator: str - The field separator to use in the EVN segment.
    Returns: bytes - The EVN segment encoded in UTF-8.
    """

    evn_0 = "EVN"
    evn_1 = event_type
    evn_2 = generate_random_date_time()

    return SEPARATOR.join([evn_0, evn_1, evn_2]).encode("utf-8")


def build_pv1_segment(sequence_number: int = 1) -> bytes:
    """
    Builds a PV1 segment for HL7 messages.

    segment_number: int - Used to differentiate multiple PV segments.
    separator: str - The field separator to use in the PV segment.
    Returns: bytes - The PV segment encoded in UTF-8.
    """

    pv1_0 = "PV1"
    pv1_1 = str(sequence_number)
    pv1_2 = generate_patient_class()
    pv1_3 = ""
    pv1_4 = generate_admission_type()
    pv1_5 = ""
    pv1_6 = ""
    pv1_7 = generate_doctor_name()
    pv1_8 = generate_doctor_name()
    pv1_9 = generate_doctor_name()
    pv1_10 = generate_hospital_service()
    pv1_ll = ""
    pv1_12 = ""
    pv1_13 = "R" if random.random() < 0.05 else ""  # 5% of admissions are readmissions
    pv1_14 = generate_admit_source()
    pv1_15 = generate_ambulatory_status()
    pv1_16 = ""
    pv1_17 = generate_doctor_name()
    pv1_18_to_43 = "^" * 26
    pv1_44 = generate_random_date_time()
    pv1_45 = generate_random_date_time()

    return SEPARATOR.join(
        [
            pv1_0,
            pv1_1,
            pv1_2,
            pv1_3,
            pv1_4,
            pv1_5,
            pv1_6,
            pv1_7,
            pv1_8,
            pv1_9,
            pv1_10,
            pv1_ll,
            pv1_12,
            pv1_13,
            pv1_14,
            pv1_15,
            pv1_16,
            pv1_17,
            pv1_18_to_43,
            pv1_44,
            pv1_45,
        ]
    ).encode("utf-8")
