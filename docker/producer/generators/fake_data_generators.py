import random
from datetime import datetime

from faker import Faker

from decorators.error_rates import with_error_rate

faker = Faker()


@with_error_rate(error_rate=0.01)
def generate_patient_name() -> str:
    """
    Generates a random patient name using Faker.

    Returns: str - A random patient name.
    """

    name = f"{faker.last_name()}^{faker.first_name()}"
    # Pretend 20% of English names include a middle name
    if random.random() < 0.2:
        name = f"{name}^{faker.first_name()}"
    return name


@with_error_rate(error_rate=0.05)
def generate_patient_birth_date(min_age: int = 0, max_age: int = 100) -> str:
    """
    Generates a random patient birth date using Faker.

    min_age: int - Minimum age of the patient.
    max_age: int - Maximum age of the patient.
    Returns: str - A random patient birth date in the format YYYYMMDDHHMMSS.
    """

    dob = faker.date_of_birth(minimum_age=min_age, maximum_age=max_age)
    return dob.strftime("%Y%m%d%H%M%S")


@with_error_rate(error_rate=0.65)
def generate_patient_race() -> str:
    """
    Generates a random race for the patient according to the HL7 table 0005.
    The race is in no way tied to the randomly generated patient name and
    any biases, stereotypes, inaccuracies, or offenses are unintentional.

    Returns: str - An HL7v2 appropriate value based on a random value from HL7 table 0005.
    """

    race = random.choice(
        [
            "American Indian or Alaska Native",
            "Asian",
            "Black or African American",
            "Native Hawaiian or Other Pacific Islander",
            "White",
            "Other Race",
        ]
    )

    return f"2106-3^{race}^HL70005"


@with_error_rate(error_rate=0.33)
def generate_patient_address() -> str:
    """
    Generates a random patient address using Faker.

    Returns: str - A random patient address.
    """

    street = faker.street_address()
    city = faker.city()
    state = faker.state_abbr()
    zip_code = faker.zipcode()

    return f"{street}^^{city}^{state}^{zip_code}"


@with_error_rate(error_rate=0.2)
def generate_patient_phone_number() -> str:
    """
    Generates a random patient phone number using Faker.

    Returns: str - A random patient phone number.
    """
    return faker.phone_number()


@with_error_rate(error_rate=0.1)
def generate_patient_marital_status() -> str:
    """
    Generates a random marital status for the patient.

    Returns: str - A random marital status.
    """

    return random.choice(
        ["A", "B", "C", "D", "E", "G", "I", "M", "N", "O", "P", "R", "S", "T", "U", "W"]
    )


@with_error_rate(error_rate=0.95)
def generate_patient_ssn() -> str:
    """
    Generates a random patient Social Security Number (SSN).

    Returns: str - A random SSN in the format XXX-XX-XXXX.
    """
    return faker.ssn()


@with_error_rate(error_rate=0.25)
def generate_patient_class() -> str:
    """
    Generates a random patient class.

    Returns: str - A random patient class according to HL7 table 0004.
    """
    return random.choice(["B", "C", "E", "I", "N", "O", "P", "R", "U"])


@with_error_rate(error_rate=0.1)
def generate_admission_type() -> str:
    """
    Generates a random admission type.

    Returns: str - A random admission type according to HL7 table 0007.
    """
    return random.choice(["A", "C", "E", "L", "N", "R", "U"])


@with_error_rate(error_rate=0.15)
def generate_doctor_name() -> str:
    """
    Generates a random doctor name.

    Returns: str - A random doctor name formatted to adhere to HL7v2 PV1-7, PV1-8, and PV1-9 standards.
    """
    return f"^{faker.last_name()}^{faker.first_name()}^^^MD"


@with_error_rate(error_rate=0.5)
def generate_hospital_service() -> str:
    """
    Generates a random hospital service.

    Returns: str - A random hospital service according to HL7 table 0069.
    """
    return random.choice(
        [
            "CAR",
            "MED",
            "PUL",
            "SUR",
            "URO",
        ]
    )


@with_error_rate(error_rate=0.1)
def generate_admit_source() -> str:
    """
    Generates a random admit source.

    Returns: str - A random admit source according to HL7 table 0023.
    """
    return random.choice(list(map(str, range(1, 10))))


def generate_ambulatory_status() -> str:
    """
    Generates a random ambulatory status.

    Returns: str - A random ambulatory status according to HL7 table 0009.
    """
    return random.choice(
        [
            "A0",
            "A1",
            "A2",
            "A3",
            "A4",
            "A5",
            "A6",
            "A7",
            "A8",
            "A9",
            "B0",
            "B1",
            "B2",
            "B3",
            "B4",
            "B5",
            "B6",
        ]
    )


@with_error_rate(error_rate=0.05)
def generate_random_date_time() -> str:
    """
    Generates a random date and time in the format YYYYMMDDHHMMSS.

    Returns: str - A random date and time.
    """
    start = datetime.strptime("2021-01-01", "%Y-%m-%d")
    end = datetime.strptime("2025-07-19", "%Y-%m-%d")
    return faker.date_time_between(start_date=start, end_date=end).strftime("%Y%m%d%H%M%S")


@with_error_rate(error_rate=0.10)
def generate_gender() -> str:
    """
    Generates a random gender value.

    Returns: str - Either "M", "F", or "O"
    """
    return random.choice(["M", "F", "O"])


@with_error_rate(error_rate=0.01)
def generate_uuid() -> str:
    """
    Generates a random UUID value.

    Returns: str - A random UUID
    """
    return faker.uuid4()
