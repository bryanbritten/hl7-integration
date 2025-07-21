from datetime import datetime, timezone
from hl7apy.core import Message


class ADTA01QualityChecker:
    def __init__(self, message: Message):
        self.message = message
        self.issues = []

    @staticmethod
    def is_placeholder(
        value: str,
        placeholder_values: set[str] = {"test", "n/a", "null", "asdf", "unknown", ""},
    ) -> bool:
        return value in placeholder_values

    def check_demographic_completeness(self) -> None:
        msg = self.message
        name = msg.pid.patient_name[0].to_er7() if msg.pid.patient_name else ""
        dob = msg.pid.date_time_of_birth.value if msg.pid.date_time_of_birth else None
        sex = msg.pid.administrative_sex.value if msg.pid.administrative_sex else ""
        address = msg.pid.patient_address[0].to_er7() if msg.pid.patient_address else ""

        if not name or ADTA01QualityChecker.is_placeholder(name):
            issue = {
                "type": "demographic",
                "severity": 5,
                "message": "Missing or invalid patient name (PID-5).",
            }
            self.issues.append(issue)

        if not dob:
            issue = {
                "type": "demographic",
                "severity": 1,
                "message": "Missing date of birth (PID-7).",
            }
            self.issues.append(issue)
        else:
            try:
                birth_date = datetime.strptime(dob, "%Y%m%d%H%M%S")
                age = (
                    datetime.now(timezone.utc) - birth_date.replace(tzinfo=timezone.utc)
                ).days // 365
                if age < 0 or age > 120:
                    issue = {
                        "type": "demographic",
                        "severity": 3,
                        "message": "Unrealistic age based on DOB (PID-7).",
                    }
                    self.issues.append(issue)
            except ValueError:
                issue = {
                    "type": "demographic",
                    "severity": 3,
                    "message": "Malformed date of birth (PID-7).",
                }
                self.issues.append(issue)

        if sex and (
            sex not in {"M", "F", "O"} or ADTA01QualityChecker.is_placeholder(sex)
        ):
            issue = {
                "type": "demographic",
                "severity": 3,
                "message": "Invalid sex code (PID-8).",
            }
            self.issues.append(issue)

        if not address or ADTA01QualityChecker.is_placeholder(address):
            issue = {
                "type": "demographic",
                "severity": 3,
                "message": "Missing or invalid patient address (PID-11).",
            }
            self.issues.append(issue)

    def check_temporal_logic(self) -> None:
        msg = self.message
        admit_str = msg.pv1.admit_date_time.value if msg.pv1.admit_date_time else None
        discharge_str = (
            msg.pv1.discharge_date_time.value if msg.pv1.discharge_date_time else None
        )
        now = datetime.now(timezone.utc)

        try:
            if admit_str:
                admit = datetime.strptime(admit_str, "%Y%m%d%H%M%S").replace(
                    tzinfo=timezone.utc
                )
                if admit > now:
                    issue = {
                        "type": "temporal",
                        "severity": 4,
                        "message": "Admission date/time is in the future (PV1-44).",
                    }
                    self.issues.append(issue)
            if discharge_str:
                discharge = datetime.strptime(discharge_str, "%Y%m%d%H%M%S").replace(
                    tzinfo=timezone.utc
                )
                if discharge > now:
                    issue = {
                        "type": "temporal",
                        "severity": 4,
                        "message": "Discharge date/time is in the future (PV1-45).",
                    }
                    self.issues.append(issue)
            if admit_str and discharge_str:
                # admit and discharge will already be defined here because of necessary conditions to reach this point
                if discharge < admit:  # type: ignore
                    issue = {
                        "type": "temporal",
                        "severity": 4,
                        "message": "Discharge time precedes admission time (PV1-45).",
                    }
                    self.issues.append(issue)
        except ValueError:
            issue = {
                "type": "temporal",
                "severity": 3,
                "message": "Malformed admit or discharge datetime.",
            }
            self.issues.append(issue)

    def check_provider_info(self) -> None:
        msg = self.message
        provider = (
            msg.pv1.attending_doctor[0].to_er7() if msg.pv1.attending_doctor else ""
        )

        if not provider or ADTA01QualityChecker.is_placeholder(provider):
            issue = {
                "type": "provider",
                "severity": 1,
                "message": "Missing or invalid attending provider (PV1-7).",
            }
            self.issues.append(issue)

    def check_message_header_info(self) -> None:
        msg = self.message
        message_type = msg.msh.message_type.to_er7() if msg.msh.message_type else ""
        sending_fac = (
            msg.msh.sending_facility.to_er7() if msg.msh.sending_facility else ""
        )
        receiving_fac = (
            msg.msh.receiving_facility.to_er7() if msg.msh.receiving_facility else ""
        )

        if message_type != "ADT^A01":
            issue = {
                "type": "header",
                "severity": 5,
                "message": "Unexpected message type in MSH-9. Expected ADT^A01.",
            }
            self.issues.append(issue)

        if not sending_fac or ADTA01QualityChecker.is_placeholder(sending_fac):
            issue = {
                "type": "header",
                "severity": 3,
                "message": "Missing or invalid sending facility (MSH-4).",
            }
            self.issues.append(issue)

        if not receiving_fac or ADTA01QualityChecker.is_placeholder(receiving_fac):
            issue = {
                "tyep": "header",
                "severity": 3,
                "message": "Missing or invalid receiving facility (MSH-6).",
            }
            self.issues.append(issue)

    def run_all_checks(self):
        self.check_demographic_completeness()
        self.check_temporal_logic()
        self.check_provider_info()
        self.check_message_header_info()
        return self.issues
