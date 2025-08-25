from dataclasses import dataclass
from typing import Any, Callable, Literal

from hl7apy.core import Message, Segment
from hl7apy.exceptions import ParserError
from hl7apy.parser import parse_message, parse_segment

PLACEHOLDERS = {"test", "n/a", "null", "asdf", "unknown", ""}

IssueType = Literal[
    "rule",
    "demographic",
    "temporal",
    "provider",
    "header",
    "structure",
    "other",
]


@dataclass(frozen=True)
class Issue:
    type: IssueType
    severity: int
    message: str
    field: str | None = None


Rule = Callable[[Message], list[Issue]]


class RuleError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class ValidationError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class QualityCheckError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class HL7Parser:
    def __init__(self):
        pass

    @staticmethod
    def parse(message: bytes) -> Message:
        """
        Parses the raw HL7 message if valid.

        Returns: hl7apy.core.Message - The parsed HL7 message if the message is valid.
        Otherwise returns an empty message.
        """
        parsed_message = parse_message(message.decode("utf-8"))
        return parsed_message


class HL7Validator:
    def __init__(self, message: Message, message_type: str):
        self.message = message
        self.message_type = message_type
        self.message_schema = self.get_message_schema()

    def get_message_schema(self) -> dict[str, Any]:
        message_schema = MESSAGE_REGISTRY.get(self.message_type)
        if not message_schema:
            raise ValidationError(f"Schema not defined for message type: {self.message_type}")
        return message_schema

    def validate_or_raise(self) -> None:
        missing = self.get_missing_required_segments()
        if missing:
            raise ValidationError(f"Missing required segments: {', '.join(missing)}")

        invalid = self.get_invalid_segments()
        if invalid:
            raise ValidationError(
                f"Invalid segments for {self.message_type}: {', '.join(invalid)}"
            )

        violations = self.get_segment_cardinality_violations()
        if violations:
            raise ValidationError(f"Cardinality violations: {', '.join(violations)}")

    def get_missing_required_segments(self) -> list[str]:
        """
        Iterates through the required segments of an HL7 message, as defined by the message
        registry, and flags if that segment is not present in the message.

        Returns: list[str] - A list of all segment names (e.g. "MSH") that are required but
        missing from the message.
        """
        if not self.message.children:
            raise ValueError(f"Invalid message provided")

        schema = self.message_schema
        required_segments = set(
            [
                segment["identifier"]
                for segment in schema.get("segments", [])
                if segment.get("required")
            ]
        )
        present_segments = {child.name for child in self.message.children}
        return list(required_segments - present_segments)

    def has_required_segments(self) -> bool:
        """
        Iterates through the required segments of an HL7 message, as defined by the message
        registry, and confirms that all required segments are present in the message.

        Returns: bool - True if all required segments are present, else False.
        """
        missing_segments = self.get_missing_required_segments()
        return len(missing_segments) == 0

    def get_invalid_segments(self) -> list[str]:
        """
        Iterates through all the segments in a given HL7 message and checks the message
        registry to determine if that segment should be present in the given message type.
        Cardinality of the segment is validated in another method. This method simply confirms
        that a segment of that type is a valid segment type given the message type.

        NOTE: Because the hl7apy library automatically groups segments based on the segment
        and the message type, segments like IN1 in an ADT_A01 message will automatically be
        grouped into a group named after the message type and segment group (e.g.
        ADT_A01_INSURANCE), which maps directly to how the segment is named in the message
        registry. Thus, the assumption is that if a segment like "ADT_A01_INSURANCE" is in
        the segment names, then the segment is valid, without the need to specifically check
        each underlying segment that make up the segment group.

        Returns: list[str] - A list of segment names (e.g "MSH") that are invalid for the given
        message type.
        """
        if not self.message.children:
            raise ValueError("Invalid message provided")

        valid_segments = set(
            [segment["identifier"] for segment in self.message_schema["segments"]]
        )
        present_segments = set([child.name for child in self.message.children])
        return list(present_segments - valid_segments)

    def all_segments_are_valid(self) -> bool:
        """
        Iterates through the segments of an HL7 message, as defined by the message
        registry, and confirms that all segments present in the message are valid.

        Returns: bool - True if all present segments are valid, else False.
        """
        invalid_segments = self.get_invalid_segments()
        return len(invalid_segments) == 0

    def get_segment_cardinality_violations(self) -> list[str]:
        """
        Iterates through all the segments in a given HL7 message and ensures each segment is
        present only once, unless the segment can be repeated.

        NOTE: HL7v2.5 ADT_A01 and ADT_A03 only contain segments that are not repeatable or are
        repeatable an infinite number of times. As a result, this function does not bother
        counting the number of occurrences, but a production-level integration engine would.

        NOTE: For simplicity, cardinality of segment groups is ignored, but a production-level
        integration engine would need to check to ensure that, for example, an IN2 segment is
        matched with an IN1 segment.

        Returns: list[str] - A list of all segment names (e.g. "MSH") that are repeated more than
        is allowed for the given message type.
        """
        if not self.message.children:
            raise ValueError("Invalid messages provided")

        seen = set()
        violators = []
        repeatable_segments = [
            segment["identifier"]
            for segment in self.message_schema["segments"]
            if segment["repeatable"] == True
        ]
        for child in self.message.children:
            if child.name in seen and child.name not in repeatable_segments:
                violators.append(child.name)
            seen.add(child.name)

        return violators

    def segment_cardinality_is_valid(self):
        """
        Iterates through the segments of an HL7 message, as defined by the message
        registry, and confirms that all segments present in the message are present
        only as frequently as allowed as defined by the HL7 schema.

        Returns: bool - True if all segments have valid cardinality, else False.
        """
        violators = self.get_segment_cardinality_violations()
        return len(violators) == 0


class HL7Checker:
    def __init__(self, message: Message, message_type: str, rule_registry: dict[str, list[Rule]]):
        self.message = message
        self.message_type = message_type
        self.rule_registry = rule_registry
        self.rules = self.get_rules()
        self.issues = []

    def get_rules(self) -> dict[str, Rule]:
        rules = self.rule_registry.get(self.message_type)
        if not rules:
            raise QualityCheckError(f"Rules not defined for message type: {self.message_type}")

        return {rule.__name__: rule for rule in rules}

    def print_rules(self) -> None:
        for rule_name in self.rules.keys():
            print(rule_name)

    def run_rule(self, rule: str) -> list[Issue]:
        fn = self.rules.get(rule)

        if not fn:
            issue = Issue(
                "rule", 1, f"{rule} is not a defined rule for {self.message_type} message types"
            )
            return [issue]

        issues = fn(self.message)
        return issues

    def run_rules(self, rules: list[str]) -> dict[str, list[Issue]]:
        issues = {}
        for rule in rules:
            results = self.run_rule(rule)
            issues[rule] = results
        return issues

    def run_all_rules(self) -> dict[str, list[Issue]]:
        all_rules = list(self.rules.keys())
        return self.run_rules(all_rules)


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
                    segment("IN3", "Insurance - Additional Information - Certification"),
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
                                    segment("PV2", "Patient Visit - Additional Information"),
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
                                    segment("OBX", "Observation Segment", required=True),
                                    segment("NTE", "Notes and Comments", repeatable=True),
                                ],
                            },
                            segment("FT1", "Financial Transaction", repeatable=True),
                            segment("CTI", "Clinical Trial Identification", repeatable=True),
                            {
                                "identifier": "ORU_R01_SPECIMEN",
                                "name": "Specimen",
                                "required": False,
                                "repeatable": True,
                                "segments": [
                                    segment("SPM", "Specimen", required=True),
                                    segment("OBX", "Observation Segment", repeatable=True),
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
