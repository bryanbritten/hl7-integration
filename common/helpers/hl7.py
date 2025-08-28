from typing import Any

from hl7apy.core import Message, Segment
from hl7apy.exceptions import ParserError
from hl7apy.parser import parse_message, parse_segment

from common.rules.types import Issue, Rule, RuleResult, Stamp


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
    def __init__(self, message: Message, message_type: str, message_registry: dict[str, Any]):
        self.message = message
        self.message_type = message_type
        self.message_registry = message_registry
        self.message_schema = self.get_message_schema()

    def get_message_schema(self) -> dict[str, Any]:
        message_schema = self.message_registry.get(self.message_type)
        if not message_schema:
            raise ValidationError(f"Schema not defined for message type: {self.message_type}")
        return message_schema

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
        rules: list[Rule] | None = self.rule_registry.get(self.message_type)
        if not rules:
            raise QualityCheckError(f"Rules not defined for message type: {self.message_type}")

        return {rule.__name__: rule for rule in rules}

    def print_rules(self) -> None:
        for rule_name in self.rules.keys():
            print(rule_name)

    def run_rule(self, rule: str) -> RuleResult:
        _run = self.rules.get(rule)

        if not _run:
            result = RuleResult(
                Stamp("Fail"),
                Issue(
                    "Rule",
                    1,
                    f"{rule} is not a defined rule for {self.message_type} message types",
                ),
            )
        else:
            result = _run(self.message)

        result.rule = rule
        return result

    def run_rules(self, rules: list[str]) -> list[RuleResult]:
        results = []
        for rule in rules:
            result = self.run_rule(rule)
            results.append(result)
        return results

    def run_all_rules(self) -> list[RuleResult]:
        all_rules = list(self.rules.keys())
        return self.run_rules(all_rules)


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


def get_message_type(message: Message) -> str:
    msg_type = message.msh.msh_9.msh_9_3.value  # type: ignore
    if msg_type:
        return msg_type

    msh_9_1 = message.msh.msh_9.msh_9_1.value  # type: ignore
    msh_9_2 = message.msh.msh_9.msh_9_2.value  # type: ignore
    if msh_9_1 and msh_9_2:
        return f"{msh_9_1}_{msh_9_2}"
    else:
        return ""
