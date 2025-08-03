import logging

from hl7_helpers import MESSAGE_REGISTRY
from hl7apy.core import Message
from hl7apy.exceptions import ParserError
from hl7apy.parser import parse_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


class HL7Validator:
    def __init__(self, message: bytes):
        self.raw_message = message.decode("utf-8")
        self.parsed_message = HL7Validator.__parse(message.decode("utf-8"))
        self.message_type = self.parsed_message.msh.msh_9.msh_9_3.to_er7()
        self.message_schema = MESSAGE_REGISTRY.get(self.message_type)

    @staticmethod
    def __parse(raw_message) -> Message:
        """
        Parses the raw HL7 message if valid.

        Returns: hl7apy.core.Message - The parsed HL7 message if the message is valid.
        Otherwise returns an empty message.
        """
        try:
            parsed_message = parse_message(raw_message)
            return parsed_message
        except ParserError as e:
            logger.error(f"Failed to parse message: {str(e)}")
            return Message("ADT_A01")

    def has_required_segments(self):
        """
        Iterates through the required segments of an HL7 message, as defined by the message
        registry, and confirms that segment is present in the message.

        Returns: bool - True if all required segments are present, else False.
        """
        if self.message_schema is None:
            logger.error(f"Unrecognized message type: {self.message_type}")
            return False

        required_segments = [
            segment["identifier"]
            for segment in self.message_schema["segments"]
            if segment["required"]
        ]

        return all(
            [
                self.parsed_message.children.get(required_segment)
                for required_segment in required_segments
            ]
        )

    def all_segments_are_valid(self):
        """
        Iterates through all the segments in a given HL7 message and checks the message
        registry to determine if that segment should be present in the given message type.

        NOTE: Because the hl7apy library automatically groups segments based on the segment
        and the message type, segments like IN1 in an ADT_A01 message will automatically be
        grouped into a group named after the message type and segment group (e.g.
        ADT_A01_INSURANCE), which maps directly to how the segment is named in the message
        registry. Thus, the assumption is that if a segment like "ADT_A01_INSURANCE" is in
        the segment names, then the segment is valid, without the need to specifically check
        each underlying segment that make up the segment group.

        Returns: bool - True if all segments are valid segments as defined by the HL7 standard,
        else False.
        """
        valid_segments = [
            segment["identifier"]
            for segment in self.message_schema["segments"]
            if segment.get("segments") is None
        ]
        return all(
            [child.name in valid_segments for child in self.parsed_message.children]
        )

    def segment_cardinality_is_valid(self):
        """
        Iterates through all the segments in a given HL7 message and ensures each segment is
        present only once, unless the segment can be repeated.

        NOTE: HL7v2.5 ADT_A01 and ADT_A03 only contain segments that are not repeatable or are
        repeatable an infinite number of times. As a result, this function does not bother
        counting the number of occurrences, but a production-level integration engine would.

        NOTE: For simplicity, cardinality of segment groups is ignored, but a production-level
        integration engine would need to check to ensure that, for example, an IN2 segment is
        matched with an IN1 segment.

        Returns: bool - True if all segments are present one time unless they can be repeated,
        else False.
        """
        seen = set()
        repeatable_segments = [
            segment["identifier"]
            for segment in self.message_schema["segments"]
            if segment["repeatable"] == True
        ]
        for child in self.parsed_message.children:
            if child.name in seen and child.name not in repeatable_segments:
                return False
            seen.add(child.name)

        return True

    def message_is_valid(self):
        return (
            self.has_required_segments()
            and self.all_segments_are_valid()
            and self.segment_cardinality_is_valid()
        )
