import boto3
from botocore.config import Config
from hl7apy.core import Message
from hl7apy.exceptions import ParserError
from hl7apy.parser import parse_message


class HL7Validator:
    def __init__(self, message: bytes):
        self.raw_message = message.decode("utf-8")
        self.parsed_message = HL7Validator._parse(message.decode("utf-8"))

    @staticmethod
    def _parse(raw_message) -> Message:
        """
        Parses the raw HL7 message if valid.

        Returns: hl7apy.core.Message - The parsed HL7 message.
        """
        try:
            parsed_message = parse_message(raw_message)
            return parsed_message
        except ParserError as e:
            print(f"Failed to parse message.")
            return Message("ADT_A01")

    def has_msh_segment(self) -> bool:
        """
        Checks if the HL7 message contains an MSH segment.

        returns: bool - True if MSH segment is present, False otherwise.
        """
        return self.parsed_message.msh is not None

    def has_pid_segment(self) -> bool:
        """
        Checks if the HL7 message contains an PID segment.

        returns: bool - True if PID segment is present, False otherwise.
        """
        return self.parsed_message.pid is not None

    def has_evn_segment(self) -> bool:
        """
        Checks if the HL7 message contains an EVN segment.

        returns: bool - True if EVN segment is present, False otherwise.
        """
        return self.parsed_message.evn is not None

    def has_pv1_segment(self) -> bool:
        """
        Checks if the HL7 message contains a PV1 segment.

        returns: bool - True if PV1 segment is present, False otherwise.
        """
        return self.parsed_message.pv1 is not None

    def msh_segment_is_valid(self) -> bool:
        """
        Validates the MSH segment of the HL7 message.

        returns: bool - True if MSH segment is valid, False otherwise.
        """
        if not self.has_msh_segment():
            return False

        msh = self.parsed_message.msh

        if not msh.msh_2 or len(msh.msh_2.value) < 4:
            return False

        if (
            not msh.msh_7
            or not msh.msh_9
            or not msh.msh_10
            or not msh.msh_11
            or not msh.msh_12
        ):
            return False

        return True

    def pid_segment_is_valid(self) -> bool:
        """
        Validates the PID segment of the HL7 message.

        returns: bool - True if PID segment is valid, False otherwise.
        """
        if not self.has_pid_segment():
            return False

        pid = self.parsed_message.pid
        if not pid.pid_3 or not pid.pid_5:
            return False

        return True

    def evn_segment_is_valid(self) -> bool:
        """
        Validates the EVN segment of the HL7 message.

        returns: bool - True if EVN segment is valid, False otherwise.
        """
        if not self.has_evn_segment():
            return False

        evn = self.parsed_message.evn
        if not evn.evn_1 or not evn.evn_2:
            return False

        return True

    def pv1_segment_is_valid(self) -> bool:
        """
        Validates the PV1 segment of the HL7 message.

        returns: bool - True if PV1 segment is valid, False otherwise.
        """
        if not self.has_pv1_segment():
            return False

        pv1 = self.parsed_message.pv1
        if not pv1.pv1_2 or len(pv1.pv1_2.value) > 1:
            return False

        return True

    def message_is_valid(self) -> bool:
        """
        Validates the entire HL7 message.

        returns: bool - True if the message is valid, False otherwise.
        """

        if not self.has_msh_segment():
            return False
        if not self.msh_segment_is_valid():
            return False
        if not self.has_pid_segment():
            return False
        if not self.pid_segment_is_valid():
            return False
        if not self.has_evn_segment():
            return False
        if not self.evn_segment_is_valid():
            return False
        if not self.has_pv1_segment():
            return False
        if not self.pv1_segment_is_valid():
            return False
        return True
