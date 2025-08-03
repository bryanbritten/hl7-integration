from hl7apy.core import Segment
from hl7apy.parser import parse_message, parse_segment


def get_msh_segment(message: bytes) -> Segment:
    msg = parse_message(message)
    return parse_segment(msg.msh.to_er7())
