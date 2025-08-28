from hl7apy.core import Message

from common.rules.constants import PLACEHOLDERS
from common.rules.types import Issue, RuleResult, Stamp


def patient_name_is_present(message: Message) -> RuleResult:
    if not message.pid.pid_5:
        return RuleResult(Stamp("Fail"), Issue("MissingField", 2, "PID-5 is missing"))
    return RuleResult(Stamp("Pass"), None)


def given_name_is_not_placeholder(message: Message) -> RuleResult:
    if message.pid.pid_5.pid_5_1 in PLACEHOLDERS:
        return RuleResult(Stamp("Fail"), Issue("InvalidValue", 2, "PID-5.1 is a placeholder"))
    return RuleResult(Stamp("Pass"), None)


def family_name_is_not_placeholder(message: Message) -> RuleResult:
    if message.pid.pid_5.pid_5_2 in PLACEHOLDERS:
        return RuleResult(Stamp("Fail"), Issue("InvalidValue", 2, "PID-5.2 is a placeholder"))
    return RuleResult(Stamp("Pass"), None)
