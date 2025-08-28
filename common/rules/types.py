from dataclasses import dataclass
from typing import Callable, Literal, Optional

from hl7apy.core import Message, Segment

IssueType = Literal[
    "InvalidValue",
    "MissingComponent",
    "MissingField",
    "Pipeline",
    "Rule",
]

StampType = Literal["Pass", "Fail"]


@dataclass(frozen=True)
class Issue:
    type: IssueType
    severity: int  # 1: Info 2: Warning 3: Critical
    message: str


@dataclass(frozen=True)
class Stamp:
    status: StampType


@dataclass(frozen=True)
class RuleResult:
    stamp: Stamp
    issues: Optional[Issue]


Rule = Callable[[Message | Segment]]
