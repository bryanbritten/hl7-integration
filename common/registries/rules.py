from common.rules.segments.pid import (
    family_name_is_not_placeholder,
    given_name_is_not_placeholder,
    patient_name_is_present,
)

RULE_REGISTRY = {
    "ADT_A01": [
        patient_name_is_present,
        given_name_is_not_placeholder,
        family_name_is_not_placeholder,
    ],
    "ADT_A03": [
        patient_name_is_present,
        given_name_is_not_placeholder,
        family_name_is_not_placeholder,
    ],
    "ORU_R01": [],
}
