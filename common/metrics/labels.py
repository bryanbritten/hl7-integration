# Used in the Consumer service when validating a message on ingest
REASON_MISSING_MSH10 = "Missing MSH-10 Field"
REASON_MISSING_SEGMENTS = "Missing Required Segments"
REASON_INVALID_SEGMENTS = "Contains Invalid Segments"
REASON_INVALID_CARDINALITY = "Invalid Segment Cardinality"
REASON_PARSING_ERROR = "Message Failed Parsing"
REASON_UNKNOWN = "Unexpected Error Occurred"

# Used in the Transformer service when trying to convert to FHIR
REASON_EMPTY_RESULTS = "Empty FHIR Conversion Results"
REASON_HTTP_ERROR = "NON-200 FHIR API Code"
