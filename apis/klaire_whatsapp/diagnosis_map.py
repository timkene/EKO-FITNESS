"""ICD-10 code to plain-English description for Klaire aftercare messages."""
from typing import Optional

# Each tuple: (start_prefix, end_prefix, plain_english_label)
_RANGES = [
    ("B50", "B54", "malaria treatment"),
    ("A00", "A09", "stomach infection / diarrhoea"),
    ("J00", "J06", "cold or throat infection"),
    ("J40", "J47", "respiratory / breathing condition"),
    ("O00", "O99", "pregnancy care"),
    ("I10", "I16", "blood pressure management"),
    ("E10", "E14", "diabetes management"),
    ("K25", "K31", "ulcer / stomach pain"),
    ("N30", "N39", "urinary tract infection"),
    ("S00", "T98", "injury treatment"),
    ("K70", "K77", "liver condition"),
    ("M00", "M99", "joint or bone pain"),
    ("F00", "F99", "mental health condition"),
    ("H00", "H59", "eye condition"),
    ("H60", "H95", "ear condition"),
]


def icd10_to_plain(code: Optional[str]) -> str:
    """Convert ICD-10 code to plain English. Falls back to generic phrase."""
    if not code:
        return "a recent health condition"
    prefix = code[:3].upper()
    for start, end, label in _RANGES:
        if start <= prefix <= end:
            return label
    return "a recent health condition"
