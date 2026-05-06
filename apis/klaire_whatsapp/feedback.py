"""Feedback extraction and MongoDB persistence for Klaire aftercare conversations."""
import os
import re
from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient
from pymongo.server_api import ServerApi

MONGO_DB = "KLAIRE"

_client: Optional[MongoClient] = None

_NOT_TAKING_PATTERNS = [
    r"stop(?:ped)?\s+tak",
    r"didn.t\s+(?:finish|take|complete)",
    r"not\s+tak",
    r"haven.t\s+tak",
    r"forgot",
    r"didn.t\s+do\s+the",
    r"haven.t\s+done",
]
_COMPLAINT_PATTERNS = [
    r"\bbad\b",
    r"terrible",
    r"complain",
    r"disappoint",
    r"poor\s+service",
    r"unfair",
    r"overcharge",
]


def _col():
    global _client
    if _client is None:
        mongo_uri = os.environ.get("MONGO_URI")
        if not mongo_uri:
            raise RuntimeError("MONGO_URI environment variable is not set.")
        _client = MongoClient(mongo_uri, server_api=ServerApi("1"))
    return _client[MONGO_DB]["klaire_feedback"]


def extract_rating(text: str) -> Optional[int]:
    """Extract a 1-5 integer rating from free text. Returns None if not found."""
    matches = re.findall(r"\b([1-5])\b", text)
    if matches:
        return int(matches[0])
    return None


def detect_adherence_flag(text: str) -> bool:
    """True if enrollee indicates they stopped or skipped medication/procedure."""
    t = text.lower()
    return any(re.search(p, t) for p in _NOT_TAKING_PATTERNS)


def detect_escalation(text: str) -> bool:
    """True if rating is 1-2 or complaint language is detected."""
    rating = extract_rating(text)
    if rating is not None and rating <= 2:
        return True
    t = text.lower()
    return any(re.search(p, t) for p in _COMPLAINT_PATTERNS)


def save_feedback(
    enrollee_id: str,
    panumber: str,
    hospital: str,
    rating: Optional[int],
    comment: Optional[str],
    adherence_flag: bool,
    escalated: bool,
) -> None:
    """Insert a feedback document into the klaire_feedback collection."""
    _col().insert_one({
        "enrollee_id": enrollee_id,
        "panumber": panumber,
        "hospital": hospital,
        "rating": rating,
        "comment": comment,
        "adherence_flag": adherence_flag,
        "escalated": escalated,
        "created_at": datetime.now(timezone.utc),
    })
