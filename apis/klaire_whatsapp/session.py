"""MongoDB session management for Klaire WhatsApp conversations."""
import os
from datetime import datetime, timezone
from typing import Optional
from pymongo import MongoClient
from pymongo.server_api import ServerApi

MONGO_DB = "KLAIRE"
MAX_MESSAGES = 10

_client: Optional[MongoClient] = None


def _col():
    global _client
    if _client is None:
        mongo_uri = os.environ.get("MONGO_URI")
        if not mongo_uri:
            raise RuntimeError("MONGO_URI environment variable is not set.")
        _client = MongoClient(mongo_uri, server_api=ServerApi("1"))
    return _client[MONGO_DB]["klaire_sessions"]


def load_session(phone: str) -> dict:
    """Load session for a phone number. Returns a default session if not found."""
    doc = _col().find_one({"phone": phone})
    if not doc:
        return {
            "phone": phone,
            "enrollee_id": None,
            "mode": "front_desk",
            "messages": [],
            "aftercare_context": None,
        }
    doc.pop("_id", None)
    return doc


def save_session(phone: str, update: dict) -> None:
    """Upsert session fields for a phone number."""
    update["updated_at"] = datetime.now(timezone.utc)
    _col().update_one(
        {"phone": phone},
        {"$set": update},
        upsert=True,
    )


def append_message(phone: str, role: str, content: str) -> None:
    """Append message to session history, capped at MAX_MESSAGES."""
    _col().update_one(
        {"phone": phone},
        {
            "$push": {
                "messages": {
                    "$each": [{"role": role, "content": content, "ts": datetime.now(timezone.utc)}],
                    "$slice": -MAX_MESSAGES,
                }
            },
            "$set": {"updated_at": datetime.now(timezone.utc)},
        },
        upsert=True,
    )
