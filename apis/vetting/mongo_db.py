"""
mongo_db.py
────────────
Primary data store for the PROCEDURE_DIAGNOSIS schema.
DuckDB is now READ-ONLY and only used for "AI DRIVEN DATA" tables.
Every PROCEDURE_DIAGNOSIS read and write goes through this module.
"""

import os
import re
import logging
from datetime import datetime
from pymongo import MongoClient, DESCENDING
from pymongo.server_api import ServerApi

logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is not set.")
MONGO_DB = "PROCEDURE_DIAGNOSIS"

_client: MongoClient = None


def _col(name: str):
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, server_api=ServerApi("1"))
        logger.info("MongoDB client connected")
    return _client[MONGO_DB][name]


def _ci(value: str) -> dict:
    """Case-insensitive exact-match regex (strips whitespace)."""
    return {"$regex": f"^{re.escape(str(value).strip())}$", "$options": "i"}


def ensure_indexes():
    """Create indexes for performance. Safe to call multiple times."""
    try:
        _col("vetting_queue").create_index("request_id", unique=True)
        _col("vetting_queue").create_index("status")
        _col("vetting_queue").create_index("created_at")
        _col("vetting_queue").create_index("enrollee_id")
        _col("ai_human_procedure_diagnosis").create_index(
            [("procedure_code", 1), ("diagnosis_code", 1)], unique=True)
        _col("ai_human_procedure_age").create_index(
            [("procedure_code", 1), ("min_age", 1), ("max_age", 1)], unique=True)
        _col("ai_human_procedure_gender").create_index(
            [("procedure_code", 1), ("gender", 1)], unique=True)
        _col("ai_human_diagnosis_age").create_index(
            [("diagnosis_code", 1), ("min_age", 1), ("max_age", 1)], unique=True)
        _col("ai_human_diagnosis_gender").create_index(
            [("diagnosis_code", 1), ("gender", 1)], unique=True)
        _col("ai_human_procedure_class").create_index(
            [("procedure_code_1", 1), ("procedure_code_2", 1)], unique=True)
        _col("ai_human_diagnosis_stacking").create_index("combo_key", unique=True)
        _col("ai_specialist_diagnosis").create_index(
            [("specialist_code", 1), ("diagnosis_code", 1)], unique=True)
        _col("klaire_review_queue").create_index("review_id", unique=True)
        _col("klaire_review_queue").create_index("status")
        _col("klaire_review_queue").create_index("created_at")
        _col("ai_first_line_treatment").create_index(
            [("procedure_code", 1), ("diagnosis_code", 1)], unique=True)
    except Exception as e:
        logger.warning(f"ensure_indexes: {e}")


# ── Master table reads ─────────────────────────────────────────────────────────

def get_age_range(age_type: str):
    """Returns (min_age, max_age) tuple or None."""
    doc = _col("AGE_RANGE").find_one({"age_type": _ci(age_type)}, {"_id": 0})
    return (int(doc["min_age"]), int(doc["max_age"])) if doc else None


def get_all_procedure_masters(limit: int = 1000) -> list:
    """Returns all PROCEDURE_MASTER entries sorted by name, for dropdowns."""
    return list(
        _col("PROCEDURE_MASTER")
        .find({}, {"_id": 0, "procedure_code": 1, "procedure_name": 1, "procedure_class": 1})
        .sort("procedure_name", 1)
        .limit(limit)
    )


def get_procedure_master(procedure_code: str):
    """Returns PROCEDURE_MASTER document dict or None."""
    return _col("PROCEDURE_MASTER").find_one(
        {"procedure_code": _ci(procedure_code)}, {"_id": 0}
    )


def get_diagnosis_master(diagnosis_code: str):
    """Returns DIAGNOSIS_MASTER document dict or None."""
    return _col("DIAGNOSIS_MASTER").find_one(
        {"diagnosis_code": _ci(diagnosis_code)}, {"_id": 0}
    )


def get_gender_type(gender_code: str):
    """Returns the description string for a gender_code, or None."""
    doc = _col("GENDER_TYPE").find_one(
        {"gender_code": _ci(gender_code)}, {"_id": 0}
    )
    return doc.get("description") if doc else None


def get_procedure_diagnosis_comp(procedure_code: str, diagnosis_code: str):
    """Returns PROCEDURE_DIAGNOSIS_COMP document dict or None."""
    return _col("PROCEDURE_DIAGNOSIS_COMP").find_one(
        {"procedure_code": _ci(procedure_code), "diagnosis_code": _ci(diagnosis_code)},
        {"_id": 0}
    )


# ── Procedure-Diagnosis learning ─────────────────────────────────────────────

def upsert_procedure_diagnosis_learning(
    procedure_code: str, procedure_name: str,
    diagnosis_code: str, diagnosis_name: str,
    is_valid: bool, confidence: int, reasoning: str,
    approved_by: str,
):
    """Write or update a procedure-diagnosis compatibility entry after agent review.
    Agent confirmation makes the entry immediately trusted (admin_approved=True)
    so the next identical request auto-decides without AI or agent review.
    """
    now = datetime.utcnow().isoformat()
    try:
        _col("ai_human_procedure_diagnosis").update_one(
            {"procedure_code": procedure_code.upper(), "diagnosis_code": diagnosis_code.upper()},
            {
                "$setOnInsert": {
                    "procedure_name": procedure_name,
                    "diagnosis_name": diagnosis_name,
                    "created_at":     now,
                },
                "$set": {
                    "is_valid_match":    is_valid,
                    "match_reason":      reasoning,
                    "ai_confidence":     confidence,
                    "ai_reasoning":      reasoning,
                    "approved_by":       approved_by,
                    "approved_date":     now,
                    "last_used_date":    now,
                    "admin_approved":    True,   # agent confirmation = immediately trusted
                    "admin_approved_at": now,
                },
                "$inc": {"usage_count": 1},
            },
            upsert=True,
        )
    except Exception as e:
        logger.warning(f"upsert_procedure_diagnosis_learning: {e}")


def get_procedure_diagnosis_learning(procedure_code: str, diagnosis_code: str):
    """Exact-match lookup in ai_human_procedure_diagnosis."""
    return _col("ai_human_procedure_diagnosis").find_one(
        {"procedure_code": procedure_code, "diagnosis_code": diagnosis_code},
        {"_id": 0}
    )


def get_procedure_class_learning(code_1: str, code_2: str):
    """Lookup in ai_human_procedure_class (sorted pair)."""
    return _col("ai_human_procedure_class").find_one(
        {"procedure_code_1": _ci(code_1), "procedure_code_2": _ci(code_2)},
        {"_id": 0}
    )


def get_diagnosis_class_learning(code_1: str, code_2: str):
    """Lookup in ai_human_diagnosis_class (sorted pair)."""
    return _col("ai_human_diagnosis_class").find_one(
        {"code_1": code_1, "code_2": code_2},
        {"_id": 0}
    )


def get_procedure_age_learning(procedure_code: str, enrollee_age: int):
    """Find matching age-range rule in ai_human_procedure_age."""
    return _col("ai_human_procedure_age").find_one(
        {
            "procedure_code": _ci(procedure_code),
            "min_age": {"$lte": enrollee_age},
            "max_age": {"$gte": enrollee_age},
        },
        {"_id": 0},
        sort=[("last_used", DESCENDING)]
    )


def get_procedure_gender_learning(procedure_code: str, enrollee_gender: str):
    """Find matching gender rule in ai_human_procedure_gender."""
    return _col("ai_human_procedure_gender").find_one(
        {
            "procedure_code": _ci(procedure_code),
            "gender": _ci(enrollee_gender),
        },
        {"_id": 0},
        sort=[("last_used", DESCENDING)]
    )


def get_diagnosis_age_learning(diagnosis_code: str, enrollee_age: int):
    """Find matching age-range rule in ai_human_diagnosis_age."""
    return _col("ai_human_diagnosis_age").find_one(
        {
            "diagnosis_code": _ci(diagnosis_code),
            "min_age": {"$lte": enrollee_age},
            "max_age": {"$gte": enrollee_age},
        },
        {"_id": 0},
        sort=[("last_used", DESCENDING)]
    )


def get_diagnosis_gender_learning(diagnosis_code: str, enrollee_gender: str):
    """Find matching gender rule in ai_human_diagnosis_gender."""
    return _col("ai_human_diagnosis_gender").find_one(
        {
            "diagnosis_code": _ci(diagnosis_code),
            "gender": _ci(enrollee_gender),
        },
        {"_id": 0},
        sort=[("last_used", DESCENDING)]
    )


# ── Learning table writes ──────────────────────────────────────────────────────

def upsert(collection_name: str, filter_doc: dict, document: dict):
    """Upsert a learning-table document (full replace)."""
    try:
        _col(collection_name).replace_one(filter_doc, document, upsert=True)
    except Exception as e:
        logger.warning(f"upsert {collection_name}: {e}")


def inc_usage(collection_name: str, filter_doc: dict, timestamp_field: str = "last_used_date"):
    """Increment usage_count and refresh timestamp in-place."""
    try:
        _col(collection_name).update_one(
            filter_doc,
            {
                "$inc": {"usage_count": 1},
                "$set": {timestamp_field: datetime.utcnow().isoformat()},
            }
        )
    except Exception as e:
        logger.warning(f"inc_usage {collection_name}: {e}")


# ── Vetting queue ──────────────────────────────────────────────────────────────

def insert_queue(doc: dict):
    """Insert (or replace) a vetting_queue document keyed by request_id."""
    try:
        _col("vetting_queue").replace_one(
            {"request_id": doc["request_id"]}, doc, upsert=True
        )
    except Exception as e:
        logger.warning(f"insert_queue: {e}")


def get_queue_item(request_id: str):
    """Return a single queue document by request_id, or None."""
    return _col("vetting_queue").find_one({"request_id": request_id}, {"_id": 0})


def get_pending_item(request_id: str):
    """Return a PENDING_REVIEW queue item, or None."""
    return _col("vetting_queue").find_one(
        {"request_id": request_id, "status": "PENDING_REVIEW"}, {"_id": 0}
    )


def update_queue(request_id: str, fields: dict):
    """Update specific fields on a queue document."""
    try:
        _col("vetting_queue").update_one(
            {"request_id": request_id}, {"$set": fields}
        )
    except Exception as e:
        logger.warning(f"update_queue: {e}")


def get_pending(limit: int = 50, offset: int = 0) -> list:
    """Return pending-review queue rows as list of dicts."""
    projection = {
        "_id": 0,
        "request_id": 1, "procedure_code": 1, "diagnosis_code": 1,
        "enrollee_id": 1, "encounter_date": 1, "enrollee_age": 1,
        "enrollee_gender": 1, "procedure_name": 1, "diagnosis_name": 1,
        "decision": 1, "confidence": 1, "reasoning": 1,
        "hospital_name": 1, "created_at": 1,
    }
    docs = list(
        _col("vetting_queue")
        .find({"status": "PENDING_REVIEW"}, projection)
        .sort("created_at", 1)
        .skip(offset)
        .limit(limit)
    )
    # rename 'decision' → 'ai_recommendation' to match original API contract
    for d in docs:
        d["ai_recommendation"] = d.pop("decision", None)
    return docs


def count_pending() -> int:
    return _col("vetting_queue").count_documents({"status": "PENDING_REVIEW"})


def get_history(status=None, enrollee_id=None, limit=50, offset=0) -> list:
    """Return request history with optional filters."""
    filt = {}
    if status:
        filt["status"] = status
    if enrollee_id:
        filt["enrollee_id"] = enrollee_id
    projection = {
        "_id": 0,
        "request_id": 1, "procedure_code": 1, "procedure_name": 1,
        "diagnosis_code": 1, "diagnosis_name": 1,
        "enrollee_id": 1, "enrollee_age": 1, "enrollee_gender": 1,
        "encounter_date": 1,
        "status": 1, "decision": 1, "confidence": 1, "reasoning": 1,
        "hospital_name": 1, "created_at": 1, "reviewed_at": 1, "reviewed_by": 1,
        "adjusted_price": 1, "adjusted_quantity": 1, "total_amount": 1,
    }
    return list(
        _col("vetting_queue")
        .find(filt, projection)
        .sort("created_at", -1)
        .skip(offset)
        .limit(limit)
    )


def get_queue_stats_by_status() -> dict:
    """Return {status: count} for all vetting_queue documents."""
    pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    return {r["_id"]: r["count"] for r in _col("vetting_queue").aggregate(pipeline)}


def get_today_queue_stats(today_date: str) -> dict:
    """Return {status: count} for documents created today."""
    pipeline = [
        {"$match": {"created_at": {"$regex": f"^{today_date}"}}},
        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
    ]
    return {r["_id"]: r["count"] for r in _col("vetting_queue").aggregate(pipeline)}


# ── ai_specialist_diagnosis learning table ────────────────────────────────────

def get_specialist_diagnosis_learning(specialist_code: str, diagnosis_code: str):
    """Look up specialist+diagnosis pair in ai_specialist_diagnosis learning table."""
    return _col("ai_specialist_diagnosis").find_one(
        {
            "specialist_code": specialist_code.strip().upper(),
            "diagnosis_code":  diagnosis_code.strip().upper(),
        },
        {"_id": 0}
    )


def upsert_specialist_diagnosis(
    specialist_code: str, specialist_name: str,
    diagnosis_code: str, diagnosis_name: str,
    decision: str, confidence: int, reasoning: str, source: str,
):
    """
    Insert or update a specialist-diagnosis learning entry.
    On insert: usage_count starts at 1.
    On update: usage_count increments, decision/confidence/reasoning update.
    """
    now = datetime.utcnow().isoformat()
    try:
        _col("ai_specialist_diagnosis").update_one(
            {
                "specialist_code": specialist_code.strip().upper(),
                "diagnosis_code":  diagnosis_code.strip().upper(),
            },
            {
                "$setOnInsert": {"created_at": now, "usage_count": 0},
                "$set": {
                    "specialist_name": specialist_name,
                    "diagnosis_name":  diagnosis_name,
                    "decision":        decision.upper(),
                    "confidence":      confidence,
                    "reasoning":       reasoning,
                    "source":          source,
                    "last_used_date":  now,
                },
                "$inc": {"usage_count": 1},
            },
            upsert=True,
        )
    except Exception as e:
        logger.warning(f"upsert_specialist_diagnosis: {e}")


def inc_specialist_diagnosis_usage(specialist_code: str, diagnosis_code: str):
    """Increment usage_count without changing the decision."""
    try:
        _col("ai_specialist_diagnosis").update_one(
            {
                "specialist_code": specialist_code.strip().upper(),
                "diagnosis_code":  diagnosis_code.strip().upper(),
            },
            {
                "$inc": {"usage_count": 1},
                "$set": {"last_used_date": datetime.utcnow().isoformat()},
            }
        )
    except Exception as e:
        logger.warning(f"inc_specialist_diagnosis_usage: {e}")


def update_specialist_diagnosis_decision(
    specialist_code: str, diagnosis_code: str,
    decision: str, reasoning: str, reviewed_by: str,
):
    """Agent override: update the stored decision in the learning table."""
    try:
        _col("ai_specialist_diagnosis").update_one(
            {
                "specialist_code": specialist_code.strip().upper(),
                "diagnosis_code":  diagnosis_code.strip().upper(),
            },
            {
                "$set": {
                    "decision":      decision.upper(),
                    "reasoning":     reasoning,
                    "source":        "human",
                    "reviewed_by":   reviewed_by,
                    "reviewed_at":   datetime.utcnow().isoformat(),
                }
            }
        )
    except Exception as e:
        logger.warning(f"update_specialist_diagnosis_decision: {e}")


# ── klaire_review_queue ────────────────────────────────────────────────────────

def insert_klaire_review(doc: dict):
    """Insert a KLAIRE specialist-diagnosis review request."""
    try:
        _col("klaire_review_queue").replace_one(
            {"review_id": doc["review_id"]}, doc, upsert=True
        )
    except Exception as e:
        logger.warning(f"insert_klaire_review: {e}")


def get_klaire_review(review_id: str):
    return _col("klaire_review_queue").find_one({"review_id": review_id}, {"_id": 0})


def get_pending_klaire_reviews(limit: int = 50, offset: int = 0) -> list:
    return list(
        _col("klaire_review_queue")
        .find({"status": "PENDING_REVIEW"}, {"_id": 0})
        .sort("created_at", 1)
        .skip(offset)
        .limit(limit)
    )


def count_pending_klaire_reviews() -> int:
    return _col("klaire_review_queue").count_documents({"status": "PENDING_REVIEW"})


def update_klaire_review(review_id: str, fields: dict):
    try:
        _col("klaire_review_queue").update_one(
            {"review_id": review_id}, {"$set": fields}
        )
    except Exception as e:
        logger.warning(f"update_klaire_review: {e}")


# ── ai_first_line_treatment ───────────────────────────────────────────────────

def get_first_line_learning(procedure_code: str, diagnosis_code: str):
    return _col("ai_first_line_treatment").find_one(
        {
            "procedure_code": procedure_code.strip().upper(),
            "diagnosis_code": diagnosis_code.strip().upper(),
        },
        {"_id": 0},
    )


def upsert_first_line_learning(
    procedure_code: str, procedure_name: str,
    diagnosis_code: str, diagnosis_name: str,
    decision: str, is_first_line: bool,
    confidence: int, reasoning: str, source: str,
):
    try:
        _col("ai_first_line_treatment").update_one(
            {
                "procedure_code": procedure_code.strip().upper(),
                "diagnosis_code": diagnosis_code.strip().upper(),
            },
            {
                "$setOnInsert": {
                    "procedure_name": procedure_name,
                    "diagnosis_name": diagnosis_name,
                    "usage_count":    0,
                    "admin_approved": False,
                    "created_at":     datetime.utcnow().isoformat(),
                },
                "$set": {
                    "decision":     decision.upper(),
                    "is_first_line": is_first_line,
                    "confidence":   confidence,
                    "reasoning":    reasoning,
                    "source":       source,
                    "updated_at":   datetime.utcnow().isoformat(),
                },
                "$inc": {"usage_count": 1},
            },
            upsert=True,
        )
    except Exception as e:
        logger.warning(f"upsert_first_line_learning: {e}")


def inc_first_line_usage(procedure_code: str, diagnosis_code: str):
    try:
        _col("ai_first_line_treatment").update_one(
            {
                "procedure_code": procedure_code.strip().upper(),
                "diagnosis_code": diagnosis_code.strip().upper(),
            },
            {"$inc": {"usage_count": 1}},
        )
    except Exception as e:
        logger.warning(f"inc_first_line_usage: {e}")


def update_first_line_decision(
    procedure_code: str, diagnosis_code: str, decision: str, reviewed_by: str
):
    try:
        _col("ai_first_line_treatment").update_one(
            {
                "procedure_code": procedure_code.strip().upper(),
                "diagnosis_code": diagnosis_code.strip().upper(),
            },
            {
                "$set": {
                    "decision":    decision.upper(),
                    "source":      "human",
                    "reviewed_by": reviewed_by,
                    "reviewed_at": datetime.utcnow().isoformat(),
                }
            },
        )
    except Exception as e:
        logger.warning(f"update_first_line_decision: {e}")


# ── Trust logic (shared across engines) ──────────────────────────────────────

def is_learning_trusted(doc: dict) -> bool:
    """
    An entry is trusted for auto-decisions (no human intervention needed) if:
      a) admin_approved == True  (supervisor approved once), OR
      b) usage_count >= 3 AND the entry was human-confirmed (approved_by or source='human')

    Anything below this threshold must still pass through a human reviewer.
    """
    if doc.get("admin_approved") is True:
        return True
    usage = int(doc.get("usage_count", 0))
    human_confirmed = bool(doc.get("approved_by")) or doc.get("source") == "human"
    return usage >= 3 and human_confirmed


# ── Admin learning-table management ──────────────────────────────────────────

_LEARNING_COLLECTIONS = [
    "ai_human_procedure_diagnosis",
    "ai_human_procedure_age",
    "ai_human_procedure_gender",
    "ai_human_diagnosis_age",
    "ai_human_diagnosis_gender",
    "ai_human_procedure_class",
    "ai_specialist_diagnosis",
    # ai_first_line_treatment excluded — clinical necessity is context-dependent, never learns
    # ai_human_diagnosis_stacking excluded — same reason: same combo can be valid or invalid
    #   depending on patient history, clinical context, and presentation
]

_COLLECTION_LABELS = {
    "ai_human_procedure_diagnosis": "Procedure-Diagnosis",
    "ai_human_procedure_age":       "Procedure Age",
    "ai_human_procedure_gender":    "Procedure Gender",
    "ai_human_diagnosis_age":       "Diagnosis Age",
    "ai_human_diagnosis_gender":    "Diagnosis Gender",
    "ai_human_procedure_class":     "Procedure Class",
    "ai_specialist_diagnosis":      "Specialist-Diagnosis",
}

_UNTRUSTED_FILTER = {
    "admin_approved": {"$ne": True},
    "usage_count":    {"$lt": 3},
}


def _is_trusted(doc: dict) -> bool:
    return bool(doc.get("admin_approved")) or int(doc.get("usage_count", 0)) >= 3


def get_all_learnings(limit: int = 500) -> list:
    """Return ALL learning entries (trusted + pending) across every learning table."""
    results = []
    for col_name in _LEARNING_COLLECTIONS:
        try:
            docs = list(
                _col(col_name)
                .find({}, {"_id": 0})
                .sort("created_at", -1)
                .limit(limit)
            )
            for d in docs:
                d["_collection"] = col_name
                d["_label"]      = _COLLECTION_LABELS.get(col_name, col_name)
                d["_trusted"]    = _is_trusted(d)
                d["_status"]     = "Trusted" if _is_trusted(d) else "Pending"
            results.extend(docs)
        except Exception as e:
            logger.warning(f"get_all_learnings {col_name}: {e}")
    # Sort: pending first, then most recent
    results.sort(key=lambda d: (d["_trusted"], d.get("created_at", "") or ""), reverse=False)
    return results


def get_all_pending_learnings(limit: int = 200) -> list:
    """Return untrusted learning entries only (kept for backwards compat)."""
    results = []
    for col_name in _LEARNING_COLLECTIONS:
        try:
            docs = list(
                _col(col_name)
                .find(_UNTRUSTED_FILTER, {"_id": 0})
                .sort("created_at", 1)
                .limit(limit)
            )
            for d in docs:
                d["_collection"] = col_name
                d["_label"]      = _COLLECTION_LABELS.get(col_name, col_name)
                d["_trusted"]    = False
                d["_status"]     = "Pending"
            results.extend(docs)
        except Exception as e:
            logger.warning(f"get_all_pending_learnings {col_name}: {e}")
    return results


def count_pending_learnings() -> int:
    total = 0
    for col_name in _LEARNING_COLLECTIONS:
        try:
            total += _col(col_name).count_documents(_UNTRUSTED_FILTER)
        except Exception as e:
            logger.warning(f"count_pending_learnings {col_name}: {e}")
    return total


def admin_approve_learning(collection_name: str, doc_filter: dict) -> bool:
    """Admin approves a learning entry — marks it trusted for auto-decisions."""
    try:
        _col(collection_name).update_one(
            doc_filter,
            {"$set": {"admin_approved": True, "admin_approved_at": datetime.utcnow().isoformat()}}
        )
        return True
    except Exception as e:
        logger.warning(f"admin_approve_learning {collection_name}: {e}")
        return False


def delete_learning_entry(collection_name: str, doc_filter: dict) -> bool:
    """Admin removes a learning entry entirely."""
    try:
        _col(collection_name).delete_one(doc_filter)
        return True
    except Exception as e:
        logger.warning(f"delete_learning_entry {collection_name}: {e}")
        return False


def get_collection_stats(collection_name: str) -> dict:
    """Return {'entries': N, 'total_usage': M} for a learning table."""
    pipeline = [{"$group": {"_id": None, "count": {"$sum": 1},
                             "total_usage": {"$sum": "$usage_count"}}}]
    result = list(_col(collection_name).aggregate(pipeline))
    if result:
        return {"entries": result[0]["count"], "total_usage": int(result[0]["total_usage"])}
    return {"entries": 0, "total_usage": 0}
