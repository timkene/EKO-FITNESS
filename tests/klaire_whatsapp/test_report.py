import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from datetime import date
from apis.klaire_whatsapp.report import _aggregate_feedback, _build_whatsapp_summary, _format_date


def test_format_date():
    assert _format_date(date(2026, 5, 5)) == "05 May 2026"


def test_aggregate_feedback_counts_correctly():
    docs = [
        {"rating": 5, "adherence_flag": False, "escalated": False, "hospital": "Kupa Medical", "comment": "Good"},
        {"rating": 2, "adherence_flag": True, "escalated": True, "hospital": "St Nick", "comment": "Terrible"},
        {"rating": 4, "adherence_flag": False, "escalated": False, "hospital": "Kupa Medical", "comment": "Fine"},
    ]
    stats = _aggregate_feedback(docs)
    assert stats["total"] == 3
    assert stats["escalated"] == 1
    assert stats["adherence_flags"] == 1
    assert round(stats["avg_rating"], 1) == 3.7
    assert stats["top_hospital"] == "St Nick"  # only escalated hospital counts


def test_aggregate_feedback_empty():
    stats = _aggregate_feedback([])
    assert stats["total"] == 0
    assert stats["avg_rating"] == 0.0
    assert stats["top_hospital"] == "—"


def test_aggregate_feedback_no_ratings():
    docs = [{"rating": None, "adherence_flag": False, "escalated": False, "hospital": "X", "comment": ""}]
    stats = _aggregate_feedback(docs)
    assert stats["avg_rating"] == 0.0


def test_build_whatsapp_summary_contains_key_fields():
    stats = {
        "total": 47, "responded": 31, "escalated": 4,
        "avg_rating": 4.1, "adherence_flags": 3,
        "top_hospital": "Kupa Medical", "top_hospital_count": 3,
        "non_responders": 16,
    }
    msg = _build_whatsapp_summary(stats, date(2026, 5, 5))
    assert "KLAIRE REPORT" in msg
    assert "47" in msg
    assert "4.1" in msg
    assert "Kupa Medical" in msg
    assert "16" in msg
