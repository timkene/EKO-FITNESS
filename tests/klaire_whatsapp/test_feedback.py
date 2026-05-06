import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from unittest.mock import patch, MagicMock
from apis.klaire_whatsapp.feedback import (
    extract_rating, detect_adherence_flag, detect_escalation, save_feedback
)


def test_extract_rating_from_digit():
    assert extract_rating("4") == 4
    assert extract_rating("I give it a 3") == 3
    assert extract_rating("5 stars") == 5


def test_extract_rating_returns_none_when_missing():
    assert extract_rating("I'm fine thank you") is None


def test_extract_rating_ignores_out_of_range():
    assert extract_rating("9 out of 10") is None


def test_detect_adherence_flag_when_not_taking_drugs():
    assert detect_adherence_flag("I stopped taking the drugs") is True
    assert detect_adherence_flag("I didn't finish the medicine") is True
    assert detect_adherence_flag("Yes I'm taking my drugs") is False


def test_detect_adherence_flag_forgot():
    assert detect_adherence_flag("I forgot to take them yesterday") is True


def test_detect_escalation_for_complaint():
    assert detect_escalation("the hospital was very bad") is True
    assert detect_escalation("service was great") is False


def test_detect_escalation_low_rating():
    assert detect_escalation("I give it a 2") is True
    assert detect_escalation("1 out of 5") is True


def test_save_feedback_calls_mongo_insert():
    col = MagicMock()
    with patch("apis.klaire_whatsapp.feedback._col", return_value=col):
        save_feedback(
            enrollee_id="CL/ARIK/001/2020",
            panumber="PA001",
            hospital="Kupa Medical",
            rating=4,
            comment="Good service",
            adherence_flag=False,
            escalated=False,
        )
    col.insert_one.assert_called_once()
    doc = col.insert_one.call_args[0][0]
    assert doc["rating"] == 4
    assert doc["hospital"] == "Kupa Medical"
    assert doc["adherence_flag"] is False
    assert "created_at" in doc
