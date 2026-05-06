import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from unittest.mock import patch, MagicMock
from apis.klaire_whatsapp.aftercare import (
    classify_procedures, build_opening_message, _already_contacted
)


def test_classify_procedures_identifies_drugs():
    procs = [
        {"code": "DRG2641", "desc": "Artemether-Lumefantrine"},
        {"code": "CONS021", "desc": "GP Consultation"},
    ]
    result = classify_procedures(procs)
    assert "Artemether-Lumefantrine" in result["drugs"]
    assert "GP Consultation" in result["procedures"]


def test_classify_procedures_med_prefix():
    procs = [{"code": "MED001", "desc": "Amoxicillin 500mg"}]
    result = classify_procedures(procs)
    assert "Amoxicillin 500mg" in result["drugs"]
    assert result["procedures"] == []


def test_classify_procedures_diag_prefix():
    procs = [{"code": "DIAG050", "desc": "Full Blood Count"}]
    result = classify_procedures(procs)
    assert "Full Blood Count" in result["procedures"]
    assert result["drugs"] == []


def test_classify_procedures_falls_back_to_code_when_no_desc():
    procs = [{"code": "DRG999", "desc": None}]
    result = classify_procedures(procs)
    assert "DRG999" in result["drugs"]


def test_build_opening_message_includes_name_and_hospital():
    msg = build_opening_message("Amaka", "Kupa Medical Centre", "malaria treatment")
    assert "Amaka" in msg
    assert "Kupa Medical Centre" in msg
    assert "malaria" in msg


def test_already_contacted_returns_false_when_not_found():
    col = MagicMock()
    col.find_one.return_value = None
    with patch("apis.klaire_whatsapp.aftercare._outreach_col", return_value=col):
        assert _already_contacted("CL/ARIK/001/2020", "PA001") is False


def test_already_contacted_returns_true_when_found():
    col = MagicMock()
    col.find_one.return_value = {"enrollee_id": "CL/ARIK/001/2020"}
    with patch("apis.klaire_whatsapp.aftercare._outreach_col", return_value=col):
        assert _already_contacted("CL/ARIK/001/2020", "PA001") is True
