import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from unittest.mock import patch, MagicMock
from apis.klaire_whatsapp.context_builder import (
    get_mapped_hospital, get_pa_status, get_limit_used, get_plan_status
)


def _mock_con(fetchone_result=None, fetchall_result=None):
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = fetchone_result
    con.execute.return_value.fetchall.return_value = fetchall_result or []
    return con


def test_get_mapped_hospital_returns_none_when_missing():
    con = _mock_con(fetchone_result=None)
    with patch("apis.klaire_whatsapp.context_builder._connect", return_value=con):
        result = get_mapped_hospital("42")
    assert result is None


def test_get_mapped_hospital_returns_dict():
    con = _mock_con(fetchone_result=("St Nicholas Hospital", "Lagos Island", "Lagos"))
    with patch("apis.klaire_whatsapp.context_builder._connect", return_value=con):
        result = get_mapped_hospital("42")
    assert result["providername"] == "St Nicholas Hospital"
    assert result["statename"] == "Lagos"


def test_get_pa_status_returns_list():
    con = _mock_con(fetchall_result=[
        ("PA001", "AUTHORIZED", "2026-05-01", "CONS021", 5000.0)
    ])
    with patch("apis.klaire_whatsapp.context_builder._connect", return_value=con):
        result = get_pa_status("CL/ARIK/001/2020")
    assert len(result) == 1
    assert result[0]["status"] == "AUTHORIZED"
    assert result[0]["panumber"] == "PA001"


def test_get_limit_used_returns_float():
    con = _mock_con(fetchone_result=(125000.50,))
    with patch("apis.klaire_whatsapp.context_builder._connect", return_value=con):
        result = get_limit_used("CL/ARIK/001/2020", "2026-01-01", "2026-12-31")
    assert result == 125000.50


def test_get_limit_used_returns_zero_when_none():
    con = _mock_con(fetchone_result=(None,))
    with patch("apis.klaire_whatsapp.context_builder._connect", return_value=con):
        result = get_limit_used("CL/ARIK/001/2020", "2026-01-01", "2026-12-31")
    assert result == 0.0


def test_get_plan_status_returns_none_when_missing():
    con = _mock_con(fetchone_result=None)
    with patch("apis.klaire_whatsapp.context_builder._connect", return_value=con):
        result = get_plan_status("CL/ARIK/001/2020")
    assert result is None


def test_get_plan_status_returns_dict():
    con = _mock_con(fetchone_result=("99", 1, "2026-01-01", "2026-12-31"))
    with patch("apis.klaire_whatsapp.context_builder._connect", return_value=con):
        result = get_plan_status("CL/ARIK/001/2020")
    assert result["planid"] == "99"
    assert result["iscurrent"] is True
    assert result["terminationdate"] == "2026-12-31"
