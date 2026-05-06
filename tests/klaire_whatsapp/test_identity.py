import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from unittest.mock import patch, MagicMock
from apis.klaire_whatsapp.identity import (
    lookup_by_phone, lookup_by_legacycode, _normalise_for_query
)

_SAMPLE_ROW = ("42", "CL/ARIK/001/2020", "Amaka", "Obi", 2, "1990-03-15")


def test_normalise_strips_country_code():
    assert _normalise_for_query("2348012345678") == "08012345678"


# ── lookup_by_legacycode ──────────────────────────────────────────────────────

def test_lookup_by_legacycode_returns_none_when_not_found():
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = None
    with patch("apis.klaire_whatsapp.identity._connect", return_value=mock_con):
        result = lookup_by_legacycode("CL/ARIK/001/2020")
    assert result is None


def test_lookup_by_legacycode_returns_enrollee_when_found():
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = _SAMPLE_ROW
    with patch("apis.klaire_whatsapp.identity._connect", return_value=mock_con):
        result = lookup_by_legacycode("CL/ARIK/001/2020")
    assert result is not None
    assert result.legacycode == "CL/ARIK/001/2020"
    assert result.firstname == "Amaka"
    assert result.memberid == "42"


def test_lookup_by_legacycode_strips_whitespace():
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = _SAMPLE_ROW
    with patch("apis.klaire_whatsapp.identity._connect", return_value=mock_con):
        result = lookup_by_legacycode("  CL/ARIK/001/2020  ")
    assert result is not None


# ── lookup_by_phone ───────────────────────────────────────────────────────────

def test_lookup_by_phone_returns_none_when_not_found():
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = None
    with patch("apis.klaire_whatsapp.identity._connect", return_value=mock_con):
        result = lookup_by_phone("2348012345678")
    assert result is None


def test_lookup_by_phone_returns_enrollee_when_found():
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = _SAMPLE_ROW
    with patch("apis.klaire_whatsapp.identity._connect", return_value=mock_con):
        result = lookup_by_phone("2348012345678")
    assert result is not None
    assert result.legacycode == "CL/ARIK/001/2020"


def test_lookup_by_phone_tries_six_params():
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = None
    with patch("apis.klaire_whatsapp.identity._connect", return_value=mock_con):
        lookup_by_phone("2348012345678")
    call_args = mock_con.execute.call_args
    assert len(call_args[0][1]) == 6  # 3 canonical + 3 local format
