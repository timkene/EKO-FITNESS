import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from unittest.mock import patch, MagicMock
from apis.klaire_whatsapp.identity import lookup_by_phone, _normalise_for_query


def test_normalise_strips_country_code():
    assert _normalise_for_query("2348012345678") == "08012345678"

def test_lookup_returns_none_when_not_found():
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = None
    with patch("apis.klaire_whatsapp.identity._connect", return_value=mock_con):
        result = lookup_by_phone("2348012345678")
    assert result is None

def test_lookup_returns_enrollee_when_found():
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = (
        "42", "CL/ARIK/001/2020", "Amaka", "Obi", 2, "1990-03-15"
    )
    with patch("apis.klaire_whatsapp.identity._connect", return_value=mock_con):
        result = lookup_by_phone("2348012345678")
    assert result is not None
    assert result.legacycode == "CL/ARIK/001/2020"
    assert result.firstname == "Amaka"

def test_lookup_tries_all_three_phone_columns():
    mock_con = MagicMock()
    mock_con.execute.return_value.fetchone.return_value = None
    with patch("apis.klaire_whatsapp.identity._connect", return_value=mock_con):
        lookup_by_phone("2348012345678")
    call_args = mock_con.execute.call_args
    # all three phone variants passed as params
    assert len(call_args[0][1]) == 6  # 3 canonical + 3 local format
