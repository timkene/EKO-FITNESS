import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from unittest.mock import patch, MagicMock
from apis.klaire_whatsapp.session import load_session, save_session, append_message


def _make_col(find_result=None):
    col = MagicMock()
    col.find_one.return_value = find_result
    return col


def test_load_session_returns_default_when_missing():
    with patch("apis.klaire_whatsapp.session._col", return_value=_make_col(None)):
        s = load_session("2348012345678")
    assert s["phone"] == "2348012345678"
    assert s["mode"] == "front_desk"
    assert s["messages"] == []


def test_load_session_returns_stored_data():
    stored = {
        "phone": "2348012345678", "mode": "aftercare",
        "messages": [{"role": "user", "content": "hi"}],
        "enrollee_id": "CL/X/1/2020", "aftercare_context": None, "_id": "abc",
    }
    with patch("apis.klaire_whatsapp.session._col", return_value=_make_col(stored)):
        s = load_session("2348012345678")
    assert s["mode"] == "aftercare"
    assert "_id" not in s


def test_save_session_calls_upsert():
    col = _make_col()
    with patch("apis.klaire_whatsapp.session._col", return_value=col):
        save_session("2348012345678", {"mode": "aftercare"})
    col.update_one.assert_called_once()
    args = col.update_one.call_args
    assert args[0][0] == {"phone": "2348012345678"}
    assert args[1]["upsert"] is True


def test_append_message_uses_slice_operator():
    col = _make_col()
    with patch("apis.klaire_whatsapp.session._col", return_value=col):
        append_message("2348012345678", "user", "I feel better")
    col.update_one.assert_called_once()
    update = col.update_one.call_args[0][1]
    assert "$push" in update
    assert "$slice" in update["$push"]["messages"]
