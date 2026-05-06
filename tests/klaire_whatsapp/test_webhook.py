import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from unittest.mock import patch, AsyncMock, MagicMock
from fastapi.testclient import TestClient

_D360_TEXT_PAYLOAD = {
    "object": "whatsapp_business_account",
    "entry": [{
        "changes": [{
            "value": {
                "messages": [{
                    "from": "2348099999999",
                    "id": "wamid.abc",
                    "type": "text",
                    "text": {"body": "CL/ARIK/001/2020"},
                }],
                "contacts": [{"profile": {"name": "Test"}, "wa_id": "2348099999999"}],
            },
            "field": "messages",
        }]
    }]
}

_D360_STATUS_PAYLOAD = {
    "object": "whatsapp_business_account",
    "entry": [{
        "changes": [{
            "value": {
                "statuses": [{"id": "wamid.xyz", "status": "delivered"}],
            },
            "field": "messages",
        }]
    }]
}

_MOCK_ENROLLEE = MagicMock(
    legacycode="CL/ARIK/001/2020",
    firstname="Amaka",
    lastname="Obi",
    memberid="42",
    genderid=2,
    dateofbirth="1990-03-15",
)


def _make_app():
    with patch("apis.klaire_whatsapp.main.scheduler"):
        from importlib import reload
        import apis.klaire_whatsapp.main as m
        reload(m)
        return m.app


def test_health_endpoint():
    client = TestClient(_make_app())
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_webhook_verify_valid_token(monkeypatch):
    monkeypatch.setenv("D360_VERIFY_TOKEN", "mytoken")
    client = TestClient(_make_app())
    resp = client.get("/webhook/whatsapp", params={
        "hub.mode": "subscribe",
        "hub.verify_token": "mytoken",
        "hub.challenge": "abc123",
    })
    assert resp.status_code == 200
    assert resp.text == "abc123"


def test_webhook_verify_wrong_token(monkeypatch):
    monkeypatch.setenv("D360_VERIFY_TOKEN", "mytoken")
    client = TestClient(_make_app())
    resp = client.get("/webhook/whatsapp", params={
        "hub.mode": "subscribe",
        "hub.verify_token": "WRONG",
        "hub.challenge": "abc123",
    })
    assert resp.status_code == 403


def test_webhook_status_event_ignored():
    client = TestClient(_make_app())
    resp = client.post("/webhook/whatsapp", json=_D360_STATUS_PAYLOAD)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"


def test_first_contact_asks_for_enrollee_id():
    """First message from an unknown phone → Klaire asks for Enrollee ID."""
    default_sess = {"phone": "2348099999999", "enrollee_id": None, "mode": "front_desk", "messages": [], "aftercare_context": None}
    with patch("apis.klaire_whatsapp.session.load_session", return_value=default_sess), \
         patch("apis.klaire_whatsapp.session.save_session"), \
         patch("apis.klaire_whatsapp.termii.send_whatsapp", new_callable=AsyncMock) as mock_send:
        client = TestClient(_make_app())
        resp = client.post("/webhook/whatsapp", json=_D360_TEXT_PAYLOAD)
    assert resp.status_code == 200
    assert resp.json()["status"] == "awaiting_id"
    mock_send.assert_awaited_once()
    assert "Enrollee ID" in mock_send.call_args[0][1]


def test_verification_mode_valid_id_succeeds():
    """In verification mode, a valid Enrollee ID completes verification."""
    verifying_sess = {"phone": "2348099999999", "enrollee_id": None, "mode": "verification", "messages": [], "aftercare_context": None}
    with patch("apis.klaire_whatsapp.session.load_session", return_value=verifying_sess), \
         patch("apis.klaire_whatsapp.session.save_session"), \
         patch("apis.klaire_whatsapp.session.append_message"), \
         patch("apis.klaire_whatsapp.identity.lookup_by_legacycode", return_value=_MOCK_ENROLLEE), \
         patch("apis.klaire_whatsapp.termii.send_whatsapp", new_callable=AsyncMock) as mock_send:
        client = TestClient(_make_app())
        resp = client.post("/webhook/whatsapp", json=_D360_TEXT_PAYLOAD)
    assert resp.status_code == 200
    assert resp.json()["status"] == "verified"
    assert "Amaka" in mock_send.call_args[0][1]


def test_verification_mode_invalid_id_asks_again():
    """Invalid Enrollee ID in verification mode → Klaire asks to retry."""
    verifying_sess = {"phone": "2348099999999", "enrollee_id": None, "mode": "verification", "messages": [], "aftercare_context": None}
    with patch("apis.klaire_whatsapp.session.load_session", return_value=verifying_sess), \
         patch("apis.klaire_whatsapp.identity.lookup_by_legacycode", return_value=None), \
         patch("apis.klaire_whatsapp.termii.send_whatsapp", new_callable=AsyncMock) as mock_send:
        client = TestClient(_make_app())
        resp = client.post("/webhook/whatsapp", json=_D360_TEXT_PAYLOAD)
    assert resp.status_code == 200
    assert resp.json()["status"] == "not_found"
    assert "couldn't find" in mock_send.call_args[0][1]


def test_verified_session_routes_to_front_desk():
    """Session with stored enrollee_id goes straight to front desk."""
    verified_sess = {"phone": "2348099999999", "enrollee_id": "CL/ARIK/001/2020", "mode": "front_desk", "messages": [], "aftercare_context": None}
    with patch("apis.klaire_whatsapp.session.load_session", return_value=verified_sess), \
         patch("apis.klaire_whatsapp.session.append_message"), \
         patch("apis.klaire_whatsapp.identity.lookup_by_legacycode", return_value=_MOCK_ENROLLEE), \
         patch("apis.klaire_whatsapp.front_desk.handle", new_callable=AsyncMock, return_value="Hi Amaka!"), \
         patch("apis.klaire_whatsapp.termii.send_whatsapp", new_callable=AsyncMock) as mock_send:
        client = TestClient(_make_app())
        resp = client.post("/webhook/whatsapp", json=_D360_TEXT_PAYLOAD)
    assert resp.status_code == 200
    assert resp.json()["status"] == "sent"
    mock_send.assert_awaited_once_with("2348099999999", "Hi Amaka!")
