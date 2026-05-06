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
                    "text": {"body": "hello"},
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


def test_webhook_unknown_phone_sends_fallback():
    with patch("apis.klaire_whatsapp.identity.lookup_by_phone", return_value=None), \
         patch("apis.klaire_whatsapp.termii.send_whatsapp", new_callable=AsyncMock) as mock_send:
        client = TestClient(_make_app())
        resp = client.post("/webhook/whatsapp", json=_D360_TEXT_PAYLOAD)
    assert resp.status_code == 200
    assert resp.json()["status"] == "not_found"
    mock_send.assert_awaited_once()
    assert "find your details" in mock_send.call_args[0][1]
