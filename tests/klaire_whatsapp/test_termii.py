import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import hmac
import hashlib
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from apis.klaire_whatsapp.termii import normalise_phone, verify_signature, _split_message, send_whatsapp


def test_normalise_phone_with_234_prefix():
    assert normalise_phone("2348012345678") == "2348012345678"

def test_normalise_phone_with_zero_prefix():
    assert normalise_phone("08012345678") == "2348012345678"

def test_normalise_phone_with_plus():
    assert normalise_phone("+2348012345678") == "2348012345678"

def test_normalise_phone_10_digits():
    assert normalise_phone("8012345678") == "2348012345678"

def test_split_message_short():
    chunks = _split_message("Hello")
    assert chunks == ["Hello"]

def test_split_message_long():
    msg = "x" * 2500
    chunks = _split_message(msg)
    assert len(chunks) == 3
    assert all(len(c) <= 1000 for c in chunks)

def test_verify_signature_no_secret(monkeypatch):
    monkeypatch.delenv("TERMII_WEBHOOK_SECRET", raising=False)
    assert verify_signature(b"payload", "anything") is True

def test_verify_signature_valid(monkeypatch):
    monkeypatch.setenv("TERMII_WEBHOOK_SECRET", "mysecret")
    payload = b"test_payload"
    sig = hmac.new(b"mysecret", payload, hashlib.sha256).hexdigest()
    assert verify_signature(payload, sig) is True

def test_verify_signature_invalid(monkeypatch):
    monkeypatch.setenv("TERMII_WEBHOOK_SECRET", "mysecret")
    assert verify_signature(b"test_payload", "wrongsig") is False

@pytest.mark.anyio
async def test_send_whatsapp_success():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
        result = await send_whatsapp("2348012345678", "Hello")
    assert result is True

@pytest.mark.anyio
async def test_send_whatsapp_api_failure():
    mock_resp = MagicMock()
    mock_resp.status_code = 400
    mock_resp.text = "Bad request"
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
        result = await send_whatsapp("2348012345678", "Hello")
    assert result is False

@pytest.mark.anyio
async def test_send_whatsapp_empty_message():
    result = await send_whatsapp("2348012345678", "")
    assert result is False

@pytest.mark.anyio
async def test_send_whatsapp_network_error():
    import httpx
    with patch("httpx.AsyncClient.post", side_effect=httpx.RequestError("timeout")):
        result = await send_whatsapp("2348012345678", "Hello")
    assert result is False
