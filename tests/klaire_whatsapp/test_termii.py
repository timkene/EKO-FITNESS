import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from apis.klaire_whatsapp.termii import normalise_phone, verify_signature, _split_message


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
    monkeypatch.setenv("TERMII_WEBHOOK_SECRET", "")
    # reimport to pick up env change
    import importlib
    import apis.klaire_whatsapp.termii as t
    importlib.reload(t)
    assert t.verify_signature(b"payload", "anything") is True
