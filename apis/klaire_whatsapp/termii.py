"""WhatsApp delivery via 360dialog (Meta WABA v2 API)."""
import os
import hashlib
import hmac
import logging
from typing import List
import httpx

D360_BASE_URL = "https://waba-v2.360dialog.io/messages"

logger = logging.getLogger(__name__)


def normalise_phone(raw: str) -> str:
    """Convert any Nigerian phone format → canonical 234XXXXXXXXXX."""
    digits = "".join(c for c in raw if c.isdigit())
    if digits.startswith("234"):
        return digits
    if digits.startswith("0") and len(digits) == 11:
        return "234" + digits[1:]
    if len(digits) == 10:
        return "234" + digits
    logger.warning("normalise_phone: unrecognised format %r", raw)
    return digits


def _split_message(message: str) -> List[str]:
    """Split long messages into WhatsApp-safe chunks (≤1000 chars)."""
    return [message[i:i + 1000] for i in range(0, len(message), 1000)]


def verify_signature(payload: bytes, signature: str) -> bool:
    """Verify Meta/360dialog webhook signature (X-Hub-Signature-256: sha256=<hex>).
    Returns True when D360_WEBHOOK_SECRET is unset (local dev).
    """
    secret = os.getenv("D360_WEBHOOK_SECRET", "")
    if not secret:
        return True
    expected = "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


async def send_whatsapp(to: str, message: str) -> bool:
    """Send WhatsApp message via 360dialog. Splits at 1000 chars. Returns False on failure."""
    if not message.strip():
        logger.warning("send_whatsapp: empty message to %s, skipping", to)
        return False
    api_key = os.getenv("D360_API_KEY", "")
    chunks = _split_message(message)
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            for chunk in chunks:
                resp = await client.post(
                    D360_BASE_URL,
                    headers={"D360-API-KEY": api_key, "Content-Type": "application/json"},
                    json={
                        "messaging_product": "whatsapp",
                        "to": to,
                        "type": "text",
                        "text": {"body": chunk},
                    },
                )
                if resp.status_code not in (200, 201):
                    logger.error("360dialog send failed: status=%s to=%s body=%s",
                                 resp.status_code, to, resp.text)
                    return False
    except httpx.RequestError as exc:
        logger.error("360dialog send network error: %s to=%s", exc, to)
        return False
    return True
