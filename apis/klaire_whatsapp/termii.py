import os
import hashlib
import hmac
import logging
from typing import List
import httpx

TERMII_BASE_URL = "https://api.ng.termii.com"

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
    """Split message into chunks of max 1000 chars for Termii's API limit."""
    return [message[i:i + 1000] for i in range(0, len(message), 1000)]


def verify_signature(payload: bytes, signature: str) -> bool:
    """Verify HMAC-SHA256 webhook signature. Returns True if secret not set (dev)."""
    secret = os.getenv("TERMII_WEBHOOK_SECRET", "")
    if not secret:
        return True
    expected = hmac.new(
        secret.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


async def send_whatsapp(to: str, message: str) -> bool:
    """Send WhatsApp message via Termii. Splits at 1000 chars. Returns False on failure."""
    if not message.strip():
        logger.warning("send_whatsapp: empty message to %s, skipping", to)
        return False
    chunks = _split_message(message)
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            for chunk in chunks:
                resp = await client.post(
                    f"{TERMII_BASE_URL}/api/send",
                    json={
                        "to": to,
                        "from": os.getenv("TERMII_SENDER_ID", ""),
                        "sms": chunk,
                        "type": "plain",
                        "channel": "whatsapp",
                        "api_key": os.getenv("TERMII_API_KEY", ""),
                    },
                )
                if resp.status_code != 200:
                    logger.error("Termii send failed: status=%s to=%s", resp.status_code, to)
                    return False
    except httpx.RequestError as exc:
        logger.error("Termii send network error: %s to=%s", exc, to)
        return False
    return True
