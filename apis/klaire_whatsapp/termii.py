import os
import hashlib
import hmac
from typing import List
import httpx

TERMII_API_KEY = os.getenv("TERMII_API_KEY", "")
TERMII_SENDER_ID = os.getenv("TERMII_SENDER_ID", "")
TERMII_WEBHOOK_SECRET = os.getenv("TERMII_WEBHOOK_SECRET", "")
TERMII_BASE_URL = "https://api.ng.termii.com"


def normalise_phone(raw: str) -> str:
    """Convert any Nigerian phone format → canonical 234XXXXXXXXXX."""
    digits = "".join(c for c in raw if c.isdigit())
    if digits.startswith("234"):
        return digits
    if digits.startswith("0") and len(digits) == 11:
        return "234" + digits[1:]
    if len(digits) == 10:
        return "234" + digits
    return digits


def _split_message(message: str) -> List[str]:
    return [message[i:i + 1000] for i in range(0, len(message), 1000)]


def verify_signature(payload: bytes, signature: str) -> bool:
    """Verify HMAC-SHA256 webhook signature. Returns True if secret not set (dev)."""
    if not TERMII_WEBHOOK_SECRET:
        return True
    expected = hmac.new(
        TERMII_WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


async def send_whatsapp(to: str, message: str) -> bool:
    """Send WhatsApp message via Termii. Splits at 1000 chars."""
    chunks = _split_message(message)
    async with httpx.AsyncClient(timeout=15.0) as client:
        for chunk in chunks:
            resp = await client.post(
                f"{TERMII_BASE_URL}/api/send",
                json={
                    "to": to,
                    "from": TERMII_SENDER_ID,
                    "sms": chunk,
                    "type": "plain",
                    "channel": "whatsapp",
                    "api_key": TERMII_API_KEY,
                },
            )
            if resp.status_code != 200:
                return False
    return True
