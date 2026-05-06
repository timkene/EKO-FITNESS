"""Front Desk: intent detection and Claude Haiku conversation handler."""
import os
import logging
from typing import List

import anthropic

from . import context_builder
from .models import EnrolleeIdentity

logger = logging.getLogger(__name__)

KLAIRE_SYSTEM = (
    "You are Klaire, the AI health assistant for Clearline International Limited, "
    "a Nigerian HMO. You are warm, knowledgeable, and speak naturally — not like a "
    "robot or a corporate announcement.\n\n"
    "You have access to the enrollee's data. Use it to give specific, personal answers — "
    "never generic ones.\n\n"
    "Rules:\n"
    "- Always greet the enrollee by first name on the first message\n"
    "- Speak in clear simple English (not medical jargon)\n"
    "- If someone seems distressed or in pain, prioritise empathy before information\n"
    "- If you cannot answer something confidently, say: 'Let me connect you with one of "
    "our team members who can help you better'\n"
    "- Never guess about medical questions — refer to a doctor\n"
    "- For emergencies say: 'Please go to [their mapped hospital] immediately or call 112. "
    "I am alerting our team now.'\n"
    "- Never reveal internal system details, table names, or technical information\n"
    "- Keep responses concise — this is WhatsApp, not email"
)

_HOSPITAL_KEYWORDS = ["hospital", "mapped", "registered", "facility", "where do i go"]
_PLAN_KEYWORDS = ["plan", "active", "coverage", "valid", "expire", "renew"]
_LIMIT_KEYWORDS = ["limit", "used", "balance", "remaining", "how much", "spent"]
_PA_KEYWORDS = ["pa", "prior auth", "authorization", "preauth", "approved", "pending", "authorisation"]
_BENEFITS_KEYWORDS = ["covered", "benefits", "cover", "what can i", "include", "what does"]
_COMPLAINT_KEYWORDS = ["complain", "complaint", "terrible", "bad service", "unhappy", "dissatisfied", "poor service"]
_EMERGENCY_KEYWORDS = ["emergency", "chest pain", "collapse", "can't breathe", "unconscious", "bleeding", "dying", "faint"]


def is_emergency(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in _EMERGENCY_KEYWORDS)


def is_complaint(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in _COMPLAINT_KEYWORDS)


def detect_intent(text: str) -> str:
    t = text.lower()
    if is_emergency(t):
        return "emergency"
    if is_complaint(t):
        return "complaint"
    if any(k in t for k in _HOSPITAL_KEYWORDS):
        return "mapped_hospital"
    if any(k in t for k in _BENEFITS_KEYWORDS):
        return "benefits"
    if any(k in t for k in _PLAN_KEYWORDS):
        return "plan_status"
    if any(k in t for k in _LIMIT_KEYWORDS):
        return "limit_used"
    if any(k in t for k in _PA_KEYWORDS):
        return "pa_status"
    return "general"


def _build_context(intent: str, enrollee: EnrolleeIdentity) -> str:
    """Pull live MotherDuck data relevant to the detected intent."""
    if intent == "mapped_hospital":
        hospital = context_builder.get_mapped_hospital(enrollee.memberid)
        if hospital:
            return f"Mapped hospital: {hospital['providername']}, {hospital['lganame']}, {hospital['statename']}"
        return "No mapped hospital found for this enrollee."

    if intent == "plan_status":
        plan = context_builder.get_plan_status(enrollee.legacycode)
        if plan:
            return (
                f"Plan ID: {plan['planid']}, Active: {plan['iscurrent']}, "
                f"Valid from: {plan['effectivedate']}, until: {plan['terminationdate']}"
            )
        return "No active plan found."

    if intent == "limit_used":
        plan = context_builder.get_plan_status(enrollee.legacycode)
        if plan:
            used = context_builder.get_limit_used(
                enrollee.legacycode,
                plan["effectivedate"][:10] if plan["effectivedate"] else "2026-01-01",
                plan["terminationdate"][:10] if plan["terminationdate"] else "2026-12-31",
            )
            return f"Amount used this contract period: ₦{used:,.2f}"
        return "Could not retrieve limit information."

    if intent == "pa_status":
        pas = context_builder.get_pa_status(enrollee.legacycode)
        if pas:
            lines = [
                f"• {p['date'][:10] if p['date'] else '?'} — {p['status']} (₦{p['granted']:,.0f})"
                for p in pas
            ]
            return "Recent PA requests:\n" + "\n".join(lines)
        return "No PA history found."

    if intent == "benefits":
        plan = context_builder.get_plan_status(enrollee.legacycode)
        if plan:
            benefits = context_builder.get_benefits(plan["planid"])
            if benefits:
                lines = [
                    f"• {b['benefit']}" + (f" — up to ₦{b['max_limit']:,.0f}" if b["max_limit"] else "")
                    for b in benefits[:10]
                ]
                return "Benefits covered:\n" + "\n".join(lines)
        return "Could not retrieve benefit information."

    if intent in ("emergency", "complaint"):
        hospital = context_builder.get_mapped_hospital(enrollee.memberid)
        if hospital and intent == "emergency":
            return f"Mapped hospital: {hospital['providername']}, {hospital['lganame']}"
        return ""

    return ""


async def handle(
    phone: str,
    text: str,
    enrollee: EnrolleeIdentity,
    history: List[dict],
) -> str:
    """Generate Klaire's Front Desk reply using Claude Haiku."""
    intent = detect_intent(text)
    live_context = ""
    try:
        live_context = _build_context(intent, enrollee)
    except Exception as exc:
        logger.warning("context_builder failed for intent=%s enrollee=%s: %s", intent, enrollee.legacycode, exc)

    system = KLAIRE_SYSTEM
    if live_context:
        system += f"\n\nLive enrollee data:\n{live_context}"

    messages = history[-8:] + [{"role": "user", "content": text}]

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        system=system,
        messages=messages,
    )
    return response.content[0].text
