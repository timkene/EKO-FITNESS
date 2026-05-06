"""Daily Klaire aftercare report: aggregation, Claude Sonnet HTML, Resend email, WhatsApp summary."""
import os
import logging
from collections import Counter
from datetime import datetime, date, timedelta, timezone
from typing import Optional, List

import anthropic
import resend
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from . import termii

logger = logging.getLogger(__name__)

MONGO_DB = "KLAIRE"
CASEY_EMAIL = os.getenv("CASEY_EMAIL", "leocasey0@gmail.com")
CASEY_WHATSAPP = os.getenv("CASEY_WHATSAPP", "")

_client: Optional[MongoClient] = None


def _db():
    global _client
    if _client is None:
        mongo_uri = os.environ.get("MONGO_URI")
        if not mongo_uri:
            raise RuntimeError("MONGO_URI environment variable is not set.")
        _client = MongoClient(mongo_uri, server_api=ServerApi("1"))
    return _client[MONGO_DB]


def _format_date(d: date) -> str:
    return d.strftime("%d %b %Y")


def _aggregate_feedback(docs: List[dict]) -> dict:
    """Compute summary statistics from klaire_feedback documents."""
    total = len(docs)
    escalated = sum(1 for d in docs if d.get("escalated"))
    adherence_flags = sum(1 for d in docs if d.get("adherence_flag"))
    ratings = [d["rating"] for d in docs if d.get("rating") is not None]
    avg_rating = round(sum(ratings) / len(ratings), 1) if ratings else 0.0

    escalated_hospitals = [d.get("hospital", "") for d in docs if d.get("escalated")]
    top_hospital, top_count = "—", 0
    if escalated_hospitals:
        top_hospital, top_count = Counter(escalated_hospitals).most_common(1)[0]

    return {
        "total": total,
        "responded": total,
        "escalated": escalated,
        "avg_rating": avg_rating,
        "adherence_flags": adherence_flags,
        "top_hospital": top_hospital,
        "top_hospital_count": top_count,
        "non_responders": 0,
    }


def _build_whatsapp_summary(stats: dict, report_date: date) -> str:
    return (
        f"KLAIRE REPORT — {_format_date(report_date)}\n\n"
        f"Outreach: {stats['total']} | Responded: {stats['responded']}\n"
        f"Escalated: {stats['escalated']} | Avg rating: {stats['avg_rating']}/5\n\n"
        f"Top complaint hospital: {stats['top_hospital']} ({stats['top_hospital_count']}x)\n"
        f"Adherence flags: {stats['adherence_flags']} stopped medication early\n"
        f"Non-responders: {stats['non_responders']} (follow up needed)"
    )


def _generate_html_report(docs: List[dict], stats: dict, report_date: date) -> str:
    """Use Claude Sonnet to generate a professional HTML report from feedback data."""
    feedback_lines = "\n".join([
        f"- Enrollee {d.get('enrollee_id', '?')} | Hospital: {d.get('hospital', '?')} "
        f"| Rating: {d.get('rating', 'N/A')} | Comment: {d.get('comment', '')} "
        f"| Adherence flag: {d.get('adherence_flag')} | Escalated: {d.get('escalated')}"
        for d in docs[:50]
    ])

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{
            "role": "user",
            "content": (
                f"Generate a professional HTML email report for Clearline's daily Klaire Aftercare report "
                f"dated {_format_date(report_date)}.\n\n"
                f"Summary stats:\n"
                f"- Total contacted: {stats['total']}\n"
                f"- Responded: {stats['responded']}\n"
                f"- Escalated: {stats['escalated']}\n"
                f"- Average rating: {stats['avg_rating']}/5\n"
                f"- Adherence flags: {stats['adherence_flags']}\n"
                f"- Non-responders: {stats['non_responders']}\n\n"
                f"Individual feedback entries:\n{feedback_lines}\n\n"
                f"Format as a clean HTML email with sections: Summary, Escalations, "
                f"Adherence Flags, Service Rating Highlights. Use a professional but warm tone. "
                f"Return only the HTML body content (no <html>/<head> tags needed)."
            ),
        }],
    )
    return response.content[0].text


async def generate_and_send() -> None:
    """Pull yesterday's feedback, generate report, email to Casey, WhatsApp summary."""
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = datetime.combine(yesterday, datetime.min.time()).replace(tzinfo=timezone.utc)
    end = datetime.combine(yesterday, datetime.max.time()).replace(tzinfo=timezone.utc)

    docs = list(_db()["klaire_feedback"].find({"created_at": {"$gte": start, "$lte": end}}))
    logger.info("Report: %d feedback entries for %s", len(docs), yesterday)

    outreach_count = _db()["klaire_outreach_log"].count_documents(
        {"contacted_at": {"$gte": start, "$lte": end}}
    )
    responded_count = _db()["klaire_outreach_log"].count_documents(
        {"contacted_at": {"$gte": start, "$lte": end}, "responded": True}
    )

    stats = _aggregate_feedback(docs)
    stats["total"] = outreach_count
    stats["responded"] = responded_count
    stats["non_responders"] = outreach_count - responded_count

    whatsapp_msg = _build_whatsapp_summary(stats, yesterday)
    html_body = _generate_html_report(docs, stats, yesterday)

    resend.api_key = os.getenv("RESEND_API_KEY", "")
    try:
        resend.Emails.send({
            "from": "Klaire <klaire@clearlinehmo.com>",
            "to": [CASEY_EMAIL],
            "subject": f"Klaire Daily Report — {_format_date(yesterday)}",
            "html": f"<html><body>{html_body}</body></html>",
        })
        logger.info("Report: email sent to %s", CASEY_EMAIL)
    except Exception as exc:
        logger.error("Report: Resend email failed — %s", exc)

    if CASEY_WHATSAPP:
        await termii.send_whatsapp(CASEY_WHATSAPP, whatsapp_msg)
        logger.info("Report: WhatsApp summary sent to %s", CASEY_WHATSAPP)
