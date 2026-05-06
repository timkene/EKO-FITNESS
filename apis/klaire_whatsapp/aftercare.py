"""Aftercare: nightly PA-triggered outreach and multi-turn conversation handler."""
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List

import duckdb
import anthropic
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from .models import EnrolleeIdentity, AftercarContext
from .diagnosis_map import icd10_to_plain
from . import session, termii

logger = logging.getLogger(__name__)

MONGO_DB = "KLAIRE"
DRUG_PREFIXES = {"DRG", "MED", "BRG", "PRE"}

_mongo_client: Optional[MongoClient] = None


def _outreach_col():
    global _mongo_client
    if _mongo_client is None:
        mongo_uri = os.environ.get("MONGO_URI")
        if not mongo_uri:
            raise RuntimeError("MONGO_URI environment variable is not set.")
        _mongo_client = MongoClient(mongo_uri, server_api=ServerApi("1"))
    return _mongo_client[MONGO_DB]["klaire_outreach_log"]


def _connect() -> duckdb.DuckDBPyConnection:
    if not os.environ.get("MOTHERDUCK_TOKEN"):
        raise RuntimeError("MOTHERDUCK_TOKEN environment variable is not set.")
    return duckdb.connect("md:ai_driven_data")


def classify_procedures(procs: list) -> dict:
    """Split procedure list into drugs and non-drug procedures by code prefix."""
    drugs: List[str] = []
    procedures: List[str] = []
    for p in procs:
        prefix = str(p.get("code", ""))[:3].upper()
        label = p.get("desc") or p.get("code", "")
        if prefix in DRUG_PREFIXES:
            drugs.append(label)
        else:
            procedures.append(label)
    return {"drugs": drugs, "procedures": procedures}


def build_opening_message(firstname: str, hospital: str, diagnosis: str) -> str:
    return (
        f"Hi {firstname}, I'm Klaire from Clearline. "
        f"I see you recently visited {hospital} for {diagnosis}. "
        f"How are you feeling today?"
    )


def _already_contacted(enrollee_id: str, panumber: str) -> bool:
    """True if this enrollee+PA was contacted in the last 7 days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    doc = _outreach_col().find_one({
        "enrollee_id": enrollee_id,
        "panumber": str(panumber),
        "contacted_at": {"$gte": cutoff},
    })
    return doc is not None


def _log_outreach(enrollee_id: str, panumber: str) -> None:
    _outreach_col().insert_one({
        "enrollee_id": enrollee_id,
        "panumber": str(panumber),
        "contacted_at": datetime.now(timezone.utc),
        "responded": False,
    })


def _get_yesterday_pas() -> list:
    con = _connect()
    try:
        rows = con.execute(
            """
            SELECT pa.panumber, pa.IID AS enrollee_id, pa.providerid,
                   CAST(pa.requestdate AS VARCHAR) AS requestdate,
                   pa.groupname, p.providername,
                   m.firstname, m.phone1, m.phone2, m.phone3
            FROM "AI DRIVEN DATA"."PA DATA" pa
            JOIN "AI DRIVEN DATA".MEMBER m ON pa.IID = m.legacycode
            JOIN "AI DRIVEN DATA".PROVIDERS p
              ON TRY_CAST(pa.providerid AS BIGINT) = TRY_CAST(p.providerid AS BIGINT)
            WHERE TRY_CAST(pa.requestdate AS DATE) = CURRENT_DATE - INTERVAL 1 DAY
              AND pa.pastatus = 'AUTHORIZED'
            """
        ).fetchall()
        return [
            {
                "panumber": r[0], "enrollee_id": r[1], "providerid": r[2],
                "requestdate": r[3], "groupname": r[4], "providername": r[5],
                "firstname": r[6], "phone1": r[7], "phone2": r[8], "phone3": r[9],
            }
            for r in rows
        ]
    finally:
        con.close()


def _get_pa_procedures(panumber: str) -> list:
    con = _connect()
    try:
        rows = con.execute(
            """
            SELECT pa.code, pd.proceduredesc, pa.granted, pa.quantity
            FROM "AI DRIVEN DATA"."PA DATA" pa
            LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd
              ON LOWER(TRIM(pa.code)) = LOWER(TRIM(pd.procedurecode))
            WHERE pa.panumber = ?
            """,
            [str(panumber)],
        ).fetchall()
        return [{"code": r[0], "desc": r[1] or r[0], "granted": r[2], "quantity": r[3]} for r in rows]
    finally:
        con.close()


def _get_pa_diagnosis(panumber: str) -> str:
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT tbd.diagnosiscode
            FROM "AI DRIVEN DATA".TBPADIAGNOSIS tbd
            WHERE tbd.panumber = ?
            LIMIT 1
            """,
            [str(panumber)],
        ).fetchone()
        return icd10_to_plain(row[0] if row else None)
    finally:
        con.close()


async def run_nightly_outreach() -> None:
    """Pull yesterday's authorised PAs and send personalised opening messages."""
    logger.info("Aftercare: starting nightly outreach")
    pas = _get_yesterday_pas()
    logger.info("Aftercare: %d PAs from yesterday", len(pas))

    for pa in pas:
        enrollee_id = pa["enrollee_id"]
        panumber = pa["panumber"]

        if _already_contacted(enrollee_id, str(panumber)):
            continue

        phone = next((p for p in [pa["phone1"], pa["phone2"], pa["phone3"]] if p), None)
        if not phone:
            continue

        phone = termii.normalise_phone(phone)
        procs = _get_pa_procedures(panumber)
        classified = classify_procedures(procs)
        diagnosis = _get_pa_diagnosis(panumber)

        opening = build_opening_message(
            pa["firstname"] or "there",
            pa["providername"],
            diagnosis,
        )

        ctx = AftercarContext(
            panumber=str(panumber),
            diagnosis=diagnosis,
            drugs=classified["drugs"],
            procedures=classified["procedures"],
            hospital=pa["providername"],
            turn=1,
        )

        session.save_session(phone, {
            "enrollee_id": enrollee_id,
            "mode": "aftercare",
            "aftercare_context": ctx.model_dump(),
        })

        sent = await termii.send_whatsapp(phone, opening)
        if sent:
            _log_outreach(enrollee_id, str(panumber))
            session.append_message(phone, "assistant", opening)

    logger.info("Aftercare: nightly outreach complete")


AFTERCARE_SYSTEM = (
    "You are Klaire, the AI health assistant for Clearline International Limited, "
    "a Nigerian HMO. You are warm, empathetic, and speak naturally.\n\n"
    "You are doing a post-visit aftercare follow-up. Your goals across turns:\n"
    "  Turn 2: Ask about drug adherence (only if drugs were prescribed)\n"
    "  Turn 3: Ask about procedure/follow-up attendance (only if procedures done)\n"
    "  Turn 4: Collect a rating (1-5) and any comments, then thank them and close.\n\n"
    "Be warm. Keep messages short for WhatsApp. One topic per message."
)


async def handle_reply(
    phone: str,
    text: str,
    enrollee: EnrolleeIdentity,
    sess: dict,
) -> str:
    """Handle enrollee reply during an active aftercare conversation."""
    ctx_data = sess.get("aftercare_context")
    if not ctx_data:
        return await _close_aftercare(phone, enrollee.firstname)

    ctx = AftercarContext(**ctx_data)
    turn = ctx.turn

    context_block = (
        f"Enrollee: {enrollee.firstname} {enrollee.lastname}\n"
        f"Hospital visited: {ctx.hospital}\n"
        f"Diagnosis: {ctx.diagnosis}\n"
        f"Drugs prescribed: {', '.join(ctx.drugs) if ctx.drugs else 'none'}\n"
        f"Procedures done: {', '.join(ctx.procedures) if ctx.procedures else 'none'}\n"
        f"Current turn: {turn} of 4"
    )

    history = sess.get("messages", [])[-6:] + [{"role": "user", "content": text}]

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=f"{AFTERCARE_SYSTEM}\n\nEnrollee context:\n{context_block}",
        messages=history,
    )
    reply = response.content[0].text

    next_turn = min(turn + 1, 4)
    ctx_updated = AftercarContext(
        panumber=ctx.panumber,
        diagnosis=ctx.diagnosis,
        drugs=ctx.drugs,
        procedures=ctx.procedures,
        hospital=ctx.hospital,
        turn=next_turn,
    )
    session.save_session(phone, {"aftercare_context": ctx_updated.model_dump()})

    if turn >= 4:
        session.save_session(phone, {"mode": "front_desk", "aftercare_context": None})

    return reply


async def _close_aftercare(phone: str, firstname: str) -> str:
    session.save_session(phone, {"mode": "front_desk", "aftercare_context": None})
    return f"Thank you {firstname}! Feel free to reach out anytime if you need help. 😊"
