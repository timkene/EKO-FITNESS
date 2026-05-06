"""Klaire WhatsApp Agent — FastAPI service (port 8004).

Webhook provider: 360dialog (Meta WABA v2).
Scheduled jobs: nightly aftercare at 20:00 WAT, daily report at 07:00 WAT.
"""
import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .models import D360Message
from . import identity, session, termii, front_desk, aftercare, report

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

scheduler = AsyncIOScheduler(timezone="Africa/Lagos")


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        aftercare.run_nightly_outreach,
        CronTrigger(hour=20, minute=0, timezone="Africa/Lagos"),
        id="nightly_aftercare",
        replace_existing=True,
    )
    scheduler.add_job(
        report.generate_and_send,
        CronTrigger(hour=7, minute=0, timezone="Africa/Lagos"),
        id="daily_report",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Klaire WhatsApp service started — "
        "nightly_aftercare at 20:00 WAT, daily_report at 07:00 WAT"
    )
    yield
    scheduler.shutdown()
    logger.info("Klaire WhatsApp service stopped")


app = FastAPI(title="Klaire WhatsApp Agent", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "klaire-whatsapp"}


@app.get("/webhook/whatsapp")
async def webhook_verify(
    hub_mode: str = Query(default="", alias="hub.mode"),
    hub_verify_token: str = Query(default="", alias="hub.verify_token"),
    hub_challenge: str = Query(default="", alias="hub.challenge"),
):
    """360dialog/Meta webhook verification handshake."""
    verify_token = os.getenv("D360_VERIFY_TOKEN", "")
    if hub_mode == "subscribe" and hub_verify_token == verify_token:
        logger.info("Webhook verification passed")
        return PlainTextResponse(hub_challenge)
    logger.warning("Webhook verification failed: mode=%s token=%s", hub_mode, hub_verify_token)
    raise HTTPException(status_code=403, detail="Verification failed")


@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    raw = await request.body()
    sig = request.headers.get("X-Hub-Signature-256", "")
    if not termii.verify_signature(raw, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid JSON")

    msg = D360Message.from_webhook(payload)
    if msg is None:
        return {"status": "ignored"}

    phone = termii.normalise_phone(msg.from_phone)
    text = msg.text.strip()
    if not text:
        return {"status": "ignored"}

    enrollee = identity.lookup_by_phone(phone)
    if not enrollee:
        await termii.send_whatsapp(
            phone,
            "I wasn't able to find your details in our system. "
            "Please contact Clearline directly on 01-234-5678.",
        )
        return {"status": "not_found"}

    sess = session.load_session(phone)

    if sess.get("mode") == "aftercare" and sess.get("aftercare_context"):
        reply = await aftercare.handle_reply(phone, text, enrollee, sess)
    else:
        reply = await front_desk.handle(phone, text, enrollee, sess.get("messages", []))

    session.append_message(phone, "user", text)
    session.append_message(phone, "assistant", reply)
    await termii.send_whatsapp(phone, reply)
    return {"status": "sent"}
