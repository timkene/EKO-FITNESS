"""Klaire WhatsApp Agent — FastAPI service (port 8004).

Webhook format: 360dialog / Meta WhatsApp Business API v2.
Aftercare routing added here; APScheduler jobs wired in Task 12.
"""
import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .models import D360Message
from . import identity, session, termii, front_desk

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

scheduler = AsyncIOScheduler(timezone="Africa/Lagos")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # APScheduler jobs (nightly aftercare + daily report) wired in Task 12
    scheduler.start()
    logger.info("Klaire WhatsApp service started on port 8004")
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
    """360dialog/Meta webhook verification endpoint."""
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
        # Non-text event (status update, template delivery report, etc.) — acknowledge silently
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
    # Aftercare routing wired in Task 12 once aftercare.py exists
    reply = await front_desk.handle(phone, text, enrollee, sess.get("messages", []))

    session.append_message(phone, "user", text)
    session.append_message(phone, "assistant", reply)
    await termii.send_whatsapp(phone, reply)
    return {"status": "sent"}
