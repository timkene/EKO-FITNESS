"""
QA Alert Notifications
======================
Sends email alerts to the QA team when flagged cases are identified.

Currently triggered by:
  - Specialist consultation: enrollee has seen another specialist in the last 30 days

Setup (add to secrets.toml or environment):
  SMTP_USER     = "your-gmail@gmail.com"
  SMTP_PASSWORD = "your-16-char-app-password"   # Gmail > Security > App Passwords

Gmail App Password guide:
  1. Enable 2-Step Verification on the sending Gmail account
  2. Go to myaccount.google.com > Security > App Passwords
  3. Create an app password for "Mail" > "Other"
  4. Paste the 16-character password into SMTP_PASSWORD
"""

import os
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

logger = logging.getLogger(__name__)

QA_RECIPIENT = "leocasey0@gmail.com"
SMTP_HOST    = "smtp.gmail.com"
SMTP_PORT    = 587


def _get_smtp_creds():
    """
    Reads Gmail credentials.
    Priority: env vars SMTP_USER / SMTP_PASSWORD → secrets.toml [gmail] section.
    The sending address is always leocasey0@gmail.com (matches the app password).
    """
    user = os.getenv("SMTP_USER", "")
    pwd  = os.getenv("SMTP_PASSWORD", "")

    if not pwd:
        try:
            try:
                import tomllib
            except ImportError:
                import tomli as tomllib
            secrets_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "../../.streamlit/secrets.toml")
            )
            with open(secrets_path, "rb") as f:
                s = tomllib.load(f)
            # [gmail] app_password in secrets.toml
            gmail_section = s.get("gmail", {})
            pwd  = gmail_section.get("app_password", "")
            user = user or QA_RECIPIENT   # send from same address
        except Exception as e:
            logger.warning(f"Could not read SMTP creds from secrets.toml: {e}")

    return user, pwd


def send_qa_alert(
    enrollee_id: str,
    hospital_name: str,
    encounter_date: str,
    specialist_code: str,
    specialist_name: str,
    diagnosis_code: str,
    diagnosis_name: str,
    last_specialist_date: str,
    days_since: int,
    qa_reason: str,
):
    """
    Send a QA alert email when an enrollee is flagged for specialist over-utilisation.
    Runs in a fire-and-forget thread so it never delays the API response.
    """
    import threading
    t = threading.Thread(
        target=_send,
        kwargs=dict(
            enrollee_id=enrollee_id,
            hospital_name=hospital_name,
            encounter_date=encounter_date,
            specialist_code=specialist_code,
            specialist_name=specialist_name,
            diagnosis_code=diagnosis_code,
            diagnosis_name=diagnosis_name,
            last_specialist_date=last_specialist_date,
            days_since=days_since,
            qa_reason=qa_reason,
        ),
        daemon=True,
    )
    t.start()


def _send(**kw):
    smtp_user, smtp_pwd = _get_smtp_creds()
    if not smtp_user or not smtp_pwd:
        logger.warning(
            "QA alert not sent: SMTP_USER / SMTP_PASSWORD not configured. "
            "Add them to secrets.toml or environment variables."
        )
        return

    subject = (
        f"[KLAIRE QA FLAG] {kw['enrollee_id']} — specialist over-utilisation · "
        f"{kw['encounter_date']}"
    )

    html = f"""
<html><body style="font-family:Arial,sans-serif;font-size:14px;color:#1e293b;">
<h2 style="color:#b45309;">⚠️ KLAIRE QA Alert — Specialist Over-Utilisation</h2>
<p>An enrollee has been seen by a specialist within the last <strong>30 days</strong>.
The consultation has been <strong>approved</strong>, but this case is flagged for your review.</p>

<table cellpadding="8" cellspacing="0" border="1" style="border-collapse:collapse;width:100%;max-width:600px;">
  <tr style="background:#f8fafc;"><td><strong>Enrollee ID</strong></td><td>{kw['enrollee_id']}</td></tr>
  <tr><td><strong>Hospital / Facility</strong></td><td>{kw['hospital_name'] or '—'}</td></tr>
  <tr style="background:#f8fafc;"><td><strong>Encounter Date</strong></td><td>{kw['encounter_date']}</td></tr>
  <tr><td><strong>Specialist Requested</strong></td><td>{kw['specialist_code']} — {kw['specialist_name']}</td></tr>
  <tr style="background:#f8fafc;"><td><strong>Diagnosis</strong></td><td>{kw['diagnosis_code']} — {kw['diagnosis_name']}</td></tr>
  <tr><td><strong>Previous Specialist Visit</strong></td><td>{kw['last_specialist_date']} ({kw['days_since']} day(s) ago)</td></tr>
</table>

<p style="margin-top:16px;padding:12px;background:#fef3c7;border-left:4px solid #d97706;border-radius:4px;">
  <strong>QA Note:</strong> {kw['qa_reason']}
</p>

<p style="color:#64748b;font-size:12px;margin-top:24px;">
  Sent by KLAIRE — Clearline International Contact Centre · {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
</p>
</body></html>
"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = smtp_user
    msg["To"]      = QA_RECIPIENT
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pwd)
            server.sendmail(smtp_user, QA_RECIPIENT, msg.as_string())
        logger.info(f"QA alert email sent for enrollee {kw['enrollee_id']} → {QA_RECIPIENT}")
    except Exception as e:
        logger.error(f"QA alert email failed: {e}")
