"""
Football (Eko React) API: signup, login, admin approve, dues, payment evidence, suspend/activate.
"""
import os
import re
import smtplib
import logging
import uuid
from datetime import datetime, date, timedelta
from typing import Optional
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from fastapi import APIRouter, HTTPException, Depends, Header, UploadFile, File, Query
from fastapi.responses import FileResponse, JSONResponse

# Tell clients/CDN not to cache member stats/leaderboard so members always see fresh data after matchday ends
NO_CACHE_HEADERS = {"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"}
from pydantic import BaseModel, EmailStr, Field

logger = logging.getLogger(__name__)

router = APIRouter()

# Upload directory for payment evidence (relative to project root)
UPLOAD_DIR = Path(__file__).resolve().parent.parent.parent / "uploads" / "football"

# ---------------------------------------------------------------------------
# Helpers: DB, password, email
# ---------------------------------------------------------------------------

def get_conn():
    from core.database import get_db_connection
    return get_db_connection(read_only=False)


def _pbkdf2_hash(password: str, salt: str) -> str:
    import hashlib
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode(), 100000).hex()


def hash_password(password: str) -> str:
    """Hash with PBKDF2-SHA256 (no length limit). Stored as hex:salt for verify_password."""
    import hashlib
    import secrets
    salt = secrets.token_hex(16)
    h = _pbkdf2_hash(password, salt)
    return h + ":" + salt


def verify_password(plain: str, hashed: str) -> bool:
    """Verify against PBKDF2 (hex:salt) or legacy bcrypt hashes."""
    if ":" in hashed and len(hashed) > 80:
        parts = hashed.rsplit(":", 1)
        if len(parts) != 2:
            return False
        h, salt = parts[0], parts[1]
        return _pbkdf2_hash(plain, salt) == h
    try:
        from passlib.hash import bcrypt
        return bcrypt.verify(plain, hashed)
    except Exception:
        return False


def generate_player_password(first_name: str, baller_name: str, year: int) -> str:
    """Short, readable format: Eko + 4 alphanumeric + year (e.g. EkoA1b2-2025). Always under 20 chars."""
    import secrets
    import string
    alphabet = string.ascii_letters + string.digits
    rnd = "".join(secrets.choice(alphabet) for _ in range(4))
    return f"Eko{rnd}-{year}"


def _gmail_credentials():
    """Return (gmail_user, gmail_app_password). App password is stripped of spaces (Gmail shows it with spaces)."""
    user = os.getenv("GMAIL_USER", "").strip() or "leocasey0@gmail.com"
    raw = os.getenv("GMAIL_APP_PASSWORD") or ""
    app_password = raw.replace(" ", "").strip() if raw else ""
    return user, app_password


def send_credentials_email(to_email: str, username: str, password: str) -> tuple[bool, str]:
    """Send login credentials email. Returns (True, '') if sent, (False, error_msg) otherwise. Never raises."""
    gmail_user, gmail_app_password = _gmail_credentials()
    if not gmail_app_password:
        logger.warning("GMAIL_APP_PASSWORD not set; skipping send email")
        return False, "GMAIL_APP_PASSWORD not set"

    subject = "Your Eko Football App Login"
    body = f"""
Hello,

Your registration has been approved. Use these details to log in to the Eko Football app:

Username: {username}
Password: {password}

Please keep this email safe and do not share your password.
"""
    msg = MIMEMultipart()
    msg["From"] = gmail_user
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body.strip(), "plain"))

    def try_send(use_ssl=True):
        if use_ssl:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(gmail_user, gmail_app_password)
                server.sendmail(gmail_user, to_email, msg.as_string())
        else:
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(gmail_user, gmail_app_password)
                server.sendmail(gmail_user, to_email, msg.as_string())

    try:
        try_send(use_ssl=True)
        logger.info(f"Credentials email sent to {to_email}")
        return True, ""
    except Exception as e1:
        logger.warning(f"SMTP 465 failed, trying 587: {e1}")
        try:
            try_send(use_ssl=False)
            logger.info(f"Credentials email sent to {to_email} (via 587)")
            return True, ""
        except Exception as e2:
            logger.exception(f"Failed to send email to {to_email}")
            return False, str(e2)


def send_email_with_attachment(to_email: str, subject: str, body: str, attachment_path: Path, filename: str) -> None:
    gmail_user, gmail_app_password = _gmail_credentials()
    if not gmail_app_password:
        logger.warning("GMAIL_APP_PASSWORD not set; skipping send email")
        return
    msg = MIMEMultipart()
    msg["From"] = gmail_user
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body.strip(), "plain"))
    if attachment_path.exists():
        with open(attachment_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(part)
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_app_password)
            server.sendmail(gmail_user, to_email, msg.as_string())
        logger.info(f"Email with attachment sent to {to_email}")
    except Exception as e:
        logger.exception(f"Failed to send email to {to_email}")
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")


def get_current_quarter():
    """Quarter 1=Jan-Mar, 2=Apr-Jun, 3=Jul-Sep, 4=Oct-Dec."""
    month = datetime.utcnow().month
    return (month - 1) // 3 + 1


def _current_sunday() -> date:
    """The Sunday for the current matchday cycle (this or next Sunday)."""
    today = date.today()
    # weekday: Mon=0 .. Sun=6
    days_until_sun = (6 - today.weekday()) % 7
    if days_until_sun == 0:
        return today
    return today + timedelta(days=days_until_sun)


def _voting_opens_closes(sunday: date):
    """Voting opens Friday 00:00, closes Sunday 15:00 (3pm)."""
    friday = sunday - timedelta(days=2)
    opens_at = datetime.combine(friday, datetime.min.time())
    closes_at = datetime.combine(sunday, datetime.min.time().replace(hour=15, minute=0, second=0))
    return opens_at, closes_at


def _can_vote(dues_status: str, waiver_due_by, suspended: bool) -> bool:
    if suspended:
        return False
    if dues_status == "owing":
        return False
    if dues_status == "waiver":
        if waiver_due_by:
            try:
                d = waiver_due_by if isinstance(waiver_due_by, date) else date.fromisoformat(str(waiver_due_by)[:10])
                if d < date.today():
                    return False  # waiver expired, treat as owing
            except Exception:
                pass
        return True
    return dues_status == "paid"


def _resolve_dues_status(status: str, waiver_due_by) -> str:
    """If waiver and past due, return 'owing'."""
    if status != "waiver" or not waiver_due_by:
        return status
    try:
        d = waiver_due_by if isinstance(waiver_due_by, date) else date.fromisoformat(str(waiver_due_by)[:10])
        if d < date.today():
            return "owing"
    except Exception:
        pass
    return "waiver"


def require_player(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization")
    token = authorization.replace("Bearer ", "")
    payload = verify_token(token)
    if not payload or payload.get("role") != "player":
        raise HTTPException(status_code=401, detail="Player access required")
    return payload


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class SignupRequest(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=100)
    surname: str = Field(..., min_length=1, max_length=100)
    baller_name: str = Field(..., min_length=1, max_length=100)
    jersey_number: int = Field(..., ge=1, le=100)
    email: EmailStr
    whatsapp_phone: str = Field(..., min_length=1, max_length=30)


class LoginRequest(BaseModel):
    username: str  # baller name
    password: str


class AdminLoginRequest(BaseModel):
    username: str
    password: str


# Simple JWT-like token (use a secret from env)
def create_token(identifier: str, role: str) -> str:
    import base64
    import json
    secret = os.getenv("FOOTBALL_JWT_SECRET", "eko-football-secret-change-me")
    payload = {"sub": identifier, "role": role, "exp": datetime.utcnow().timestamp() + 86400 * 7}
    raw = json.dumps(payload) + secret
    return base64.urlsafe_b64encode(raw.encode()).decode()


def verify_token(token: str) -> dict:
    import base64
    import json
    try:
        raw = base64.urlsafe_b64decode(token.encode()).decode()
        payload_str = raw[:-len(os.getenv("FOOTBALL_JWT_SECRET", "eko-football-secret-change-me"))]
        return json.loads(payload_str)
    except Exception:
        return None


def require_admin(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization")
    token = authorization.replace("Bearer ", "")
    payload = verify_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Admin access required")
    return payload


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/signup")
def signup(body: SignupRequest):
    """Player self-signup. Creates a pending registration."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.players"
        )
        next_id = cur.fetchone()[0]

        conn.execute("""
            INSERT INTO FOOTBALL.players (id, first_name, surname, baller_name, jersey_number, email, whatsapp_phone, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
        """, [next_id, body.first_name.strip(), body.surname.strip(), body.baller_name.strip(), body.jersey_number, body.email.strip().lower(), body.whatsapp_phone.strip()])

        return {"success": True, "message": "Registration submitted. You will receive login details after admin approval."}
    except Exception as e:
        if "UNIQUE constraint" in str(e) or "duplicate" in str(e).lower():
            raise HTTPException(status_code=400, detail="Baller name already registered.")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/login")
def login(body: LoginRequest):
    """Player login. Returns token and player info. Suspended members cannot log in."""
    conn = get_conn()
    row = conn.execute("""
        SELECT id, baller_name, password_hash, first_name, surname, jersey_number, email, status,
               COALESCE(suspended, false) as suspended
        FROM FOOTBALL.players WHERE LOWER(TRIM(baller_name)) = LOWER(TRIM(?)) AND status = 'approved'
    """, [body.username]).fetchone()

    if not row:
        raise HTTPException(status_code=401, detail="Invalid username or not approved.")
    pid, baller_name, password_hash, first_name, surname, jersey_number, email, _, suspended = row
    if suspended:
        raise HTTPException(status_code=403, detail="Your account is suspended. Contact admin.")
    if not verify_password(body.password, password_hash):
        raise HTTPException(status_code=401, detail="Invalid password.")

    token = create_token(str(pid), "player")
    return {
        "success": True,
        "token": token,
        "player": {
            "id": pid,
            "baller_name": baller_name,
            "first_name": first_name,
            "surname": surname,
            "jersey_number": jersey_number,
            "email": email,
        },
    }


@router.post("/admin/login")
def admin_login(body: AdminLoginRequest):
    """Admin login. Use ADMIN_USERNAME and ADMIN_PASSWORD env vars. Defaults: admin / admin123."""
    username = (body.username or "").strip()
    password = (body.password or "").strip()
    admin_user = os.getenv("ADMIN_USERNAME")
    admin_pass = os.getenv("ADMIN_PASSWORD")
    if not admin_user:
        admin_user = "admin"
    if not admin_pass:
        admin_pass = "admin123"
    if username != admin_user or password != admin_pass:
        raise HTTPException(status_code=401, detail="Invalid admin credentials.")
    token = create_token(username or body.username, "admin")
    return {"success": True, "token": token}


@router.get("/admin/pending")
def admin_pending(payload: dict = Depends(require_admin)):
    """List all pending sign-ups."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, first_name, surname, baller_name, jersey_number, email, whatsapp_phone, created_at
        FROM FOOTBALL.players WHERE status = 'pending' ORDER BY created_at
    """).fetchall()
    columns = ["id", "first_name", "surname", "baller_name", "jersey_number", "email", "whatsapp_phone", "created_at"]
    return {"success": True, "pending": [dict(zip(columns, r)) for r in rows]}


@router.post("/admin/approve/{player_id:int}")
def admin_approve(player_id: int, payload: dict = Depends(require_admin)):
    """Approve a player, set password, send email with username and password. Always returns 200 so the client can show the message (no 5xx)."""
    try:
        conn = get_conn()
        row = conn.execute("""
            SELECT id, first_name, surname, baller_name, email, status
            FROM FOOTBALL.players WHERE id = ?
        """, [player_id]).fetchone()
        if not row:
            return {"success": False, "message": "Player not found."}
        pid, first_name, surname, baller_name, email, status = row
        if status != "pending":
            return {"success": False, "message": "Player is not pending."}

        year = datetime.utcnow().year
        password = generate_player_password(first_name, baller_name, year)
        password_hash = hash_password(password)

        conn.execute("""
            UPDATE FOOTBALL.players SET status = 'approved', password_hash = ?, password_display = ?, year_registered = ?, approved_at = current_timestamp WHERE id = ?
        """, [password_hash, password, year, player_id])

        # Approval is already saved. Send email (never raises).
        email_sent, email_err = send_credentials_email(email, baller_name, password)
        if email_sent:
            return {"success": True, "message": f"Approved and email sent to {email}"}
        if email_err and "not set" not in email_err.lower():
            return {"success": True, "message": f"Approved. Email could not be sent: {email_err}. Check GMAIL_APP_PASSWORD on Render."}
        return {"success": True, "message": f"Approved. Email not sent (set GMAIL_USER and GMAIL_APP_PASSWORD on Render)."}
    except Exception as e:
        logger.exception("Approve failed")
        return {"success": False, "message": f"Approve failed: {str(e)}"}


@router.post("/admin/reject/{player_id:int}")
def admin_reject(player_id: int, payload: dict = Depends(require_admin)):
    """Reject a pending registration."""
    conn = get_conn()
    conn.execute("UPDATE FOOTBALL.players SET status = 'rejected' WHERE id = ? AND status = 'pending'", [player_id])
    check = conn.execute("SELECT id FROM FOOTBALL.players WHERE id = ? AND status = 'rejected'", [player_id]).fetchone()
    if not check:
        raise HTTPException(status_code=404, detail="Player not found or not pending.")
    return {"success": True, "message": "Registration rejected."}


# ---------- Approved members (with passwords), suspend, activate, dues ----------

@router.get("/admin/approved")
def admin_approved(payload: dict = Depends(require_admin)):
    """List all approved members with passwords and dues status."""
    conn = get_conn()
    # Backfill password_display for approved members who don't have it (e.g. approved before we stored it)
    missing = conn.execute("""
        SELECT id, first_name, baller_name, year_registered, approved_at
        FROM FOOTBALL.players
        WHERE status = 'approved' AND (password_display IS NULL OR password_display = '')
    """).fetchall()
    for row in (missing or []):
        pid, first_name, baller_name, year_reg, approved_at = row
        if year_reg:
            year = int(year_reg)
        elif approved_at:
            s = str(approved_at)[:4]
            year = int(s) if s.isdigit() else datetime.utcnow().year
        else:
            year = datetime.utcnow().year
        try:
            password = generate_player_password(first_name or "", baller_name or "", year)
            password_hash = hash_password(password)
            conn.execute(
                "UPDATE FOOTBALL.players SET password_display = ?, password_hash = ? WHERE id = ?",
                [password, password_hash, pid],
            )
        except Exception:
            pass
    year = datetime.utcnow().year
    q = get_current_quarter()
    rows = conn.execute("""
        SELECT p.id, p.first_name, p.surname, p.baller_name, p.jersey_number, p.email, p.whatsapp_phone,
               COALESCE(p.password_display, '') as password_display,
               COALESCE(p.suspended, false) as suspended,
               p.approved_at
        FROM FOOTBALL.players p
        WHERE p.status = 'approved'
        ORDER BY p.baller_name
    """).fetchall()
    cols = ["id", "first_name", "surname", "baller_name", "jersey_number", "email", "whatsapp_phone", "password_display", "suspended", "approved_at"]
    members = [dict(zip(cols, r)) for r in rows]
    for m in members:
        dues_row = conn.execute("""
            SELECT status, waiver_due_by FROM FOOTBALL.dues WHERE player_id = ? AND year = ? AND quarter = ?
        """, [m["id"], year, q]).fetchone()
        if dues_row:
            m["dues_status"] = _resolve_dues_status(dues_row[0], dues_row[1])
            m["waiver_due_by"] = str(dues_row[1]) if dues_row[1] else None
        else:
            m["dues_status"] = "owing"
            m["waiver_due_by"] = None
        m["dues_year"] = year
        m["dues_quarter"] = q
    return {"success": True, "approved": members, "current_quarter": q, "current_year": year}


@router.get("/admin/dues-by-quarter")
def admin_dues_by_quarter(
    year: int = Query(..., description="Year (e.g. 2026)"),
    quarter: int = Query(..., ge=1, le=4, description="Quarter 1-4"),
    payload: dict = Depends(require_admin),
):
    """List all approved members with dues status for a specific quarter. Use any year/quarter (current or past)."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT p.id, p.first_name, p.surname, p.baller_name, p.jersey_number
        FROM FOOTBALL.players p
        WHERE p.status = 'approved'
        ORDER BY p.baller_name
    """).fetchall()
    members = []
    for r in rows:
        pid, first_name, surname, baller_name, jersey_number = r
        dues_row = conn.execute(
            "SELECT status, waiver_due_by FROM FOOTBALL.dues WHERE player_id = ? AND year = ? AND quarter = ?",
            [pid, year, quarter],
        ).fetchone()
        raw_status = "owing"
        waiver_due_by = None
        if dues_row:
            raw_status = dues_row[0]
            waiver_due_by = str(dues_row[1])[:10] if dues_row[1] else None
        effective = _resolve_dues_status(raw_status, dues_row[1] if dues_row else None)
        # For display: waiver but past due = "waiver_overdue" so UI can show "Waiver (didn't pay)"
        display_status = effective
        if raw_status == "waiver" and waiver_due_by:
            try:
                if date.fromisoformat(waiver_due_by) < date.today():
                    display_status = "waiver_overdue"
            except Exception:
                pass
        members.append({
            "id": pid,
            "first_name": first_name,
            "surname": surname,
            "baller_name": baller_name,
            "jersey_number": jersey_number,
            "dues_status": effective,
            "display_status": display_status,
            "raw_status": raw_status,
            "waiver_due_by": waiver_due_by,
            "dues_year": year,
            "dues_quarter": quarter,
        })
    return {"success": True, "year": year, "quarter": quarter, "members": members}


@router.post("/admin/suspend/{player_id:int}")
def admin_suspend(player_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    conn.execute("UPDATE FOOTBALL.players SET suspended = true WHERE id = ? AND status = 'approved'", [player_id])
    if conn.execute("SELECT id FROM FOOTBALL.players WHERE id = ? AND suspended = true", [player_id]).fetchone() is None:
        raise HTTPException(status_code=404, detail="Player not found or not approved.")
    return {"success": True, "message": "Member suspended."}


@router.post("/admin/activate/{player_id:int}")
def admin_activate(player_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    conn.execute("UPDATE FOOTBALL.players SET suspended = false WHERE id = ?", [player_id])
    return {"success": True, "message": "Member activated."}


class SetDuesBody(BaseModel):
    year: int
    quarter: int = Field(..., ge=1, le=4)
    status: str = Field(..., pattern="^(paid|owing|waiver)$")
    waiver_due_by: Optional[str] = None  # ISO date when status is waiver


@router.put("/admin/dues/{player_id:int}")
def admin_set_dues(player_id: int, body: SetDuesBody, payload: dict = Depends(require_admin)):
    conn = get_conn()
    waiver_date = None
    if body.status == "waiver" and body.waiver_due_by:
        try:
            waiver_date = body.waiver_due_by[:10]
        except Exception:
            pass
    cur = conn.execute("SELECT id FROM FOOTBALL.dues WHERE player_id = ? AND year = ? AND quarter = ?", [player_id, body.year, body.quarter])
    if cur.fetchone():
        conn.execute("""
            UPDATE FOOTBALL.dues SET status = ?, paid_at = CASE WHEN ? = 'paid' THEN current_timestamp ELSE NULL END,
            waiver_due_by = ?
            WHERE player_id = ? AND year = ? AND quarter = ?
        """, [body.status, body.status, waiver_date if body.status == "waiver" else None, player_id, body.year, body.quarter])
    else:
        next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.dues").fetchone()[0]
        conn.execute("""
            INSERT INTO FOOTBALL.dues (id, player_id, year, quarter, status, paid_at, waiver_due_by)
            VALUES (?, ?, ?, ?, ?, CASE WHEN ? = 'paid' THEN current_timestamp ELSE NULL END, ?)
        """, [next_id, player_id, body.year, body.quarter, body.status, body.status, waiver_date])
    return {"success": True, "message": f"Dues set to {body.status}."}


# ---------- Payment evidence ----------

@router.get("/admin/payment-evidence")
def admin_payment_evidence(payload: dict = Depends(require_admin)):
    """List payment evidence pending approval."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT pe.id, pe.player_id, pe.year, pe.quarter, pe.file_name, pe.submitted_at,
               p.baller_name, p.first_name, p.surname
        FROM FOOTBALL.payment_evidence pe
        JOIN FOOTBALL.players p ON p.id = pe.player_id
        WHERE pe.status = 'pending'
        ORDER BY pe.submitted_at
    """).fetchall()
    cols = ["id", "player_id", "year", "quarter", "file_name", "submitted_at", "baller_name", "first_name", "surname"]
    return {"success": True, "pending": [dict(zip(cols, r)) for r in rows]}


@router.get("/admin/payment-evidence/{evidence_id:int}/file")
def admin_payment_evidence_file(evidence_id: int, payload: dict = Depends(require_admin)):
    """View/download payment evidence file before approving or rejecting."""
    conn = get_conn()
    row = conn.execute(
        "SELECT file_path, file_name FROM FOOTBALL.payment_evidence WHERE id = ? AND status = 'pending'",
        [evidence_id],
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Evidence not found or already reviewed.")
    file_path, file_name = row
    full_path = Path(file_path)
    if not full_path.is_absolute():
        full_path = UPLOAD_DIR / (full_path.name if full_path.name else full_path)
    if not full_path.exists():
        raise HTTPException(status_code=404, detail="File no longer available.")
    suffix = (Path(file_name).suffix or "").lower()
    media_types = {".pdf": "application/pdf", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp"}
    media_type = media_types.get(suffix, "application/octet-stream")
    return FileResponse(
        path=str(full_path),
        filename=file_name,
        media_type=media_type,
    )


@router.post("/admin/approve-payment/{evidence_id:int}")
def admin_approve_payment(evidence_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    row = conn.execute("SELECT id, player_id, year, quarter, file_path, file_name FROM FOOTBALL.payment_evidence WHERE id = ? AND status = 'pending'", [evidence_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Evidence not found or already reviewed.")
    eid, player_id, year, quarter, file_path, file_name = row
    full_path = Path(file_path)
    if not full_path.is_absolute():
        full_path = UPLOAD_DIR / full_path
    delete_after = (date.today() + timedelta(days=7)).isoformat()
    conn.execute("UPDATE FOOTBALL.payment_evidence SET status = 'approved', reviewed_at = current_timestamp, delete_after = ? WHERE id = ?", [delete_after, evidence_id])
    conn.execute("""
        UPDATE FOOTBALL.dues SET status = 'paid', paid_at = current_timestamp
        WHERE player_id = ? AND year = ? AND quarter = ?
    """, [player_id, year, quarter])
    cur = conn.execute("SELECT id FROM FOOTBALL.dues WHERE player_id = ? AND year = ? AND quarter = ?", [player_id, year, quarter])
    if not cur.fetchone():
        next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.dues").fetchone()[0]
        conn.execute("INSERT INTO FOOTBALL.dues (id, player_id, year, quarter, status, paid_at) VALUES (?, ?, ?, ?, 'paid', current_timestamp)", [next_id, player_id, year, quarter])
    gmail_user = os.getenv("GMAIL_USER", "leocasey0@gmail.com")
    send_email_with_attachment(
        gmail_user,
        f"Payment evidence approved – {file_name}",
        f"Payment evidence from player id {player_id} for {year} Q{quarter} has been approved. Attachment: {file_name}. File will be deleted after 1 week.",
        full_path,
        file_name,
    )
    return {"success": True, "message": "Approved. You will receive the attachment by email. File will be deleted after 1 week."}


@router.post("/admin/reject-payment/{evidence_id:int}")
def admin_reject_payment(evidence_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    conn.execute("UPDATE FOOTBALL.payment_evidence SET status = 'rejected', reviewed_at = current_timestamp WHERE id = ? AND status = 'pending'", [evidence_id])
    if conn.execute("SELECT id FROM FOOTBALL.payment_evidence WHERE id = ? AND status = 'rejected'", [evidence_id]).fetchone() is None:
        raise HTTPException(status_code=404, detail="Evidence not found or already reviewed.")
    return {"success": True, "message": "Rejected."}


# ---------- Member: dues status and submit payment evidence ----------

@router.get("/member/dues")
def member_dues(payload: dict = Depends(require_player)):
    conn = get_conn()
    player_id = int(payload["sub"])
    year = datetime.utcnow().year
    q = get_current_quarter()
    row = conn.execute(
        "SELECT status, waiver_due_by FROM FOOTBALL.dues WHERE player_id = ? AND year = ? AND quarter = ?",
        [player_id, year, q],
    ).fetchone()
    if row:
        status = _resolve_dues_status(row[0], row[1])
        waiver_due_by = str(row[1]) if row[1] else None
        if status == "owing" and row[0] == "waiver" and row[1]:
            conn.execute(
                "UPDATE FOOTBALL.dues SET status = 'owing', waiver_due_by = NULL WHERE player_id = ? AND year = ? AND quarter = ?",
                [player_id, year, q],
            )
            waiver_due_by = None
    else:
        status = "owing"
        waiver_due_by = None
    pending_row = conn.execute(
        "SELECT id FROM FOOTBALL.payment_evidence WHERE player_id = ? AND year = ? AND quarter = ? AND status = 'pending'",
        [player_id, year, q],
    ).fetchone()
    pending_evidence = pending_row is not None
    return {"success": True, "year": year, "quarter": q, "status": status, "waiver_due_by": waiver_due_by, "pending_evidence": pending_evidence}


@router.post("/member/payment-evidence")
async def member_payment_evidence(
    file: UploadFile = File(...),
    payload: dict = Depends(require_player),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Please upload a file.")
    conn = get_conn()
    player_id = int(payload["sub"])
    year = datetime.utcnow().year
    q = get_current_quarter()
    dues_row = conn.execute("SELECT status FROM FOOTBALL.dues WHERE player_id = ? AND year = ? AND quarter = ?", [player_id, year, q]).fetchone()
    if dues_row and dues_row[0] == "paid":
        raise HTTPException(status_code=400, detail="Already paid for this quarter. You cannot send payment evidence.")
    pending_row = conn.execute(
        "SELECT id FROM FOOTBALL.payment_evidence WHERE player_id = ? AND year = ? AND quarter = ? AND status = 'pending'",
        [player_id, year, q],
    ).fetchone()
    if pending_row:
        raise HTTPException(status_code=400, detail="You already have payment evidence under review. Wait for it to be approved or rejected before sending another.")
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    ext = Path(file.filename).suffix or ""
    safe_name = f"{uuid.uuid4().hex}{ext}"
    path = UPLOAD_DIR / safe_name
    content = await file.read()
    with open(path, "wb") as f:
        f.write(content)
    next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.payment_evidence").fetchone()[0]
    conn.execute("""
        INSERT INTO FOOTBALL.payment_evidence (id, player_id, year, quarter, file_path, file_name, status)
        VALUES (?, ?, ?, ?, ?, ?, 'pending')
    """, [next_id, player_id, year, q, str(path), file.filename or safe_name])
    return {"success": True, "message": "Payment evidence submitted. Admin will review it."}


class WaiverApplyBody(BaseModel):
    due_by: str  # ISO date when member will pay


@router.post("/member/waiver")
def member_apply_waiver(body: WaiverApplyBody, payload: dict = Depends(require_player)):
    """Apply for waiver for current quarter; set date when you will pay."""
    conn = get_conn()
    player_id = int(payload["sub"])
    year = datetime.utcnow().year
    q = get_current_quarter()
    try:
        due_date = body.due_by[:10]
        date.fromisoformat(due_date)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid due_by date (use YYYY-MM-DD).")
    cur = conn.execute("SELECT id FROM FOOTBALL.dues WHERE player_id = ? AND year = ? AND quarter = ?", [player_id, year, q])
    if cur.fetchone():
        conn.execute(
            "UPDATE FOOTBALL.dues SET status = 'waiver', waiver_due_by = ? WHERE player_id = ? AND year = ? AND quarter = ?",
            [due_date, player_id, year, q],
        )
    else:
        next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.dues").fetchone()[0]
        conn.execute(
            "INSERT INTO FOOTBALL.dues (id, player_id, year, quarter, status, waiver_due_by) VALUES (?, ?, ?, ?, 'waiver', ?)",
            [next_id, player_id, year, q, due_date],
        )
    return {"success": True, "message": f"Waiver applied. Pay by {due_date}. Shown in your and admin portal as reminder."}


# ---------- Matchday: module (list, create, by id), voting, groups, fixtures ----------

def _matchday_row_to_dict(conn, row):
    """Build matchday dict from (id, sunday_date, status, voting_opens_at, voting_closes_at, created_at, reviewed_at, groups_published, fixtures_published, matchday_ended)."""
    (mid, sunday_date, status, voting_opens_at, voting_closes_at, created_at, reviewed_at,
     groups_published, fixtures_published, matchday_ended) = row
    return {
        "id": mid,
        "sunday_date": sunday_date.isoformat() if hasattr(sunday_date, "isoformat") else str(sunday_date)[:10],
        "status": status,
        "voting_opens_at": voting_opens_at.isoformat() if voting_opens_at and hasattr(voting_opens_at, "isoformat") else (str(voting_opens_at) if voting_opens_at else None),
        "voting_closes_at": voting_closes_at.isoformat() if voting_closes_at and hasattr(voting_closes_at, "isoformat") else (str(voting_closes_at) if voting_closes_at else None),
        "created_at": str(created_at) if created_at else None,
        "reviewed_at": str(reviewed_at) if reviewed_at else None,
        "groups_published": bool(groups_published) if groups_published is not None else False,
        "fixtures_published": bool(fixtures_published) if fixtures_published is not None else False,
        "matchday_ended": bool(matchday_ended) if matchday_ended is not None else False,
    }


def _get_matchday_by_id(conn, matchday_id: int):
    """Get matchday dict by id or None."""
    row = conn.execute("""
        SELECT id, sunday_date, status, voting_opens_at, voting_closes_at, created_at, reviewed_at,
               COALESCE(groups_published, false), COALESCE(fixtures_published, false), COALESCE(matchday_ended, false)
        FROM FOOTBALL.matchdays WHERE id = ?
    """, [matchday_id]).fetchone()
    if not row:
        return None
    return _matchday_row_to_dict(conn, row)


# Others (guests): one pseudo id per group so goal/assist/card attach to the right group. Encoded as -matchday_id*OTHERS_GROUP_MULT - group_id.
OTHERS_GROUP_MULT = 1_000_000


def _others_id(matchday_id: int) -> int:
    """Legacy pseudo player_id for 'Others' (guests) on this matchday. Prefer _others_id_for_group for new data."""
    return -matchday_id


def _others_id_for_group(matchday_id: int, group_id: int) -> int:
    """Pseudo player_id for 'Others' representing a specific group. Used in goal/assist/card choices and storage."""
    return -matchday_id * OTHERS_GROUP_MULT - group_id


def _decode_others_group(matchday_id: int, player_id: int) -> Optional[int]:
    """If player_id is an Others-per-group pseudo id, return that group_id; else None."""
    if player_id >= 0:
        return None
    gid = -player_id - matchday_id * OTHERS_GROUP_MULT
    if gid <= 0:
        return None
    return gid


def _resolve_player_name(conn, matchday_id: int, player_id: int) -> str:
    """Resolve scorer/assister to display name: 'Others' or 'Others (Group N)', else baller_name from players."""
    gid = _decode_others_group(matchday_id, player_id)
    if gid is not None:
        row = conn.execute("SELECT group_index FROM FOOTBALL.matchday_groups WHERE matchday_id = ? AND id = ?", [matchday_id, gid]).fetchone()
        if row:
            return f"Others (Group {row[0]})"
        return "Others"
    if player_id == _others_id(matchday_id):
        return "Others"
    row = conn.execute("SELECT baller_name FROM FOOTBALL.players WHERE id = ?", [player_id]).fetchone()
    return row[0] if row else str(player_id)


def _is_present(conn, matchday_id: int, player_id: int) -> bool:
    """True if player has no attendance row (default present) or present=true."""
    if player_id == _others_id(matchday_id):
        return True
    row = conn.execute("SELECT present FROM FOOTBALL.matchday_attendance WHERE matchday_id = ? AND player_id = ?", [matchday_id, player_id]).fetchone()
    return row is None or bool(row[0])


def _goal_choices_for_fixture(conn, matchday_id: int, group_a_id: int, group_b_id: int) -> list:
    """List of {id, baller_name, is_others} for scorer/assister: present players in both groups + Others (Group A), Others (Group B)."""
    ga_idx = conn.execute("SELECT group_index FROM FOOTBALL.matchday_groups WHERE id = ?", [group_a_id]).fetchone()
    gb_idx = conn.execute("SELECT group_index FROM FOOTBALL.matchday_groups WHERE id = ?", [group_b_id]).fetchone()
    choices = [
        {"id": _others_id_for_group(matchday_id, group_a_id), "baller_name": f"Others (Group {ga_idx[0] if ga_idx else group_a_id})", "is_others": True},
        {"id": _others_id_for_group(matchday_id, group_b_id), "baller_name": f"Others (Group {gb_idx[0] if gb_idx else group_b_id})", "is_others": True},
    ]
    for gid in (group_a_id, group_b_id):
        rows = conn.execute("""
            SELECT mgm.player_id, p.baller_name
            FROM FOOTBALL.matchday_group_members mgm
            JOIN FOOTBALL.players p ON p.id = mgm.player_id
            WHERE mgm.matchday_id = ? AND mgm.group_id = ? AND mgm.player_id > 0
        """, [matchday_id, gid]).fetchall()
        for pid, baller in rows:
            if _is_present(conn, matchday_id, pid):
                choices.append({"id": pid, "baller_name": baller or str(pid), "is_others": False})
    return choices


class CreateMatchdayBody(BaseModel):
    matchday_date: str  # YYYY-MM-DD (typically a Sunday)


@router.post("/admin/matchdays")
def admin_create_matchday(body: CreateMatchdayBody, payload: dict = Depends(require_admin)):
    """Create a new matchday and open voting."""
    try:
        dt = datetime.strptime(body.matchday_date[:10], "%Y-%m-%d")
        sunday_date = dt.date().isoformat()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid matchday_date (use YYYY-MM-DD).")
    opens_at = datetime.utcnow()
    closes_at = datetime.combine(dt.date(), datetime.min.time().replace(hour=15, minute=0, second=0))
    opens_ts = opens_at.strftime("%Y-%m-%d %H:%M:%S")
    closes_ts = closes_at.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    try:
        # Use sequence so ids are never reused after delete (avoids "Duplicate key id: 1" when recreating for same day)
        try:
            next_id = conn.execute("SELECT nextval('FOOTBALL.matchday_id_seq')").fetchone()[0]
        except Exception:
            next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.matchdays").fetchone()[0]
        conn.execute("""
            INSERT INTO FOOTBALL.matchdays (id, sunday_date, status, voting_opens_at, voting_closes_at, groups_published, fixtures_published, matchday_ended)
            VALUES (?, ?, 'voting_open', ?, ?, false, false, false)
        """, [next_id, sunday_date, opens_ts, closes_ts])
        md = _get_matchday_by_id(conn, next_id)
        return {"success": True, "matchday": md, "message": "Matchday created. Voting is open."}
    except Exception as e:
        logger.exception("Create matchday failed")
        raise HTTPException(status_code=500, detail=f"Create matchday failed: {str(e)}")


@router.get("/admin/matchdays")
def admin_list_matchdays(payload: dict = Depends(require_admin)):
    """List all matchdays (newest first)."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, sunday_date, status, voting_opens_at, voting_closes_at, created_at, reviewed_at,
               COALESCE(groups_published, false), COALESCE(fixtures_published, false), COALESCE(matchday_ended, false)
        FROM FOOTBALL.matchdays ORDER BY sunday_date DESC, id DESC
    """).fetchall()
    return {"success": True, "matchdays": [_matchday_row_to_dict(conn, r) for r in rows]}


@router.get("/admin/matchdays/{matchday_id:int}")
def admin_get_matchday(matchday_id: int, payload: dict = Depends(require_admin)):
    """Get one matchday with vote count, voted_players, and eligible_players (for add/remove vote UI)."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    vote_count = conn.execute("SELECT COUNT(*) FROM FOOTBALL.matchday_votes WHERE matchday_id = ?", [matchday_id]).fetchone()[0]
    voted_rows = conn.execute("""
        SELECT v.player_id, p.baller_name
        FROM FOOTBALL.matchday_votes v
        JOIN FOOTBALL.players p ON p.id = v.player_id
        WHERE v.matchday_id = ?
        ORDER BY p.baller_name
    """, [matchday_id]).fetchall()
    voted_players = [{"player_id": r[0], "baller_name": r[1]} for r in voted_rows]
    year, q = datetime.utcnow().year, get_current_quarter()
    eligible_rows = conn.execute("""
        SELECT p.id, p.baller_name FROM FOOTBALL.players p
        JOIN FOOTBALL.dues d ON d.player_id = p.id AND d.year = ? AND d.quarter = ?
        WHERE p.status = 'approved' AND COALESCE(p.suspended, false) = false
        AND (d.status = 'paid' OR (d.status = 'waiver' AND d.waiver_due_by >= current_date))
        ORDER BY p.baller_name
    """, [year, q]).fetchall()
    eligible_players = [{"player_id": r[0], "baller_name": r[1]} for r in eligible_rows]
    # All approved non-suspended players for manual "Add vote" dropdown (not restricted by dues)
    add_vote_rows = conn.execute("""
        SELECT id, baller_name FROM FOOTBALL.players
        WHERE status = 'approved' AND COALESCE(suspended, false) = false
        ORDER BY baller_name
    """).fetchall()
    add_vote_choices = [{"player_id": r[0], "baller_name": r[1]} for r in add_vote_rows]
    return {"success": True, "matchday": md, "vote_count": vote_count, "voted_players": voted_players, "eligible_players": eligible_players, "add_vote_choices": add_vote_choices}


@router.get("/member/matchdays")
def member_list_matchdays(payload: dict = Depends(require_player)):
    """List matchdays: past (ended/rejected) and current (voting_open or groups/fixtures published). Members only see matchdays that exist; no 'current' until admin creates one."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, sunday_date, status, voting_opens_at, voting_closes_at, created_at, reviewed_at,
               COALESCE(groups_published, false), COALESCE(fixtures_published, false), COALESCE(matchday_ended, false)
        FROM FOOTBALL.matchdays ORDER BY sunday_date DESC, id DESC
    """).fetchall()
    matchdays = [_matchday_row_to_dict(conn, r) for r in rows]
    return {"success": True, "matchdays": matchdays}


def _top_scorers_assists(conn, matchday_id: int):
    """Return (top_scorers, top_assists) for matchday: baller_name or 'Others', counts."""
    fixture_ids = [r[0] for r in conn.execute("SELECT id FROM FOOTBALL.matchday_fixtures WHERE matchday_id = ?", [matchday_id]).fetchall()]
    if not fixture_ids:
        return [], []
    placeholders = ",".join("?" * len(fixture_ids))
    rows = conn.execute(
        f"SELECT scorer_player_id, assister_player_id FROM FOOTBALL.fixture_goals WHERE fixture_id IN ({placeholders})",
        fixture_ids,
    ).fetchall()
    scorer_counts = {}
    assister_counts = {}
    for sp, ap in rows:
        sn = _resolve_player_name(conn, matchday_id, sp)
        scorer_counts[sn] = scorer_counts.get(sn, 0) + 1
        if ap is not None:
            an = _resolve_player_name(conn, matchday_id, ap)
            assister_counts[an] = assister_counts.get(an, 0) + 1
    top_scorers = [{"baller_name": n, "goals": c} for n, c in sorted(scorer_counts.items(), key=lambda x: -x[1])]
    top_assists = [{"baller_name": n, "assists": c} for n, c in sorted(assister_counts.items(), key=lambda x: -x[1])]
    return top_scorers, top_assists


def _player_matchday_rating(conn, matchday_id: int, player_id: int) -> float:
    """Compute rating for one player for one matchday. Only real players (player_id > 0). Before any fixture is completed, all present get 5."""
    if player_id <= 0:
        return 0.0
    in_group = conn.execute("SELECT group_id FROM FOOTBALL.matchday_group_members WHERE matchday_id = ? AND player_id = ?", [matchday_id, player_id]).fetchone()
    if not in_group:
        return 0.0
    group_id = in_group[0]
    if not _is_present(conn, matchday_id, player_id):
        return 0.0
    completed = conn.execute(
        "SELECT 1 FROM FOOTBALL.matchday_fixtures WHERE matchday_id = ? AND status = 'completed' LIMIT 1",
        [matchday_id],
    ).fetchone()
    if not completed:
        return 5.0  # all present players get same points before first fixture
    rating = 5.0
    fixture_ids = [r[0] for r in conn.execute("SELECT id FROM FOOTBALL.matchday_fixtures WHERE matchday_id = ?", [matchday_id]).fetchall()]
    if fixture_ids:
        placeholders = ",".join("?" * len(fixture_ids))
        goals = conn.execute(f"SELECT COUNT(*) FROM FOOTBALL.fixture_goals WHERE fixture_id IN ({placeholders}) AND scorer_player_id = ?", fixture_ids + [player_id]).fetchone()[0]
        rating += goals * 2
        # 9. Hat-trick: +5
        if goals >= 3:
            rating += 5
    # 3. Assists: +1 each
    if fixture_ids:
        placeholders = ",".join("?" * len(fixture_ids))
        assists = conn.execute(f"SELECT COUNT(*) FROM FOOTBALL.fixture_goals WHERE fixture_id IN ({placeholders}) AND assister_player_id = ?", fixture_ids + [player_id]).fetchone()[0]
        rating += assists
    # 4-7. Position: 1st +5, 2nd +3, 3rd +2, 4th +1
    table = _league_table(conn, matchday_id)
    for pos, row in enumerate(table, 1):
        if row["group_id"] == group_id:
            if pos == 1: rating += 5
            elif pos == 2: rating += 3
            elif pos == 3: rating += 2
            elif pos == 4: rating += 1
            break
    # 8. Clean sheet: +1 per fixture in which the group kept a clean sheet (conceded 0 in that fixture)
    clean_sheet_fixtures = _group_clean_sheet_fixtures_count(conn, matchday_id, group_id)
    rating += clean_sheet_fixtures
    # 10. Yellow: -5 each
    # 11. Red: -10 each
    cards = conn.execute("SELECT yellow_count, red_count FROM FOOTBALL.matchday_cards WHERE matchday_id = ? AND player_id = ?", [matchday_id, player_id]).fetchone()
    if cards:
        rating -= cards[0] * 5 + cards[1] * 10
    return round(rating, 2)


def _top_ratings_for_matchday(conn, matchday_id: int):
    """Top player ratings for matchday (computed from present, goals, assists, position, clean sheet, hat-trick, cards). Excludes Others."""
    player_ids = [r[0] for r in conn.execute("SELECT DISTINCT player_id FROM FOOTBALL.matchday_group_members WHERE matchday_id = ? AND player_id > 0", [matchday_id]).fetchall()]
    out = []
    for pid in player_ids:
        row = conn.execute("SELECT baller_name FROM FOOTBALL.players WHERE id = ?", [pid]).fetchone()
        if row:
            out.append({"baller_name": row[0], "rating": _player_matchday_rating(conn, matchday_id, pid)})
    return sorted(out, key=lambda x: -x["rating"])


def _player_career_stats(conn, player_id: int) -> dict:
    """Career stats for one player: goals, assists, yellows, reds, clean_sheets, matchdays_present, per_matchday_ratings, average_rating."""
    if player_id <= 0:
        return {"goals": 0, "assists": 0, "yellow_cards": 0, "red_cards": 0, "clean_sheets": 0, "matchdays_present": 0, "matchday_ratings": [], "average_rating": 0.0}
    # Goals/assists: all fixture_goals
    goals = conn.execute("SELECT COUNT(*) FROM FOOTBALL.fixture_goals g JOIN FOOTBALL.matchday_fixtures f ON f.id = g.fixture_id WHERE g.scorer_player_id = ?", [player_id]).fetchone()[0]
    assists = conn.execute("SELECT COUNT(*) FROM FOOTBALL.fixture_goals g JOIN FOOTBALL.matchday_fixtures f ON f.id = g.fixture_id WHERE g.assister_player_id = ?", [player_id]).fetchone()[0]
    # Cards: sum matchday_cards
    cards = conn.execute("SELECT COALESCE(SUM(yellow_count), 0), COALESCE(SUM(red_count), 0) FROM FOOTBALL.matchday_cards WHERE player_id = ?", [player_id]).fetchone()
    yellows = cards[0] or 0
    reds = cards[1] or 0
    # Matchdays present: count where in group and present
    matchdays_present = 0
    clean_sheets = 0
    matchday_ratings_list = []
    md_rows = conn.execute("""
        SELECT mgm.matchday_id, m.sunday_date, COALESCE(m.matchday_ended, false)
        FROM FOOTBALL.matchday_group_members mgm
        JOIN FOOTBALL.matchdays m ON m.id = mgm.matchday_id
        WHERE mgm.player_id = ?
    """, [player_id]).fetchall()
    for mid, sunday_date, ended in md_rows:
        if _is_present(conn, mid, player_id):
            matchdays_present += 1
        # Only include rating in career average / leaderboard when matchday has been ended (so stars and leaderboard update after "End matchday")
        ended_flag = bool(ended) if ended is not None else False
        if ended_flag:
            rt = _player_matchday_rating(conn, mid, player_id)
            if rt != 0:
                matchday_ratings_list.append({"matchday_id": mid, "sunday_date": str(sunday_date)[:10], "rating": rt})
        # Clean sheets: count fixtures (not matchdays) where the group kept a clean sheet (only for ended matchdays)
        if ended_flag:
            g = conn.execute("SELECT group_id FROM FOOTBALL.matchday_group_members WHERE matchday_id = ? AND player_id = ?", [mid, player_id]).fetchone()
            if g:
                clean_sheets += _group_clean_sheet_fixtures_count(conn, mid, g[0])
    avg = round(sum(r["rating"] for r in matchday_ratings_list) / len(matchday_ratings_list), 2) if matchday_ratings_list else 0.0
    return {
        "goals": goals, "assists": assists, "yellow_cards": yellows, "red_cards": reds,
        "clean_sheets": clean_sheets, "matchdays_present": matchdays_present,
        "matchday_ratings": matchday_ratings_list, "average_rating": avg,
    }


@router.get("/member/matchdays/{matchday_id:int}")
def member_get_matchday(matchday_id: int, payload: dict = Depends(require_player)):
    """Get one matchday for member: my vote, my group, all groups (baller_name + present), fixtures with goal details, table, top scorers/assists/ratings."""
    conn = get_conn()
    player_id = int(payload["sub"])
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    voted = conn.execute("SELECT id FROM FOOTBALL.matchday_votes WHERE matchday_id = ? AND player_id = ?", [matchday_id, player_id]).fetchone() is not None
    year, q = datetime.utcnow().year, get_current_quarter()
    dues_row = conn.execute("SELECT status, waiver_due_by FROM FOOTBALL.dues WHERE player_id = ? AND year = ? AND quarter = ?", [player_id, year, q]).fetchone()
    suspended = conn.execute("SELECT COALESCE(suspended, false) FROM FOOTBALL.players WHERE id = ?", [player_id]).fetchone()[0]
    can_vote = _can_vote(dues_row[0] if dues_row else "owing", dues_row[1] if dues_row else None, suspended) if md["status"] == "voting_open" else False
    my_group = None
    all_groups = []
    if md.get("groups_published"):
        # All groups with baller_name and present (members see baller names only for privacy)
        group_rows = conn.execute("""
            SELECT mg.id, mg.group_index, mgm.player_id, p.baller_name, a.present
            FROM FOOTBALL.matchday_groups mg
            JOIN FOOTBALL.matchday_group_members mgm ON mgm.group_id = mg.id AND mgm.matchday_id = mg.matchday_id
            JOIN FOOTBALL.players p ON p.id = mgm.player_id
            LEFT JOIN FOOTBALL.matchday_attendance a ON a.matchday_id = mgm.matchday_id AND a.player_id = mgm.player_id
            WHERE mg.matchday_id = ?
            ORDER BY mg.group_index, p.baller_name
        """, [matchday_id]).fetchall()
        by_group = {}
        for gid, gidx, pid, baller, present in group_rows:
            if gid not in by_group:
                by_group[gid] = {"group_index": gidx, "members": []}
            by_group[gid]["members"].append({"baller_name": baller or str(pid), "present": bool(present) if present is not None else True})
        all_groups = list(by_group.values())
        # Add "Others" to each group for display (guests)
        for g in all_groups:
            g["members"].append({"baller_name": "Others", "present": True})
        # My group (full for current user's group)
        g = conn.execute("SELECT mg.id, mg.group_index FROM FOOTBALL.matchday_groups mg JOIN FOOTBALL.matchday_group_members mgm ON mgm.group_id = mg.id AND mgm.matchday_id = mg.matchday_id WHERE mg.matchday_id = ? AND mgm.player_id = ?", [matchday_id, player_id]).fetchone()
        if g:
            group_id, group_index = g
            members = conn.execute("SELECT p.id, p.baller_name, p.first_name, p.surname, p.jersey_number FROM FOOTBALL.matchday_group_members mgm JOIN FOOTBALL.players p ON p.id = mgm.player_id WHERE mgm.matchday_id = ? AND mgm.group_id = ?", [matchday_id, group_id]).fetchall()
            my_group = {"group_index": group_index, "members": [{"id": r[0], "baller_name": r[1], "first_name": r[2], "surname": r[3], "jersey_number": r[4]} for r in members]}
    fixtures = []
    if md.get("fixtures_published"):
        fix_rows = conn.execute("""
            SELECT f.id, f.group_a_id, f.group_b_id, f.status, f.home_goals, f.away_goals, f.started_at, f.ended_at,
                   ga.group_index AS group_a_index, gb.group_index AS group_b_index
            FROM FOOTBALL.matchday_fixtures f
            JOIN FOOTBALL.matchday_groups ga ON ga.id = f.group_a_id
            JOIN FOOTBALL.matchday_groups gb ON gb.id = f.group_b_id
            WHERE f.matchday_id = ? ORDER BY f.id
        """, [matchday_id]).fetchall()
        for r in fix_rows:
            fid = r[0]
            goals_rows = conn.execute("SELECT scorer_player_id, assister_player_id, minute, is_home_goal FROM FOOTBALL.fixture_goals WHERE fixture_id = ? ORDER BY id", [fid]).fetchall()
            goals_with_details = [
                {"minute": gr[2], "is_home_goal": gr[3], "scorer_name": _resolve_player_name(conn, matchday_id, gr[0]), "assister_name": _resolve_player_name(conn, matchday_id, gr[1]) if gr[1] is not None else None}
                for gr in goals_rows
            ]
            fixtures.append({
                "id": fid, "group_a_id": r[1], "group_b_id": r[2], "status": r[3],
                "home_goals": r[4] or 0, "away_goals": r[5] or 0,
                "started_at": str(r[6]) if r[6] else None, "ended_at": str(r[7]) if r[7] else None,
                "group_a_index": r[8], "group_b_index": r[9],
                "goals": goals_with_details,
            })
    table = _league_table(conn, matchday_id) if md.get("fixtures_published") or md.get("groups_published") else []
    top_scorers, top_assists = _top_scorers_assists(conn, matchday_id)
    top_ratings = _top_ratings_for_matchday(conn, matchday_id)
    vote_count = conn.execute("SELECT COUNT(*) FROM FOOTBALL.matchday_votes WHERE matchday_id = ?", [matchday_id]).fetchone()[0]
    return {
        "success": True, "matchday": md, "voted": voted, "can_vote": can_vote, "vote_count": vote_count,
        "my_group": my_group, "all_groups": all_groups, "fixtures": fixtures, "table": table,
        "top_scorers": top_scorers, "top_assists": top_assists, "top_ratings": top_ratings,
    }


def _group_clean_sheet_fixtures_count(conn, matchday_id: int, group_id: int) -> int:
    """Number of completed fixtures in this matchday where the group kept a clean sheet (conceded 0 in that fixture)."""
    rows = conn.execute("""
        SELECT group_a_id, group_b_id, home_goals, away_goals
        FROM FOOTBALL.matchday_fixtures
        WHERE matchday_id = ? AND status = 'completed'
    """, [matchday_id]).fetchall()
    count = 0
    for ga, gb, hg, ag in rows:
        hg, ag = hg or 0, ag or 0
        if group_id == ga and ag == 0:
            count += 1
        elif group_id == gb and hg == 0:
            count += 1
    return count


def _league_table(conn, matchday_id: int):
    """League table: group_index, played, won, drawn, lost, goals_for, goals_against, points (win=3, draw=1)."""
    groups = conn.execute("SELECT id, group_index FROM FOOTBALL.matchday_groups WHERE matchday_id = ? ORDER BY group_index", [matchday_id]).fetchall()
    if not groups:
        return []
    stats = {}
    for gid, gidx in groups:
        stats[gid] = {"group_id": gid, "group_index": gidx, "played": 0, "won": 0, "drawn": 0, "lost": 0, "goals_for": 0, "goals_against": 0, "points": 0}
    fixtures = conn.execute("SELECT group_a_id, group_b_id, home_goals, away_goals, status FROM FOOTBALL.matchday_fixtures WHERE matchday_id = ? AND status = 'completed'", [matchday_id]).fetchall()
    for ga, gb, hg, ag in [(r[0], r[1], r[2] or 0, r[3] or 0) for r in fixtures if r[4] == "completed"]:
        stats[ga]["played"] += 1
        stats[ga]["goals_for"] += hg
        stats[ga]["goals_against"] += ag
        stats[gb]["played"] += 1
        stats[gb]["goals_for"] += ag
        stats[gb]["goals_against"] += hg
        if hg > ag:
            stats[ga]["won"] += 1
            stats[ga]["points"] += 3
            stats[gb]["lost"] += 1
        elif ag > hg:
            stats[gb]["won"] += 1
            stats[gb]["points"] += 3
            stats[ga]["lost"] += 1
        else:
            stats[ga]["drawn"] += 1
            stats[ga]["points"] += 1
            stats[gb]["drawn"] += 1
            stats[gb]["points"] += 1
    return sorted(stats.values(), key=lambda x: (-x["points"], -(x["goals_for"] - x["goals_against"])))


@router.post("/member/matchdays/{matchday_id:int}/vote")
def member_vote_matchday(matchday_id: int, payload: dict = Depends(require_player)):
    """Cast vote for a matchday (only if voting open and member paid/waiver)."""
    conn = get_conn()
    player_id = int(payload["sub"])
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "voting_open":
        raise HTTPException(status_code=400, detail="Voting is closed.")
    year, q = datetime.utcnow().year, get_current_quarter()
    dues_row = conn.execute("SELECT status, waiver_due_by FROM FOOTBALL.dues WHERE player_id = ? AND year = ? AND quarter = ?", [player_id, year, q]).fetchone()
    suspended = conn.execute("SELECT COALESCE(suspended, false) FROM FOOTBALL.players WHERE id = ?", [player_id]).fetchone()[0]
    if not _can_vote(dues_row[0] if dues_row else "owing", dues_row[1] if dues_row else None, suspended):
        raise HTTPException(status_code=403, detail="Only paid or waiver members can vote.")
    existing = conn.execute("SELECT id FROM FOOTBALL.matchday_votes WHERE matchday_id = ? AND player_id = ?", [matchday_id, player_id]).fetchone()
    if existing:
        raise HTTPException(status_code=400, detail="You already voted.")
    next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.matchday_votes").fetchone()[0]
    conn.execute("INSERT INTO FOOTBALL.matchday_votes (id, matchday_id, player_id) VALUES (?, ?, ?)", [next_id, matchday_id, player_id])
    return {"success": True, "message": "Vote recorded."}


@router.post("/admin/matchdays/{matchday_id:int}/vote-all")
def admin_matchday_vote_all(matchday_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    status = md.get("status") or "unknown"
    if status != "voting_open":
        raise HTTPException(
            status_code=400,
            detail=f"Voting is not open for this matchday. Current status: {status}. Only matchdays with status 'voting_open' can use 'Approve all to vote'."
        )
    try:
        year, q = datetime.utcnow().year, get_current_quarter()
        rows = conn.execute("""SELECT p.id FROM FOOTBALL.players p JOIN FOOTBALL.dues d ON d.player_id = p.id AND d.year = ? AND d.quarter = ?
            WHERE p.status = 'approved' AND COALESCE(p.suspended, false) = false
            AND (d.status = 'paid' OR (d.status = 'waiver' AND (d.waiver_due_by IS NULL OR d.waiver_due_by >= current_date)))""", [year, q]).fetchall()
        eligible = [r[0] for r in rows]
        existing = set(r[0] for r in conn.execute("SELECT player_id FROM FOOTBALL.matchday_votes WHERE matchday_id = ?", [matchday_id]).fetchall())
        to_vote = [pid for pid in eligible if pid not in existing]
        next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.matchday_votes").fetchone()[0]
        for i, pid in enumerate(to_vote):
            conn.execute("INSERT INTO FOOTBALL.matchday_votes (id, matchday_id, player_id) VALUES (?, ?, ?)", [next_id + i, matchday_id, pid])
        return {"success": True, "message": f"Recorded {len(to_vote)} vote(s). Total: {len(existing) + len(to_vote)}.", "votes_added": len(to_vote)}
    except Exception as e:
        logger.exception("vote-all failed for matchday %s", matchday_id)
        raise HTTPException(status_code=500, detail=f"Could not record votes: {str(e)}")


@router.post("/admin/matchdays/{matchday_id:int}/close-voting")
def admin_matchday_close_voting(matchday_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "voting_open":
        raise HTTPException(status_code=400, detail="Voting is not open.")
    conn.execute("UPDATE FOOTBALL.matchdays SET status = 'closed_pending_review' WHERE id = ?", [matchday_id])
    return {"success": True, "message": "Voting closed. You can now approve or reject."}


@router.post("/admin/matchdays/{matchday_id:int}/reopen-voting")
def admin_matchday_reopen_voting(matchday_id: int, payload: dict = Depends(require_admin)):
    """Reopen voting so you can add/remove votes, then close again. Only allowed if no fixture has been completed."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "closed_pending_review":
        raise HTTPException(status_code=400, detail="Can only reopen when status is closed_pending_review.")
    completed = conn.execute(
        "SELECT 1 FROM FOOTBALL.matchday_fixtures WHERE matchday_id = ? AND status = 'completed' LIMIT 1",
        [matchday_id],
    ).fetchone()
    if completed:
        raise HTTPException(status_code=400, detail="Cannot reopen voting after a fixture has been completed.")
    conn.execute("UPDATE FOOTBALL.matchdays SET status = 'voting_open' WHERE id = ?", [matchday_id])
    return {"success": True, "message": "Voting reopened. Add/remove votes then close again."}


class AdminAddVoteBody(BaseModel):
    player_id: int


@router.post("/admin/matchdays/{matchday_id:int}/vote-add")
def admin_matchday_vote_add(matchday_id: int, body: AdminAddVoteBody, payload: dict = Depends(require_admin)):
    """Manually add a vote for one member (e.g. if they had app issues)."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "voting_open":
        raise HTTPException(status_code=400, detail="Voting is not open.")
    player_id = body.player_id
    existing = conn.execute("SELECT id FROM FOOTBALL.matchday_votes WHERE matchday_id = ? AND player_id = ?", [matchday_id, player_id]).fetchone()
    if existing:
        return {"success": True, "message": "Member already voted."}
    next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.matchday_votes").fetchone()[0]
    conn.execute("INSERT INTO FOOTBALL.matchday_votes (id, matchday_id, player_id) VALUES (?, ?, ?)", [next_id, matchday_id, player_id])
    return {"success": True, "message": "Vote added for member."}


@router.post("/admin/matchdays/{matchday_id:int}/vote-remove")
def admin_matchday_vote_remove(matchday_id: int, body: AdminAddVoteBody, payload: dict = Depends(require_admin)):
    """Remove a member's vote so they can vote again or be voted out."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "voting_open":
        raise HTTPException(status_code=400, detail="Voting is not open.")
    conn.execute("DELETE FROM FOOTBALL.matchday_votes WHERE matchday_id = ? AND player_id = ?", [matchday_id, body.player_id])
    return {"success": True, "message": "Vote removed."}


@router.post("/admin/matchdays/{matchday_id:int}/approve")
def admin_matchday_approve(matchday_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "closed_pending_review":
        raise HTTPException(status_code=400, detail="Matchday is not pending review.")
    conn.execute("UPDATE FOOTBALL.matchdays SET status = 'approved', reviewed_at = current_timestamp WHERE id = ?", [matchday_id])
    _ensure_groups(conn, matchday_id)
    return {"success": True, "message": "Matchday approved. Assign groups and publish when ready."}


@router.post("/admin/matchdays/{matchday_id:int}/reject")
def admin_matchday_reject(matchday_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "closed_pending_review":
        raise HTTPException(status_code=400, detail="Matchday is not pending review.")
    conn.execute("UPDATE FOOTBALL.matchdays SET status = 'rejected', reviewed_at = current_timestamp WHERE id = ?", [matchday_id])
    return {"success": True, "message": "Matchday rejected."}


@router.delete("/admin/matchdays/{matchday_id:int}")
def admin_delete_matchday(matchday_id: int, payload: dict = Depends(require_admin)):
    """Permanently delete a matchday and all its data (votes, groups, fixtures, goals, etc.). Use to remove example/test matchdays."""
    conn = get_conn()
    if _get_matchday_by_id(conn, matchday_id) is None:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    fixture_ids = [r[0] for r in conn.execute("SELECT id FROM FOOTBALL.matchday_fixtures WHERE matchday_id = ?", [matchday_id]).fetchall()]
    if fixture_ids:
        placeholders = ",".join("?" * len(fixture_ids))
        conn.execute(f"DELETE FROM FOOTBALL.fixture_goals WHERE fixture_id IN ({placeholders})", fixture_ids)
        conn.execute(f"DELETE FROM FOOTBALL.fixture_ratings WHERE fixture_id IN ({placeholders})", fixture_ids)
        conn.execute(f"DELETE FROM FOOTBALL.fixture_cards WHERE fixture_id IN ({placeholders})", fixture_ids)
    conn.execute("DELETE FROM FOOTBALL.matchday_fixtures WHERE matchday_id = ?", [matchday_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_cards WHERE matchday_id = ?", [matchday_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_attendance WHERE matchday_id = ?", [matchday_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_group_members WHERE matchday_id = ?", [matchday_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_groups WHERE matchday_id = ?", [matchday_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_votes WHERE matchday_id = ?", [matchday_id])
    conn.execute("DELETE FROM FOOTBALL.matchdays WHERE id = ?", [matchday_id])
    return {"success": True, "message": "Matchday deleted."}


@router.delete("/admin/players/{player_id:int}")
def admin_delete_player(player_id: int, payload: dict = Depends(require_admin)):
    """Permanently delete a member and all their data. Use to remove test/fake members."""
    conn = get_conn()
    row = conn.execute("SELECT id, status FROM FOOTBALL.players WHERE id = ?", [player_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Player not found.")
    conn.execute("DELETE FROM FOOTBALL.fixture_ratings WHERE player_id = ?", [player_id])
    conn.execute("DELETE FROM FOOTBALL.fixture_goals WHERE scorer_player_id = ? OR assister_player_id = ?", [player_id, player_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_cards WHERE player_id = ?", [player_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_attendance WHERE player_id = ?", [player_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_group_members WHERE player_id = ?", [player_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_votes WHERE player_id = ?", [player_id])
    conn.execute("DELETE FROM FOOTBALL.payment_evidence WHERE player_id = ?", [player_id])
    conn.execute("DELETE FROM FOOTBALL.dues WHERE player_id = ?", [player_id])
    conn.execute("DELETE FROM FOOTBALL.players WHERE id = ?", [player_id])
    return {"success": True, "message": "Member deleted."}


def _ensure_groups(conn, matchday_id: int):
    """Create random groups of 5 players + implicit Others (6 slots) from players who voted on this matchday."""
    existing = conn.execute("SELECT id FROM FOOTBALL.matchday_group_members WHERE matchday_id = ?", [matchday_id]).fetchone()
    if existing:
        return
    rows = conn.execute("""
        SELECT player_id FROM FOOTBALL.matchday_votes WHERE matchday_id = ? AND player_id > 0
    """, [matchday_id]).fetchall()
    player_ids = [r[0] for r in rows if r[0]]
    import random
    random.shuffle(player_ids)
    group_size = 5  # 5 players + 1 (Others) per team = 6 slots; Others is implicit, not stored
    group_index = 0
    next_gid = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.matchday_groups").fetchone()[0]
    next_mid = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.matchday_group_members").fetchone()[0]
    for i in range(0, len(player_ids), group_size):
        group_index += 1
        gid = next_gid
        next_gid += 1
        conn.execute("INSERT INTO FOOTBALL.matchday_groups (id, matchday_id, group_index) VALUES (?, ?, ?)", [gid, matchday_id, group_index])
        for pid in player_ids[i : i + group_size]:
            conn.execute("INSERT INTO FOOTBALL.matchday_group_members (id, matchday_id, group_id, player_id) VALUES (?, ?, ?, ?)", [next_mid, matchday_id, gid, pid])
            next_mid += 1


@router.post("/admin/matchdays/{matchday_id:int}/groups/regenerate")
def admin_matchday_regenerate_groups(matchday_id: int, payload: dict = Depends(require_admin)):
    """Recreate groups from voters only (5 players + Others per group). Use after closing voting to fix wrong groups."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "approved":
        raise HTTPException(status_code=400, detail="Matchday must be approved first.")
    conn.execute("DELETE FROM FOOTBALL.matchday_group_members WHERE matchday_id = ?", [matchday_id])
    conn.execute("DELETE FROM FOOTBALL.matchday_groups WHERE matchday_id = ?", [matchday_id])
    conn.execute("UPDATE FOOTBALL.matchdays SET groups_published = false WHERE id = ?", [matchday_id])
    _ensure_groups(conn, matchday_id)
    return {"success": True, "message": "Groups regenerated from voters only (5 players + Others per group). Re-publish when ready."}


@router.get("/admin/matchdays/{matchday_id:int}/groups")
def admin_matchday_groups(matchday_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "approved":
        return {"success": True, "matchday": md, "groups": [], "message": "Approve matchday first to see groups."}
    rows = conn.execute("""
        SELECT mg.id, mg.group_index, mgm.player_id, p.baller_name, p.first_name, p.surname, p.jersey_number
        FROM FOOTBALL.matchday_groups mg
        JOIN FOOTBALL.matchday_group_members mgm ON mgm.group_id = mg.id AND mgm.matchday_id = mg.matchday_id
        JOIN FOOTBALL.players p ON p.id = mgm.player_id
        WHERE mg.matchday_id = ?
        ORDER BY mg.group_index, p.baller_name
    """, [matchday_id]).fetchall()
    groups_dict = {}
    for r in rows:
        gid, gidx, pid, baller, first, last, jersey = r
        if gid not in groups_dict:
            groups_dict[gid] = {"group_id": gid, "group_index": gidx, "members": []}
        groups_dict[gid]["members"].append({"player_id": pid, "baller_name": baller, "first_name": first, "surname": last, "jersey_number": jersey})
    # Others is in every group (5 voted + 1 Others = max 6 per group). Append for display.
    others_id = _others_id(matchday_id)
    for g in groups_dict.values():
        g["members"].append({"player_id": others_id, "baller_name": "Others", "first_name": "—", "surname": "", "jersey_number": None, "is_others": True})
    return {"success": True, "matchday": md, "groups": list(groups_dict.values())}


class MoveMemberBody(BaseModel):
    from_group_id: int
    to_group_id: int
    player_id: int


@router.put("/admin/matchdays/{matchday_id:int}/groups/move")
def admin_matchday_move_member(matchday_id: int, body: MoveMemberBody, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "approved":
        raise HTTPException(status_code=400, detail="Matchday must be approved first.")
    if md.get("groups_published"):
        raise HTTPException(status_code=400, detail="Unpublish groups first to move members.")
    conn.execute(
        "UPDATE FOOTBALL.matchday_group_members SET group_id = ? WHERE matchday_id = ? AND group_id = ? AND player_id = ?",
        [body.to_group_id, matchday_id, body.from_group_id, body.player_id],
    )
    return {"success": True, "message": "Member moved."}


class MoveBatchBody(BaseModel):
    moves: list[MoveMemberBody]


@router.put("/admin/matchdays/{matchday_id:int}/groups/move-batch")
def admin_matchday_move_batch(matchday_id: int, body: MoveBatchBody, payload: dict = Depends(require_admin)):
    """Apply multiple moves at once. Unpublish groups first if already published."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "approved":
        raise HTTPException(status_code=400, detail="Matchday must be approved first.")
    if md.get("groups_published"):
        raise HTTPException(status_code=400, detail="Unpublish groups first to move members.")
    for move in body.moves:
        conn.execute(
            "UPDATE FOOTBALL.matchday_group_members SET group_id = ? WHERE matchday_id = ? AND group_id = ? AND player_id = ?",
            [move.to_group_id, matchday_id, move.from_group_id, move.player_id],
        )
    return {"success": True, "message": f"{len(body.moves)} move(s) applied."}


@router.post("/admin/matchdays/{matchday_id:int}/groups/unpublish")
def admin_matchday_unpublish_groups(matchday_id: int, payload: dict = Depends(require_admin)):
    """Recall published groups so you can edit and re-publish. Only allowed before matchday has ended."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if not md.get("groups_published"):
        return {"success": True, "message": "Groups were not published."}
    if md.get("matchday_ended"):
        raise HTTPException(status_code=400, detail="Cannot unpublish after matchday has ended.")
    conn.execute("UPDATE FOOTBALL.matchdays SET groups_published = false WHERE id = ?", [matchday_id])
    return {"success": True, "message": "Groups unpublished. You can edit and publish again."}


@router.get("/admin/matchdays/{matchday_id:int}/attendance")
def admin_matchday_attendance(matchday_id: int, payload: dict = Depends(require_admin)):
    """List all group members for this matchday with present/absent. Used to mark who showed up."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if not md.get("groups_published"):
        return {"success": True, "matchday": md, "attendance": [], "message": "Publish groups first."}
    rows = conn.execute("""
        SELECT mg.id, mg.group_index, mgm.player_id, p.baller_name, p.first_name, p.surname, p.jersey_number,
               a.present
        FROM FOOTBALL.matchday_groups mg
        JOIN FOOTBALL.matchday_group_members mgm ON mgm.group_id = mg.id AND mgm.matchday_id = mg.matchday_id
        JOIN FOOTBALL.players p ON p.id = mgm.player_id
        LEFT JOIN FOOTBALL.matchday_attendance a ON a.matchday_id = mgm.matchday_id AND a.player_id = mgm.player_id
        WHERE mg.matchday_id = ?
        ORDER BY mg.group_index, p.baller_name
    """, [matchday_id]).fetchall()
    attendance = []
    for r in rows:
        gid, gidx, pid, baller, first, last, jersey, present = r
        attendance.append({
            "group_id": gid, "group_index": gidx, "player_id": pid,
            "baller_name": baller, "first_name": first, "surname": last, "jersey_number": jersey,
            "present": bool(present) if present is not None else True,
        })
    return {"success": True, "matchday": md, "attendance": attendance}


class SetAttendanceBody(BaseModel):
    player_id: int
    present: bool


@router.put("/admin/matchdays/{matchday_id:int}/attendance")
def admin_matchday_set_attendance(matchday_id: int, body: SetAttendanceBody, payload: dict = Depends(require_admin)):
    """Mark a player present or absent for this matchday."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if not md.get("groups_published"):
        raise HTTPException(status_code=400, detail="Publish groups first.")
    in_group = conn.execute(
        "SELECT 1 FROM FOOTBALL.matchday_group_members WHERE matchday_id = ? AND player_id = ?",
        [matchday_id, body.player_id],
    ).fetchone()
    if not in_group:
        raise HTTPException(status_code=400, detail="Player not in any group for this matchday.")
    conn.execute("DELETE FROM FOOTBALL.matchday_attendance WHERE matchday_id = ? AND player_id = ?", [matchday_id, body.player_id])
    conn.execute(
        "INSERT INTO FOOTBALL.matchday_attendance (matchday_id, player_id, present) VALUES (?, ?, ?)",
        [matchday_id, body.player_id, bool(body.present)],
    )
    return {"success": True, "message": "Attendance updated."}


class BulkAttendanceBody(BaseModel):
    updates: list[SetAttendanceBody]


@router.put("/admin/matchdays/{matchday_id:int}/attendance/bulk")
def admin_matchday_set_attendance_bulk(matchday_id: int, body: BulkAttendanceBody, payload: dict = Depends(require_admin)):
    """Mark multiple players present or absent in one request. Only real players (player_id > 0) in groups are updated."""
    try:
        conn = get_conn()
        md = _get_matchday_by_id(conn, matchday_id)
        if not md:
            raise HTTPException(status_code=404, detail="Matchday not found.")
        if not md.get("groups_published"):
            raise HTTPException(status_code=400, detail="Publish groups first.")
        count = 0
        for u in body.updates:
            if u.player_id <= 0:
                continue  # skip Others / pseudo ids
            in_group = conn.execute(
                "SELECT 1 FROM FOOTBALL.matchday_group_members WHERE matchday_id = ? AND player_id = ?",
                [matchday_id, u.player_id],
            ).fetchone()
            if in_group:
                conn.execute("DELETE FROM FOOTBALL.matchday_attendance WHERE matchday_id = ? AND player_id = ?", [matchday_id, u.player_id])
                conn.execute(
                    "INSERT INTO FOOTBALL.matchday_attendance (matchday_id, player_id, present) VALUES (?, ?, ?)",
                    [matchday_id, u.player_id, bool(u.present)],
                )
                count += 1
        return {"success": True, "message": f"Attendance updated for {count} player(s)."}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Bulk attendance failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Bulk attendance failed: {str(e)}")


@router.get("/admin/matchdays/{matchday_id:int}/attendance/summary")
def admin_matchday_attendance_summary(matchday_id: int, payload: dict = Depends(require_admin)):
    """Lightweight: list of present and absent player ids and names for dropdowns (no full table)."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if not md.get("groups_published"):
        return {"success": True, "matchday": md, "present": [], "absent": [], "message": "Publish groups first."}
    rows = conn.execute("""
        SELECT mgm.player_id, p.baller_name, a.present
        FROM FOOTBALL.matchday_group_members mgm
        JOIN FOOTBALL.players p ON p.id = mgm.player_id
        LEFT JOIN FOOTBALL.matchday_attendance a ON a.matchday_id = mgm.matchday_id AND a.player_id = mgm.player_id
        WHERE mgm.matchday_id = ? AND mgm.player_id > 0
        ORDER BY p.baller_name
    """, [matchday_id]).fetchall()
    # Only True = present; False or NULL (no row yet) = absent, so everyone starts in Absent until marked Present
    present = [{"player_id": r[0], "baller_name": r[1]} for r in rows if r[2] is True]
    absent = [{"player_id": r[0], "baller_name": r[1]} for r in rows if r[2] is not True]
    return {"success": True, "matchday": md, "present": present, "absent": absent}


class AddCardBody(BaseModel):
    player_id: int
    card_type: str  # 'yellow' | 'red'
    fixture_id: Optional[int] = None  # if set, card is tied to this fixture


@router.get("/admin/matchdays/{matchday_id:int}/cards")
def admin_matchday_cards(matchday_id: int, payload: dict = Depends(require_admin)):
    """List yellow/red card counts per player for this matchday."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    rows = conn.execute("""
        SELECT c.player_id, p.baller_name, c.yellow_count, c.red_count
        FROM FOOTBALL.matchday_cards c
        JOIN FOOTBALL.players p ON p.id = c.player_id
        WHERE c.matchday_id = ?
        ORDER BY p.baller_name
    """, [matchday_id]).fetchall()
    cards = [{"player_id": r[0], "baller_name": r[1], "yellow_count": r[2], "red_count": r[3]} for r in rows]
    # Include all group members with 0 cards if not in table
    group_players = conn.execute("""
        SELECT DISTINCT mgm.player_id, p.baller_name FROM FOOTBALL.matchday_group_members mgm
        JOIN FOOTBALL.players p ON p.id = mgm.player_id
        WHERE mgm.matchday_id = ? AND mgm.player_id > 0
        ORDER BY p.baller_name
    """, [matchday_id]).fetchall()
    seen = {c["player_id"] for c in cards}
    for pid, baller in group_players:
        if pid not in seen:
            cards.append({"player_id": pid, "baller_name": baller, "yellow_count": 0, "red_count": 0})
    cards.sort(key=lambda x: x["baller_name"])
    return {"success": True, "matchday": md, "cards": cards}


@router.post("/admin/matchdays/{matchday_id:int}/cards")
def admin_matchday_add_card(matchday_id: int, body: AddCardBody, payload: dict = Depends(require_admin)):
    """Add one yellow or red card. If fixture_id is set, card is tied to that fixture."""
    if body.card_type not in ("yellow", "red"):
        raise HTTPException(status_code=400, detail="card_type must be 'yellow' or 'red'.")
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if not md.get("groups_published"):
        raise HTTPException(status_code=400, detail="Publish groups first.")
    # Others (guests) use pseudo player_id per group; they are not in matchday_group_members
    is_others = body.player_id < 0 and _decode_others_group(matchday_id, body.player_id) is not None
    if not is_others:
        in_group = conn.execute("SELECT 1 FROM FOOTBALL.matchday_group_members WHERE matchday_id = ? AND player_id = ?", [matchday_id, body.player_id]).fetchone()
        if not in_group:
            raise HTTPException(status_code=400, detail="Player not in any group for this matchday.")
    if body.fixture_id is not None:
        fixture_row = conn.execute(
            "SELECT id FROM FOOTBALL.matchday_fixtures WHERE id = ? AND matchday_id = ?",
            [body.fixture_id, matchday_id],
        ).fetchone()
        if not fixture_row:
            raise HTTPException(status_code=400, detail="Fixture not found.")
        next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.fixture_cards").fetchone()[0]
        conn.execute(
            "INSERT INTO FOOTBALL.fixture_cards (id, fixture_id, player_id, card_type) VALUES (?, ?, ?, ?)",
            [next_id, body.fixture_id, body.player_id, body.card_type],
        )
    # Others do not get rating deductions; only real players go into matchday_cards
    if not is_others:
        row = conn.execute("SELECT yellow_count, red_count FROM FOOTBALL.matchday_cards WHERE matchday_id = ? AND player_id = ?", [matchday_id, body.player_id]).fetchone()
        y, r = (row[0], row[1]) if row else (0, 0)
        if body.card_type == "yellow":
            y += 1
        else:
            r += 1
        conn.execute(
            "INSERT OR REPLACE INTO FOOTBALL.matchday_cards (matchday_id, player_id, yellow_count, red_count) VALUES (?, ?, ?, ?)",
            [matchday_id, body.player_id, y, r],
        )
        return {"success": True, "message": f"Card added. Yellows: {y}, Reds: {r}."}
    return {"success": True, "message": "Card added (Others – no rating impact)."}


def _star_rating_by_quartile(conn) -> dict:
    """Return dict player_id -> stars (0-5). 0 = no matchday concluded yet. Once at least one player has ratings, 5/4/3/1 by quartile."""
    all_players = conn.execute("SELECT id FROM FOOTBALL.players WHERE status = 'approved' AND id > 0").fetchall()
    rated = []
    for (pid,) in all_players:
        s = _player_career_stats(conn, pid)
        if s["matchday_ratings"]:
            rated.append((pid, s["average_rating"]))
    if not rated:
        return {pid: 0 for (pid,) in all_players}
    rated.sort(key=lambda x: -x[1])
    n = len(rated)
    stars = {}
    for i, (pid, _) in enumerate(rated):
        if i < n // 4:
            stars[pid] = 5
        elif i < n // 2:
            stars[pid] = 4
        elif i < (3 * n) // 4:
            stars[pid] = 3
        else:
            stars[pid] = 1
    for (pid,) in all_players:
        if pid not in stars:
            stars[pid] = 0
    return stars


@router.get("/member/stats")
def member_my_stats(payload: dict = Depends(require_player)):
    """Current member's stats: goals, assists, cards, clean sheets, matchdays present, per-matchday ratings, average rating, global rank, and star rating (1-5 by quartile)."""
    conn = get_conn()
    player_id = int(payload["sub"])
    stats = _player_career_stats(conn, player_id)
    stars_map = _star_rating_by_quartile(conn)
    star_rating = stars_map.get(player_id, 0)
    # Global rank: all players with at least one matchday rating, sorted by average_rating desc; rank = 1-based index
    all_players = conn.execute("SELECT id FROM FOOTBALL.players WHERE status = 'approved' AND id > 0").fetchall()
    leader = []
    for (pid,) in all_players:
        s = _player_career_stats(conn, pid)
        if s["matchday_ratings"]:
            leader.append({"player_id": pid, "average_rating": s["average_rating"]})
    leader.sort(key=lambda x: -x["average_rating"])
    rank = None
    for i, row in enumerate(leader, 1):
        if row["player_id"] == player_id:
            rank = i
            break
    return JSONResponse(
        content={"success": True, "stats": stats, "global_rank": rank, "star_rating": star_rating},
        headers=NO_CACHE_HEADERS,
    )


@router.get("/member/leaderboard")
def member_leaderboard(payload: dict = Depends(require_player)):
    """Global rating table plus top-X tables and star rating per player."""
    conn = get_conn()
    all_players = conn.execute("SELECT id, baller_name, jersey_number FROM FOOTBALL.players WHERE status = 'approved' AND id > 0 ORDER BY baller_name").fetchall()
    out = []
    for pid, baller, jersey in all_players:
        s = _player_career_stats(conn, pid)
        out.append({
            "player_id": pid, "baller_name": baller, "jersey_number": jersey or 0,
            "goals": s["goals"], "assists": s["assists"],
            "yellow_cards": s["yellow_cards"], "red_cards": s["red_cards"],
            "clean_sheets": s["clean_sheets"], "matchdays_present": s["matchdays_present"],
            "average_rating": s["average_rating"],
        })
    out.sort(key=lambda x: (-x["average_rating"], -x["goals"], -x["assists"]))
    stars = _star_rating_by_quartile(conn)
    for row in out:
        row["star_rating"] = stars.get(row["player_id"], 0)
    top_goals = sorted(out, key=lambda x: (-x["goals"], -x["assists"], -x["average_rating"]))[:20]
    top_assists = sorted(out, key=lambda x: (-x["assists"], -x["goals"], -x["average_rating"]))[:20]
    top_present = sorted(out, key=lambda x: (-x["matchdays_present"], -x["average_rating"]))[:20]
    top_clean_sheets = sorted(out, key=lambda x: (-x["clean_sheets"], -x["average_rating"]))[:20]
    return JSONResponse(
        content={
            "success": True, "leaderboard": out,
            "top_goals": top_goals, "top_assists": top_assists, "top_present": top_present, "top_clean_sheets": top_clean_sheets,
        },
        headers=NO_CACHE_HEADERS,
    )


@router.get("/member/top-five-ballers")
def member_top_five_ballers(payload: dict = Depends(require_player)):
    """Top 5 players by average rating for dashboard spotlight (jersey_number for avatar)."""
    conn = get_conn()
    all_players = conn.execute("SELECT id, baller_name, jersey_number FROM FOOTBALL.players WHERE status = 'approved' AND id > 0").fetchall()
    out = []
    for pid, baller, jersey in all_players:
        s = _player_career_stats(conn, pid)
        if not s["matchday_ratings"]:
            continue
        out.append({
            "player_id": pid, "baller_name": baller, "jersey_number": jersey or 0,
            "average_rating": s["average_rating"], "goals": s["goals"], "assists": s["assists"], "matchdays_present": s["matchdays_present"],
        })
    out.sort(key=lambda x: (-x["average_rating"], -x["goals"], -x["assists"]))
    return JSONResponse(content={"success": True, "top_five": out[:5]}, headers=NO_CACHE_HEADERS)


@router.post("/admin/matchdays/{matchday_id:int}/groups/publish")
def admin_matchday_publish_groups(matchday_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "approved":
        raise HTTPException(status_code=400, detail="Approve matchday first.")
    conn.execute("UPDATE FOOTBALL.matchdays SET groups_published = true WHERE id = ?", [matchday_id])
    return {"success": True, "message": "Groups published. Members can now see their group."}


# ---------- Fixtures (round-robin), goals, end matchday ----------

@router.post("/admin/matchdays/{matchday_id:int}/fixtures/generate")
def admin_generate_fixtures(matchday_id: int, payload: dict = Depends(require_admin)):
    """Create round-robin fixtures: each group plays every other group once."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "approved":
        raise HTTPException(status_code=400, detail="Approve matchday first.")
    existing = conn.execute("SELECT id FROM FOOTBALL.matchday_fixtures WHERE matchday_id = ?", [matchday_id]).fetchone()
    if existing:
        raise HTTPException(status_code=400, detail="Fixtures already generated.")
    groups = conn.execute("SELECT id FROM FOOTBALL.matchday_groups WHERE matchday_id = ? ORDER BY group_index", [matchday_id]).fetchall()
    group_ids = [r[0] for r in groups]
    if len(group_ids) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 groups.")
    next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.matchday_fixtures").fetchone()[0]
    for i in range(len(group_ids)):
        for j in range(i + 1, len(group_ids)):
            conn.execute("""
                INSERT INTO FOOTBALL.matchday_fixtures (id, matchday_id, group_a_id, group_b_id, status, home_goals, away_goals)
                VALUES (?, ?, ?, ?, 'pending', 0, 0)
            """, [next_id, matchday_id, group_ids[i], group_ids[j]])
            next_id += 1
    count = (len(group_ids) * (len(group_ids) - 1)) // 2
    return {"success": True, "message": f"Generated {count} fixtures.", "fixture_count": count}


@router.get("/admin/matchdays/{matchday_id:int}/fixtures")
def admin_list_fixtures(matchday_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    rows = conn.execute("""
        SELECT f.id, f.group_a_id, f.group_b_id, f.status, f.home_goals, f.away_goals, f.started_at, f.ended_at,
               ga.group_index, gb.group_index
        FROM FOOTBALL.matchday_fixtures f
        JOIN FOOTBALL.matchday_groups ga ON ga.id = f.group_a_id
        JOIN FOOTBALL.matchday_groups gb ON gb.id = f.group_b_id
        WHERE f.matchday_id = ? ORDER BY f.id
    """, [matchday_id]).fetchall()
    fixtures = []
    for r in rows:
        fid, ga_id, gb_id, status, hg, ag, started, ended, ga_idx, gb_idx = r
        goal_choices = _goal_choices_for_fixture(conn, matchday_id, ga_id, gb_id)
        goals_rows = conn.execute("""
            SELECT g.id, g.scorer_player_id, g.assister_player_id, g.minute, g.is_home_goal
            FROM FOOTBALL.fixture_goals g WHERE g.fixture_id = ? ORDER BY g.id
        """, [fid]).fetchall()
        goals = [
            {
                "id": gr[0], "scorer_player_id": gr[1], "assister_player_id": gr[2],
                "minute": gr[3], "is_home_goal": gr[4],
                "scorer_name": _resolve_player_name(conn, matchday_id, gr[1]),
                "assister_name": _resolve_player_name(conn, matchday_id, gr[2]) if gr[2] is not None else None,
            }
            for gr in goals_rows
        ]
        fixtures.append({
            "id": fid, "group_a_id": ga_id, "group_b_id": gb_id, "status": status,
            "home_goals": hg or 0, "away_goals": ag or 0,
            "started_at": str(started) if started else None, "ended_at": str(ended) if ended else None,
            "group_a_index": ga_idx, "group_b_index": gb_idx,
            "goal_choices": goal_choices,
            "goals": goals,
        })
    return {"success": True, "matchday": md, "fixtures": fixtures}


@router.post("/admin/matchdays/{matchday_id:int}/fixtures/publish")
def admin_publish_fixtures(matchday_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    if md["status"] != "approved":
        raise HTTPException(status_code=400, detail="Approve matchday first.")
    conn.execute("UPDATE FOOTBALL.matchdays SET fixtures_published = true WHERE id = ?", [matchday_id])
    return {"success": True, "message": "Fixtures published. Members can see them."}


@router.post("/admin/matchdays/{matchday_id:int}/fixtures/{fixture_id:int}/start")
def admin_start_fixture(matchday_id: int, fixture_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    row = conn.execute("SELECT id, status FROM FOOTBALL.matchday_fixtures WHERE id = ? AND matchday_id = ?", [fixture_id, matchday_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Fixture not found.")
    if row[1] != "pending":
        raise HTTPException(status_code=400, detail="Fixture already started or completed.")
    conn.execute("UPDATE FOOTBALL.matchday_fixtures SET status = 'in_progress', started_at = current_timestamp WHERE id = ?", [fixture_id])
    return {"success": True, "message": "Fixture started."}


class AddGoalBody(BaseModel):
    scorer_player_id: int
    assister_player_id: Optional[int] = None
    minute: Optional[int] = None
    is_home_goal: Optional[bool] = None  # if omitted, inferred from scorer's group (group_a = home)


@router.post("/admin/matchdays/{matchday_id:int}/fixtures/{fixture_id:int}/goals")
def admin_add_goal(matchday_id: int, fixture_id: int, body: AddGoalBody, payload: dict = Depends(require_admin)):
    conn = get_conn()
    row = conn.execute("SELECT id, status, home_goals, away_goals, group_a_id, group_b_id FROM FOOTBALL.matchday_fixtures WHERE id = ? AND matchday_id = ?", [fixture_id, matchday_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Fixture not found.")
    if row[1] not in ("in_progress", "completed"):
        raise HTTPException(status_code=400, detail="Can only add goals to a fixture in progress or completed.")
    goal_choices = _goal_choices_for_fixture(conn, matchday_id, row[4], row[5])
    valid_ids = {c["id"] for c in goal_choices}
    if body.scorer_player_id not in valid_ids:
        raise HTTPException(status_code=400, detail="Scorer must be a present player or Others.")
    if body.assister_player_id is not None and body.assister_player_id not in valid_ids:
        raise HTTPException(status_code=400, detail="Assister must be a present player or Others.")
    group_a_id, group_b_id = row[4], row[5]
    is_home_goal = body.is_home_goal
    if is_home_goal is None and body.scorer_player_id > 0:
        scorer_group = conn.execute(
            "SELECT group_id FROM FOOTBALL.matchday_group_members WHERE matchday_id = ? AND player_id = ?",
            [matchday_id, body.scorer_player_id],
        ).fetchone()
        if scorer_group:
            is_home_goal = scorer_group[0] == group_a_id
    if is_home_goal is None:
        others_gid = _decode_others_group(matchday_id, body.scorer_player_id)
        if others_gid is not None:
            is_home_goal = others_gid == group_a_id
        else:
            is_home_goal = True
    next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.fixture_goals").fetchone()[0]
    conn.execute("INSERT INTO FOOTBALL.fixture_goals (id, fixture_id, scorer_player_id, assister_player_id, minute, is_home_goal) VALUES (?, ?, ?, ?, ?, ?)", [next_id, fixture_id, body.scorer_player_id, body.assister_player_id, body.minute, is_home_goal])
    if is_home_goal:
        conn.execute("UPDATE FOOTBALL.matchday_fixtures SET home_goals = home_goals + 1 WHERE id = ?", [fixture_id])
    else:
        conn.execute("UPDATE FOOTBALL.matchday_fixtures SET away_goals = away_goals + 1 WHERE id = ?", [fixture_id])
    return {"success": True, "message": "Goal added."}


@router.get("/admin/matchdays/{matchday_id:int}/fixtures/{fixture_id:int}/goals")
def admin_fixture_goals(matchday_id: int, fixture_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    row = conn.execute("SELECT id FROM FOOTBALL.matchday_fixtures WHERE id = ? AND matchday_id = ?", [fixture_id, matchday_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Fixture not found.")
    rows = conn.execute("""
        SELECT g.id, g.scorer_player_id, g.assister_player_id, g.minute, g.is_home_goal
        FROM FOOTBALL.fixture_goals g WHERE g.fixture_id = ? ORDER BY g.id
    """, [fixture_id]).fetchall()
    goals = []
    for r in rows:
        gid, sp, ap, minute, is_home = r
        goals.append({
            "id": gid, "scorer_player_id": sp, "assister_player_id": ap, "minute": minute, "is_home_goal": is_home,
            "scorer_name": _resolve_player_name(conn, matchday_id, sp),
            "assister_name": _resolve_player_name(conn, matchday_id, ap) if ap is not None else None,
        })
    return {"success": True, "goals": goals}


@router.delete("/admin/matchdays/{matchday_id:int}/fixtures/{fixture_id:int}/goals/{goal_id:int}")
def admin_remove_goal(matchday_id: int, fixture_id: int, goal_id: int, payload: dict = Depends(require_admin)):
    """Remove a goal (and its assist, if any). Allowed for in_progress or completed fixtures."""
    conn = get_conn()
    row = conn.execute("SELECT id, status, home_goals, away_goals FROM FOOTBALL.matchday_fixtures WHERE id = ? AND matchday_id = ?", [fixture_id, matchday_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Fixture not found.")
    if row[1] not in ("in_progress", "completed"):
        raise HTTPException(status_code=400, detail="Can only remove goals from a fixture in progress or completed.")
    goal_row = conn.execute("SELECT id, is_home_goal FROM FOOTBALL.fixture_goals WHERE id = ? AND fixture_id = ?", [goal_id, fixture_id]).fetchone()
    if not goal_row:
        raise HTTPException(status_code=404, detail="Goal not found.")
    is_home_goal = goal_row[1]
    conn.execute("DELETE FROM FOOTBALL.fixture_goals WHERE id = ?", [goal_id])
    if is_home_goal:
        conn.execute("UPDATE FOOTBALL.matchday_fixtures SET home_goals = GREATEST(0, home_goals - 1) WHERE id = ?", [fixture_id])
    else:
        conn.execute("UPDATE FOOTBALL.matchday_fixtures SET away_goals = GREATEST(0, away_goals - 1) WHERE id = ?", [fixture_id])
    return {"success": True, "message": "Goal removed (assist removed with it)."}


@router.get("/admin/matchdays/{matchday_id:int}/fixtures/{fixture_id:int}/cards")
def admin_fixture_cards(matchday_id: int, fixture_id: int, payload: dict = Depends(require_admin)):
    """List yellow/red cards for this fixture (cards added from fixture view)."""
    conn = get_conn()
    row = conn.execute("SELECT id FROM FOOTBALL.matchday_fixtures WHERE id = ? AND matchday_id = ?", [fixture_id, matchday_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Fixture not found.")
    rows = conn.execute("""
        SELECT fc.player_id, fc.card_type
        FROM FOOTBALL.fixture_cards fc
        WHERE fc.fixture_id = ?
        ORDER BY fc.id
    """, [fixture_id]).fetchall()
    cards = [{"player_id": r[0], "card_type": r[1], "baller_name": _resolve_player_name(conn, matchday_id, r[0])} for r in rows]
    return {"success": True, "cards": cards}


@router.post("/admin/matchdays/{matchday_id:int}/fixtures/{fixture_id:int}/end")
def admin_end_fixture(matchday_id: int, fixture_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    row = conn.execute("SELECT id, status FROM FOOTBALL.matchday_fixtures WHERE id = ? AND matchday_id = ?", [fixture_id, matchday_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Fixture not found.")
    if row[1] != "in_progress":
        raise HTTPException(status_code=400, detail="Fixture not in progress.")
    conn.execute("UPDATE FOOTBALL.matchday_fixtures SET status = 'completed', ended_at = current_timestamp WHERE id = ?", [fixture_id])
    return {"success": True, "message": "Fixture ended."}


@router.post("/admin/matchdays/{matchday_id:int}/end-matchday")
def admin_end_matchday(matchday_id: int, payload: dict = Depends(require_admin)):
    """End the matchday (even if not all fixtures are played)."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    conn.execute("UPDATE FOOTBALL.matchdays SET matchday_ended = true WHERE id = ?", [matchday_id])
    conn.execute("UPDATE FOOTBALL.matchday_fixtures SET status = 'completed', ended_at = COALESCE(ended_at, current_timestamp) WHERE matchday_id = ? AND status = 'in_progress'", [matchday_id])
    return {"success": True, "message": "Matchday ended."}


@router.post("/admin/matchdays/{matchday_id:int}/reopen-matchday")
def admin_reopen_matchday(matchday_id: int, payload: dict = Depends(require_admin)):
    """Reopen an ended matchday (set matchday_ended = false). Use to re-end and refresh leaderboard/stats."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    conn.execute("UPDATE FOOTBALL.matchdays SET matchday_ended = false WHERE id = ?", [matchday_id])
    return {"success": True, "message": "Matchday reopened. End it again to refresh leaderboard and stats."}


@router.get("/admin/matchdays/{matchday_id:int}/table")
def admin_matchday_table(matchday_id: int, payload: dict = Depends(require_admin)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    table = _league_table(conn, matchday_id)
    return {"success": True, "matchday": md, "table": table}


@router.get("/admin/matchdays/{matchday_id:int}/player-ratings")
def admin_matchday_player_ratings(matchday_id: int, payload: dict = Depends(require_admin)):
    """Player ratings for this matchday (updates as fixtures complete). For admin table below matchday."""
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    rows = conn.execute("""
        SELECT mgm.player_id, p.baller_name, p.jersey_number, mg.group_index
        FROM FOOTBALL.matchday_group_members mgm
        JOIN FOOTBALL.players p ON p.id = mgm.player_id
        JOIN FOOTBALL.matchday_groups mg ON mg.id = mgm.group_id AND mg.matchday_id = mgm.matchday_id
        WHERE mgm.matchday_id = ? AND mgm.player_id > 0
        ORDER BY mg.group_index, p.baller_name
    """, [matchday_id]).fetchall()
    ratings = []
    for r in rows:
        pid, baller_name, jersey_number, group_index = r
        rating = _player_matchday_rating(conn, matchday_id, pid)
        ratings.append({"player_id": pid, "baller_name": baller_name or "", "jersey_number": jersey_number or 0, "group_index": group_index, "rating": rating})
    ratings.sort(key=lambda x: (-x["rating"], x["baller_name"]))
    return {"success": True, "matchday": md, "ratings": ratings}


@router.get("/member/matchdays/{matchday_id:int}/table")
def member_matchday_table(matchday_id: int, payload: dict = Depends(require_player)):
    conn = get_conn()
    md = _get_matchday_by_id(conn, matchday_id)
    if not md:
        raise HTTPException(status_code=404, detail="Matchday not found.")
    table = _league_table(conn, matchday_id)
    return {"success": True, "matchday": md, "table": table}


def seed_fake_football_players(conn):
    """Ensure 21 approved paid members exist (1 existing + 20 fake)."""
    c = conn.execute("SELECT COUNT(*) FROM FOOTBALL.players WHERE status = 'approved'").fetchone()[0]
    if c >= 21:
        return
    year = datetime.utcnow().year
    q = get_current_quarter()
    first_names = ["Alex", "Jordan", "Sam", "Taylor", "Morgan", "Casey", "Riley", "Quinn", "Avery", "Parker",
                   "Dakota", "Reese", "Cameron", "Jamie", "Skyler", "Finley", "River", "Phoenix", "Blake", "Drew"]
    surnames = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
    ballers = ["Ace", "Bolt", "Cobra", "Duke", "Echo", "Flash", "Ghost", "Hawk", "Ivy", "Jade",
               "King", "Lion", "Maze", "Nova", "Onyx", "Prime", "Queen", "Raven", "Storm", "Tank"]
    used_ballers = set((r[0] or "").lower() for r in conn.execute("SELECT baller_name FROM FOOTBALL.players").fetchall())
    used_jerseys = set(r[0] for r in conn.execute("SELECT jersey_number FROM FOOTBALL.players").fetchall())
    import random
    next_pid = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.players").fetchone()[0]
    for i in range(20):
        first = first_names[i]
        last = surnames[i]
        baller = ballers[i] if ballers[i].lower() not in used_ballers else f"Baller{i+10}"
        used_ballers.add(baller.lower())
        jersey = random.choice([j for j in range(1, 101) if j not in used_jerseys])
        used_jerseys.add(jersey)
        email = f"eko.fake{i+1}@test.com"
        phone = f"+1555000{i+1:04d}"
        password = generate_player_password(first, baller, year)
        password_hash = hash_password(password)
        conn.execute("""
            INSERT INTO FOOTBALL.players (id, first_name, surname, baller_name, jersey_number, email, whatsapp_phone, status, password_hash, password_display, year_registered, approved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'approved', ?, ?, ?, current_timestamp)
        """, [next_pid, first, last, baller, jersey, email, phone, password_hash, password, year])
        next_dues = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM FOOTBALL.dues").fetchone()[0]
        conn.execute("INSERT INTO FOOTBALL.dues (id, player_id, year, quarter, status, paid_at) VALUES (?, ?, ?, ?, 'paid', current_timestamp)", [next_dues, next_pid, year, q])
        next_pid += 1
    logger.info("Seeded 20 fake football players (paid, approved)")
