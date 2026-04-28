#!/usr/bin/env python3
"""
CLEARLINE AI VETTING API
=========================

FastAPI service wrapping the comprehensive vetting engine.
Other systems call this to validate PA requests.

Endpoints:
    POST /api/v1/validate          - Submit PA for validation
    GET  /api/v1/requests/{id}     - Get request status/details
    GET  /api/v1/pending           - Get pending review queue (for agent)
    POST /api/v1/review/{id}       - Agent confirms/rejects + stores learning
    GET  /api/v1/stats             - System statistics
    GET  /api/v1/history           - Request history with filters
    GET  /api/v1/health            - Health check

Decision States:
    AUTO_APPROVED    - All rules passed from master/learning (no AI involved)
    AUTO_DENIED      - Master denial or high-confidence learned denial (≥3 uses)
    PENDING_REVIEW   - AI was involved; human must confirm to store learning
    HUMAN_APPROVED   - Agent confirmed approval (learning stored)
    HUMAN_DENIED     - Agent confirmed denial (learning stored)

Run:
    python vetting_api.py
    → API at http://localhost:8000
    → Docs at http://localhost:8000/docs

Author: Casey's AI Assistant
Date: February 2026
Version: 1.0
"""

import os
import json
import uuid
import logging
import threading
import concurrent.futures
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Literal
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from . import mongo_db
from .klaire_consultation import (
    evaluate_gp_consultation,
    evaluate_gp_review_consultation,
    evaluate_specialist_consultation,
    get_consult_df,
    GP_CODES,
)
from .klaire_pa import validate_pa_request

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

MOTHERDUCK_DB = os.getenv("MOTHERDUCK_DB", "ai_driven_data")

# ── MotherDuck token: env var → secrets.toml fallback ─────────────────────────
def _load_motherduck_token() -> str:
    token = os.getenv("MOTHERDUCK_TOKEN", "")
    if not token:
        try:
            try:
                import tomllib
            except ImportError:
                import tomli as tomllib
            secrets_path = os.path.join(os.path.dirname(__file__), "../../../.streamlit/secrets.toml")
            with open(os.path.abspath(secrets_path), "rb") as f:
                token = tomllib.load(f).get("MOTHERDUCK_TOKEN", "")
        except Exception:
            pass
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN is not set. Cannot start without MotherDuck.")
    return token

MOTHERDUCK_TOKEN = _load_motherduck_token()


def get_db_path() -> str:
    return f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}"


# ============================================================================
# CAPITATION TABLES (loaded once at startup from Excel files)
# ============================================================================

def _load_capitation_data():
    """
    Returns:
      capitated_procs  : set of procedure codes (uppercase, stripped)
      capitated_enrollees: dict { enrollee_id -> {"provider_key": str, "provider_name": str} }
    """
    import pandas as pd
    base = os.path.join(os.path.dirname(__file__), "../..")

    # ── Capitated procedures ──────────────────────────────────────────────────
    capitated_procs: set = set()
    try:
        proc_path = os.path.join(base, "Private Capitation List.xlsx")
        df_proc = pd.read_excel(os.path.abspath(proc_path))
        for code in df_proc["Procedure Code"].dropna():
            capitated_procs.add(str(code).strip().upper())
        logger.info(f"✅ Capitation procedures loaded: {len(capitated_procs)} codes")
    except Exception as e:
        logger.warning(f"Could not load capitation procedure list: {e}")

    # ── Capitated enrollees ───────────────────────────────────────────────────
    capitated_enrollees: Dict = {}
    try:
        enr_path = os.path.join(base, "cba-capitation-details-report (38).xlsx")
        df_enr = pd.read_excel(os.path.abspath(enr_path), header=5)
        for _, row in df_enr.iterrows():
            eid = str(row.get("Insured ID", "")).strip()
            pkey = str(row.get("Provider Key", "")).strip()
            pname = str(row.get("Provider Name", "")).strip()
            if eid and eid != "nan" and pkey and pkey != "nan":
                # Remove trailing .0 from numeric provider keys
                pkey = pkey.rstrip("0").rstrip(".") if "." in pkey else pkey
                capitated_enrollees[eid] = {"provider_key": pkey, "provider_name": pname}
        logger.info(f"✅ Capitation enrollees loaded: {len(capitated_enrollees)} enrollees")
    except Exception as e:
        logger.warning(f"Could not load capitation enrollee list: {e}")

    return capitated_procs, capitated_enrollees


CAPITATED_PROCS: set = set()
CAPITATED_ENROLLEES: Dict = {}
_capitation_loaded = False


def get_capitation():
    global CAPITATED_PROCS, CAPITATED_ENROLLEES, _capitation_loaded
    if not _capitation_loaded:
        CAPITATED_PROCS, CAPITATED_ENROLLEES = _load_capitation_data()
        _capitation_loaded = True
    return CAPITATED_PROCS, CAPITATED_ENROLLEES



# ============================================================================
# PYDANTIC REQUEST/RESPONSE MODELS
# ============================================================================

class PARequest(BaseModel):
    """Hospital submits this to request pre-authorization"""
    procedure_code: str = Field(..., description="Procedure/drug code e.g. DRG1106")
    diagnosis_code: str = Field(..., description="ICD-10 diagnosis code e.g. B509")
    enrollee_id: str = Field(..., description="Enrollee ID e.g. CL/OCTA/723449/2023-A")
    encounter_date: Optional[str] = Field(None, description="YYYY-MM-DD, defaults to today")
    hospital_name: Optional[str] = Field(None, description="Requesting hospital")
    notes: Optional[str] = Field(None, description="Additional clinical notes")


class ProcedureItem(BaseModel):
    """One procedure+diagnosis line in a bulk PA request"""
    procedure_code: str
    diagnosis_code: str
    price: Optional[float] = Field(None, description="Stated price per unit (₦)")
    quantity: int = Field(1, ge=1, description="Number of units requested")
    notes: Optional[str] = None


class BulkPARequest(BaseModel):
    """Bulk PA request: one enrollee, multiple procedure+diagnosis lines"""
    enrollee_id: str
    encounter_date: Optional[str] = None
    hospital_name: Optional[str] = None
    provider_id: Optional[str] = Field(None, description="Provider ID / Provider Key e.g. 118")
    pa_number: Optional[str] = Field(None, description="PA number — used to exclude the pre-auth record itself from the 14-day frequency lookback")
    encounter_type: Literal["OUTPATIENT", "INPATIENT"] = Field("OUTPATIENT", description="OUTPATIENT or INPATIENT — set by hospital at PA submission")
    procedures: List[ProcedureItem] = Field(..., min_items=1)


class ReviewAction(BaseModel):
    """Agent submits to confirm/override AI decision"""
    action: str = Field(..., description="CONFIRM or OVERRIDE")
    override_decision: Optional[str] = Field(None, description="If OVERRIDE: APPROVE or DENY")
    reviewed_by: str = Field(default="Casey", description="Reviewer name")
    notes: Optional[str] = Field(None, description="Review notes")


class RuleDetail(BaseModel):
    rule_name: str
    passed: bool
    source: str
    confidence: int
    reasoning: str
    details: Dict[str, Any] = {}


class LineItemResult(BaseModel):
    request_id: str
    procedure_code: str
    procedure_name: Optional[str] = None
    diagnosis_code: str
    diagnosis_name: Optional[str] = None
    status: str
    decision: str
    confidence: int
    reasoning: str
    rules: List[RuleDetail] = []
    # Pipeline tracking
    pipeline_stage: str = "APPROVED"   # DROPPED_STEP1 | DROPPED_STEP2 | DROPPED_STEP3 | PASSED
    drop_reason: Optional[str] = None  # human-readable reason for dropped lines
    # Tariff & quantity (Step 4)
    stated_price: Optional[float] = None
    tariff_price: Optional[float] = None
    adjusted_price: Optional[float] = None
    stated_quantity: Optional[int] = None
    max_allowed_quantity: Optional[int] = None
    adjusted_quantity: Optional[int] = None
    total_amount: Optional[float] = None   # adjusted_price × adjusted_quantity


class BulkValidationResponse(BaseModel):
    batch_id: str
    enrollee_id: str
    encounter_date: str
    hospital_name: Optional[str] = None
    encounter_type: str = "OUTPATIENT"
    overall_status: str
    overall_decision: str
    enrollee_age: Optional[int] = None
    enrollee_gender: Optional[str] = None
    line_items: List[LineItemResult] = []
    total_approved_amount: float = 0.0
    created_at: str


class ValidationResponse(BaseModel):
    request_id: str
    status: str
    decision: str           # APPROVE or DENY (final or AI recommendation)
    confidence: int
    reasoning: str
    enrollee_id: str
    enrollee_age: Optional[int] = None
    enrollee_gender: Optional[str] = None
    procedure_code: str
    procedure_name: Optional[str] = None
    diagnosis_code: str
    diagnosis_name: Optional[str] = None
    encounter_date: str
    hospital_name: Optional[str] = None
    rules: List[RuleDetail] = []
    summary: Dict[str, Any] = {}
    created_at: str
    reviewed_at: Optional[str] = None
    reviewed_by: Optional[str] = None


# ============================================================================
# QUEUE TABLE + HELPERS
# ============================================================================

def _norm_diag(code: str) -> str:
    """Normalize ICD-10 diagnosis code: uppercase, strip whitespace and dots.
    Both DB tables store codes without dots (J069 not J06.9).
    """
    return code.strip().upper().replace(".", "")


def _setup_mongo():
    """Ensure MongoDB indexes exist for vetting_queue and learning tables."""
    mongo_db.ensure_indexes()


def determine_status(validation) -> str:
    """
    Map engine output → queue status. AI decisions are final — no human review queue.

    AUTO_APPROVED  → all rules passed (master, learning, or AI)
    AUTO_DENIED    → any rule failed (master, learning, or AI)
    """
    if validation.overall_decision == "DENY":
        return "AUTO_DENIED"
    return "AUTO_APPROVED"


def rules_to_list(rule_results) -> list:
    """Serialize rule results"""
    return [
        {
            "rule_name": r.rule_name,
            "passed": r.passed,
            "source": r.source,
            "confidence": r.confidence,
            "reasoning": r.reasoning,
            "details": _safe(r.details) if r.details else {}
        }
        for r in rule_results
    ]


def _safe(obj) -> Any:
    """Make JSON-serializable"""
    if isinstance(obj, dict):
        return {k: _safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_safe(i) for i in obj]
    elif isinstance(obj, (datetime, date)):
        return str(obj)
    return obj


def row_to_response(row: dict) -> ValidationResponse:
    """Convert DB row → API response"""
    rules_raw = json.loads(row.get('rules_json') or '[]')
    summary = json.loads(row.get('summary_json') or '{}')
    return ValidationResponse(
        request_id=row['request_id'],
        status=row['status'],
        decision=row['decision'],
        confidence=row.get('confidence') or 0,
        reasoning=row.get('reasoning', ''),
        enrollee_id=row['enrollee_id'],
        enrollee_age=row.get('enrollee_age'),
        enrollee_gender=row.get('enrollee_gender'),
        procedure_code=row['procedure_code'],
        procedure_name=row.get('procedure_name'),
        diagnosis_code=row['diagnosis_code'],
        diagnosis_name=row.get('diagnosis_name'),
        encounter_date=row['encounter_date'],
        hospital_name=row.get('hospital_name'),
        rules=[RuleDetail(**r) for r in rules_raw],
        summary=summary,
        created_at=str(row.get('created_at', '')),
        reviewed_at=str(row['reviewed_at']) if row.get('reviewed_at') else None,
        reviewed_by=row.get('reviewed_by')
    )


# ============================================================================
# ENGINE SINGLETON
# ============================================================================

engine = None


def get_engine():
    global engine
    if engine is None:
        from .comprehensive import ComprehensiveVettingEngine
        db = get_db_path()
        engine = ComprehensiveVettingEngine(db)
        _setup_mongo()
        logger.info(f"✅ Vetting engine initialized (DB: {db})")
    return engine


# Thread-local engine: each worker thread gets its own DuckDB connection.
# This makes parallel Step 3 / Step 5 in validate_bulk truly thread-safe.
_tl = threading.local()

def _get_thread_engine():
    if not hasattr(_tl, "engine"):
        from .comprehensive import ComprehensiveVettingEngine
        _tl.engine = ComprehensiveVettingEngine(get_db_path())
        logger.debug("Thread-local engine created")
    return _tl.engine


# ============================================================================
# FASTAPI APP
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    get_engine()
    get_capitation()
    get_providers()   # pre-load 10k providers into memory so first request is instant
    yield
    logger.info("Shutting down vetting API")

app = FastAPI(
    title="Clearline AI Vetting API",
    description="Pre-Authorization validation with AI-powered learning",
    version="1.0.0",
    lifespan=lifespan
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)


# ============================================================================
# PROVIDERS CACHE (loaded once at startup from MotherDuck)
# ============================================================================

PROVIDERS_CACHE: List[Dict] = []
_providers_loaded = False


def get_providers() -> List[Dict]:
    global PROVIDERS_CACHE, _providers_loaded
    if not _providers_loaded:
        try:
            eng = get_engine()
            rows = eng.conn.execute("""
                SELECT TRIM(providerid)   AS id,
                       TRIM(providername) AS name,
                       TRIM(statename)    AS state,
                       TRIM(lganame)      AS lga
                FROM "AI DRIVEN DATA".PROVIDERS
                WHERE providerid IS NOT NULL AND TRIM(providerid) <> ''
                  AND providername IS NOT NULL AND TRIM(providername) <> ''
                ORDER BY providername
            """).fetchall()
            PROVIDERS_CACHE = [
                {"id": r[0], "name": r[1], "state": r[2] or "", "lga": r[3] or ""}
                for r in rows
            ]
            logger.info(f"✅ Providers loaded: {len(PROVIDERS_CACHE)} records")
        except Exception as e:
            logger.warning(f"Could not load providers: {e}")
        _providers_loaded = True
    return PROVIDERS_CACHE


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/api/v1/klaire/is-capitated")
def klaire_is_capitated(enrollee_id: str, provider_id: str = ""):
    """Return whether an enrollee is capitated and whether the given provider matches."""
    cap_procs, cap_enrollees = get_capitation()
    if enrollee_id not in cap_enrollees:
        return {"is_capitated": False, "provider_key": None, "provider_name": None, "at_capitated_provider": False}
    info    = cap_enrollees[enrollee_id]
    cpk     = info["provider_key"]
    cpn     = info["provider_name"]
    at_cap  = bool(provider_id and str(provider_id).strip() == str(cpk).strip())
    return {"is_capitated": True, "provider_key": cpk, "provider_name": cpn, "at_capitated_provider": at_cap}


@app.get("/api/v1/health")
def health_check():
    get_engine()
    return {
        "status": "healthy",
        "db_source": "motherduck",
        "database": f"md:{MOTHERDUCK_DB}",
        "timestamp": datetime.now().isoformat(),
    }


@app.post("/api/v1/validate", response_model=ValidationResponse)
def validate_pa(request: PARequest):
    """
    Submit a PA request for AI validation.
    
    Returns immediately:
    - AUTO_APPROVED / AUTO_DENIED → final decision
    - PENDING_REVIEW → queued; `decision` field = AI recommendation
    """
    eng = get_engine()
    encounter_date = request.encounter_date or date.today().strftime('%Y-%m-%d')
    request_id = str(uuid.uuid4())[:12]
    
    # Run validation
    try:
        validation = eng.validate_comprehensive(
            procedure_code=request.procedure_code,
            diagnosis_code=_norm_diag(request.diagnosis_code),
            enrollee_id=request.enrollee_id,
            encounter_date=encounter_date
        )
    except Exception as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=500, detail=f"Validation engine error: {str(e)}")
    
    status = determine_status(validation)

    # ── Terminal log ─────────────────────────────────────────────────────────
    rule_lines = "\n".join(
        f"    {'✅' if r.passed else '❌'} {r.rule_name} [{r.source.upper()}] {r.confidence}% — {r.reasoning}"
        for r in validation.rule_results
    )
    logger.info(
        "\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  VALIDATE REQUEST\n"
        f"  Enrollee : {request.enrollee_id}\n"
        f"  Procedure: {request.procedure_code.upper()}\n"
        f"  Diagnosis: {request.diagnosis_code.upper()}\n"
        f"  Hospital : {request.hospital_name or '—'}\n"
        f"  ─────────────────────────────────────────────────────\n"
        f"  STATUS   : {status}\n"
        f"  DECISION : {validation.overall_decision}  ({validation.overall_confidence}%)\n"
        f"  REASONING: {validation.overall_reasoning}\n"
        f"  RULES:\n{rule_lines}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    # ─────────────────────────────────────────────────────────────────────────

    # Context
    enrollee_ctx = eng.base_engine.get_enrollee_context(request.enrollee_id, encounter_date)
    proc_info = eng._resolve_procedure_info(request.procedure_code)
    diag_info = eng._resolve_diagnosis_info(request.diagnosis_code)
    rules_list = rules_to_list(validation.rule_results)
    summary = validation.get_summary()
    now = datetime.now().isoformat()
    
    # Store in MongoDB queue
    mongo_db.insert_queue({
        "request_id": request_id,
        "procedure_code": request.procedure_code.upper(),
        "diagnosis_code": request.diagnosis_code.upper(),
        "enrollee_id": request.enrollee_id,
        "encounter_date": encounter_date,
        "hospital_name": request.hospital_name,
        "notes": request.notes,
        "enrollee_age": enrollee_ctx.age,
        "enrollee_gender": enrollee_ctx.gender,
        "procedure_name": proc_info.get("name", "Unknown"),
        "diagnosis_name": diag_info.get("name", "Unknown"),
        "status": status,
        "decision": validation.overall_decision,
        "confidence": validation.overall_confidence,
        "reasoning": validation.overall_reasoning,
        "rules_json": json.dumps(rules_list),
        "summary_json": json.dumps(summary),
        "created_at": now,
    })

    return ValidationResponse(
        request_id=request_id, status=status,
        decision=validation.overall_decision, confidence=validation.overall_confidence,
        reasoning=validation.overall_reasoning,
        enrollee_id=request.enrollee_id,
        enrollee_age=enrollee_ctx.age, enrollee_gender=enrollee_ctx.gender,
        procedure_code=request.procedure_code.upper(),
        procedure_name=proc_info.get('name', 'Unknown'),
        diagnosis_code=request.diagnosis_code.upper(),
        diagnosis_name=diag_info.get('name', 'Unknown'),
        encounter_date=encounter_date, hospital_name=request.hospital_name,
        rules=[RuleDetail(**r) for r in rules_list], summary=summary,
        created_at=now
    )


@app.post("/api/v1/validate/bulk", response_model=BulkValidationResponse)
def validate_bulk(request: BulkPARequest):
    """
    5-step PA validation pipeline:
      Step 1 — 14-day frequency check (batch-level deny)
      Step 2 — Duplicate-in-batch + Capitation (per-line drop)
      Step 3 — Core rules 1-6 (per-line drop, skip Rule 7)
      Step 4 — Tariff & Quantity adjustment (per-line adjust)
      Step 5 — Rule 7 Clinical Necessity with FULL context incl. dropped procs
    """
    eng = get_engine()
    encounter_date = request.encounter_date or date.today().strftime('%Y-%m-%d')
    batch_id = str(uuid.uuid4())[:12]
    now = datetime.now().isoformat()

    enrollee_ctx = eng.base_engine.get_enrollee_context(request.enrollee_id, encounter_date)

    # Pre-resolve all procedure+diagnosis names once upfront
    resolved_procedures = []
    for item in request.procedures:
        pcode = item.procedure_code.strip().upper()
        dcode = _norm_diag(item.diagnosis_code)
        pinfo = eng._resolve_procedure_info(pcode)
        dinfo = eng._resolve_diagnosis_info(dcode)
        resolved_procedures.append({
            "procedure_code":  pcode,
            "procedure_name":  pinfo.get("name", pcode),
            "procedure_class": pinfo.get("category", pinfo.get("class", "")),
            "diagnosis_code":  dcode,
            "diagnosis_name":  dinfo.get("name", dcode),
            "price":           item.price,
            "quantity":        item.quantity,
            "notes":           item.notes,
        })

    # ══════════════════════════════════════════════════════════════════
    # STEP 1 — 14-day excess care frequency (batch-level denial)
    # ══════════════════════════════════════════════════════════════════
    freq_check = eng.check_excess_care_frequency(request.enrollee_id, encounter_date, days=14, pa_number=request.pa_number)
    if freq_check["triggered"]:
        freq_reason = (
            f"❌ EXCESS_CARE_FREQUENCY: Enrollee already accessed care "
            f"{freq_check['days_since']} day(s) ago ({freq_check['last_date']}) "
            f"via {freq_check['source']}. Minimum 14-day gap required between visits."
        )
        logger.info(
            f"\n━━ BATCH DENIED — STEP 1 EXCESS CARE FREQUENCY ━━\n"
            f"  Enrollee: {request.enrollee_id}  last={freq_check['last_date']} "
            f"({freq_check['days_since']}d ago)\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        freq_rule = {"rule_name": "EXCESS_CARE_FREQUENCY", "passed": False,
                     "source": "master_table", "confidence": 100, "reasoning": freq_reason,
                     "details": {"last_visit_date": freq_check["last_date"],
                                 "days_since": freq_check["days_since"],
                                 "source": freq_check["source"], "required_gap_days": 14}}
        deny_items = []
        for i, rp in enumerate(resolved_procedures):
            rid = str(uuid.uuid4())[:12]
            mongo_db.insert_queue({
                "request_id": rid, "batch_id": batch_id,
                "procedure_code": rp["procedure_code"], "diagnosis_code": rp["diagnosis_code"],
                "enrollee_id": request.enrollee_id, "encounter_date": encounter_date,
                "hospital_name": request.hospital_name, "notes": rp["notes"],
                "enrollee_age": enrollee_ctx.age, "enrollee_gender": enrollee_ctx.gender,
                "procedure_name": rp["procedure_name"], "diagnosis_name": rp["diagnosis_name"],
                "status": "AUTO_DENIED", "decision": "DENY", "confidence": 100,
                "reasoning": freq_reason, "rules_json": json.dumps([freq_rule]),
                "summary_json": json.dumps({}), "created_at": now,
            })
            deny_items.append(LineItemResult(
                request_id=rid, pipeline_stage="DROPPED_STEP1", drop_reason="14-day frequency",
                procedure_code=rp["procedure_code"], procedure_name=rp["procedure_name"],
                diagnosis_code=rp["diagnosis_code"], diagnosis_name=rp["diagnosis_name"],
                status="AUTO_DENIED", decision="DENY", confidence=100,
                reasoning=freq_reason, rules=[RuleDetail(**freq_rule)],
            ))
        return BulkValidationResponse(
            batch_id=batch_id, enrollee_id=request.enrollee_id,
            encounter_date=encounter_date, hospital_name=request.hospital_name,
            encounter_type=request.encounter_type,
            overall_status="AUTO_DENIED", overall_decision="DENY",
            enrollee_age=enrollee_ctx.age, enrollee_gender=enrollee_ctx.gender,
            line_items=deny_items, total_approved_amount=0.0, created_at=now,
        )

    # ── Provider ID (needed by both Step 1.5 and Step 2) ─────────────
    cap_procs, cap_enrollees = get_capitation()
    req_provider = str(request.provider_id or "").strip()

    # ══════════════════════════════════════════════════════════════════
    # STEP 1.5 — Clinical batch rules
    #   Whole-batch denials : same-diagnosis double-billing,
    #                         polypharmacy, diagnosis stacking
    #   Per-line drops      : GP freq, specialist referral,
    #                         post-discharge, level-of-care, IV fluid,
    #                         shotgun labs, symptom-only labs,
    #                         vitamin padding, acuity mismatch
    # ══════════════════════════════════════════════════════════════════

    # ── helper: build a denial item for one resolved_procedure ──────
    def _make_deny_item(rp, rule_dict, stage, drop_label):
        rid = str(uuid.uuid4())[:12]
        mongo_db.insert_queue({
            "request_id": rid, "batch_id": batch_id,
            "procedure_code": rp["procedure_code"], "diagnosis_code": rp["diagnosis_code"],
            "enrollee_id": request.enrollee_id, "encounter_date": encounter_date,
            "hospital_name": request.hospital_name, "notes": rp["notes"],
            "enrollee_age": enrollee_ctx.age, "enrollee_gender": enrollee_ctx.gender,
            "procedure_name": rp["procedure_name"], "diagnosis_name": rp["diagnosis_name"],
            "status": "AUTO_DENIED", "decision": "DENY", "confidence": 100,
            "reasoning": rule_dict["reasoning"],
            "rules_json": json.dumps([rule_dict]),
            "summary_json": json.dumps({}), "created_at": now,
        })
        return LineItemResult(
            request_id=rid, pipeline_stage=stage, drop_reason=drop_label,
            procedure_code=rp["procedure_code"], procedure_name=rp["procedure_name"],
            diagnosis_code=rp["diagnosis_code"], diagnosis_name=rp["diagnosis_name"],
            status="AUTO_DENIED", decision="DENY", confidence=100,
            reasoning=rule_dict["reasoning"], rules=[RuleDetail(**rule_dict)],
            stated_price=rp.get("price"), stated_quantity=rp.get("quantity", 1),
        )

    def _batch_rule(rule_name, reasoning):
        return {"rule_name": rule_name, "passed": False,
                "source": "master_table", "confidence": 100,
                "reasoning": reasoning, "details": {}}

    # ── Track lines to drop at Step 1.5 (code → rule_dict) ──────────
    s15_line_drop: Dict[str, Dict] = {}    # procedure_code → rule_dict
    s15_batch_denied   = False
    s15_batch_rule     = None

    # 1. Same diagnosis at different provider (double-billing) --------
    all_diag_codes = list({rp["diagnosis_code"] for rp in resolved_procedures})
    dbl_check = eng.check_same_diagnosis_different_provider(
        request.enrollee_id, encounter_date, all_diag_codes, req_provider
    )
    if dbl_check["triggered"]:
        s15_batch_denied = True
        s15_batch_rule   = _batch_rule("SAME_DIAGNOSIS_DIFFERENT_PROVIDER", dbl_check["reason"])
        logger.info(f"  [{batch_id}] S1.5 BATCH DENY: SAME_DIAGNOSIS_DIFFERENT_PROVIDER")

    # 2. Polypharmacy (>5 DRG drugs) -----------------------------------
    if not s15_batch_denied:
        poly_check = eng.check_polypharmacy(resolved_procedures)
        if poly_check["triggered"]:
            s15_batch_denied = True
            s15_batch_rule   = _batch_rule("POLYPHARMACY", poly_check["reason"])
            logger.info(f"  [{batch_id}] S1.5 BATCH DENY: POLYPHARMACY ({poly_check['drug_count']} drugs)")

    # 3. Diagnosis stacking AI check -----------------------------------
    if not s15_batch_denied:
        unique_diags = list({rp["diagnosis_code"]: {"code": rp["diagnosis_code"], "name": rp["diagnosis_name"]}
                             for rp in resolved_procedures}.values())
        if len(unique_diags) >= 3:
            stack_check = eng.check_diagnosis_stacking(unique_diags)
            if stack_check["triggered"]:
                s15_batch_denied = True
                s15_batch_rule   = _batch_rule("DIAGNOSIS_STACKING", stack_check["reason"])
                logger.info(f"  [{batch_id}] S1.5 BATCH DENY: DIAGNOSIS_STACKING")

    # If whole batch is denied, emit items and return now -------------
    if s15_batch_denied:
        deny_items = [
            _make_deny_item(rp, s15_batch_rule, "DROPPED_STEP15", s15_batch_rule["rule_name"].lower())
            for rp in resolved_procedures
        ]
        logger.info(f"  [{batch_id}] S1.5 BATCH DENY → {s15_batch_rule['rule_name']}")
        return BulkValidationResponse(
            batch_id=batch_id, enrollee_id=request.enrollee_id,
            encounter_date=encounter_date, hospital_name=request.hospital_name,
            encounter_type=request.encounter_type,
            overall_status="AUTO_DENIED", overall_decision="DENY",
            enrollee_age=enrollee_ctx.age, enrollee_gender=enrollee_ctx.gender,
            line_items=deny_items, total_approved_amount=0.0, created_at=now,
        )

    # 4. Per-line clinical checks (drop specific lines) ---------------
    # -- 4a. GP consult frequency
    gp_check = eng.check_gp_consult_frequency(resolved_procedures, request.enrollee_id, encounter_date, req_provider)
    for code in gp_check.get("flagged_codes", []):
        if code not in s15_line_drop:
            s15_line_drop[code] = _batch_rule("GP_CONSULT_FREQUENCY", gp_check["reason"])
            logger.info(f"  [{batch_id}] S1.5 DROP {code}: GP_CONSULT_FREQUENCY")

    # -- 4b. Specialist without referral
    spec_check = eng.check_specialist_without_referral(resolved_procedures, request.enrollee_id, encounter_date, req_provider)
    for code in spec_check.get("flagged_codes", []):
        if code not in s15_line_drop:
            s15_line_drop[code] = _batch_rule("SPECIALIST_WITHOUT_REFERRAL", spec_check["reason"])
            logger.info(f"  [{batch_id}] S1.5 DROP {code}: SPECIALIST_WITHOUT_REFERRAL")

    # -- 4c. Post-discharge investigations
    pd_check = eng.check_post_discharge(request.enrollee_id, encounter_date, resolved_procedures)
    for code in pd_check.get("flagged_codes", []):
        if code not in s15_line_drop:
            s15_line_drop[code] = _batch_rule("POST_DISCHARGE_CHECK", pd_check["reason"])
            logger.info(f"  [{batch_id}] S1.5 DROP {code}: POST_DISCHARGE_CHECK")

    # -- 4d. Level of care (ADM with mild diagnosis) + IV fluid padding
    # These checks are OUTPATIENT-only — inpatient admissions legitimately
    # require IV fluids, room charges, and higher-acuity interventions.
    has_adm_in_batch = any(
        rp["procedure_code"].upper().startswith("ADM")
        or "ADMISSION" in rp["procedure_name"].upper()
        for rp in resolved_procedures
    )
    if request.encounter_type == "OUTPATIENT":
        for rp in resolved_procedures:
            if rp["procedure_code"] in s15_line_drop:
                continue
            loc = eng.check_level_of_care(
                rp["procedure_code"], rp["procedure_name"],
                rp["diagnosis_code"], rp["diagnosis_name"],
            )
            if loc["triggered"]:
                s15_line_drop[rp["procedure_code"]] = _batch_rule("LEVEL_OF_CARE", loc["reason"])
                logger.info(f"  [{batch_id}] S1.5 DROP {rp['procedure_code']}: LEVEL_OF_CARE")
                continue
            iv = eng.check_iv_fluid_padding(
                rp["procedure_code"], rp["procedure_name"],
                rp["diagnosis_code"], rp["diagnosis_name"],
                has_adm_in_batch,
            )
            if iv["triggered"]:
                s15_line_drop[rp["procedure_code"]] = _batch_rule("IV_FLUID_PADDING", iv["reason"])
                logger.info(f"  [{batch_id}] S1.5 DROP {rp['procedure_code']}: IV_FLUID_PADDING")

    # -- 4e. Vitamin padding
    for rp in resolved_procedures:
        if rp["procedure_code"] in s15_line_drop:
            continue
        vit = eng.check_vitamin_padding(
            rp["procedure_code"], rp["procedure_name"],
            rp["diagnosis_code"], rp["diagnosis_name"],
        )
        if vit["triggered"]:
            s15_line_drop[rp["procedure_code"]] = _batch_rule("VITAMIN_PADDING", vit["reason"])
            logger.info(f"  [{batch_id}] S1.5 DROP {rp['procedure_code']}: VITAMIN_PADDING")

    # -- 4f. Shotgun labs (≥4 panel labs)
    shot_check = eng.check_shotgun_labs(resolved_procedures)
    for code in shot_check.get("flagged_codes", []):
        if code not in s15_line_drop:
            s15_line_drop[code] = _batch_rule("SHOTGUN_LABS", shot_check["reason"])
            logger.info(f"  [{batch_id}] S1.5 DROP {code}: SHOTGUN_LABS")

    # -- 4g. Symptom-only investigations (R-code + ≥3 labs)
    #    Use primary diagnosis = most common dx in batch
    diag_counts: Dict[str, int] = {}
    for rp in resolved_procedures:
        diag_counts[rp["diagnosis_code"]] = diag_counts.get(rp["diagnosis_code"], 0) + 1
    primary_diag = max(diag_counts, key=diag_counts.get) if diag_counts else ""
    sym_check = eng.check_symptom_only_investigations(resolved_procedures, primary_diag)
    for code in sym_check.get("flagged_codes", []):
        if code not in s15_line_drop:
            s15_line_drop[code] = _batch_rule("SYMPTOM_ONLY_INVESTIGATION", sym_check["reason"])
            logger.info(f"  [{batch_id}] S1.5 DROP {code}: SYMPTOM_ONLY_INVESTIGATION")

    # -- 4h. Diagnosis acuity mismatch (IV antimalarial + uncomplicated malaria)
    # Skip for INPATIENT — IV antimalarials are standard inpatient care.
    if request.encounter_type == "OUTPATIENT":
        for rp in resolved_procedures:
            if rp["procedure_code"] in s15_line_drop:
                continue
            acuity = eng.check_diagnosis_acuity_mismatch(
                rp["procedure_code"], rp["procedure_name"],
                rp["diagnosis_code"], rp["diagnosis_name"],
            )
            if acuity["triggered"]:
                s15_line_drop[rp["procedure_code"]] = _batch_rule("DIAGNOSIS_ACUITY_MISMATCH", acuity["reason"])
                logger.info(f"  [{batch_id}] S1.5 DROP {rp['procedure_code']}: DIAGNOSIS_ACUITY_MISMATCH")

    # Separate Step 1.5 drops from remaining procedures ---------------
    s15_dropped: List[Dict] = []
    s15_remaining: List[Dict] = []
    for rp in resolved_procedures:
        if rp["procedure_code"] in s15_line_drop:
            s15_dropped.append({"rp": rp, "rule": s15_line_drop[rp["procedure_code"]],
                                 "label": s15_line_drop[rp["procedure_code"]]["rule_name"].lower()})
        else:
            s15_remaining.append(rp)

    if not s15_remaining:
        deny_items = [
            _make_deny_item(d["rp"], d["rule"], "DROPPED_STEP15", d["label"])
            for d in s15_dropped
        ]
        return BulkValidationResponse(
            batch_id=batch_id, enrollee_id=request.enrollee_id,
            encounter_date=encounter_date, hospital_name=request.hospital_name,
            encounter_type=request.encounter_type,
            overall_status="AUTO_DENIED", overall_decision="DENY",
            enrollee_age=enrollee_ctx.age, enrollee_gender=enrollee_ctx.gender,
            line_items=deny_items, total_approved_amount=0.0, created_at=now,
        )

    # Replace resolved_procedures with the filtered set for subsequent steps
    resolved_procedures = s15_remaining

    # ══════════════════════════════════════════════════════════════════
    # STEP 2 — Duplicate-in-batch + Capitation (per-line drop)
    # ══════════════════════════════════════════════════════════════════
    seen_codes: Dict = {}
    for i, rp in enumerate(resolved_procedures):
        if rp["procedure_code"] not in seen_codes:
            seen_codes[rp["procedure_code"]] = i
    duplicate_indices = {
        i for i, rp in enumerate(resolved_procedures)
        if seen_codes.get(rp["procedure_code"], i) != i
    }

    # req_provider already set above

    step2_dropped: List[Dict] = []   # {rp, rule_dict, reason_label}
    step2_remaining: List[Dict] = [] # rp dicts

    for i, rp in enumerate(resolved_procedures):
        pcode = rp["procedure_code"]
        dcode = rp["diagnosis_code"]

        if i in duplicate_indices:
            reason = (
                f"❌ DUPLICATE_IN_BATCH: {pcode} already appears in line "
                f"{seen_codes[pcode]+1} of this submission."
            )
            rule = {"rule_name": "DUPLICATE_IN_BATCH", "passed": False,
                    "source": "master_table", "confidence": 100, "reasoning": reason,
                    "details": {"duplicate_of_line": seen_codes[pcode] + 1}}
            step2_dropped.append({"rp": rp, "rule": rule, "label": "duplicate in batch"})
            logger.info(f"  [{batch_id}] S2 DROP {pcode}: DUPLICATE_IN_BATCH")

        elif pcode in cap_procs and request.enrollee_id in cap_enrollees:
            enr_cap = cap_enrollees[request.enrollee_id]
            cpk     = enr_cap["provider_key"]
            cpn     = enr_cap["provider_name"]
            if req_provider and req_provider == cpk:
                reason = (
                    f"❌ CAPITATION: {pcode} is covered under your monthly capitation "
                    f"fee (Provider {cpk} — {cpn}). Cannot be claimed separately."
                )
            else:
                reason = (
                    f"❌ CAPITATION: {pcode} is a capitated procedure. This enrollee is "
                    f"capitated to {cpn} (Provider {cpk}). Cannot pay at "
                    f"{'Provider ' + req_provider if req_provider else 'another provider'}."
                )
            rule = {"rule_name": "CAPITATION", "passed": False,
                    "source": "master_table", "confidence": 100, "reasoning": reason,
                    "details": {"capitated_provider_key": cpk, "capitated_provider_name": cpn,
                                "requesting_provider_id": req_provider or None,
                                "same_provider": req_provider == cpk}}
            step2_dropped.append({"rp": rp, "rule": rule, "label": "capitation"})
            logger.info(f"  [{batch_id}] S2 DROP {pcode}: CAPITATION (capitated@{cpk})")

        else:
            step2_remaining.append(rp)

    if not step2_remaining:
        # All procedures eliminated at Step 2 (plus any already dropped at 1.5)
        deny_items = [
            _make_deny_item(d["rp"], d["rule"], "DROPPED_STEP15", d["label"])
            for d in s15_dropped
        ]
        for d in step2_dropped:
            rid = str(uuid.uuid4())[:12]
            rp  = d["rp"]
            mongo_db.insert_queue({
                "request_id": rid, "batch_id": batch_id,
                "procedure_code": rp["procedure_code"], "diagnosis_code": rp["diagnosis_code"],
                "enrollee_id": request.enrollee_id, "encounter_date": encounter_date,
                "hospital_name": request.hospital_name, "notes": rp["notes"],
                "enrollee_age": enrollee_ctx.age, "enrollee_gender": enrollee_ctx.gender,
                "procedure_name": rp["procedure_name"], "diagnosis_name": rp["diagnosis_name"],
                "status": "AUTO_DENIED", "decision": "DENY", "confidence": 100,
                "reasoning": d["rule"]["reasoning"], "rules_json": json.dumps([d["rule"]]),
                "summary_json": json.dumps({}), "created_at": now,
            })
            deny_items.append(LineItemResult(
                request_id=rid, pipeline_stage="DROPPED_STEP2", drop_reason=d["label"],
                procedure_code=rp["procedure_code"], procedure_name=rp["procedure_name"],
                diagnosis_code=rp["diagnosis_code"], diagnosis_name=rp["diagnosis_name"],
                status="AUTO_DENIED", decision="DENY", confidence=100,
                reasoning=d["rule"]["reasoning"], rules=[RuleDetail(**d["rule"])],
            ))
        return BulkValidationResponse(
            batch_id=batch_id, enrollee_id=request.enrollee_id,
            encounter_date=encounter_date, hospital_name=request.hospital_name,
            encounter_type=request.encounter_type,
            overall_status="AUTO_DENIED", overall_decision="DENY",
            enrollee_age=enrollee_ctx.age, enrollee_gender=enrollee_ctx.gender,
            line_items=deny_items, total_approved_amount=0.0, created_at=now,
        )

    # ══════════════════════════════════════════════════════════════════
    # STEP 3 — Core rules 1-6 (skip Rule 7), drop failures
    # Runs all procedures IN PARALLEL — each thread has its own DuckDB
    # connection via _get_thread_engine(), so there's no shared-state risk.
    # ══════════════════════════════════════════════════════════════════
    step3_dropped: List[Dict] = []
    step3_remaining: List[Dict] = []

    def _s3_validate(rp):
        """Worker: validate one procedure on a thread-local engine."""
        pcode = rp["procedure_code"]
        dcode = rp["diagnosis_code"]
        try:
            teng = _get_thread_engine()
            v = teng.validate_comprehensive(
                procedure_code=pcode,
                diagnosis_code=dcode,
                enrollee_id=request.enrollee_id,
                encounter_date=encounter_date,
                skip_rule7=True,
                encounter_type=request.encounter_type,
                prefetched_enrollee_context=enrollee_ctx,
                prefetched_proc_info={"name": rp["procedure_name"], "category": rp["procedure_class"]},
                prefetched_diag_info={"name": rp["diagnosis_name"], "category": ""},
            )
            return rp, v, None
        except Exception as e:
            return rp, None, e

    max_workers = min(len(step2_remaining), 5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        for rp, v, err in pool.map(_s3_validate, step2_remaining):
            pcode = rp["procedure_code"]
            if err:
                logger.error(f"  [{batch_id}] S3 error {pcode}: {err}")
                step3_dropped.append({
                    "rp": rp, "validation": None,
                    "rule": {"rule_name": "ENGINE_ERROR", "passed": False,
                             "source": "master_table", "confidence": 0,
                             "reasoning": f"Validation engine error: {err}", "details": {}},
                    "label": "engine error",
                })
            elif v.overall_decision == "DENY":
                failed_names = ", ".join(r.rule_name for r in v.rule_results if not r.passed)
                step3_dropped.append({"rp": rp, "validation": v, "label": failed_names})
                logger.info(f"  [{batch_id}] S3 DROP {pcode}: {failed_names}")
            else:
                step3_remaining.append({"rp": rp, "validation": v})
                logger.info(f"  [{batch_id}] S3 PASS {pcode}")

            # ── Auto-store learning for any AI-validated rules ────────────
            # Since AI decisions are final (no human review), we store learning
            # immediately so future identical cases hit the learning table
            # instead of calling Claude again.
            if v and v.can_store_ai_approvals:
                try:
                    stored = eng.store_ai_validated_rules(
                        procedure_code=rp["procedure_code"],
                        diagnosis_code=rp["diagnosis_code"],
                        validation=v,
                        approved_by="AUTO",
                    )
                    if stored:
                        logger.info(f"  [{batch_id}] LEARN {pcode}: stored {list(stored.keys())}")
                except Exception as e:
                    logger.warning(f"  [{batch_id}] LEARN error {pcode}: {e}")

    if not step3_remaining:
        # All remaining procedures failed core rules — build final response
        all_line_items = [
            _make_deny_item(d["rp"], d["rule"], "DROPPED_STEP15", d["label"])
            for d in s15_dropped
        ]
        for d in step2_dropped:
            rid = str(uuid.uuid4())[:12]
            rp  = d["rp"]
            mongo_db.insert_queue({
                "request_id": rid, "batch_id": batch_id,
                "procedure_code": rp["procedure_code"], "diagnosis_code": rp["diagnosis_code"],
                "enrollee_id": request.enrollee_id, "encounter_date": encounter_date,
                "hospital_name": request.hospital_name, "notes": rp["notes"],
                "enrollee_age": enrollee_ctx.age, "enrollee_gender": enrollee_ctx.gender,
                "procedure_name": rp["procedure_name"], "diagnosis_name": rp["diagnosis_name"],
                "status": "AUTO_DENIED", "decision": "DENY", "confidence": 100,
                "reasoning": d["rule"]["reasoning"], "rules_json": json.dumps([d["rule"]]),
                "summary_json": json.dumps({}), "created_at": now,
            })
            all_line_items.append(LineItemResult(
                request_id=rid, pipeline_stage="DROPPED_STEP2", drop_reason=d["label"],
                procedure_code=rp["procedure_code"], procedure_name=rp["procedure_name"],
                diagnosis_code=rp["diagnosis_code"], diagnosis_name=rp["diagnosis_name"],
                status="AUTO_DENIED", decision="DENY", confidence=100,
                reasoning=d["rule"]["reasoning"], rules=[RuleDetail(**d["rule"])],
            ))
        for d in step3_dropped:
            rid = str(uuid.uuid4())[:12]
            rp  = d["rp"]
            v   = d.get("validation")
            rules_list = rules_to_list(v.rule_results) if v else [d.get("rule", {})]
            reasoning  = v.overall_reasoning if v else d.get("label", "")
            confidence = v.overall_confidence if v else 0
            mongo_db.insert_queue({
                "request_id": rid, "batch_id": batch_id,
                "procedure_code": rp["procedure_code"], "diagnosis_code": rp["diagnosis_code"],
                "enrollee_id": request.enrollee_id, "encounter_date": encounter_date,
                "hospital_name": request.hospital_name, "notes": rp["notes"],
                "enrollee_age": enrollee_ctx.age, "enrollee_gender": enrollee_ctx.gender,
                "procedure_name": rp["procedure_name"], "diagnosis_name": rp["diagnosis_name"],
                "status": "AUTO_DENIED", "decision": "DENY", "confidence": confidence,
                "reasoning": reasoning, "rules_json": json.dumps(rules_list),
                "summary_json": json.dumps(v.get_summary() if v else {}), "created_at": now,
            })
            all_line_items.append(LineItemResult(
                request_id=rid, pipeline_stage="DROPPED_STEP3", drop_reason=d["label"],
                procedure_code=rp["procedure_code"], procedure_name=rp["procedure_name"],
                diagnosis_code=rp["diagnosis_code"], diagnosis_name=rp["diagnosis_name"],
                status="AUTO_DENIED", decision="DENY", confidence=confidence,
                reasoning=reasoning, rules=[RuleDetail(**r) for r in rules_list],
            ))
        return BulkValidationResponse(
            batch_id=batch_id, enrollee_id=request.enrollee_id,
            encounter_date=encounter_date, hospital_name=request.hospital_name,
            encounter_type=request.encounter_type,
            overall_status="AUTO_DENIED", overall_decision="DENY",
            enrollee_age=enrollee_ctx.age, enrollee_gender=enrollee_ctx.gender,
            line_items=all_line_items, total_approved_amount=0.0, created_at=now,
        )

    # ══════════════════════════════════════════════════════════════════
    # STEP 4 — Tariff & Quantity adjustment
    # ══════════════════════════════════════════════════════════════════
    for item in step3_remaining:
        rp = item["rp"]
        stated_price    = rp.get("price")
        stated_qty      = rp.get("quantity", 1)
        tariff_price    = None
        adjusted_price  = stated_price
        max_qty         = None
        adjusted_qty    = stated_qty

        # ── Tariff check ──────────────────────────────────────────────
        if stated_price is not None and req_provider:
            tariff_price = eng.get_tariff_price(req_provider, rp["procedure_code"])
            if tariff_price is not None:
                adjusted_price = min(stated_price, tariff_price)
                if adjusted_price < stated_price:
                    logger.info(
                        f"  [{batch_id}] S4 TARIFF {rp['procedure_code']}: "
                        f"₦{stated_price:,.2f} → ₦{adjusted_price:,.2f} (tariff: ₦{tariff_price:,.2f})"
                    )

        # ── Quantity check ────────────────────────────────────────────
        max_qty = eng.get_max_quantity(rp["procedure_code"])
        if max_qty is None:
            # Not in master — ask AI
            qty_result = eng.ai_check_quantity(
                rp["procedure_name"], rp["procedure_class"], stated_qty
            )
            if not qty_result["is_reasonable"]:
                max_qty = qty_result["max_reasonable_quantity"]
                logger.info(
                    f"  [{batch_id}] S4 QTY AI {rp['procedure_code']}: "
                    f"{stated_qty} → max {max_qty} ({qty_result['reasoning']})"
                )
        if max_qty is not None and stated_qty > max_qty:
            adjusted_qty = max_qty
            logger.info(
                f"  [{batch_id}] S4 QTY {rp['procedure_code']}: "
                f"{stated_qty} → {adjusted_qty} (max allowed: {max_qty})"
            )

        total = round(adjusted_price * adjusted_qty, 2) if adjusted_price is not None else None

        rp["_tariff_price"]   = tariff_price
        rp["_adjusted_price"] = adjusted_price
        rp["_max_qty"]        = max_qty
        rp["_adjusted_qty"]   = adjusted_qty
        rp["_total"]          = total

    # ══════════════════════════════════════════════════════════════════
    # STEP 5 — Rule 7 Clinical Necessity (batch-aware)
    # AI sees ALL submitted procedures + dropped procs with reasons
    # ══════════════════════════════════════════════════════════════════

    # Build the full-context list for every Rule 7 call
    rule7_context: List[Dict] = []
    for item in step3_remaining:
        rp = item["rp"]
        rule7_context.append({
            "procedure_code":  rp["procedure_code"],
            "procedure_name":  rp["procedure_name"],
            "procedure_class": rp["procedure_class"],
            "diagnosis_code":  rp["diagnosis_code"],
            "diagnosis_name":  rp["diagnosis_name"],
        })
    for dropped_list, label_key in [
        (s15_dropped, "label"), (step2_dropped, "label"), (step3_dropped, "label")
    ]:
        for d in dropped_list:
            rp = d["rp"]
            rule7_context.append({
                "procedure_code":  rp["procedure_code"],
                "procedure_name":  rp["procedure_name"],
                "procedure_class": rp["procedure_class"],
                "diagnosis_code":  rp["diagnosis_code"],
                "diagnosis_name":  rp["diagnosis_name"],
                "drop_reason":     d[label_key],
            })

    final_line_items: List[LineItemResult] = []
    total_approved_amount = 0.0
    overall_status  = "AUTO_APPROVED"
    overall_decision = "APPROVE"
    status_priority  = {"AUTO_DENIED": 2, "AUTO_APPROVED": 1}

    # ── Emit Step 1.5 dropped lines to final response & MongoDB ──────
    for d in s15_dropped:
        final_line_items.append(_make_deny_item(d["rp"], d["rule"], "DROPPED_STEP15", d["label"]))
        overall_status   = "AUTO_DENIED"
        overall_decision = "DENY"

    # ── Emit dropped lines (Step 2 & 3) to final response & MongoDB ──
    for d in step2_dropped:
        rp  = d["rp"]
        rid = str(uuid.uuid4())[:12]
        mongo_db.insert_queue({
            "request_id": rid, "batch_id": batch_id,
            "procedure_code": rp["procedure_code"], "diagnosis_code": rp["diagnosis_code"],
            "enrollee_id": request.enrollee_id, "encounter_date": encounter_date,
            "hospital_name": request.hospital_name, "notes": rp["notes"],
            "enrollee_age": enrollee_ctx.age, "enrollee_gender": enrollee_ctx.gender,
            "procedure_name": rp["procedure_name"], "diagnosis_name": rp["diagnosis_name"],
            "status": "AUTO_DENIED", "decision": "DENY", "confidence": 100,
            "reasoning": d["rule"]["reasoning"], "rules_json": json.dumps([d["rule"]]),
            "summary_json": json.dumps({}), "created_at": now,
        })
        final_line_items.append(LineItemResult(
            request_id=rid, pipeline_stage="DROPPED_STEP2", drop_reason=d["label"],
            procedure_code=rp["procedure_code"], procedure_name=rp["procedure_name"],
            diagnosis_code=rp["diagnosis_code"], diagnosis_name=rp["diagnosis_name"],
            status="AUTO_DENIED", decision="DENY", confidence=100,
            reasoning=d["rule"]["reasoning"], rules=[RuleDetail(**d["rule"])],
            stated_price=rp.get("price"), stated_quantity=rp.get("quantity", 1),
        ))
        overall_status = "AUTO_DENIED"
        overall_decision = "DENY"

    for d in step3_dropped:
        rp  = d["rp"]
        rid = str(uuid.uuid4())[:12]
        v   = d.get("validation")
        rules_list = rules_to_list(v.rule_results) if v else []
        reasoning  = v.overall_reasoning if v else d.get("label", "")
        confidence = v.overall_confidence if v else 0
        mongo_db.insert_queue({
            "request_id": rid, "batch_id": batch_id,
            "procedure_code": rp["procedure_code"], "diagnosis_code": rp["diagnosis_code"],
            "enrollee_id": request.enrollee_id, "encounter_date": encounter_date,
            "hospital_name": request.hospital_name, "notes": rp["notes"],
            "enrollee_age": enrollee_ctx.age, "enrollee_gender": enrollee_ctx.gender,
            "procedure_name": rp["procedure_name"], "diagnosis_name": rp["diagnosis_name"],
            "status": "AUTO_DENIED", "decision": "DENY", "confidence": confidence,
            "reasoning": reasoning, "rules_json": json.dumps(rules_list),
            "summary_json": json.dumps(v.get_summary() if v else {}), "created_at": now,
        })
        final_line_items.append(LineItemResult(
            request_id=rid, pipeline_stage="DROPPED_STEP3", drop_reason=d["label"],
            procedure_code=rp["procedure_code"], procedure_name=rp["procedure_name"],
            diagnosis_code=rp["diagnosis_code"], diagnosis_name=rp["diagnosis_name"],
            status="AUTO_DENIED", decision="DENY", confidence=confidence,
            reasoning=reasoning, rules=[RuleDetail(**r) for r in rules_list],
            stated_price=rp.get("price"), stated_quantity=rp.get("quantity", 1),
        ))
        if status_priority["AUTO_DENIED"] > status_priority.get(overall_status, 0):
            overall_status = "AUTO_DENIED"
        overall_decision = "DENY"

    # ── Process remaining lines through Rule 7 (parallel) ────────────
    def _s5_clinical(item):
        """Worker: run Rule 7 for one procedure on a thread-local engine."""
        rp    = item["rp"]
        pcode = rp["procedure_code"]
        try:
            teng = _get_thread_engine()
            cn = teng.run_clinical_necessity(
                procedure_code=pcode,
                diagnosis_code=rp["diagnosis_code"],
                enrollee_id=request.enrollee_id,
                encounter_date=encounter_date,
                all_request_procedures=rule7_context if len(rule7_context) > 1 else None,
            )
            return item, cn
        except Exception as e:
            logger.error(f"  [{batch_id}] S5 Rule7 error {pcode}: {e}")
            return item, None

    max_workers5 = min(len(step3_remaining), 5)
    s5_results = list(
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers5).map(
            _s5_clinical, step3_remaining
        )
    ) if step3_remaining else []

    for item, cn_result in s5_results:
        rp  = item["rp"]
        v   = item["validation"]
        rid = str(uuid.uuid4())[:12]
        pcode = rp["procedure_code"]

        final_rules = list(v.rule_results)
        cn_rule = None

        if cn_result:
            cn_rule = cn_result["rule"]
            if cn_rule:
                final_rules.append(cn_rule)

        # AI decisions are final — no PENDING_REVIEW
        if cn_rule and not cn_rule.passed:
            # Rule 7 failed → AUTO_DENIED
            line_status    = "AUTO_DENIED"
            final_decision = "DENY"
            final_reasoning = f"⚠️ Clinical necessity concern: {cn_rule.reasoning}"
        else:
            # All rules passed (core + Rule 7 if triggered)
            line_status    = "AUTO_APPROVED"
            final_decision = "APPROVE"
            final_reasoning = v.overall_reasoning

        rules_list = rules_to_list(final_rules)
        total = rp.get("_total")
        if line_status == "AUTO_APPROVED" and total is not None:
            total_approved_amount += total

        mongo_db.insert_queue({
            "request_id": rid, "batch_id": batch_id,
            "procedure_code": pcode, "diagnosis_code": rp["diagnosis_code"],
            "enrollee_id": request.enrollee_id, "encounter_date": encounter_date,
            "hospital_name": request.hospital_name, "notes": rp["notes"],
            "enrollee_age": enrollee_ctx.age, "enrollee_gender": enrollee_ctx.gender,
            "procedure_name": rp["procedure_name"], "diagnosis_name": rp["diagnosis_name"],
            "status": line_status, "decision": final_decision,
            "confidence": v.overall_confidence,
            "reasoning": final_reasoning,
            "rules_json": json.dumps(rules_list),
            "summary_json": json.dumps(v.get_summary()), "created_at": now,
            "adjusted_price":    rp.get("_adjusted_price"),
            "adjusted_quantity": rp.get("_adjusted_qty", rp.get("quantity", 1)),
            "total_amount":      rp.get("_total"),
        })
        final_line_items.append(LineItemResult(
            request_id=rid, pipeline_stage="PASSED",
            procedure_code=pcode, procedure_name=rp["procedure_name"],
            diagnosis_code=rp["diagnosis_code"], diagnosis_name=rp["diagnosis_name"],
            status=line_status, decision=final_decision,
            confidence=v.overall_confidence, reasoning=final_reasoning,
            rules=[RuleDetail(**r) for r in rules_list],
            stated_price=rp.get("price"),
            tariff_price=rp.get("_tariff_price"),
            adjusted_price=rp.get("_adjusted_price"),
            stated_quantity=rp.get("quantity", 1),
            max_allowed_quantity=rp.get("_max_qty"),
            adjusted_quantity=rp.get("_adjusted_qty", rp.get("quantity", 1)),
            total_amount=total,
        ))

        if status_priority.get(line_status, 0) > status_priority.get(overall_status, 0):
            overall_status = line_status
        if final_decision == "DENY":
            overall_decision = "DENY"

    logger.info(
        f"\n━━ BATCH {batch_id} COMPLETE ━━\n"
        f"  Enrollee : {request.enrollee_id}\n"
        f"  S1.5 dropped: {len(s15_dropped)}  S2 dropped: {len(step2_dropped)}  "
        f"S3 dropped: {len(step3_dropped)}  Passed: {len(step3_remaining)}\n"
        f"  Overall  : {overall_status} / {overall_decision}  Total: ₦{total_approved_amount:,.2f}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )

    return BulkValidationResponse(
        batch_id=batch_id, enrollee_id=request.enrollee_id,
        encounter_date=encounter_date, hospital_name=request.hospital_name,
        encounter_type=request.encounter_type,
        overall_status=overall_status, overall_decision=overall_decision,
        enrollee_age=enrollee_ctx.age, enrollee_gender=enrollee_ctx.gender,
        line_items=final_line_items,
        total_approved_amount=round(total_approved_amount, 2),
        created_at=now,
    )


@app.get("/api/v1/requests/{request_id}", response_model=ValidationResponse)
def get_request(request_id: str):
    """Get full details for a specific request"""
    doc = mongo_db.get_queue_item(request_id)
    if not doc:
        raise HTTPException(status_code=404, detail=f"Request {request_id} not found")
    return row_to_response(doc)


@app.get("/api/v1/pending")
def get_pending(limit: int = Query(50, le=200), offset: int = Query(0)):
    """Get PENDING_REVIEW queue for agent"""
    return {
        "total_pending": mongo_db.count_pending(),
        "requests": mongo_db.get_pending(limit=limit, offset=offset),
    }


@app.post("/api/v1/review/{request_id}")
def review_request(request_id: str, review: ReviewAction):
    """
    Agent reviews a PENDING request.
    
    CONFIRM  → agrees with AI → re-runs validation → stores learning
    OVERRIDE → disagrees with AI → NO learning stored
    """
    eng = get_engine()

    row = mongo_db.get_pending_item(request_id)
    if not row:
        raise HTTPException(404, f"Request {request_id} not found or already reviewed")

    ai_rec = row.get('decision', 'DENY')
    now = datetime.now().isoformat()
    
    if review.action == "CONFIRM":
        final_decision = ai_rec
        final_status = "HUMAN_APPROVED" if final_decision == "APPROVE" else "HUMAN_DENIED"
        
        # Re-run to get fresh validation object for learning
        validation = eng.validate_comprehensive(
            procedure_code=row['procedure_code'],
            diagnosis_code=row['diagnosis_code'],
            enrollee_id=row['enrollee_id'],
            encounter_date=row['encounter_date']
        )
        
        stored = {}
        if validation.can_store_ai_approvals:
            stored = eng.store_ai_validated_rules(
                procedure_code=row['procedure_code'],
                diagnosis_code=row['diagnosis_code'],
                validation=validation,
                approved_by=review.reviewed_by
            )
        
        mongo_db.update_queue(request_id, {
            "status": final_status,
            "reviewed_at": now,
            "reviewed_by": review.reviewed_by,
            "review_notes": review.notes or f"Confirmed AI {ai_rec}",
        })

        return {
            "request_id": request_id,
            "final_decision": final_decision,
            "status": final_status,
            "learning_stored": stored,
            "message": f"✅ Confirmed AI {ai_rec}. "
                       f"{'Learning stored for ' + str(len(stored)) + ' rule(s).' if stored else 'No new learning.'}"
        }
    
    elif review.action == "OVERRIDE":
        if not review.override_decision or review.override_decision not in ("APPROVE", "DENY"):
            raise HTTPException(400, "OVERRIDE requires override_decision: APPROVE or DENY")
        
        final_decision = review.override_decision
        final_status = "HUMAN_APPROVED" if final_decision == "APPROVE" else "HUMAN_DENIED"
        
        mongo_db.update_queue(request_id, {
            "status": final_status,
            "decision": final_decision,
            "reviewed_at": now,
            "reviewed_by": review.reviewed_by,
            "review_notes": review.notes or f"Overrode AI {ai_rec} → {final_decision}",
        })

        return {
            "request_id": request_id,
            "final_decision": final_decision,
            "status": final_status,
            "learning_stored": {},
            "message": f"⚠️ Overrode AI {ai_rec} → {final_decision}. No learning stored."
        }
    
    raise HTTPException(400, "action must be CONFIRM or OVERRIDE")


@app.get("/api/v1/stats")
def get_stats():
    """System statistics"""
    stats = {}

    stats['queue'] = mongo_db.get_queue_stats_by_status()
    stats['today'] = mongo_db.get_today_queue_stats(date.today().isoformat())

    learning = {}
    for tbl in ['ai_human_procedure_age', 'ai_human_procedure_gender',
                'ai_human_diagnosis_age', 'ai_human_diagnosis_gender',
                'ai_human_procedure_diagnosis', 'ai_human_procedure_class']:
        learning[tbl] = mongo_db.get_collection_stats(tbl)
    stats['learning'] = learning
    stats['learning_summary'] = {
        'total_entries': sum(v['entries'] for v in learning.values()),
        'total_ai_calls_saved': sum(v['total_usage'] for v in learning.values()),
    }

    total = sum(stats['queue'].values()) if stats['queue'] else 0
    auto = stats['queue'].get('AUTO_APPROVED', 0) + stats['queue'].get('AUTO_DENIED', 0)
    stats['automation_rate'] = round(auto / total * 100, 1) if total > 0 else 0.0

    return stats


@app.get("/api/v1/history")
def get_history(
    status: Optional[str] = Query(None),
    enrollee_id: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0)
):
    """Request history with optional filters"""
    return {"requests": mongo_db.get_history(status=status, enrollee_id=enrollee_id,
                                              limit=limit, offset=offset)}


# ============================================================================
# KLAIRE — Consultation Request Endpoints
# ============================================================================

class KlaireConsultRequest(BaseModel):
    enrollee_id: str
    provider_id: Optional[str] = None
    hospital_name: Optional[str] = None
    encounter_date: Optional[str] = None
    consultation_type: Literal["GP", "SPECIALIST"]
    # GP only
    gp_type: Optional[Literal["INITIAL", "REVIEW"]] = Field("INITIAL", description="GP Initial or GP Review")
    symptoms: Optional[List[str]] = Field(None, description="ICD-10 symptom codes selected by agent")
    # Specialist only
    specialist_code: Optional[str] = Field(None, description="CONS code for the specialist (e.g. CONS035)")
    diagnosis_code: Optional[str] = Field(None, description="ICD-10 / internal diagnosis code")
    diagnosis_name: Optional[str] = Field(None, description="Human-readable diagnosis name")
    # Same-day GP referral: pass the GP PA reference if the GP and specialist
    # are being submitted together in the same session (GP not yet in PA DATA).
    same_day_gp_pa_ref: Optional[str] = Field(
        None,
        description=(
            "PA reference / approval code for the GP consultation submitted in the same session. "
            "Provide this when the GP and specialist requests are submitted together so the system "
            "can confirm the same-day referral even before the GP is stored in PA DATA."
        ),
    )


@app.post("/api/v1/klaire/consult")
def klaire_consult(request: KlaireConsultRequest):
    """
    KLAIRE contact-centre consultation decision.
    Returns step-by-step trace + final decision (APPROVE / DENY / CHANGE).
    """
    enc_date = request.encounter_date or date.today().strftime("%Y-%m-%d")
    db_path  = get_db_path()

    if request.consultation_type == "GP":
        if (request.gp_type or "INITIAL") == "REVIEW":
            result = evaluate_gp_review_consultation(
                enrollee_id   = request.enrollee_id,
                provider_id   = request.provider_id or "",
                hospital_name = request.hospital_name or "",
                encounter_date= enc_date,
                symptoms      = request.symptoms or [],
                db_path       = db_path,
            )
        else:
            result = evaluate_gp_consultation(
                enrollee_id   = request.enrollee_id,
                provider_id   = request.provider_id or "",
                hospital_name = request.hospital_name or "",
                encounter_date= enc_date,
                symptoms      = request.symptoms or [],
                db_path       = db_path,
            )
    else:
        if not request.specialist_code:
            raise HTTPException(status_code=422, detail="specialist_code is required for SPECIALIST consultations")
        result = evaluate_specialist_consultation(
            enrollee_id         = request.enrollee_id,
            provider_id         = request.provider_id or "",
            hospital_name       = request.hospital_name or "",
            encounter_date      = enc_date,
            specialist_code     = request.specialist_code,
            diagnosis_code      = _norm_diag(request.diagnosis_code or ""),
            diagnosis_name      = request.diagnosis_name or "",
            db_path             = db_path,
            same_day_gp_pa_ref  = request.same_day_gp_pa_ref,
        )

        # Fire QA alert email (non-blocking) if 30-day flag triggered
        if result.get("qa_flag"):
            try:
                from .qa_alerts import send_qa_alert
                # Extract last specialist date from the step that set the flag
                last_date  = ""
                days_since = 0
                for step in result.get("steps", []):
                    if step.get("name", "").startswith("Any-Specialist"):
                        last_date  = step.get("data", {}).get("last_specialist_date", "")
                        days_since = step.get("data", {}).get("days_since", 0)
                        break
                send_qa_alert(
                    enrollee_id         = request.enrollee_id,
                    hospital_name       = request.hospital_name or "",
                    encounter_date      = enc_date,
                    specialist_code     = request.specialist_code,
                    specialist_name     = result.get("specialist_name", request.specialist_code),
                    diagnosis_code      = request.diagnosis_code or "",
                    diagnosis_name      = request.diagnosis_name or "",
                    last_specialist_date= last_date,
                    days_since          = days_since,
                    qa_reason           = result.get("qa_reason", ""),
                )
            except Exception as _e:
                logger.warning(f"QA alert dispatch failed (non-fatal): {_e}")

    result["encounter_date"] = enc_date
    result["enrollee_id"]    = request.enrollee_id
    return result


class KlaireReviewAction(BaseModel):
    # PA reviews: AGREE (trust AI's per-rule results) | OVERRIDE (agent overrides AI)
    # Specialist reviews: APPROVE | DENY (kept for backwards compat)
    action: Literal["APPROVE", "DENY", "AGREE", "OVERRIDE"]
    reviewed_by: str = Field(default="Agent", description="Name of the reviewing agent")
    notes: Optional[str] = None


@app.delete("/api/v1/klaire/reviews/clear-all")
def klaire_clear_all_reviews():
    """Delete every entry in the agent review queue."""
    try:
        result = mongo_db._col("klaire_review_queue").delete_many({})
        return {"deleted": result.deleted_count, "message": f"Cleared {result.deleted_count} review(s) from the queue."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/klaire/reviews")
def klaire_get_reviews(limit: int = Query(50, le=200), offset: int = Query(0)):
    """Return pending KLAIRE specialist-diagnosis reviews."""
    return {
        "total_pending": mongo_db.count_pending_klaire_reviews(),
        "reviews": mongo_db.get_pending_klaire_reviews(limit=limit, offset=offset),
    }


@app.post("/api/v1/klaire/review/{review_id}")
def klaire_submit_review(review_id: str, action: KlaireReviewAction):
    """
    Agent confirms or overrides any KLAIRE AI recommendation (specialist or PA).
    Updates the review queue and the appropriate learning table with the human decision.
    """
    doc = mongo_db.get_klaire_review(review_id)
    if not doc:
        raise HTTPException(status_code=404, detail=f"Review {review_id} not found")
    if doc.get("status") != "PENDING_REVIEW":
        raise HTTPException(status_code=409, detail=f"Review {review_id} is already {doc['status']}")

    now = datetime.now().isoformat()
    # For PA items (PA_OUTPATIENT/PA_PREAUTH), ai_recommendation is stored as "PENDING_REVIEW"
    # — the actual AI decision lives in first_line.decision (APPROVE or DENY).
    # For specialist/admission reviews, ai_recommendation holds the real value.
    review_type_early = doc.get("review_type", "SPECIALIST")
    if review_type_early in ("PA_OUTPATIENT", "PA_PREAUTH"):
        ai_rec = (doc.get("first_line") or {}).get("decision", "APPROVE")
    else:
        ai_rec = doc.get("ai_recommendation", "APPROVE")

    if action.action == "AGREE":
        # Agent agrees with whatever AI decided
        status = "HUMAN_DENIED" if ai_rec == "DENY" else "HUMAN_APPROVED"
    elif action.action == "OVERRIDE":
        # Agent DISAGREES with AI — flip the AI's decision
        status = "HUMAN_APPROVED" if ai_rec == "DENY" else "HUMAN_DENIED"
    elif action.action == "APPROVE":
        status = "HUMAN_APPROVED"
    else:  # DENY
        status = "HUMAN_DENIED"

    mongo_db.update_klaire_review(review_id, {
        "status":       status,
        "reviewed_by":  action.reviewed_by,
        "review_notes": action.notes or "",
        "reviewed_at":  now,
    })

    review_type = doc.get("review_type", "SPECIALIST")
    message = f"Decision recorded: {action.action}."

    if review_type in ("PA_OUTPATIENT", "PA_PREAUTH"):
        # Learning strategy:
        #   AGREE  → write AI's per-rule compatibility results exactly as assessed.
        #            A denial for clinical necessity still lets a good compatibility match be learnt.
        #   OVERRIDE → write based on agent's decision across all diagnoses.
        # First-line check (clinical necessity) is NEVER written — context-dependent.
        proc_code        = doc.get("procedure_code", "")
        proc_name        = doc.get("procedure_name", proc_code)
        diag_names       = doc.get("diag_names", {})
        diag_compat      = doc.get("diag_compatibility", {})  # AI's per-diag compatibility results
        agent_note       = action.notes or ""
        all_diag_codes   = list(doc.get("approved_diagnoses", [])) + list(doc.get("denied_diagnoses", []))

        if action.action == "AGREE":
            # Learn exactly what the AI assessed for each diagnosis's compatibility
            for dc, compat in diag_compat.items():
                dn = diag_names.get(dc, dc)
                mongo_db.upsert_procedure_diagnosis_learning(
                    proc_code, proc_name, dc, dn,
                    is_valid=compat["compatible"],
                    confidence=compat.get("confidence", 85),
                    reasoning=compat.get("reasoning", ""),
                    approved_by=action.reviewed_by or "agent",
                )
            # Any diagnosis without a compatibility rule result — mark compatible if it was approved
            for dc in doc.get("approved_diagnoses", []):
                if dc not in diag_compat:
                    dn = diag_names.get(dc, dc)
                    mongo_db.upsert_procedure_diagnosis_learning(
                        proc_code, proc_name, dc, dn,
                        is_valid=True, confidence=85,
                        reasoning=f"Agent agreed with AI approval. {agent_note}".strip(),
                        approved_by=action.reviewed_by or "agent",
                    )

        else:  # OVERRIDE — agent disagrees with AI; no learning written
            pass

        message = f"Decision recorded for {proc_code}."

    elif review_type == "PA_ADMISSION":
        message = (
            f"Admission {'approved' if status == 'HUMAN_APPROVED' else 'denied'} "
            f"for {doc.get('admission_code', '')} "
            f"({doc.get('admission_name', '')}) — {doc.get('days', '?')} day(s)."
        )

    else:
        # Specialist-diagnosis review — update ai_specialist_diagnosis
        mongo_db.upsert_specialist_diagnosis(
            specialist_code=doc["specialist_code"],
            specialist_name=doc["specialist_name"],
            diagnosis_code =doc["diagnosis_code"],
            diagnosis_name =doc["diagnosis_name"],
            decision       =action.action,
            confidence     =doc.get("ai_confidence", 80),
            reasoning      =action.notes or doc.get("ai_reasoning", ""),
            source         ="human",
        )
        message = (
            f"Decision recorded. Learning table updated for "
            f"{doc['specialist_code']} + {doc['diagnosis_code']}."
        )

    return {
        "review_id":   review_id,
        "status":      status,
        "action":      action.action,
        "reviewed_by": action.reviewed_by,
        "reviewed_at": now,
        "review_type": review_type,
        "message":     message,
    }


@app.get("/api/v1/klaire/tariff")
def klaire_tariff(provider_id: str, procedure_code: str):
    """Return contracted tariff price for a provider + procedure pair."""
    try:
        eng = get_engine()
        row = eng.conn.execute("""
            SELECT t.tariffamount
            FROM   "AI DRIVEN DATA".PROVIDERS p
            JOIN   "AI DRIVEN DATA".PROVIDERS_TARIFF pt
                   ON p.protariffid = pt.protariffid
            JOIN   "AI DRIVEN DATA".TARIFF t
                   ON CAST(pt.tariffid AS VARCHAR) = t.tariffid
            WHERE  CAST(p.providerid AS VARCHAR) = CAST(? AS VARCHAR)
              AND  UPPER(TRIM(t.procedurecode))  = UPPER(TRIM(?))
              AND  t.tariffamount                > 0
            LIMIT 1
        """, [provider_id, procedure_code]).fetchone()
        return {"tariff_price": float(row[0]) if row else None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/klaire/procedures")
def klaire_procedures():
    """Return all procedure codes from PROCEDURE DATA for the KLAIRE PA dropdown.

    Each entry includes 'branch' (NO-AUTH | PRE-AUTH) from PROCEDURE_MASTER,
    or None if the code is not in PROCEDURE_MASTER (treated as PRE-AUTH at runtime).
    """
    try:
        eng = get_engine()
        rows = eng.conn.execute("""
            SELECT DISTINCT
                UPPER(TRIM(procedurecode))                    AS procedure_code,
                TRIM(TRIM(proceduredesc), '"')                AS procedure_name
            FROM "AI DRIVEN DATA"."PROCEDURE DATA"
            WHERE procedurecode IS NOT NULL
              AND TRIM(procedurecode) <> ''
            ORDER BY procedure_code
        """).fetchall()

        # Build branch lookup from MongoDB PROCEDURE_MASTER
        from apis.vetting import mongo_db as _mdb
        master_docs = list(_mdb._col("PROCEDURE_MASTER").find({}, {"_id": 0, "procedure_code": 1, "branch": 1}))
        branch_map = {d["procedure_code"].upper(): d.get("branch", "PRE-AUTH").upper() for d in master_docs}

        return {
            "procedures": [
                {
                    "procedure_code":  r[0],
                    "procedure_name":  r[1],
                    "procedure_class": "",
                    "branch":          branch_map.get(r[0].upper()),  # None = not in master
                }
                for r in rows
                if r[0] and r[1]
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/klaire/providers")
def klaire_providers():
    """Return all providers (id, name, state, lga) for the KLAIRE sidebar dropdown. Served from cache."""
    try:
        return {"providers": get_providers()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/klaire/search-diagnoses")
def klaire_search_diagnoses(q: str = "", limit: int = 50):
    """Search diagnosis codes by code prefix or name fragment. Returns up to `limit` results."""
    if not q or len(q.strip()) < 2:
        return {"diagnoses": []}
    try:
        eng = get_engine()
        term = q.strip().upper()
        rows = eng.conn.execute("""
            SELECT UPPER(TRIM(diagnosiscode)) AS code,
                   TRIM(diagnosisdesc)        AS name
            FROM "AI DRIVEN DATA".DIAGNOSIS
            WHERE diagnosiscode IS NOT NULL AND TRIM(diagnosiscode) <> ''
              AND (UPPER(TRIM(diagnosiscode)) LIKE ? OR UPPER(TRIM(diagnosisdesc)) LIKE ?)
            ORDER BY
                CASE WHEN UPPER(TRIM(diagnosiscode)) LIKE ? THEN 0 ELSE 1 END,
                diagnosiscode
            LIMIT ?
        """, [f"{term}%", f"%{term}%", f"{term}%", limit]).fetchall()
        def _dot(code: str) -> str:
            if "." in code or len(code) <= 3:
                return code
            return code[:3] + "." + code[3:]
        return {
            "diagnoses": [
                {"code": _dot(r[0]), "name": r[1]}
                for r in rows if r[0] and r[1]
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/klaire/diagnoses")
def klaire_diagnoses():
    """Return diagnosis list from DIAGNOSIS_MASTER for the KLAIRE specialist dropdown."""
    try:
        eng = get_engine()
        rows = eng.conn.execute("""
            SELECT diagnosis_code, diagnosis_name, diagnosis_class
            FROM PROCEDURE_DIAGNOSIS.DIAGNOSIS_MASTER
            ORDER BY diagnosis_class, diagnosis_name
        """).fetchall()
        return {
            "diagnoses": [
                {"code": r[0], "name": r[1], "class": r[2]}
                for r in rows
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/klaire/all-diagnoses")
def klaire_all_diagnoses():
    """Return all diagnosis codes+names from the broader DIAGNOSIS table for name lookups."""
    try:
        eng = get_engine()
        rows = eng.conn.execute("""
            SELECT UPPER(TRIM(diagnosiscode)) AS code,
                   TRIM(diagnosisdesc)        AS name
            FROM "AI DRIVEN DATA".DIAGNOSIS
            WHERE diagnosiscode IS NOT NULL AND TRIM(diagnosiscode) <> ''
            ORDER BY diagnosiscode
        """).fetchall()
        return {"diagnoses": [{"code": r[0], "name": r[1]} for r in rows if r[0] and r[1]]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/klaire/consultation-codes")
def klaire_consult_codes():
    """Return all consultation codes — used by the KLAIRE UI to populate dropdowns."""
    df = get_consult_df()
    gp   = df[df["code"].isin(GP_CODES)].to_dict("records")
    spec = df[~df["code"].isin(GP_CODES)].to_dict("records")
    return {"gp": gp, "specialists": spec}


ADMISSION_CODES = {
    "ADM01": "Private Room",
    "ADM02": "Semi-Private Room",
    "ADM03": "General Room",
}


class KlaireAdmissionRequest(BaseModel):
    enrollee_id:                str
    provider_id:                str
    hospital_name:              Optional[str] = None
    encounter_date:             str
    admission_code:             Literal["ADM01", "ADM02", "ADM03"]
    days:                       int = Field(..., ge=1)
    admitting_diagnosis_codes:  List[str] = Field(..., min_length=1)
    admitting_diagnosis_names:  Dict[str, str] = Field(default_factory=dict)


@app.get("/api/v1/klaire/admission-codes")
def get_admission_codes():
    """Return the three room-type admission codes."""
    return {"codes": [{"code": k, "name": v} for k, v in ADMISSION_CODES.items()]}


@app.post("/api/v1/klaire/admission")
def klaire_admission(req: KlaireAdmissionRequest):
    """
    Submit an inpatient admission request.

    Flow:
      1. Rule-based pre-checks (severity, room type, duration, readmission).
      2. DENY  → stored as DENIED immediately, no AI call, no agent queue.
      3. APPROVE → stored as APPROVED immediately, no agent queue.
      4. PENDING_REVIEW → AI advisory call, stored for agent review.

    Returns review_id and the pre-check outcome.
    """
    from .klaire_pa import _call_claude
    from .klaire_admission import run_admission_prechecks

    room_name = ADMISSION_CODES[req.admission_code]
    eng       = get_engine()

    # ── Step 1: Rule-based pre-checks ────────────────────────────────────────
    pre = run_admission_prechecks(
        enrollee_id      = req.enrollee_id,
        encounter_date   = req.encounter_date,
        admission_code   = req.admission_code,
        days             = req.days,
        diagnosis_codes  = req.admitting_diagnosis_codes,
        diagnosis_names  = req.admitting_diagnosis_names,
        conn             = eng.conn,
    )

    review_id = str(uuid.uuid4())[:16]
    base_doc  = {
        "review_id":                 review_id,
        "review_type":               "PA_ADMISSION",
        "enrollee_id":               req.enrollee_id,
        "provider_id":               req.provider_id,
        "hospital_name":             req.hospital_name or "",
        "encounter_date":            req.encounter_date,
        "admission_code":            req.admission_code,
        "admission_name":            room_name,
        "days":                      req.days,
        "admitting_diagnosis_codes": req.admitting_diagnosis_codes,
        "admitting_diagnosis_names": req.admitting_diagnosis_names,
        "pre_check_severity":        pre["severity"],
        "pre_check_triggered_rules": pre["triggered_rules"],
        "pre_check_summary":         pre["summary"],
        "pre_check_readmission":     pre["readmission"],
        "reviewed_by":               None,
        "review_notes":              None,
        "reviewed_at":               None,
        "created_at":                datetime.now().isoformat(),
    }

    # ── Step 2: Auto-DENY (outpatient diagnoses only) ────────────────────────
    if pre["decision"] == "DENY":
        mongo_db.insert_klaire_review({
            **base_doc,
            "status":       "DENIED",
            "auto_decided": True,
            "ai_reasoning": pre["summary"],
        })
        return {
            "review_id":    review_id,
            "status":       "DENIED",
            "auto_decided": True,
            "pre_check":    pre,
        }

    # ── Step 3: PENDING_REVIEW — run AI for advisory context ─────────────────
    diag_list = ", ".join(
        f"{c} ({req.admitting_diagnosis_names.get(c, c)})"
        for c in req.admitting_diagnosis_codes
    )
    flags_text = "\n".join(
        f"- [{r['rule']}] {r['detail']}" for r in pre["triggered_rules"]
    ) or "None"

    prompt = f"""You are a clinical reviewer for a Nigerian HMO cost-control unit.

A provider is requesting inpatient admission. Review the pre-check advisory and give
a clinical recommendation for the agent.

Room type: {req.admission_code} — {room_name}
Requested duration: {req.days} day(s)
Admitting diagnoses: {diag_list}
Severity (pre-check): {pre['severity']}

Pre-check advisory flags:
{flags_text}

Assess:
1. Is inpatient admission clinically necessary for these diagnoses?
2. Is the requested duration reasonable for this condition in Nigerian HMO practice?

Respond in JSON only (no markdown):
{{
  "appropriate": true or false,
  "duration_reasonable": true or false,
  "confidence": 0-100,
  "reasoning": "One or two concise sentences."
}}"""

    try:
        ai = _call_claude(prompt)
    except Exception:
        ai = {"appropriate": False, "confidence": 0,
              "reasoning": "AI call failed — agent to assess independently."}

    mongo_db.insert_klaire_review({
        **base_doc,
        "status":                 "PENDING_REVIEW",
        "auto_decided":           False,
        "ai_appropriate":         ai.get("appropriate", False),
        "ai_duration_reasonable": ai.get("duration_reasonable", True),
        "ai_confidence":          int(ai.get("confidence", 0)),
        "ai_reasoning":           ai.get("reasoning", ""),
    })

    return {
        "review_id":    review_id,
        "status":       "PENDING_REVIEW",
        "auto_decided": False,
        "pre_check":    pre,
        "ai_advice": {
            "appropriate":         ai.get("appropriate", False),
            "duration_reasonable": ai.get("duration_reasonable", True),
            "confidence":          int(ai.get("confidence", 0)),
            "reasoning":           ai.get("reasoning", ""),
        },
    }


# ── KLAIRE PA Request ─────────────────────────────────────────────────────────

class KlairePAItem(BaseModel):
    procedure_code:  str
    procedure_name:  Optional[str]  = None    # display name from dropdown
    diagnosis_codes: List[str] = Field(..., min_length=1)
    diagnosis_names: Dict[str, str] = Field(default_factory=dict)
    quantity:        int            = Field(1, ge=1)
    tariff_price:    Optional[float] = None   # contracted tariff from lookup
    provider_price:  Optional[float] = None   # provider override (triggers review)
    comment:         Optional[str]   = None   # free-text comment from provider


class KlairePARequest(BaseModel):
    enrollee_id:           str
    provider_id:           str
    hospital_name:         Optional[str] = None
    encounter_date:        str
    encounter_type:        Literal["OUTPATIENT", "INPATIENT"] = "OUTPATIENT"
    admission_status:      Literal["ADMITTED", "NOT_ADMITTED"] = "NOT_ADMITTED"
    admission_approved_id: Optional[str] = None
    items:                 List[KlairePAItem] = Field(..., min_length=1)
    session_basket:        List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Procedures approved earlier this session — not yet in live DB. "
                    "Injected into DDI and clinical necessity checks as same-day history.",
    )


@app.post("/api/v1/klaire/pa")
def klaire_pa(req: KlairePARequest):
    """Validate a PA request (multiple procedures with 1+ diagnoses each) via KLAIRE."""
    try:
        result = validate_pa_request(
            items=[item.model_dump() for item in req.items],
            enrollee_id=req.enrollee_id,
            provider_id=req.provider_id,
            hospital_name=req.hospital_name or "",
            encounter_date=req.encounter_date,
            encounter_type=req.encounter_type,
            db_path=get_db_path(),
            admission_status=req.admission_status,
            session_basket=req.session_basket or [],
        )
        return result
    except Exception as e:
        logger.exception("klaire_pa error")
        raise HTTPException(status_code=500, detail=str(e))


# ── Admin: Learning Review ────────────────────────────────────────────────────

@app.get("/api/v1/klaire/all-learnings")
def klaire_all_learnings(limit: int = Query(500, ge=1, le=1000)):
    """Return all learning entries (trusted + pending) across all learning tables."""
    try:
        entries  = mongo_db.get_all_learnings(limit=limit)
        total    = len(entries)
        pending  = sum(1 for e in entries if not e.get("_trusted"))
        trusted  = total - pending
        return {"total": total, "pending": pending, "trusted": trusted, "entries": entries}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class LearningActionRequest(BaseModel):
    collection: str
    doc_filter: Dict[str, Any]


@app.post("/api/v1/klaire/learning/approve")
def klaire_learning_approve(body: LearningActionRequest):
    """Admin approves a learning entry — marks it trusted for auto-decisions."""
    allowed = set(mongo_db._LEARNING_COLLECTIONS)
    if body.collection not in allowed:
        raise HTTPException(status_code=400, detail=f"Unknown collection: {body.collection}")
    ok = mongo_db.admin_approve_learning(body.collection, body.doc_filter)
    if not ok:
        raise HTTPException(status_code=500, detail="Approve failed")
    return {"status": "approved"}


@app.delete("/api/v1/klaire/learning/remove")
def klaire_learning_remove(body: LearningActionRequest):
    """Admin deletes a learning entry."""
    allowed = set(mongo_db._LEARNING_COLLECTIONS)
    if body.collection not in allowed:
        raise HTTPException(status_code=400, detail=f"Unknown collection: {body.collection}")
    ok = mongo_db.delete_learning_entry(body.collection, body.doc_filter)
    if not ok:
        raise HTTPException(status_code=500, detail="Delete failed")
    return {"status": "deleted"}


if __name__ == "__main__":
    import uvicorn
    print("=" * 60)
    print("🚀 CLEARLINE AI VETTING API")
    print("=" * 60)
    print(f"📦 Database: {DB_PATH}")
    print(f"📄 API Docs: http://localhost:8002/docs")
    print(f"🏥 Hospital:  streamlit run streamlit/hospital_app.py --server.port 8501")
    print(f"🛡️  Agent:     streamlit run streamlit/agent_app.py --server.port 8502")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8002)