#!/usr/bin/env python3
"""
KLAIRE — Consultation Request Engine
======================================
Handles GP and Specialist consultation PA requests for the Clearline
contact centre. Runs all rule checks step-by-step and returns a full
decision trace so the agent can see exactly what fired and why.

Rules
-----
GP Consultation (CONS021):
  Step 1 — Capitation: provider capitated AND enrollee capitated → DENY
  Step 2 — 14-day GP frequency: GP visit in last 14 days → CHANGE to CONS022

Specialist Consultation (any other CONS code):
  Step 1 — GP referral within 7 days → DENY if missing
  Step 2 — Same specialist in last 14 days → CHANGE to review code
  Step 3 — Any specialist in last 30 days → APPROVE + QA flag
"""

import os
import json
import logging
import duckdb
import pandas as pd
from datetime import datetime, timedelta, date as _date
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

from . import mongo_db

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
_CONSULT_CSV   = os.path.join(_BASE, "KLAIRE AGENT_CONSULATATION.csv")
_CAP_PROC_XL   = os.path.join(_BASE, "Private Capitation List.xlsx")
_CAP_ENR_XL    = os.path.join(_BASE, "cba-capitation-details-report (38).xlsx")


# ── Consultation code lookup (loaded once) ─────────────────────────────────────
def _load_consult_codes() -> pd.DataFrame:
    df = pd.read_csv(_CONSULT_CSV)
    df.columns = ["code", "name", "type"]
    df["code"] = df["code"].str.strip().str.upper()
    df["type"] = df["type"].str.strip().str.upper()
    return df

_CONSULT_DF: Optional[pd.DataFrame] = None

def get_consult_df() -> pd.DataFrame:
    global _CONSULT_DF
    if _CONSULT_DF is None:
        _CONSULT_DF = _load_consult_codes()
    return _CONSULT_DF

GP_INITIAL = "CONS021"
GP_REVIEW  = "CONS022"
GP_CODES   = {GP_INITIAL, GP_REVIEW}


def get_specialist_codes() -> List[str]:
    """All CONS codes that are NOT GP."""
    df = get_consult_df()
    return df[~df["code"].isin(GP_CODES)]["code"].tolist()


def get_review_code(initial_code: str) -> Optional[str]:
    """
    Given an INITIAL specialist code, return its paired REVIEW code.
    Strategy: codes are usually paired as consecutive odd/even numbers
    (CONS035 → CONS036). Falls back to CONS070 (generic specialist review).
    """
    df = get_consult_df()
    code = initial_code.strip().upper()
    row = df[df["code"] == code]
    if row.empty:
        return None
    if row.iloc[0]["type"] == "REVIEW":
        return code  # already a review code

    # Try numeric pairing: CONS035 → CONS036
    prefix = ''.join(c for c in code if c.isalpha())
    num_str = ''.join(c for c in code if c.isdigit())
    if num_str:
        num = int(num_str)
        candidate = f"{prefix}{str(num + 1).zfill(len(num_str))}"
        cand_row = df[df["code"] == candidate]
        if not cand_row.empty and cand_row.iloc[0]["type"] == "REVIEW":
            return candidate

    # Fallback to generic specialist review
    return "CONS070"


def get_code_info(code: str) -> Dict:
    df = get_consult_df()
    row = df[df["code"] == code.strip().upper()]
    if row.empty:
        return {"code": code, "name": code, "type": "UNKNOWN"}
    return {"code": code, "name": row.iloc[0]["name"], "type": row.iloc[0]["type"]}


# ── Capitation lookup (loaded once) ───────────────────────────────────────────
_CAP_PROCS: Optional[set] = None
_CAP_ENROLLEES: Optional[Dict] = None


def _load_capitation() -> Tuple[set, Dict]:
    cap_procs: set = set()
    cap_enrollees: Dict = {}
    try:
        df_p = pd.read_excel(_CAP_PROC_XL)
        for c in df_p["Procedure Code"].dropna():
            cap_procs.add(str(c).strip().upper())
    except Exception as e:
        logger.warning(f"Could not load capitation procedure list: {e}")
    try:
        df_e = pd.read_excel(_CAP_ENR_XL, header=5)
        for _, row in df_e.iterrows():
            eid  = str(row.get("Insured ID", "")).strip()
            pkey = str(row.get("Provider Key", "")).strip()
            pname = str(row.get("Provider Name", "")).strip()
            if eid and eid != "nan" and pkey and pkey != "nan":
                pkey = pkey.rstrip("0").rstrip(".") if "." in pkey else pkey
                cap_enrollees[eid] = {"provider_key": pkey, "provider_name": pname}
    except Exception as e:
        logger.warning(f"Could not load capitation enrollee list: {e}")
    return cap_procs, cap_enrollees


def get_capitation():
    global _CAP_PROCS, _CAP_ENROLLEES
    if _CAP_PROCS is None:
        _CAP_PROCS, _CAP_ENROLLEES = _load_capitation()
    return _CAP_PROCS, _CAP_ENROLLEES


# ── DuckDB helpers ─────────────────────────────────────────────────────────────
def _get_conn(db_path: str):
    return duckdb.connect(db_path, read_only=True)


def _last_gp_visit(conn, enrollee_id: str, before_date: _date, days: int) -> Optional[_date]:
    """Returns the most recent GP consult date within `days` before `before_date`, or None."""
    lookback = (before_date - timedelta(days=days)).strftime("%Y-%m-%d")
    enc_str  = before_date.strftime("%Y-%m-%d")
    try:
        row = conn.execute("""
            SELECT MAX(CAST(requestdate AS DATE))
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE IID = ?
              AND UPPER(TRIM(code)) IN ('CONS021', 'CONS022')
              AND CAST(requestdate AS DATE) >= ?
              AND CAST(requestdate AS DATE) < ?
        """, [enrollee_id, lookback, enc_str]).fetchone()
        if row and row[0]:
            v = row[0]
            return v if isinstance(v, _date) else _date.fromisoformat(str(v)[:10])
    except Exception as e:
        logger.warning(f"_last_gp_visit error: {e}")
    return None


def _last_specialist_visit(
    conn, enrollee_id: str, codes: List[str],
    before_date: _date, days: int
) -> Optional[_date]:
    """Returns most recent date of any of the given specialist codes within `days`."""
    if not codes:
        return None
    placeholders = ",".join("?" * len(codes))
    lookback = (before_date - timedelta(days=days)).strftime("%Y-%m-%d")
    enc_str  = before_date.strftime("%Y-%m-%d")
    try:
        row = conn.execute(f"""
            SELECT MAX(CAST(requestdate AS DATE))
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE IID = ?
              AND UPPER(TRIM(code)) IN ({placeholders})
              AND CAST(requestdate AS DATE) >= ?
              AND CAST(requestdate AS DATE) < ?
        """, [enrollee_id] + [c.upper() for c in codes] + [lookback, enc_str]).fetchone()
        if row and row[0]:
            v = row[0]
            return v if isinstance(v, _date) else _date.fromisoformat(str(v)[:10])
    except Exception as e:
        logger.warning(f"_last_specialist_visit error: {e}")
    return None


# ── Step result builder ───────────────────────────────────────────────────────
def _step(num: int, name: str, result: str, details: str, data: Dict = None) -> Dict:
    return {
        "step": num,
        "name": name,
        "result": result,   # PASS | FAIL | CHANGE | INFO
        "details": details,
        "data": data or {},
    }


# ── GP Review helper ──────────────────────────────────────────────────────────
def _last_gp_review(conn, enrollee_id: str, before_date: _date, days: int) -> Optional[_date]:
    """Returns most recent CONS022 (GP Review) date within `days` before `before_date`."""
    lookback = (before_date - timedelta(days=days)).strftime("%Y-%m-%d")
    enc_str  = before_date.strftime("%Y-%m-%d")
    try:
        row = conn.execute("""
            SELECT MAX(CAST(requestdate AS DATE))
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE IID = ?
              AND UPPER(TRIM(code)) = 'CONS022'
              AND CAST(requestdate AS DATE) >= ?
              AND CAST(requestdate AS DATE) < ?
        """, [enrollee_id, lookback, enc_str]).fetchone()
        if row and row[0]:
            v = row[0]
            return v if isinstance(v, _date) else _date.fromisoformat(str(v)[:10])
    except Exception as e:
        logger.warning(f"_last_gp_review error: {e}")
    return None


# ── GP Consultation Decision ──────────────────────────────────────────────────
def evaluate_gp_consultation(
    enrollee_id: str,
    provider_id: str,
    hospital_name: str,
    encounter_date: str,
    symptoms: List[str],
    db_path: str,
) -> Dict:
    """
    Returns full decision trace for a GP consultation request.

    decision: APPROVE | DENY | CHANGE
    approved_code / change_to_code / change_reason
    steps: list of step dicts
    """
    steps: List[Dict] = []
    enc_dt = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()

    # ── Step 1: Capitation ─────────────────────────────────────────────────────
    # Rule: if the enrollee is capitated AND the procedure is a capitated code,
    # deny regardless of which hospital they visit. Capitation covers all such
    # procedures at the enrollee's attached provider — any PA for it is invalid.
    cap_procs, cap_enrollees = get_capitation()
    enrollee_capitated = enrollee_id in cap_enrollees
    gp_capitated       = GP_INITIAL in cap_procs

    if enrollee_capitated and gp_capitated:
        attached = cap_enrollees[enrollee_id].get("provider_name", "their capitated provider")
        steps.append(_step(
            1, "Capitation Check", "FAIL",
            f"Enrollee {enrollee_id} is on capitation (attached to {attached}). "
            f"GP consultations (CONS021) are covered under the capitation agreement. "
            f"This applies regardless of which hospital the enrollee visits — "
            f"no PA is required or payable. Request rejected.",
            {"enrollee_capitated": True, "gp_procedure_capitated": True,
             "attached_provider": attached},
        ))
        return {
            "consultation_type": "GP",
            "decision": "DENY",
            "approved_code": None,
            "change_to_code": None,
            "change_reason": None,
            "qa_flag": False,
            "qa_reason": None,
            "steps": steps,
        }

    steps.append(_step(
        1, "Capitation Check", "PASS",
        f"{'Enrollee is NOT on capitation' if not enrollee_capitated else 'CONS021 is NOT a capitated procedure'}. "
        f"PA process applies.",
        {"enrollee_capitated": enrollee_capitated, "gp_procedure_capitated": gp_capitated},
    ))

    # ── Step 2: 14-day GP frequency ────────────────────────────────────────────
    conn = _get_conn(db_path)
    try:
        last_gp = _last_gp_visit(conn, enrollee_id, enc_dt, days=14)
    finally:
        conn.close()

    if last_gp:
        days_ago = (enc_dt - last_gp).days
        review_info = get_code_info(GP_REVIEW)
        steps.append(_step(
            2, "14-Day GP Frequency Check", "CHANGE",
            f"Enrollee last saw a GP {days_ago} day(s) ago ({last_gp}). "
            f"Within the 14-day window — converting to GP Review (CONS022). "
            f"Message to provider: 'Contact us if this is a different diagnosis from the initial one.'",
            {"last_gp_date": str(last_gp), "days_since": days_ago},
        ))
        return {
            "consultation_type": "GP",
            "decision": "CHANGE",
            "approved_code": GP_INITIAL,
            "change_to_code": GP_REVIEW,
            "change_to_name": review_info["name"],
            "change_reason": (
                f"Enrollee visited a GP {days_ago} day(s) ago ({last_gp}). "
                f"Approving as GP Review (CONS022) instead. "
                f"Contact us if this is a different diagnosis from the initial one."
            ),
            "qa_flag": False,
            "qa_reason": None,
            "steps": steps,
        }

    days_since_msg = "No GP visit found in the last 14 days."
    steps.append(_step(
        2, "14-Day GP Frequency Check", "PASS",
        f"{days_since_msg} Enrollee is eligible for an initial GP consultation (CONS021).",
        {"last_gp_date": None},
    ))

    return {
        "consultation_type": "GP",
        "gp_type": "INITIAL",
        "decision": "APPROVE",
        "approved_code": GP_INITIAL,
        "approved_code_name": get_code_info(GP_INITIAL)["name"],
        "change_to_code": None,
        "change_reason": None,
        "qa_flag": False,
        "qa_reason": None,
        "steps": steps,
    }


# ── GP Review Consultation Decision ───────────────────────────────────────────
def evaluate_gp_review_consultation(
    enrollee_id: str,
    provider_id: str,
    hospital_name: str,
    encounter_date: str,
    symptoms: List[str],
    db_path: str,
) -> Dict:
    """
    GP Review (CONS022) request.

    Step 1 — Capitation: provider AND enrollee capitated → DENY.
    Step 2 — 14-day GP review frequency: CONS022 in last 14 days → DENY; else APPROVE.
    """
    steps: List[Dict] = []
    enc_dt = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()

    # ── Step 1: Capitation ─────────────────────────────────────────────────────
    cap_procs, cap_enrollees = get_capitation()
    enrollee_capitated = enrollee_id in cap_enrollees
    gp_capitated       = GP_REVIEW in cap_procs or GP_INITIAL in cap_procs

    if enrollee_capitated and gp_capitated:
        attached = cap_enrollees[enrollee_id].get("provider_name", "their capitated provider")
        steps.append(_step(
            1, "Capitation Check", "FAIL",
            f"Enrollee {enrollee_id} is on capitation (attached to {attached}). "
            f"GP consultations are covered under the capitation agreement. "
            f"This applies regardless of which hospital the enrollee visits — "
            f"no PA is required or payable. Request rejected.",
            {"enrollee_capitated": True, "gp_procedure_capitated": True,
             "attached_provider": attached},
        ))
        return {
            "consultation_type": "GP",
            "gp_type": "REVIEW",
            "decision": "DENY",
            "approved_code": None,
            "change_to_code": None,
            "change_reason": None,
            "qa_flag": False,
            "qa_reason": None,
            "steps": steps,
        }

    steps.append(_step(
        1, "Capitation Check", "PASS",
        f"{'Enrollee is NOT on capitation' if not enrollee_capitated else 'GP review code is NOT a capitated procedure'}. "
        f"PA process applies.",
        {"enrollee_capitated": enrollee_capitated, "gp_procedure_capitated": gp_capitated},
    ))

    # ── Step 2: 14-day GP review frequency ────────────────────────────────────
    conn = _get_conn(db_path)
    try:
        last_review = _last_gp_review(conn, enrollee_id, enc_dt, days=14)
    finally:
        conn.close()

    if last_review:
        days_ago = (enc_dt - last_review).days
        steps.append(_step(
            2, "GP Review 14-Day Check", "FAIL",
            f"A GP Review (CONS022) was already approved {days_ago} day(s) ago ({last_review}). "
            f"Only one GP review is permitted within a 14-day window. Request denied.",
            {"last_review_date": str(last_review), "days_since": days_ago},
        ))
        return {
            "consultation_type": "GP",
            "gp_type": "REVIEW",
            "decision": "DENY",
            "approved_code": None,
            "change_to_code": None,
            "change_reason": None,
            "qa_flag": False,
            "qa_reason": None,
            "steps": steps,
        }

    steps.append(_step(
        2, "GP Review 14-Day Check", "PASS",
        f"No GP Review (CONS022) found in the last 14 days. Enrollee is eligible for a GP Review visit.",
        {"last_review_date": None},
    ))
    return {
        "consultation_type": "GP",
        "gp_type": "REVIEW",
        "decision": "APPROVE",
        "approved_code": GP_REVIEW,
        "approved_code_name": get_code_info(GP_REVIEW)["name"],
        "change_to_code": None,
        "change_reason": None,
        "qa_flag": False,
        "qa_reason": None,
        "steps": steps,
    }


# ── Claude call ───────────────────────────────────────────────────────────────
def _call_claude(prompt: str, model: str = "claude-opus-4-6") -> Dict:
    """Standalone Claude call for KLAIRE specialist-diagnosis checks."""
    import anthropic
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"action": "DENY", "confidence": 0,
                "reasoning": "ANTHROPIC_API_KEY not set — cannot evaluate."}
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp   = client.messages.create(
            model=model, max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        text = "".join(b.text for b in resp.content if hasattr(b, "text")).strip()
        if "```" in text:
            text = text[text.find("{"):text.rfind("}") + 1]
        return json.loads(text)
    except Exception as e:
        logger.error(f"_call_claude error: {e}")
        return {"action": "DENY", "confidence": 0, "reasoning": f"AI call failed: {e}"}


AUTO_APPROVE_THRESHOLD = 3   # uses in learning table before auto-decision kicks in


def check_specialist_diagnosis_compatibility(
    specialist_code: str,
    specialist_name: str,
    diagnosis_code: str,
    diagnosis_name: str,
) -> Dict:
    """
    Check whether the requested specialist is appropriate for the given diagnosis.

    Decision logic:
      1. Look up (specialist_code, diagnosis_code) in ai_specialist_diagnosis learning table
         a. Found + usage_count >= 3 → AUTO_APPROVE or AUTO_DENY (no agent review)
         b. Found + usage_count  < 3 → cached decision, but still PENDING_REVIEW
      2. Not found → call Claude Opus, store result, PENDING_REVIEW

    Returns:
      {
        decision:          "APPROVE" | "DENY",
        confidence:        int,
        reasoning:         str,
        source:            "learning_table" | "ai",
        requires_review:   bool,
        auto:              bool,   # True = AUTO_APPROVE/DENY, no agent needed
        usage_count:       int,
      }
    """
    cached = mongo_db.get_specialist_diagnosis_learning(specialist_code, diagnosis_code)

    if cached:
        usage = int(cached.get("usage_count", 0))
        decision   = cached["decision"].upper()
        confidence = int(cached.get("confidence", 80))
        reasoning  = cached.get("reasoning", "")

        if mongo_db.is_learning_trusted(cached):
            # Trusted: admin-approved OR confirmed ≥3 times — auto-decide
            mongo_db.inc_specialist_diagnosis_usage(specialist_code, diagnosis_code)
            return {
                "decision":        decision,
                "confidence":      confidence,
                "reasoning":       reasoning,
                "source":          "learning_table",
                "requires_review": False,
                "auto":            True,
                "usage_count":     usage + 1,
            }
        else:
            # Found but not yet trusted — still need agent eyes
            mongo_db.inc_specialist_diagnosis_usage(specialist_code, diagnosis_code)
            return {
                "decision":        decision,
                "confidence":      confidence,
                "reasoning":       reasoning,
                "source":          "learning_table",
                "requires_review": True,
                "auto":            False,
                "usage_count":     usage + 1,
            }

    # ── Not in learning table → call AI ──────────────────────────────────────
    prompt = f"""You are a medical referral validator for a Nigerian HMO (Health Maintenance Organisation).

Determine if the following SPECIALIST TYPE is CLINICALLY APPROPRIATE to manage or treat a patient with the given DIAGNOSIS.

Specialist: {specialist_code} — {specialist_name}
Diagnosis:  {diagnosis_code} — {diagnosis_name}

RULES:
- APPROVE if the specialist's clinical domain is directly relevant to diagnosing, managing, monitoring, or treating this condition.
- APPROVE for reasonable co-morbidity referrals (e.g. diabetic patient seeing a cardiologist for hypertension).
- DENY if the specialist has no clinical relevance to this condition whatsoever.
- Consider common Nigerian HMO practice: GPs refer to specialists for confirmed diagnoses.
- Do NOT deny solely because another specialist might also be appropriate — evaluate only whether THIS specialist is a clinically valid choice.

Respond in JSON only:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "One sentence explaining why this specialist is or is not appropriate for this diagnosis."
}}"""

    ai_result  = _call_claude(prompt)
    decision   = ai_result.get("action", "DENY").upper()
    confidence = int(ai_result.get("confidence", 0))
    reasoning  = ai_result.get("reasoning", "")

    # Store in learning table (usage_count will be 1 after upsert)
    mongo_db.upsert_specialist_diagnosis(
        specialist_code=specialist_code,
        specialist_name=specialist_name,
        diagnosis_code=diagnosis_code,
        diagnosis_name=diagnosis_name,
        decision=decision,
        confidence=confidence,
        reasoning=reasoning,
        source="ai",
    )

    return {
        "decision":        decision,
        "confidence":      confidence,
        "reasoning":       reasoning,
        "source":          "ai",
        "requires_review": True,
        "auto":            False,
        "usage_count":     1,
    }


# ── Specialist Consultation Decision ──────────────────────────────────────────
def evaluate_specialist_consultation(
    enrollee_id: str,
    provider_id: str,
    hospital_name: str,
    encounter_date: str,
    specialist_code: str,
    diagnosis_code: str,
    db_path: str,
    diagnosis_name: str = "",
) -> Dict:
    """
    Returns full decision trace for a Specialist consultation request.

    Steps 1-3: frequency/referral rules (existing)
    Step 1:    Capitation check
    Step 2:    GP referral (7 days)
    Step 3:    Same specialist (14 days)
    Step 4:    Any specialist (30 days)
    Step 5:    Specialist-Diagnosis Compatibility (AI + learning table)

    decision: APPROVE | DENY | CHANGE | PENDING_REVIEW
    """
    steps: List[Dict] = []
    specialist_code = specialist_code.strip().upper()
    diagnosis_code  = diagnosis_code.strip().upper()
    enc_dt    = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
    code_info = get_code_info(specialist_code)
    review_code  = get_review_code(specialist_code)
    review_info  = get_code_info(review_code) if review_code else {}
    diag_display = diagnosis_name or diagnosis_code

    # ── Step 1: Capitation ─────────────────────────────────────────────────────
    cap_procs, cap_enrollees = get_capitation()
    enrollee_capitated  = enrollee_id in cap_enrollees
    spec_capitated      = specialist_code in cap_procs

    if enrollee_capitated and spec_capitated:
        attached = cap_enrollees[enrollee_id].get("provider_name", "their capitated provider")
        steps.append(_step(
            1, "Capitation Check", "FAIL",
            f"Enrollee {enrollee_id} is on capitation (attached to {attached}). "
            f"{specialist_code} ({code_info['name']}) is a capitated procedure. "
            f"This is covered under the capitation agreement regardless of which hospital "
            f"the enrollee visits — no PA is required or payable. Request rejected.",
            {"enrollee_capitated": True, "specialist_procedure_capitated": True,
             "attached_provider": attached},
        ))
        return {
            "consultation_type": "SPECIALIST",
            "specialist_code": specialist_code, "specialist_name": code_info["name"],
            "decision": "DENY", "approved_code": None,
            "change_to_code": None, "change_reason": None,
            "qa_flag": False, "qa_reason": None,
            "requires_agent_review": False, "review_id": None,
            "steps": steps,
        }

    steps.append(_step(
        1, "Capitation Check", "PASS",
        f"{'Enrollee is NOT on capitation' if not enrollee_capitated else f'{specialist_code} is NOT a capitated procedure'}. "
        f"PA process applies.",
        {"enrollee_capitated": enrollee_capitated, "specialist_procedure_capitated": spec_capitated},
    ))

    conn = _get_conn(db_path)
    try:
        # ── Step 2: GP referral within 7 days ─────────────────────────────────
        last_gp = _last_gp_visit(conn, enrollee_id, enc_dt, days=7)

        if not last_gp:
            steps.append(_step(
                2, "GP Referral Check (7 days)", "FAIL",
                f"No GP consultation found for enrollee {enrollee_id} in the last 7 days. "
                f"A specialist visit requires a GP referral. "
                f"Provider message: 'Enrollee has no GP consult history in the last 7 days. "
                f"Contact us to establish whether a GP referred this patient.'",
                {"last_gp_date": None, "window_days": 7},
            ))
            return {
                "consultation_type": "SPECIALIST",
                "specialist_code": specialist_code, "specialist_name": code_info["name"],
                "decision": "DENY", "approved_code": None,
                "change_to_code": None, "change_reason": None,
                "qa_flag": False, "qa_reason": None,
                "requires_agent_review": False, "review_id": None,
                "steps": steps,
            }

        days_since_gp = (enc_dt - last_gp).days
        steps.append(_step(
            2, "GP Referral Check (7 days)", "PASS",
            f"GP consultation found {days_since_gp} day(s) ago ({last_gp}). Referral established.",
            {"last_gp_date": str(last_gp), "days_since": days_since_gp},
        ))

        # ── Step 3: Same specialist in last 14 days ────────────────────────────
        same_spec_codes = list({specialist_code, review_code} - {None})
        last_same = _last_specialist_visit(conn, enrollee_id, same_spec_codes, enc_dt, days=14)

        if last_same:
            days_since_same = (enc_dt - last_same).days
            steps.append(_step(
                3, "Same-Specialist 14-Day Check", "CHANGE",
                f"Enrollee saw the same specialist ({code_info['name']}) "
                f"{days_since_same} day(s) ago ({last_same}). "
                f"Within the 14-day window — converting to specialist review ({review_code}).",
                {"last_visit_date": str(last_same), "days_since": days_since_same},
            ))
            return {
                "consultation_type": "SPECIALIST",
                "specialist_code": specialist_code, "specialist_name": code_info["name"],
                "decision": "CHANGE", "approved_code": specialist_code,
                "change_to_code": review_code,
                "change_to_name": review_info.get("name", review_code),
                "change_reason": (
                    f"Same specialist seen {days_since_same} day(s) ago ({last_same}). "
                    f"Approving as specialist review ({review_code}) instead."
                ),
                "qa_flag": False, "qa_reason": None,
                "requires_agent_review": False, "review_id": None,
                "steps": steps,
            }

        steps.append(_step(
            3, "Same-Specialist 14-Day Check", "PASS",
            f"No visit to this specialist in the last 14 days. Proceeding.",
            {"last_visit_date": None},
        ))

        # ── Step 4: Any specialist in last 30 days ─────────────────────────────
        all_spec_codes = get_specialist_codes()
        last_any  = _last_specialist_visit(conn, enrollee_id, all_spec_codes, enc_dt, days=30)
        qa_flag   = False
        qa_reason = None

        if last_any:
            days_since_any = (enc_dt - last_any).days
            qa_flag  = True
            qa_reason = (
                f"Enrollee {enrollee_id} has seen a specialist in the last 30 days "
                f"(last: {last_any}, {days_since_any}d ago). "
                f"Please review for potential specialist over-utilisation."
            )
            steps.append(_step(
                4, "Any-Specialist 30-Day Check", "INFO",
                f"Enrollee saw a specialist {days_since_any} day(s) ago ({last_any}). "
                f"Flagged to QA — proceeding to compatibility check.",
                {"last_specialist_date": str(last_any), "days_since": days_since_any},
            ))
        else:
            steps.append(_step(
                4, "Any-Specialist 30-Day Check", "PASS",
                f"No specialist visit in the last 30 days. Clean record.",
                {"last_specialist_date": None},
            ))

    finally:
        conn.close()

    # ── Step 5: Specialist-Diagnosis Compatibility ─────────────────────────────
    compat = check_specialist_diagnosis_compatibility(
        specialist_code=specialist_code,
        specialist_name=code_info["name"],
        diagnosis_code=diagnosis_code,
        diagnosis_name=diag_display,
    )

    source_label = (
        f"AUTO ({compat['usage_count']} uses)" if compat["auto"]
        else f"Learning table ({compat['usage_count']} uses)" if compat["source"] == "learning_table"
        else "AI (first evaluation)"
    )
    step5_result = (
        "PASS" if (compat["decision"] == "APPROVE" and compat["auto"])
        else "FAIL" if (compat["decision"] == "DENY" and compat["auto"])
        else "INFO"
    )
    steps.append(_step(
        5, "Specialist-Diagnosis Compatibility", step5_result,
        f"{source_label} → {compat['decision']} ({compat['confidence']}% confidence). "
        f"{compat['reasoning']}",
        compat,
    ))

    # Auto-deny from learning table (high confidence, ≥3 uses, DENY)
    if compat["auto"] and compat["decision"] == "DENY":
        return {
            "consultation_type": "SPECIALIST",
            "specialist_code": specialist_code, "specialist_name": code_info["name"],
            "decision": "DENY", "approved_code": None,
            "change_to_code": None, "change_reason": None,
            "qa_flag": qa_flag, "qa_reason": qa_reason,
            "requires_agent_review": False, "review_id": None,
            "steps": steps,
        }

    # Auto-approve from learning table (high confidence, ≥3 uses, APPROVE)
    if compat["auto"] and compat["decision"] == "APPROVE":
        return {
            "consultation_type": "SPECIALIST",
            "specialist_code": specialist_code, "specialist_name": code_info["name"],
            "decision": "APPROVE",
            "approved_code": specialist_code, "approved_code_name": code_info["name"],
            "change_to_code": None, "change_reason": None,
            "qa_flag": qa_flag, "qa_reason": qa_reason,
            "requires_agent_review": False, "review_id": None,
            "steps": steps,
        }

    # Needs agent review (AI first call or learning table < 3 uses)
    import uuid as _uuid
    review_id = str(_uuid.uuid4())[:16]
    now = datetime.utcnow().isoformat()
    mongo_db.insert_klaire_review({
        "review_id":       review_id,
        "specialist_code": specialist_code,
        "specialist_name": code_info["name"],
        "diagnosis_code":  diagnosis_code,
        "diagnosis_name":  diag_display,
        "enrollee_id":     enrollee_id,
        "encounter_date":  encounter_date,
        "hospital_name":   hospital_name,
        "ai_decision":     compat["decision"],
        "ai_confidence":   compat["confidence"],
        "ai_reasoning":    compat["reasoning"],
        "learning_source": compat["source"],
        "usage_count":     compat["usage_count"],
        "qa_flag":         qa_flag,
        "qa_reason":       qa_reason,
        "status":          "PENDING_REVIEW",
        "reviewed_by":     None,
        "review_notes":    None,
        "reviewed_at":     None,
        "created_at":      now,
    })

    return {
        "consultation_type": "SPECIALIST",
        "specialist_code": specialist_code, "specialist_name": code_info["name"],
        "diagnosis_code": diagnosis_code, "diagnosis_name": diag_display,
        "decision": "PENDING_REVIEW",
        "ai_recommendation": compat["decision"],
        "ai_confidence":     compat["confidence"],
        "ai_reasoning":      compat["reasoning"],
        "learning_source":   compat["source"],
        "approved_code": specialist_code, "approved_code_name": code_info["name"],
        "change_to_code": None, "change_reason": None,
        "qa_flag": qa_flag, "qa_reason": qa_reason,
        "requires_agent_review": True,
        "review_id": review_id,
        "steps": steps,
    }
