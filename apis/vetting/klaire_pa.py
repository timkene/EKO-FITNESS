"""
KLAIRE PA Request Validation
============================
Validates outpatient/inpatient PA requests submitted after consultation.

Handles multiple diagnoses per procedure with merge logic:
  - If ALL diagnoses fail a rule → procedure DENY / RECOMMEND_DENY
  - If ≥1 diagnosis passes → procedure approved with passing diagnoses only
  - Delisted (failed) diagnoses are returned so provider knows what was dropped

Rules applied (capitation and 14-day visit already handled at consultation stage):
  1. Procedure Age
  2. Procedure Gender
  3. Diagnosis Age          ← per-diagnosis, merge applied
  4. Diagnosis Gender       ← per-diagnosis, merge applied
  5. Procedure-Diagnosis Compatibility  ← per-diagnosis, merge applied
  6. Procedure 30-Day Duplicate
  7. Clinical Necessity (AI)
  8. Diagnosis Stacking
  9. First-Line Treatment Check (AI — no learning, always fresh)
 10. Disease Combination Check (AI — request-level, no learning)
 12. Injection Without Admission (pre-auth, non-admitted only)

Trust model:
  - Master table / trusted learning table → auto-decide (no agent needed)
  - Untrusted learning table / AI evaluation → PENDING_REVIEW (agent must act)
"""

import os
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from . import mongo_db

logger = logging.getLogger(__name__)

# Rule names that apply at the procedure level (same regardless of which diagnosis)
_PROC_RULES = {"PROCEDURE_AGE", "PROCEDURE_GENDER", "PROCEDURE_30DAY_DUPLICATE"}

# Rule names that are diagnosis-specific (merge logic applies)
_DIAG_RULES = {
    "DIAGNOSIS_AGE", "DIAGNOSIS_GENDER",
    "PROC_DIAG_COMPATIBILITY", "CLINICAL_NECESSITY",
    "DIAGNOSIS_STACKING",
}


# ── Claude helper ─────────────────────────────────────────────────────────────

def _call_claude(prompt: str) -> Dict:
    import anthropic, time
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"action": "DENY", "confidence": 0,
                "reasoning": "ANTHROPIC_API_KEY not set — cannot evaluate."}
    client = anthropic.Anthropic(api_key=api_key, max_retries=0)
    last_err = None
    for attempt in range(3):  # up to 3 tries: 0, 2, 4 s delays
        try:
            resp = client.messages.create(
                model="claude-opus-4-6", max_tokens=900, temperature=0,
                messages=[{"role": "user", "content": prompt}],
            )
            text = "".join(b.text for b in resp.content if hasattr(b, "text")).strip()
            # Strip markdown fences if present
            if "```" in text:
                text = text[text.find("{"):text.rfind("}") + 1]
            # Use raw_decode to stop at end of first valid JSON object,
            # ignoring any trailing commentary Claude may append
            obj, _ = json.JSONDecoder().raw_decode(text, text.find("{"))
            return obj
        except Exception as e:
            last_err = e
            if "529" in str(e) or "overloaded" in str(e).lower():
                wait = 2 ** attempt
                logger.warning(f"_call_claude: Anthropic overloaded, retry {attempt+1}/3 in {wait}s")
                time.sleep(wait)
            else:
                break
    logger.error(f"_call_claude error: {last_err}")
    return {"action": "DENY", "confidence": 0, "reasoning": f"AI call failed: {last_err}"}


# ── First-line treatment check ────────────────────────────────────────────────

def check_first_line_treatment(
    procedure_code: str,
    procedure_name: str,
    diagnosis_code: str,
    diagnosis_name: str,
    encounter_type: str = "OUTPATIENT",
    prior_treatments: Optional[List[Dict]] = None,
) -> Dict:
    """
    Clinical necessity check — is this procedure appropriate for this diagnosis?

    Unlike other rules, this NEVER learns and NEVER auto-decides.
    Every request is evaluated fresh by AI because necessity is context-dependent.

    prior_treatments: list of {code, description, date, source} from the last 30 days.
    When provided, the AI can recognise that first-line alternatives were already tried,
    upgrading a would-be DENY to APPROVE (step-therapy justification).

    Always returns requires_review=True, auto=False.
    """
    context = (
        "The patient has been admitted (INPATIENT)." if encounter_type == "INPATIENT"
        else "This is an outpatient visit."
    )

    history_block = ""
    if prior_treatments:
        lines = [
            f"  • {t.get('code','').upper()} — {t.get('description') or t.get('code','')} "
            f"(on {str(t.get('date',''))[:10]}, source: {t.get('source','')})"
            for t in prior_treatments
        ]
        history_block = (
            "\n\nPATIENT TREATMENT HISTORY (last 30 days from PA and claims data):\n"
            + "\n".join(lines)
            + "\n\nIMPORTANT: If the history shows the patient has already received "
            "first-line alternatives for this diagnosis, treat the current request as "
            "justified step-up therapy and APPROVE. Do not penalise a second-line drug "
            "when the first line has already been tried."
        )

    prompt = f"""You are a strict clinical reviewer for a Nigerian HMO cost-control unit.

{context}

Your job is to determine if the procedure/medication below is a NECESSARY first-line treatment or investigation for the given diagnosis in standard Nigerian outpatient HMO practice.

Procedure/Medication: {procedure_code} — {procedure_name}
Diagnosis: {diagnosis_code} — {diagnosis_name}{history_block}

DECISION RULES (apply strictly):
- APPROVE if this is unambiguously a standard first-line treatment or essential investigation for this exact diagnosis.
- APPROVE if it is a well-recognised symptomatic treatment for this diagnosis (e.g. cough syrup for URTI).
- APPROVE if the patient history above shows they have already tried the standard first-line options for this diagnosis (step-up therapy justification).
- DENY if the diagnosis is typically managed clinically without this investigation (e.g. simple URTI does not require FBC or CRP unless complications are clinically documented).
- DENY if this is a second-line, specialist-reserved, or confirmatory test where simpler options suffice AND there is no prior first-line treatment in the history.
- DENY if you cannot clearly identify what the procedure is from the name given.
- When in doubt, DENY — the agent will review.

Respond in JSON only (no markdown):
{{
  "action": "APPROVE" or "DENY",
  "is_first_line": true or false,
  "confidence": 0-100,
  "reasoning": "One concise sentence explaining the decision. If approving due to prior treatment history, state that explicitly."
}}"""

    ai = _call_claude(prompt)
    decision   = ai.get("action", "DENY").upper()
    confidence = int(ai.get("confidence", 0))
    reasoning  = ai.get("reasoning", "")
    is_fl      = bool(ai.get("is_first_line", decision == "APPROVE"))

    return {
        "decision":        decision,
        "is_first_line":   is_fl,
        "confidence":      confidence,
        "reasoning":       reasoning,
        "source":          "ai",
        "requires_review": True,
        "auto":            False,
    }


# ── Rule 12 — Injection Without Admission ─────────────────────────────────────

_INJECTION_KEYWORDS = [
    "IV ", "I.V.", "INFUSION", "INJECTION", "INJECTABLE",
    "AMPOULE", " AMP ", "AMP.", " IM ", "I.M.", "INJ ", "INJ.",
    "INTRAVENOUS", "INTRAMUSCULAR", "PARENTERAL",
]


def _is_injection_procedure(proc_name: str, proc_class: str = "") -> bool:
    text = f" {proc_name} {proc_class} ".upper()
    return any(kw in text for kw in _INJECTION_KEYWORDS)


def check_injection_without_admission(
    procedure_code: str,
    procedure_name: str,
    procedure_class: str,
    diagnosis_codes: List[str],
    diagnosis_names: Dict[str, str],
) -> Dict:
    """
    Rule 12 — fired for INPATIENT + NOT_ADMITTED when an IV/IM procedure is prescribed.
    AI assesses whether oral alternatives should have been tried first.
    Never auto-denies. Always PENDING_REVIEW if triggered.
    """
    if not _is_injection_procedure(procedure_name, procedure_class):
        return {"triggered": False}
    if not diagnosis_codes:
        return {"triggered": False}

    diag_list = ", ".join(
        f"{c} ({diagnosis_names.get(c, c)})" for c in diagnosis_codes
    )
    prompt = f"""You are a clinical reviewer for a Nigerian HMO cost-control unit.

A provider has prescribed a parenteral (injection/infusion) medication for a patient who is NOT admitted.

Procedure: {procedure_code} — {procedure_name}
Diagnoses: {diag_list}

Assess strictly:
1. Is there an oral/tablet equivalent that should normally be tried first in Nigerian HMO practice?
2. Is any of these diagnoses critical enough to justify skipping oral treatment entirely?
   (Examples of critical: severe malaria with vomiting, sepsis, status epilepticus, acute severe asthma, cerebral malaria)
3. Overall: is the direct parenteral route clinically justified for a non-admitted patient?

Respond in JSON only (no markdown):
{{
  "oral_alternative_exists": true or false,
  "diagnosis_critical": true or false,
  "justified": true or false,
  "confidence": 0-100,
  "reasoning": "One concise sentence."
}}"""

    ai = _call_claude(prompt)
    return {
        "triggered": True,
        "justified": bool(ai.get("justified", False)),
        "confidence": int(ai.get("confidence", 0)),
        "reasoning": ai.get("reasoning", ""),
        "oral_alternative_exists": bool(ai.get("oral_alternative_exists", True)),
        "diagnosis_critical": bool(ai.get("diagnosis_critical", False)),
    }


# ── Rule-result serialiser ────────────────────────────────────────────────────

def _r(rule) -> Dict:
    return {
        "rule_name":  rule.rule_name,
        "passed":     rule.passed,
        "source":     rule.source,
        "confidence": rule.confidence,
        "reasoning":  rule.reasoning,
        "details":    rule.details or {},
    }


# ── Per-procedure validation with multi-diagnosis merge ───────────────────────

def _validate_one_procedure(
    engine,
    proc_code: str,
    diag_codes: List[str],
    diag_names: Dict[str, str],
    enrollee_id: str,
    encounter_date: str,
    encounter_type: str,
    all_proc_codes: List[str],
    tariff_price: Optional[float] = None,
    provider_price: Optional[float] = None,
    comment: Optional[str] = None,
    quantity: int = 1,
    proc_name_hint: str = "",
    admission_status: str = "NOT_ADMITTED",
) -> Dict:
    """
    Run comprehensive validation for every (procedure, diagnosis) pair,
    then apply merge logic across diagnoses.
    """
    # Use UI-provided name first (already resolved from dropdown), fall back to DB lookup
    if proc_name_hint:
        proc_name = proc_name_hint
    else:
        proc_info = engine._resolve_procedure_info(proc_code)
        proc_name = proc_info.get("name", proc_code) if proc_info else proc_code

    proc_master = mongo_db.get_procedure_master(proc_code)
    proc_class  = (proc_master or {}).get("procedure_class", "")
    proc_branch = (proc_master or {}).get("branch", "PRE-AUTH").upper() if proc_master else None

    # ── Branch enforcement ────────────────────────────────────────────────────
    # NO-AUTH branch: blocked on INPATIENT (unless ADMITTED, which allows all)
    # PRE-AUTH/unknown branch: blocked on OUTPATIENT
    branch_violation = None
    if proc_branch == "NO-AUTH" and encounter_type == "INPATIENT" and admission_status != "ADMITTED":
        branch_violation = (
            f"Procedure {proc_code} is a No-Auth (outpatient) procedure and cannot be "
            f"submitted on a Pre-Auth encounter without admission."
        )
    elif encounter_type == "OUTPATIENT" and proc_branch is not None and proc_branch != "NO-AUTH":
        branch_violation = (
            f"Procedure {proc_code} is a Pre-Auth-only procedure and cannot be "
            f"submitted on a No-Auth (outpatient) encounter."
        )

    if branch_violation:
        return {
            "procedure_code":      proc_code,
            "procedure_name":      proc_name,
            "decision":            "DENY",
            "approved_diagnoses":  [],
            "denied_diagnoses":    list(diag_codes),
            "diag_detail":         {c: {"name": diag_names.get(c, c), "passed": False, "rules": []} for c in diag_codes},
            "diag_names":          diag_names,
            "proc_rules":          [],
            "first_line":          {},
            "review_reasons":      [branch_violation],
            "requires_review":     False,
            "quantity":            quantity,
            "adjusted_qty":        quantity,
            "qty_adjusted":        False,
            "injection_check":     {"triggered": False},
        }

    # Run comprehensive validation for each (proc, diag) pair
    per_diag: Dict[str, object] = {}
    for diag_code in diag_codes:
        try:
            validation = engine.validate_comprehensive(
                procedure_code=proc_code,
                diagnosis_code=diag_code,
                enrollee_id=enrollee_id,
                encounter_date=encounter_date,
                all_request_procedures=all_proc_codes,
                encounter_type=encounter_type,
            )
            per_diag[diag_code] = validation
        except Exception as e:
            logger.error(f"validate_comprehensive({proc_code}, {diag_code}): {e}")
            per_diag[diag_code] = None

    # ── Separate procedure-level rules (take from first successful run) ────────
    first_valid = next((v for v in per_diag.values() if v), None)
    proc_rules  = []
    if first_valid:
        proc_rules = [r for r in first_valid.rule_results if r.rule_name in _PROC_RULES]

    proc_level_passed  = all(r.passed for r in proc_rules) if proc_rules else True
    proc_level_has_ai  = any(r.source == "ai" for r in proc_rules)
    proc_level_trusted = not any(
        r.source == "learning_table" and not mongo_db.is_learning_trusted(r.details)
        for r in proc_rules
    )

    # ── Diagnosis-level rules per diagnosis ───────────────────────────────────
    diag_detail: Dict[str, Dict] = {}
    for diag_code in diag_codes:
        v = per_diag.get(diag_code)
        if v:
            d_rules = [r for r in v.rule_results if r.rule_name in _DIAG_RULES]
            d_passed = all(r.passed for r in d_rules) if d_rules else True
            d_ai     = any(r.source == "ai" for r in d_rules)
            diag_detail[diag_code] = {
                "name":   diag_names.get(diag_code, diag_code),
                "passed": d_passed,
                "has_ai": d_ai,
                "rules":  [_r(r) for r in d_rules],
            }
        else:
            diag_detail[diag_code] = {
                "name":   diag_names.get(diag_code, diag_code),
                "passed": False,
                "has_ai": False,
                "rules":  [],
                "error":  "Validation did not run",
            }

    passing_diags = [c for c, d in diag_detail.items() if d["passed"]]
    failing_diags = [c for c, d in diag_detail.items() if not d["passed"]]
    all_failed    = len(passing_diags) == 0
    any_diag_ai   = any(d["has_ai"] for d in diag_detail.values())

    # ── First-line check (run against the first available diagnosis) ──────────
    # Fetch 30-day treatment history so the AI can recognise step-up therapy
    anchor_diag      = diag_codes[0] if diag_codes else ""
    anchor_diag_name = diag_names.get(anchor_diag, anchor_diag)
    prior_treatments: List[Dict] = []
    if enrollee_id and encounter_date:
        try:
            from apis.vetting.thirty_day import ThirtyDayValidator
            _tdv = ThirtyDayValidator(engine.conn)
            prior_treatments = _tdv._get_30_day_procedures(enrollee_id, encounter_date)
        except Exception:
            prior_treatments = []
    first_line = check_first_line_treatment(
        procedure_code=proc_code, procedure_name=proc_name,
        diagnosis_code=anchor_diag, diagnosis_name=anchor_diag_name,
        encounter_type=encounter_type,
        prior_treatments=prior_treatments or None,
    )

    # ── Rule 12 — Injection Without Admission (pre-auth, non-admitted only) ──────
    injection_check: Dict = {"triggered": False}
    if encounter_type == "INPATIENT" and admission_status == "NOT_ADMITTED":
        injection_check = check_injection_without_admission(
            proc_code, proc_name, proc_class, diag_codes, diag_names
        )

    # ── Quantity check ────────────────────────────────────────────────────────
    # Only master table max is authoritative. If no master max, pass through as-is.
    # Quantity cap is a silent auto-correction — never escalates to agent review.
    max_qty      = engine.get_max_quantity(proc_code)
    qty_source   = "master"
    qty_reason   = ""
    adjusted_qty = min(quantity, max_qty) if max_qty is not None else quantity
    qty_adjusted = adjusted_qty < quantity
    if qty_adjusted:
        logger.info(f"QTY CAP {proc_code}: {quantity} → {adjusted_qty} (master max: {max_qty})")

    # ── Price override check ──────────────────────────────────────────────────
    price_override = (
        provider_price is not None
        and tariff_price is not None
        and round(provider_price, 2) != round(tariff_price, 2)
    ) or (provider_price is not None and tariff_price is None)

    # ── Decision logic ────────────────────────────────────────────────────────
    review_reasons: List[str] = []
    requires_review = False

    if not proc_level_passed:
        if proc_level_has_ai or not proc_level_trusted:
            decision = "PENDING_REVIEW"
            requires_review = True
            review_reasons.append("Procedure-level rule failed — AI evaluation needs agent confirmation.")
        else:
            decision = "DENY"

    elif all_failed:
        if any_diag_ai:
            decision = "PENDING_REVIEW"
            requires_review = True
            review_reasons.append("All diagnoses failed diagnosis-level rules — AI evaluation needs agent confirmation.")
        else:
            decision = "DENY"

    elif first_line["decision"] == "DENY":
        # First-line check never auto-decides — always escalates to agent
        decision = "PENDING_REVIEW"
        requires_review = True
        review_reasons.append(
            f"First-line check: {first_line['reasoning']} "
            f"(AI recommendation — agent confirmation required)."
        )

    elif proc_level_has_ai or any_diag_ai or not proc_level_trusted:
        decision = "PENDING_REVIEW"
        requires_review = True
        review_reasons.append("AI was involved in at least one rule — agent review required before auto-decision.")

    else:
        decision = "APPROVE"

    # Price override always forces review (even if everything else auto-approved)
    if price_override and decision != "DENY":
        if tariff_price is not None:
            diff_pct = abs(provider_price - tariff_price) / tariff_price * 100
            review_reasons.append(
                f"Provider price (₦{provider_price:,.2f}) differs from tariff "
                f"(₦{tariff_price:,.2f}) by {diff_pct:.1f}% — manual price review required."
            )
        else:
            review_reasons.append(
                f"Provider submitted own price ₦{provider_price:,.2f} — "
                f"no contracted tariff found. Manual price review required."
            )
        decision = "PENDING_REVIEW"
        requires_review = True

    # Rule 12 — injection advisory (non-admitted pre-auth)
    if injection_check.get("triggered"):
        requires_review = True
        if not injection_check.get("justified"):
            review_reasons.append(
                f"Injection-Without-Admission: oral alternative not documented — "
                f"agent to verify. ({injection_check.get('reasoning', '')})"
            )
        else:
            review_reasons.append(
                f"Injection-Without-Admission: diagnosis justifies direct parenteral route. "
                f"({injection_check.get('reasoning', '')})"
            )
        if decision != "DENY":
            decision = "PENDING_REVIEW"

    # Pre-Auth: force all non-DENY to PENDING_REVIEW — AI advises only, agent decides
    if encounter_type == "INPATIENT" and decision != "DENY":
        decision = "PENDING_REVIEW"
        requires_review = True
        if "Pre-Auth: agent decision required." not in review_reasons:
            review_reasons.append("Pre-Auth: agent decision required.")

    approved_diagnoses = passing_diags if not all_failed else []
    denied_diagnoses   = failing_diags

    # Store a review record if needed (for agent queue)
    review_id = None
    if requires_review:
        review_id = str(uuid.uuid4())[:16]
        # Build per-diagnosis compatibility map so the agent's "Agree/Override"
        # can write the AI's actual rule results — not the blanket request outcome.
        diag_compatibility = {}
        for dc, dd in diag_detail.items():
            compat_rule = next(
                (r for r in dd.get("rules", []) if r["rule_name"] == "PROC_DIAG_COMPATIBILITY"),
                None
            )
            if compat_rule is not None:
                diag_compatibility[dc] = {
                    "compatible":  compat_rule["passed"],
                    "confidence":  compat_rule["confidence"],
                    "reasoning":   compat_rule["reasoning"],
                    "source":      compat_rule["source"],
                }

        mongo_db.insert_klaire_review({
            "review_id":           review_id,
            "review_type":         "PA_PREAUTH" if encounter_type == "INPATIENT" else "PA_OUTPATIENT",
            "enrollee_id":         enrollee_id,
            "encounter_date":      encounter_date,
            "procedure_code":      proc_code,
            "procedure_name":      proc_name,
            "decision":            decision,
            "ai_recommendation":   decision,
            "approved_diagnoses":  approved_diagnoses,
            "denied_diagnoses":    denied_diagnoses,
            "diag_names":          diag_names,
            "diag_compatibility":  diag_compatibility,
            "encounter_type":      encounter_type,
            "quantity":            quantity,
            "adjusted_qty":        adjusted_qty,
            "max_qty":             max_qty,
            "qty_source":          qty_source,
            "qty_reason":          qty_reason,
            "tariff_price":        tariff_price,
            "provider_price":      provider_price,
            "price_override":      price_override,
            "comment":             comment or "",
            "first_line":          first_line,
            "injection_check":     injection_check,
            "review_reasons":      review_reasons,
            "status":              "PENDING_REVIEW",
            "reviewed_by":         None,
            "review_notes":        None,
            "reviewed_at":         None,
            "created_at":          datetime.utcnow().isoformat(),
        })

    return {
        "procedure_code":      proc_code,
        "procedure_name":      proc_name,
        "decision":            decision,
        "approved_diagnoses":  approved_diagnoses,
        "denied_diagnoses":    denied_diagnoses,
        "quantity":            quantity,
        "adjusted_qty":        adjusted_qty,
        "max_qty":             max_qty,
        "qty_adjusted":        qty_adjusted,
        "qty_source":          qty_source,
        "qty_reason":          qty_reason,
        "tariff_price":        tariff_price,
        "provider_price":      provider_price,
        "price_override":      price_override,
        "comment":             comment or "",
        "diag_detail":         diag_detail,
        "proc_rules":          [_r(r) for r in proc_rules],
        "first_line":          first_line,
        "injection_check":     injection_check,
        "requires_agent_review": requires_review,
        "review_reasons":      review_reasons,
        "review_id":           review_id,
    }


# ── Diagnosis-Encounter Mismatch Check ───────────────────────────────────────

def check_diagnosis_encounter_mismatch(
    diagnoses: List[Dict],   # [{"code": str, "name": str}, ...]
    encounter_type: str = "OUTPATIENT",
) -> Dict:
    """
    Flags when a diagnosis that typically requires hospital admission is submitted
    on an outpatient PA request.

    Pattern caught: provider uses a severe/inpatient diagnosis (e.g. sepsis, MI,
    stroke) to justify investigations on a No-Auth outpatient visit — classic
    upcoding.  If the diagnosis is genuinely that severe, the patient should be
    admitted and a Pre-Auth submitted instead.

    Only runs on OUTPATIENT encounters. Always AI. Always requires review if flagged.
    """
    if encounter_type != "OUTPATIENT" or not diagnoses:
        return {"flagged": False, "flagged_diagnoses": [], "reasoning": "", "requires_review": False}

    diag_list = "\n".join(f"- {d['code']}: {d['name']}" for d in diagnoses)

    prompt = f"""You are a Nigerian HMO medical reviewer assessing an OUTPATIENT PA request.

The following diagnoses were submitted on a No-Auth outpatient encounter:

{diag_list}

Your task: identify any diagnosis that is typically a HOSPITAL ADMISSION condition — i.e., a condition that in standard Nigerian clinical practice would require the patient to be admitted rather than managed outpatient.

Examples of admission-level diagnoses: sepsis, severe malaria with complications, myocardial infarction, stroke, peritonitis, severe pre-eclampsia, meningitis, pulmonary embolism, severe pneumonia (requiring IV antibiotics or O2), diabetic ketoacidosis, severe anaemia requiring transfusion.

For each diagnosis, respond whether it is:
- "outpatient_ok": routinely managed outpatient
- "inpatient_required": typically requires admission

Flag ONLY diagnoses that are clearly inpatient-level. Do not flag conditions that can sometimes be severe but are routinely managed outpatient (e.g. uncomplicated malaria, hypertension, mild pneumonia).

Respond in JSON only (no markdown):
{{
  "flagged": true or false,
  "flagged_diagnoses": [
    {{
      "code": "diagnosis code",
      "name": "diagnosis name",
      "reason": "one sentence — why this is an admission-level diagnosis used on an outpatient request"
    }}
  ],
  "reasoning": "One sentence summary of the overall concern, or empty string if nothing flagged."
}}"""

    ai = _call_claude(prompt)
    flagged        = bool(ai.get("flagged", False))
    flagged_diags  = ai.get("flagged_diagnoses", [])
    reasoning      = ai.get("reasoning", "")

    return {
        "flagged":            flagged,
        "flagged_diagnoses":  flagged_diags,
        "reasoning":          reasoning,
        "requires_review":    flagged,
    }


# ── Disease Combination Check (request-level) ─────────────────────────────────

def check_disease_combination(
    diagnoses: List[Dict],   # [{"code": str, "name": str}, ...]
    encounter_type: str = "OUTPATIENT",
) -> Dict:
    """
    Given all unique diagnoses across the entire PA request, evaluate whether
    a single patient can plausibly present with all these conditions simultaneously.

    Never learns. Always AI. Always requires agent review if flagged.

    Returns:
        plausible: bool
        confidence: int
        reasoning: str
        flagged_pairs: list of (code_a, code_b, reason) that are implausible together
        requires_review: bool
    """
    if len(diagnoses) < 2:
        return {"plausible": True, "confidence": 100,
                "reasoning": "Only one diagnosis — no combination to check.",
                "flagged_pairs": [], "requires_review": False}

    diag_list = "\n".join(f"- {d['code']}: {d['name']}" for d in diagnoses)
    context = "INPATIENT admission" if encounter_type == "INPATIENT" else "outpatient visit"

    prompt = f"""You are a senior medical reviewer for a Nigerian HMO ({context}).

The following diagnoses have all been submitted together in a single PA request for one patient:

{diag_list}

Your task: determine whether it is clinically plausible for a SINGLE patient to present with ALL of these conditions simultaneously in one encounter.

RULES:
- PLAUSIBLE if all conditions can reasonably co-exist in one patient (e.g. hypertension + diabetes + URTI is normal).
- NOT PLAUSIBLE if any pair of diagnoses is mutually exclusive, contradictory, or highly implausible together (e.g. pregnancy + male-only condition, or two contradictory diagnoses).
- NOT PLAUSIBLE if the combination strongly suggests diagnosis inflation or upcoding.
- When in doubt, mark PLAUSIBLE — only flag clear contradictions.

Respond in JSON only (no markdown):
{{
  "plausible": true or false,
  "confidence": 0-100,
  "reasoning": "One concise sentence.",
  "flagged_pairs": [
    {{"code_a": "...", "code_b": "...", "reason": "why these two are implausible together"}}
  ]
}}"""

    ai = _call_claude(prompt)
    plausible   = bool(ai.get("plausible", True))
    confidence  = int(ai.get("confidence", 0))
    reasoning   = ai.get("reasoning", "")
    flagged     = ai.get("flagged_pairs", [])

    return {
        "plausible":       plausible,
        "confidence":      confidence,
        "reasoning":       reasoning,
        "flagged_pairs":   flagged,
        "requires_review": not plausible,
    }


# ── Procedure Combination Necessity Check (request-level) ────────────────────

def check_procedure_combination(
    procedures: List[Dict],   # [{"code": str, "name": str, "diagnoses": [{"code","name"}]}]
    encounter_type: str = "OUTPATIENT",
) -> Dict:
    """
    Given all procedures in the PA request (after individual rule checks), evaluate
    whether the COMBINATION is medically necessary — i.e. no redundant treatments,
    no duplicate drug classes, no tests that add nothing given what else is prescribed.

    Classic flags:
    - Two antibiotics for the same infection (unless dual-therapy is protocol)
    - Two antipyretics / two NSAIDs
    - A diagnostic test that is unnecessary given another test already covers it
    - Two drugs in the same pharmacological class treating the same condition

    Never learns. Always AI. Always requires agent review if flagged.

    Returns:
        necessary: bool
        confidence: int
        reasoning: str
        flagged_items: list of {code_a, code_b, reason}
        requires_review: bool
    """
    if len(procedures) < 2:
        return {"necessary": True, "confidence": 100,
                "reasoning": "Only one procedure — no combination to assess.",
                "flagged_items": [], "requires_review": False}

    proc_lines = []
    for p in procedures:
        diag_str = ", ".join(f"{d['code']} ({d['name']})" for d in p.get("diagnoses", []))
        proc_lines.append(f"- {p['code']}: {p['name']}" + (f"  [for: {diag_str}]" if diag_str else ""))
    proc_list = "\n".join(proc_lines)
    context = "INPATIENT admission" if encounter_type == "INPATIENT" else "outpatient visit"

    prompt = f"""You are a senior HMO medical reviewer in Nigeria ({context}).

The following procedures/medications have been requested together in a single PA for one patient:

{proc_list}

Your task: assess whether the COMBINATION of these procedures is collectively medically necessary, or whether any items are redundant, duplicative, or unjustified given what else is already being prescribed.

STRICT RULES:
- NECESSARY: all items serve distinct clinical purposes and are each justified.
- NOT NECESSARY: flag if two items treat the same condition via the same mechanism (e.g. two antibiotics covering the same spectrum for the same infection without a dual-therapy indication; two antipyretics; two NSAIDs; two antihypertensives of the same class for an outpatient).
- NOT NECESSARY: flag a diagnostic test that is clinically redundant given another test already requested.
- Do NOT flag items that treat genuinely different conditions or that are standard combination therapy (e.g. ACT + paracetamol for malaria, HAART combinations, dual antihypertensives for resistant hypertension).
- When in doubt, mark NECESSARY — only flag CLEAR redundancy or unjustified duplication.
- Be strict with antibiotics: two broad-spectrum antibiotics for a simple outpatient infection are almost never justified unless there is a documented synergy or resistant-organism protocol.

Respond in JSON only (no markdown). No text before or after the JSON object:
{{
  "necessary": true or false,
  "confidence": 0-100,
  "reasoning": "One or two concise sentences summarising the overall assessment.",
  "procedure_verdicts": {{
    "<procedure_code>": "keep" or "deny"
  }},
  "flagged_items": [
    {{
      "code_a": "procedure code",
      "name_a": "procedure name",
      "code_b": "procedure code",
      "name_b": "procedure name",
      "reason": "why this pair is redundant or unjustified together"
    }}
  ]
}}

RULES for procedure_verdicts:
- Include EVERY procedure code listed above as a key.
- Value must be exactly "keep" or "deny".
- A procedure that is clinically justified and should be approved = "keep".
- A procedure that is redundant, unjustified, or unnecessary given what else is prescribed = "deny".
- If necessary=true, all verdicts must be "keep".
- Be consistent: if you say a procedure is appropriate in your reasoning, mark it "keep"."""

    ai = _call_claude(prompt)
    necessary  = bool(ai.get("necessary", True))
    confidence = int(ai.get("confidence", 0))
    reasoning  = ai.get("reasoning", "")
    flagged    = ai.get("flagged_items", [])
    verdicts   = ai.get("procedure_verdicts", {})

    # Derive procedures_to_deny from per-procedure verdicts
    procedures_to_deny = [
        code.upper() for code, verdict in verdicts.items()
        if str(verdict).lower() == "deny"
    ]

    return {
        "necessary":          necessary,
        "confidence":         confidence,
        "reasoning":          reasoning,
        "flagged_items":      flagged,
        "procedure_verdicts": verdicts,
        "procedures_to_deny": procedures_to_deny,
        "requires_review":    not necessary,
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def validate_pa_request(
    items: List[Dict],
    enrollee_id: str,
    provider_id: str,
    hospital_name: str,
    encounter_date: str,
    encounter_type: str,
    db_path: str,
    admission_status: str = "NOT_ADMITTED",
) -> Dict:
    """
    Validate a full PA request (multiple procedures, each with 1+ diagnoses).

    items: [
        {
            "procedure_code":  str,
            "diagnosis_codes": [str, ...],
            "diagnosis_names": {code: name, ...},   # optional display names
        },
        ...
    ]
    """
    from .comprehensive import ComprehensiveVettingEngine
    from concurrent.futures import ThreadPoolExecutor, as_completed

    all_proc_codes = [item["procedure_code"].strip().upper() for item in items]

    def _run_item(item):
        # Each thread gets its own engine+DuckDB connection — no shared-state risk
        thread_engine = ComprehensiveVettingEngine(db_path)
        return _validate_one_procedure(
            engine=thread_engine,
            proc_code=item["procedure_code"].strip().upper(),
            proc_name_hint=item.get("procedure_name") or "",
            diag_codes=[d.strip().upper() for d in item.get("diagnosis_codes", [])],
            diag_names=item.get("diagnosis_names", {}),
            enrollee_id=enrollee_id,
            encounter_date=encounter_date,
            encounter_type=encounter_type,
            all_proc_codes=all_proc_codes,
            tariff_price=item.get("tariff_price"),
            provider_price=item.get("provider_price"),
            comment=item.get("comment"),
            quantity=int(item.get("quantity") or 1),
            admission_status=admission_status,
        )

    # Run all procedures in parallel — each thread has its own DuckDB connection
    with ThreadPoolExecutor(max_workers=min(len(items), 6)) as pool:
        futures = {pool.submit(_run_item, item): i for i, item in enumerate(items)}
        results_map = {}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results_map[idx] = future.result()
            except Exception as e:
                logger.error(f"_validate_one_procedure failed for item {idx}: {e}")
                results_map[idx] = {"procedure_code": items[idx].get("procedure_code", ""),
                                    "decision": "PENDING_REVIEW", "error": str(e)}
    results = [results_map[i] for i in range(len(items))]

    # ── Disease Combination Check (request-level) ─────────────────────────────
    # Collect all unique diagnoses across the whole request
    seen_diag_codes: set = set()
    all_diags_for_combo: List[Dict] = []
    for item in items:
        for code in item.get("diagnosis_codes", []):
            code = code.strip().upper()
            if code not in seen_diag_codes:
                seen_diag_codes.add(code)
                name = item.get("diagnosis_names", {}).get(code, code)
                all_diags_for_combo.append({"code": code, "name": name})

    combo_check = check_disease_combination(all_diags_for_combo, encounter_type)
    mismatch_check = check_diagnosis_encounter_mismatch(all_diags_for_combo, encounter_type)

    # ── Procedure Combination Necessity Check (request-level) ─────────────────
    # Build procedure list with their approved diagnoses for the AI to reason over
    proc_combo_input: List[Dict] = []
    seen_proc_codes: set = set()
    for item, res in zip(items, results):
        pc = item["procedure_code"].strip().upper()
        if pc in seen_proc_codes:
            continue
        seen_proc_codes.add(pc)
        # Use all submitted diagnosis codes (not just approved) so combo AI has full context
        diag_names_map = item.get("diagnosis_names", {})
        all_diag_codes = item.get("diagnosis_codes", [])
        proc_combo_input.append({
            "code": pc,
            "name": res.get("procedure_name", pc),
            "individual_decision": res.get("decision", "APPROVE"),
            "diagnoses": [
                {"code": dc, "name": diag_names_map.get(dc, dc)}
                for dc in all_diag_codes
            ],
        })

    proc_combo_check = check_procedure_combination(proc_combo_input, encounter_type)

    # ── Escalate individually flagged procedures to PENDING_REVIEW ────────────
    # If procedure combo check calls out specific procedures, those individual
    # results must also become PENDING_REVIEW — not stay APPROVE with only a banner.
    # procedures_to_deny is derived from per-procedure AI verdicts ("deny"/"keep").
    # Disease combo flags DIAGNOSES only — banner warning, never denies procedures.
    flagged_proc_codes: set = set()
    for code in proc_combo_check.get("procedures_to_deny", []):
        if code:
            flagged_proc_codes.add(str(code).strip().upper())

    def _escalate_to_review(res, escalated_by: str, flag_reason: str, ai_recommendation: str):
        """Mutate a result dict: mark PENDING_REVIEW and write a review doc to Mongo."""
        res["decision"] = "PENDING_REVIEW"
        res["requires_agent_review"] = True
        res["ai_recommendation"] = ai_recommendation
        res["escalated_by"] = escalated_by
        res["combo_flag_reason"] = flag_reason
        res.setdefault("review_reasons", []).append(flag_reason)
        if not res.get("review_id"):
            review_id = str(uuid.uuid4())[:16]
            res["review_id"] = review_id
            mongo_db.insert_klaire_review({
                "review_id":          review_id,
                "review_type":        "PA_PREAUTH" if encounter_type == "INPATIENT" else "PA_OUTPATIENT",
                "enrollee_id":        enrollee_id,
                "encounter_date":     encounter_date,
                "procedure_code":     res.get("procedure_code"),
                "procedure_name":     res.get("procedure_name"),
                "decision":           "PENDING_REVIEW",
                "approved_diagnoses": res.get("approved_diagnoses", []),
                "denied_diagnoses":   res.get("denied_diagnoses", []),
                "diag_names":         {},
                "diag_compatibility": {},
                "encounter_type":     encounter_type,
                "quantity":           res.get("quantity", 1),
                "adjusted_qty":       res.get("adjusted_qty", res.get("quantity", 1)),
                "max_qty":            res.get("max_qty"),
                "qty_source":         res.get("qty_source", "master"),
                "qty_reason":         res.get("qty_reason", ""),
                "tariff_price":       res.get("tariff_price"),
                "provider_price":     res.get("provider_price"),
                "price_override":     res.get("price_override", False),
                "comment":            res.get("comment", ""),
                "first_line":         res.get("first_line", {}),
                "review_reasons":     res.get("review_reasons", []),
                "ai_recommendation":  ai_recommendation,
                "escalated_by":       escalated_by,
                "combo_flag_reason":  flag_reason,
                "status":             "PENDING_REVIEW",
                "reviewed_by":        None,
                "review_notes":       None,
                "reviewed_at":        None,
                "created_at":         datetime.utcnow().isoformat(),
            })

    for res in results:
        if res.get("decision") != "APPROVE":
            continue
        pc = res.get("procedure_code", "").upper()
        # Escalate only if procedure was explicitly identified as redundant by the
        # procedure combination check. Disease combo flags stay as banners only.
        if pc in flagged_proc_codes:
            _escalate_to_review(
                res,
                escalated_by="combo_check",
                flag_reason="Flagged by procedure combination necessity check — AI recommends denying this procedure as redundant given the other items in this request.",
                ai_recommendation="DENY",
            )

    decisions = [r["decision"] for r in results]
    if all(d == "APPROVE" for d in decisions):
        overall = "APPROVE"
    elif all(d == "DENY" for d in decisions):
        overall = "DENY"
    elif any(d == "PENDING_REVIEW" for d in decisions):
        overall = "PENDING_REVIEW"
    else:
        overall = "PARTIAL"

    # Combo checks failure escalates overall to PENDING_REVIEW
    if combo_check["requires_review"] and overall not in ("DENY", "PENDING_REVIEW"):
        overall = "PENDING_REVIEW"
    if proc_combo_check["requires_review"] and overall not in ("DENY", "PENDING_REVIEW"):
        overall = "PENDING_REVIEW"
    if mismatch_check["requires_review"] and overall not in ("DENY", "PENDING_REVIEW"):
        overall = "PENDING_REVIEW"

    return {
        "overall_decision":        overall,
        "encounter_type":          encounter_type,
        "enrollee_id":             enrollee_id,
        "provider_id":             provider_id,
        "hospital_name":           hospital_name,
        "encounter_date":          encounter_date,
        "items":                   results,
        "disease_combination":     combo_check,
        "procedure_combination":   proc_combo_check,
        "diagnosis_encounter_mismatch": mismatch_check,
        "requires_agent_review":   any(r.get("requires_agent_review") for r in results)
                                   or combo_check["requires_review"]
                                   or proc_combo_check["requires_review"]
                                   or mismatch_check["requires_review"],
    }
