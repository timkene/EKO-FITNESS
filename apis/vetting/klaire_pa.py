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
  3. Diagnosis Age                        ← per-diagnosis, merge applied
  4. Diagnosis Gender                     ← per-diagnosis, merge applied
  5. Procedure-Diagnosis Compatibility    ← per-diagnosis, merge applied
  6. Procedure 30-Day Duplicate
  7. Clinical Necessity (AI)
  8. Diagnosis Stacking
  9. Clinical Necessity (ClinicalNecessityEngine — admission auto-detect, route, step-therapy, tests)
     ↑ SHORT-CIRCUIT: skipped if rules 1-8 already denied (saves AI tokens)
 10. Disease Combination Check (AI — request-level, no learning)
 11. Diagnosis-Encounter Mismatch (AI — upcoding detection, outpatient only)
 12. Injection Without Admission (pre-auth, non-admitted only)
 13. Drug-Drug Interaction check (RxClass pre-filter → OpenFDA + AI — DRG codes only)
 14. Quantity vs Diagnosis Appropriateness (RxNorm class + rules-based + AI fallback)
 15. Post-Discharge Overlap (DB — labs billed within 14 days of discharge)
 16. OpenFDA Indication vs Diagnosis (drug label indications_and_usage — DRG only)

Trust model:
  - Master table / trusted learning table → auto-decide (no agent needed)
  - Untrusted learning table / AI evaluation → PENDING_REVIEW (agent must act)
"""

import os
import json
import logging
import uuid
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from . import mongo_db
from .drug_apis import (
    rxnorm_lookup,
    rxclass_get_drug_classes,
    rxclass_get_may_treat,
    is_high_risk_class,
    who_eml_lookup,
    openfda_get_indications,
    openfda_get_label,
)

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
    session_basket: Optional[List[Dict]] = None,
    batch_items: Optional[List[Dict]] = None,
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

    # ── Clinical necessity check ─────────────────────────────────────────────────
    # SHORT-CIRCUIT: skip the expensive AI call if cheap master-table rules already
    # denied this procedure. Clinical necessity only matters if the procedure+diagnosis
    # pairing is otherwise valid.
    anchor_diag      = diag_codes[0] if diag_codes else ""
    anchor_diag_name = diag_names.get(anchor_diag, anchor_diag)
    first_line: Dict = {"decision": "APPROVE", "is_first_line": True, "confidence": 100,
                        "reasoning": "Skipped — prior rule already denied.", "source": "skip",
                        "requires_review": False, "auto": True}

    _skip_necessity = (not proc_level_passed) or all_failed
    if not _skip_necessity and anchor_diag and enrollee_id and encounter_date:
        try:
            from .clinical_necessity import ClinicalNecessityEngine
            _cne    = ClinicalNecessityEngine(conn=engine.conn)

            # Build full regimen context from the current PA batch so the AI
            # knows ALL co-prescribed procedures (e.g. metronidazole alongside
            # amoxicillin = dual therapy, not monotherapy).
            _all_req_procs: List[Dict] = []
            for _bi in (batch_items or []):
                _bpc = (_bi.get("procedure_code") or "").strip().upper()
                _bpn = _bi.get("procedure_name") or _bpc
                _bdiag_codes = _bi.get("diagnosis_codes") or []
                _bdiag_names = _bi.get("diagnosis_names") or {}
                _bdc = _bdiag_codes[0] if _bdiag_codes else ""
                _bdn = _bdiag_names.get(_bdc, _bdc)
                if _bpc:
                    _all_req_procs.append({
                        "procedure_code": _bpc,
                        "procedure_name": _bpn,
                        "diagnosis_code": _bdc,
                        "diagnosis_name": _bdn,
                        "procedure_class": "",
                    })

            _cn_res = _cne.check(
                procedure_code=proc_code,
                procedure_name=proc_name,
                procedure_class=proc_class or "",
                diagnosis_code=anchor_diag,
                diagnosis_name=anchor_diag_name,
                enrollee_id=enrollee_id,
                encounter_date=encounter_date,
                session_basket=session_basket,
                all_request_procedures=_all_req_procs or None,
            )
            first_line = {
                "decision":         "APPROVE" if _cn_res.passed else "DENY",
                "is_first_line":    _cn_res.passed,
                "confidence":       _cn_res.confidence,
                "reasoning":        _cn_res.reasoning,
                "source":           _cn_res.source,
                "requires_review":  True,
                "auto":             False,
                "route":            _cn_res.route,
                "route_appropriate": _cn_res.route_appropriate,
                "step_down":        _cn_res.step_down_applicable,
                "severity":         _cn_res.severity,
                "concerns":         _cn_res.concerns,
            }
        except Exception as _e:
            logger.warning(f"ClinicalNecessityEngine failed for {proc_code}: {_e}, falling back to simple check")
            first_line = check_first_line_treatment(
                procedure_code=proc_code, procedure_name=proc_name,
                diagnosis_code=anchor_diag, diagnosis_name=anchor_diag_name,
                encounter_type=encounter_type,
            )
    elif not _skip_necessity:
        # anchor_diag missing — fall back to simple check
        first_line = check_first_line_treatment(
            procedure_code=proc_code, procedure_name=proc_name,
            diagnosis_code=anchor_diag, diagnosis_name=anchor_diag_name,
            encounter_type=encounter_type,
        )
    # else: _skip_necessity=True → first_line already set to skip sentinel above

    # ── Rule 12 — Injection Without Admission (pre-auth, non-admitted only) ──────
    # Also fires when ClinicalNecessityEngine detected the route is injectable
    # but the engine's admission check found no active ADM code in PA DATA.
    injection_check: Dict = {"triggered": False}
    _route_is_injectable = (
        first_line.get("route", "") == "INJECTABLE"
        or _is_injection_procedure(proc_name, proc_class or "")
    )
    _admitted_per_engine = (
        first_line.get("route_appropriate", True)  # engine approves injectable → admitted or justified
        and first_line.get("route", "") == "INJECTABLE"
    )
    _trigger_rule12 = (
        encounter_type == "INPATIENT"
        and admission_status == "NOT_ADMITTED"
        and _route_is_injectable
    )
    if _trigger_rule12 and not _admitted_per_engine:
        injection_check = check_injection_without_admission(
            proc_code, proc_name, proc_class, diag_codes, diag_names
        )

    # ── Quantity cap (master table max — silent correction) ───────────────────
    max_qty      = engine.get_max_quantity(proc_code)
    qty_source   = "master"
    qty_reason   = ""
    adjusted_qty = min(quantity, max_qty) if max_qty is not None else quantity
    qty_adjusted = adjusted_qty < quantity
    if qty_adjusted:
        logger.info(f"QTY CAP {proc_code}: {quantity} → {adjusted_qty} (master max: {max_qty})")

    # ── Rule 14 — Quantity vs diagnosis clinical appropriateness ──────────────
    # Checks if the quantity submitted is clinically reasonable for the diagnosis.
    # Fast rules-based for known drug classes; AI fallback for unknowns.
    qty_appropriateness: Dict = {"flagged": False}
    if adjusted_qty > 1 and anchor_diag:
        qty_appropriateness = check_quantity_appropriateness(
            proc_code=proc_code,
            proc_name=proc_name,
            proc_class=proc_class or "",
            diagnosis_code=anchor_diag,
            diagnosis_name=anchor_diag_name,
            quantity=adjusted_qty,
            encounter_type=encounter_type,
        )

    # ── Rule 13 — Drug-Drug Interaction check (OpenFDA + AI) ─────────────────
    # Only runs for drug procedures (DRG prefix) that haven't already been denied.
    ddi_check: Dict = {"triggered": False}
    if proc_code.upper().startswith("DRG") and enrollee_id and encounter_date:
        ddi_check = check_drug_drug_interactions(
            proc_code=proc_code,
            proc_name=proc_name,
            enrollee_id=enrollee_id,
            encounter_date=encounter_date,
            conn=engine.conn,
            session_basket=session_basket,
        )

    # ── Rule 16 — OpenFDA indication vs diagnosis ─────────────────────────────
    # Checks if FDA label indications_and_usage matches the submitted diagnosis.
    # Only for DRG codes; uses Haiku (cheap). Flags off-label use for review.
    indication_check: Dict = {"triggered": False}
    if proc_code.upper().startswith("DRG") and anchor_diag:
        indication_check = check_openfda_indication(
            proc_code=proc_code,
            proc_name=proc_name,
            diagnosis_code=anchor_diag,
            diagnosis_name=anchor_diag_name,
        )

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

    # Rule 13 — DDI advisory
    if ddi_check.get("triggered"):
        requires_review = True
        combos = "; ".join(
            f"{c['prior_drug']} ({c['reason']})"
            for c in ddi_check.get("flagged_combinations", [])
        )
        review_reasons.append(
            f"Drug Interaction [{ddi_check.get('severity','').upper()}]: {proc_name} + prior meds — "
            f"{combos or ddi_check.get('reasoning', '')}"
        )
        if decision != "DENY":
            decision = "PENDING_REVIEW"

    # Rule 14 — quantity clinical appropriateness
    if qty_appropriateness.get("flagged"):
        requires_review = True
        rec = qty_appropriateness.get("recommended_quantity")
        rec_str = f" (recommended max: {rec})" if rec else ""
        review_reasons.append(
            f"Quantity Concern: {qty_appropriateness.get('reasoning', '')}{rec_str}"
        )
        if decision != "DENY":
            decision = "PENDING_REVIEW"

    # Rule 16 — OpenFDA indication off-label advisory
    if indication_check.get("triggered"):
        requires_review = True
        review_reasons.append(
            f"Off-Label Use: {proc_name} — {indication_check.get('reasoning', 'may not be indicated for this diagnosis')} "
            f"(FDA label check, confidence {indication_check.get('confidence', 0)}%)"
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
            "ddi_check":           ddi_check,
            "qty_appropriateness": qty_appropriateness,
            "indication_check":    indication_check,
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
        "ddi_check":           ddi_check,
        "qty_appropriateness": qty_appropriateness,
        "indication_check":    indication_check,
        "requires_agent_review": requires_review,
        "review_reasons":      review_reasons,
        "review_id":           review_id,
    }


# ── Drug-Drug Interaction Check (OpenFDA + AI) ───────────────────────────────


def check_drug_drug_interactions(
    proc_code: str,
    proc_name: str,
    enrollee_id: str,
    encounter_date: str,
    conn,
    session_basket: Optional[List[Dict]] = None,
) -> Dict:
    """
    Rule 13 — Drug-Drug Interaction check.

    Only runs for drug procedures (DRG prefix).
    1. Fetches 30-day prior medications from PA DATA.
    2. RxClass pre-filter: only proceed if current drug OR any prior med is in a
       high-risk interaction class (anticoagulants, NSAIDs, antifungals, etc.).
       This avoids calling OpenFDA for safe low-risk combinations.
    3. Queries OpenFDA for the current drug's known interaction warnings.
    4. If interaction text found, asks Claude whether any prior med is dangerous.

    Never auto-denies. Flags PENDING_REVIEW + advisory if a dangerous combination found.
    """
    if not proc_code.upper().startswith("DRG") or not enrollee_id or not encounter_date:
        return {"triggered": False}

    # Fetch 30-day prior meds
    prior_meds: List[Dict] = []
    try:
        enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
        lookback = (enc_dt - timedelta(days=30)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT UPPER(TRIM(p.code)) as code,
                   COALESCE(TRIM(pd.proceduredesc), UPPER(TRIM(p.code))) as name,
                   CAST(p.requestdate AS DATE) as med_date
            FROM "AI DRIVEN DATA"."PA DATA" p
            LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd
                ON LOWER(TRIM(pd.procedurecode)) = LOWER(TRIM(p.code))
            WHERE p.IID = ?
              AND UPPER(LEFT(TRIM(p.code), 3)) = 'DRG'
              AND CAST(p.requestdate AS DATE) >= ?
              AND CAST(p.requestdate AS DATE) <= ?
        """, [enrollee_id, lookback, encounter_date]).fetchall()
        prior_meds = [{"code": r[0], "name": r[1], "date": str(r[2])} for r in rows]
    except Exception as e:
        logger.warning(f"DDI: prior meds fetch failed: {e}")

    # Merge session basket DRG items — approved this session but not yet in live DB
    if session_basket:
        existing_codes = {m["code"].upper() for m in prior_meds}
        for bitem in session_basket:
            bcode = bitem.get("procedure_code", "").upper()
            if bcode.startswith("DRG") and bcode not in existing_codes:
                prior_meds.append({
                    "code": bcode,
                    "name": bitem.get("procedure_name", bcode),
                    "date": encounter_date,
                })
                existing_codes.add(bcode)

    if not prior_meds:
        return {"triggered": False}

    # RxClass pre-filter: skip OpenFDA + AI unless at least one drug is high-risk
    current_high_risk = is_high_risk_class(proc_name)
    prior_high_risk   = any(is_high_risk_class(m["name"]) for m in prior_meds)
    if not current_high_risk and not prior_high_risk:
        return {
            "triggered": False,
            "note":      "RxClass pre-filter: no high-risk drug classes detected — DDI check skipped",
        }

    # Get full drug label from OpenFDA (single call — interactions + contraindications)
    label = openfda_get_label(proc_name)
    interaction_text   = label["drug_interactions"]
    contraindications  = label["contraindications"]
    if not interaction_text and not contraindications:
        return {"triggered": False, "note": "No OpenFDA label data found"}

    # Ask Claude to match prior meds against interaction warnings and contraindications
    prior_list = "\n".join(f"  - {m['code']}: {m['name']} (on {m['date']})" for m in prior_meds[-20:])

    contra_section = (
        f"\nFDA label contraindications:\n{contraindications[:800]}"
        if contraindications else ""
    )

    prompt = f"""You are a clinical pharmacist reviewing a Nigerian HMO PA request for drug safety.

The patient is being prescribed:
  {proc_code}: {proc_name}

FDA label drug interactions section:
{interaction_text[:1200]}
{contra_section}

The patient's prior medications (last 30 days from PA records):
{prior_list}

Task: identify if any prior medication is listed as a dangerous interaction with the requested drug,
OR if any prior medication / active condition is listed in the contraindications.
Only flag CLINICALLY SIGNIFICANT interactions (major/severe — not theoretical or minor).
Ignore interactions with drugs not present in the prior medication list.

Respond in JSON only (no markdown):
{{
  "interaction_found": true or false,
  "flagged_combinations": [
    {{
      "prior_drug": "name",
      "reason": "one sentence — why this combination is dangerous"
    }}
  ],
  "severity": "major" or "moderate" or "minor" or "none",
  "reasoning": "One sentence summary."
}}"""

    ai = _call_claude(prompt)
    found    = bool(ai.get("interaction_found", False))
    flagged  = ai.get("flagged_combinations", [])
    severity = ai.get("severity", "none")
    reasoning = ai.get("reasoning", "")

    return {
        "triggered":           found,
        "flagged_combinations": flagged,
        "severity":            severity,
        "reasoning":           reasoning,
        "requires_review":     found,
        "source":              "openfda+ai",
    }


# ── Quantity vs Diagnosis Clinical Appropriateness ────────────────────────────

# Fast rules-based checks: (proc_name_keyword, max_outpatient_days, label)
_QTY_RULES = [
    # Antibiotics — outpatient courses should be 5-14 days max
    (["CIPROFLOXACIN", "CIPROFLOX"],                    10,  "Ciprofloxacin courses are typically 5–7 days for uncomplicated UTI"),
    (["METRONIDAZOLE", "METRONID"],                     10,  "Metronidazole courses are typically 5–7 days"),
    (["AMOXICILLIN", "AMOXYCLAV", "AUGMENTIN"],         14,  "Amoxicillin/Augmentin courses are typically 5–10 days"),
    (["CEFUROXIME", "CEFIXIME", "CEFALEXIN", "CEPHALEXIN"], 14, "Oral cephalosporins are typically 7–14 day courses"),
    (["AZITHROMYCIN", "ZITHROMAX"],                      5,  "Azithromycin is typically a 3–5 day course"),
    (["DOXYCYCLINE"],                                   14,  "Doxycycline for most infections is 7–14 days"),
    # Steroids — short courses only unless chronic condition
    (["PREDNISOLONE", "PREDNISONE"],                    14,  "Prednisolone outpatient courses should be ≤14 days without specialist oversight"),
    (["DEXAMETHASONE"],                                  7,  "Dexamethasone short courses are typically ≤7 days outpatient"),
    # IV fluids outpatient — physically impossible in large quantities
    (["NORMAL SALINE", "RINGERS", "RINGER'S", "DEXTROSE", "IV FLUID", "HARTMANN"],
                                                         5,  "IV fluid bags in large quantities are only feasible on admission"),
    # Antimalarials
    (["ARTEMETHER", "ARTESUNATE", "COARTEM", "LUMEFANTRINE"], 3, "ACT courses are 3 days"),
]


def check_quantity_appropriateness(
    proc_code: str,
    proc_name: str,
    proc_class: str,
    diagnosis_code: str,
    diagnosis_name: str,
    quantity: int,
    encounter_type: str = "OUTPATIENT",
) -> Dict:
    """
    Rule 14 — Quantity vs diagnosis clinical appropriateness.

    Fast rules-based check first (no AI cost). AI fallback for unknown procedures.
    Only flags quantities that are clinically implausible — never the master max cap.
    Never auto-denies. Always PENDING_REVIEW if flagged.
    """
    if quantity <= 1:
        return {"flagged": False}

    name_upper = proc_name.upper()

    # RxNorm class enrichment: use authoritative class for drugs not matched by keyword
    rx_class: Optional[str] = None
    if proc_code.upper().startswith("DRG"):
        rx = rxnorm_lookup(proc_name)
        if rx:
            rx_class = (rx.get("therapeutic_class") or "").lower()

    # Fast rules-based pass (keyword matching)
    for keywords, max_days, label in _QTY_RULES:
        if any(kw in name_upper for kw in keywords):
            if encounter_type == "OUTPATIENT" and quantity > max_days:
                return {
                    "flagged":              True,
                    "recommended_quantity": max_days,
                    "reasoning":            f"{label} — {quantity} units submitted exceeds typical course length.",
                    "source":               "rules",
                    "requires_review":      True,
                }
            return {"flagged": False, "source": "rules"}  # known drug, quantity within range

    # RxNorm class-based check for drugs not matched by keyword
    if rx_class and encounter_type == "OUTPATIENT":
        if any(kw in rx_class for kw in ("antibiotic", "antibacterial", "antimicrobial")) and quantity > 14:
            return {
                "flagged":              True,
                "recommended_quantity": 14,
                "reasoning":            f"Antibiotic course ({rx_class}) — {quantity} units likely exceeds standard treatment duration.",
                "source":               "rxnorm",
                "requires_review":      True,
            }
        if any(kw in rx_class for kw in ("antimalarial", "antiparasitic", "artemisinin")) and quantity > 3:
            return {
                "flagged":              True,
                "recommended_quantity": 3,
                "reasoning":            f"Antimalarial ({rx_class}) — standard ACT course is 3 days; {quantity} units submitted.",
                "source":               "rxnorm",
                "requires_review":      True,
            }
        if any(kw in rx_class for kw in ("corticosteroid", "glucocorticoid")) and quantity > 14:
            return {
                "flagged":              True,
                "recommended_quantity": 14,
                "reasoning":            f"Corticosteroid ({rx_class}) — outpatient courses >14 days require specialist oversight.",
                "source":               "rxnorm",
                "requires_review":      True,
            }

    # AI check for drug procedures not covered by fast rules or RxNorm class
    if not proc_code.upper().startswith("DRG") or quantity < 10:
        return {"flagged": False}

    prompt = f"""You are a clinical pharmacist reviewing a Nigerian HMO PA request.

Procedure: {proc_code} — {proc_name} (class: {proc_class or 'unknown'})
Diagnosis:  {diagnosis_code} — {diagnosis_name}
Quantity submitted: {quantity} unit(s)
Encounter type: {encounter_type}

Is this quantity clinically appropriate for the standard treatment course for this diagnosis in Nigerian HMO practice?

Flag ONLY if the quantity is clearly excessive (more than 1.5× the maximum standard course).
Do NOT flag for: chronic conditions needing long-term medication, specialty drugs, imaging, procedures.

Respond in JSON only (no markdown):
{{
  "flagged": true or false,
  "recommended_max_quantity": integer or null,
  "reasoning": "one sentence"
}}"""

    ai = _call_claude(prompt)
    flagged = bool(ai.get("flagged", False))
    return {
        "flagged":              flagged,
        "recommended_quantity": ai.get("recommended_max_quantity"),
        "reasoning":            ai.get("reasoning", ""),
        "source":               "ai",
        "requires_review":      flagged,
    }


# ── Post-Discharge Overlap Check ──────────────────────────────────────────────

_LAB_KEYWORDS = (
    "BLOOD COUNT", "FBC", "CBC", "HAEMATOLOGY",
    "WIDAL", "MALARIA", "PARASITE", "MP TEST",
    "URINALYSIS", "URINE M/C", "URINE MCS",
    "GLUCOSE", "RBS", "FBS", "FASTING BLOOD",
    "CULTURE", "SENSITIVITY", "MCS",
    "LIVER FUNCTION", "RENAL FUNCTION", "KIDNEY FUNCTION",
    "ELECTROLYTE", "E/U/CR", "UREA", "CREATININE",
    "THYROID", "TSH", "T3", "T4",
    "HEPATITIS", "HIV", "VIRAL LOAD",
    "X-RAY", "XRAY", "CHEST X", "ULTRA SOUND", "ULTRASOUND", "SCAN",
    "ECG", "ELECTROCARDIOGRAM",
    "PCV", "PACKED CELL", "WBC",
)


def check_post_discharge_overlap(
    enrollee_id: str,
    encounter_date: str,
    proc_code: str,
    proc_name: str,
    conn,
    lookback_days: int = 14,
) -> Dict:
    """
    Rule 15 — Post-discharge investigation overlap.

    If the enrollee was discharged from admission within the last 14 days,
    lab/radiology investigations in this request were likely already performed
    during the admission and should not be billed again.

    Returns: {triggered, discharge_date, days_since, reason}
    """
    if not enrollee_id or not encounter_date:
        return {"triggered": False}

    # Only applies to investigations (labs, radiology, ECG, etc.)
    name_upper = proc_name.upper()
    is_investigation = any(kw in name_upper for kw in _LAB_KEYWORDS)
    if not is_investigation:
        return {"triggered": False}

    try:
        enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
        lookback = (enc_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        enc_str  = enc_dt.strftime("%Y-%m-%d")

        row = conn.execute("""
            SELECT MAX(CAST(requestdate AS DATE)) as last_adm,
                   COALESCE(MAX(TRY_CAST(quantity AS INTEGER)), 1) as days_granted
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE IID = ?
              AND UPPER(LEFT(TRIM(code), 3)) = 'ADM'
              AND CAST(requestdate AS DATE) >= ?
              AND CAST(requestdate AS DATE) <= ?
        """, [enrollee_id, lookback, enc_str]).fetchone()

        if not (row and row[0]):
            return {"triggered": False}

        adm_date     = row[0] if hasattr(row[0], 'date') else datetime.strptime(str(row[0])[:10], "%Y-%m-%d").date()
        days_granted = int(row[1]) if row[1] else 1
        discharge_dt = adm_date + timedelta(days=days_granted)
        days_since   = (enc_dt - discharge_dt).days

        # Only flag if the current encounter is AFTER discharge but within lookback window
        if discharge_dt < enc_dt <= adm_date + timedelta(days=lookback_days):
            return {
                "triggered":     True,
                "discharge_date": str(discharge_dt),
                "days_since":    days_since,
                "reason": (
                    f"Enrollee was discharged {days_since} day(s) ago (discharge: {discharge_dt}). "
                    f"{proc_name} is a lab/investigation that was likely performed during the admission "
                    f"and cannot be billed separately within {lookback_days} days of discharge."
                ),
                "requires_review": True,
            }
    except Exception as e:
        logger.warning(f"check_post_discharge_overlap error: {e}")

    return {"triggered": False}


# ── OpenFDA Indication vs Diagnosis Check ────────────────────────────────────

def check_openfda_indication(
    proc_code: str,
    proc_name: str,
    diagnosis_code: str,
    diagnosis_name: str,
) -> Dict:
    """
    Rule 16 — OpenFDA drug label indication vs submitted diagnosis.

    Only runs for DRG codes.  Fetches the FDA label's indications_and_usage section
    and asks Claude (Haiku — cheap) whether this drug is indicated for the diagnosis.

    Never auto-denies. Flags PENDING_REVIEW if the diagnosis appears outside the
    drug's approved indications.

    Off-label use is flagged as a soft concern, not a hard deny.
    """
    if not proc_code.upper().startswith("DRG"):
        return {"triggered": False}

    # Fetch structured MED-RT data and full FDA label in parallel (both cached)
    may_treat_data = rxclass_get_may_treat(proc_name)
    label          = openfda_get_label(proc_name)
    indications    = label["indications"]
    contraindicated_conditions = label["contraindications"]

    if not indications and not may_treat_data["may_treat"]:
        return {"triggered": False, "note": "No FDA label or MED-RT data available"}

    # MED-RT structured pre-check: if drug explicitly may_treat a condition that
    # closely matches the diagnosis name, skip AI and fast-approve.
    diag_lower = diagnosis_name.lower()
    for condition in may_treat_data["may_treat"]:
        cond_lower = condition.lower()
        # Match if either name contains a meaningful keyword from the other
        if (cond_lower in diag_lower or diag_lower in cond_lower or
                any(w in cond_lower for w in diag_lower.split() if len(w) > 4)):
            return {
                "triggered":       False,
                "indicated":       True,
                "off_label":       False,
                "confidence":      95,
                "reasoning":       f"MED-RT structured data: {proc_name} may_treat '{condition}' — matches submitted diagnosis.",
                "requires_review": False,
                "source":          "rxclass_medrt",
            }

    # MED-RT CI_with pre-check: if drug is explicitly contraindicated with the
    # submitted diagnosis → flag immediately without AI call.
    for condition in may_treat_data["ci_with"]:
        cond_lower = condition.lower()
        if (cond_lower in diag_lower or diag_lower in cond_lower or
                any(w in cond_lower for w in diag_lower.split() if len(w) > 4)):
            return {
                "triggered":       True,
                "indicated":       False,
                "off_label":       True,
                "confidence":      92,
                "reasoning":       f"MED-RT structured data: {proc_name} is contraindicated with '{condition}' — matches submitted diagnosis.",
                "requires_review": True,
                "source":          "rxclass_medrt_ci",
            }

    # AI fallback — include all structured data as context for richer reasoning
    may_treat_str = ", ".join(may_treat_data["may_treat"][:15]) or "not available"
    ci_with_str   = ", ".join(may_treat_data["ci_with"][:10])   or "none listed"

    prompt = f"""You are a clinical pharmacist reviewing a Nigerian HMO PA request.

Drug requested: {proc_code} — {proc_name}
Submitted diagnosis: {diagnosis_code} — {diagnosis_name}

MED-RT structured indications (conditions this drug may_treat):
{may_treat_str}

MED-RT disease contraindications (conditions this drug is CI_with):
{ci_with_str}

FDA label indications_and_usage (excerpt):
{indications[:1000]}

Question: Is {proc_name} indicated (standard approved use) for the diagnosis "{diagnosis_name}"?
Consider primary indications AND commonly accepted clinical uses.
Flag as off-label ONLY if the drug clearly has no clinical basis for this diagnosis.
If it appears in the MED-RT may_treat list or has a closely related indication, it is indicated.

Respond in JSON only (no markdown):
{{
  "indicated": true or false,
  "confidence": 0-100,
  "reasoning": "one sentence",
  "off_label": true or false
}}"""

    import anthropic as _anthropic
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"triggered": False}

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        resp   = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300, temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = resp.content[0].text.strip()
        if "```" in raw:
            raw = raw[raw.find("{"):raw.rfind("}") + 1]
        data = json.loads(raw)

        indicated  = bool(data.get("indicated", True))
        off_label  = bool(data.get("off_label", False))
        confidence = int(data.get("confidence", 80))
        reasoning  = data.get("reasoning", "")

        return {
            "triggered":       off_label and not indicated,
            "indicated":       indicated,
            "off_label":       off_label,
            "confidence":      confidence,
            "reasoning":       reasoning,
            "requires_review": off_label and not indicated,
            "source":          "openfda+medrt+ai",
        }
    except Exception as e:
        logger.debug(f"OpenFDA indication check failed for {proc_code}: {e}")
        return {"triggered": False}


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
    procedures: List[Dict],   # [{"code", "name", "diagnoses", "from_basket"(optional bool)}]
    encounter_type: str = "OUTPATIENT",
) -> Dict:
    """
    Evaluate whether the combination of procedures is medically necessary.

    procedures may include items with from_basket=True — these are already
    approved this session and cannot be denied; they provide context only.
    Only procedures without from_basket (or from_basket=False) can be denied.

    Returns:
        necessary: bool
        confidence: int
        reasoning: str
        flagged_items: list of {code_a, code_b, reason}
        procedures_to_deny: list of codes (current-request only)
        requires_review: bool
    """
    current = [p for p in procedures if not p.get("from_basket")]
    basket  = [p for p in procedures if p.get("from_basket")]

    # Need at least 2 total (current + basket) to check combinations
    if len(current) + len(basket) < 2:
        return {"necessary": True, "confidence": 100,
                "reasoning": "Only one procedure — no combination to assess.",
                "flagged_items": [], "requires_review": False,
                "procedures_to_deny": [], "procedure_verdicts": {}}

    context = "INPATIENT admission" if encounter_type == "INPATIENT" else "outpatient visit"

    def _fmt(p: Dict) -> str:
        diag_str = ", ".join(f"{d['code']} ({d['name']})" for d in p.get("diagnoses", []))
        dec = p.get("individual_decision", "APPROVE")
        if p.get("from_basket"):
            status = "[ALREADY APPROVED THIS SESSION]"
        elif dec == "DENY":
            status = "[HARD DENIED — will NOT be prescribed]"
        elif dec == "PENDING_REVIEW":
            status = "[PENDING — individual check flagged concerns, agent may DENY this]"
        else:
            status = "[APPROVED by individual checks]"
        return f"- {p['code']}: {p['name']}  {status}" + (f"  [for: {diag_str}]" if diag_str else "")

    all_lines = "\n".join(_fmt(p) for p in (current + basket))

    prompt = f"""You are a senior HMO medical reviewer in Nigeria ({context}).

A provider has submitted a PA request. Each item below has a status showing its individual check outcome.

ALL PROCEDURES IN THIS REQUEST:
{all_lines}

Your task: Identify whether any APPROVED or PENDING item is REDUNDANT or UNJUSTIFIED given the OTHER items that are actually going to be prescribed.

CRITICAL RULES:
- HARD DENIED items ([HARD DENIED]) will NOT be prescribed. Do NOT use them as the basis for denying another item.
- PENDING items ([PENDING]) may or may not be approved by the agent. If an item would ONLY be redundant because of a PENDING item (which itself may be denied), do NOT deny it — the agent will resolve that.
- Only flag an item as redundant if it conflicts with an [APPROVED] or [ALREADY APPROVED] item that is certain to be prescribed.
- Procedures for DIFFERENT diagnoses are generally NOT redundant — an antibiotic for H. pylori + a different antibiotic for URTI serve different conditions and should each be evaluated independently.
- Standard combination therapy is NEVER redundant (amoxicillin + metronidazole for H. pylori is dual therapy, not redundancy).
- Flag redundancy ONLY when two items treat the SAME diagnosis via the SAME mechanism AND one is already confirmed to be prescribed.
- When in doubt, mark NECESSARY — only flag CLEAR, CERTAIN redundancy.

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
      "reason": "why this item is redundant given a CONFIRMED co-prescription"
    }}
  ]
}}

RULES for procedure_verdicts:
- Include every procedure code as a key.
- HARD DENIED and ALREADY APPROVED items must always be "keep" (they are excluded from denial).
- A PENDING or APPROVED item that is redundant with a CONFIRMED prescription = "deny".
- A PENDING or APPROVED item serving a distinct purpose = "keep".
- If necessary=true, all verdicts must be "keep"."""

    ai = _call_claude(prompt)
    necessary  = bool(ai.get("necessary", True))
    confidence = int(ai.get("confidence", 0))
    reasoning  = ai.get("reasoning", "")
    flagged    = ai.get("flagged_items", [])
    verdicts   = ai.get("procedure_verdicts", {})

    # Only deny current-request procedures — never basket items
    basket_codes = {p["code"].upper() for p in basket}
    procedures_to_deny = [
        code.upper() for code, verdict in verdicts.items()
        if str(verdict).lower() == "deny" and code.upper() not in basket_codes
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


# ── Rule 17 — Cost-Effectiveness & Therapeutic Substitution ──────────────────

def check_cost_effectiveness(
    procedures: List[Dict],  # same shape as proc_combo_input
    encounter_type: str = "OUTPATIENT",
) -> Dict:
    """
    Evaluate whether the FULL regimen (basket already approved + current batch)
    is the most cost-effective evidence-based option.

    Basket items are already approved and cannot be auto-denied by this system,
    but they are included in the assessment. When a basket drug + current batch
    together form a sub-optimal regimen, the AI:
      - Denies the current-batch redundant drug(s)
      - Returns a basket_action instruction naming the basket drug the agent
        should ask the provider to return/exchange

    Flags patterns like:
    - Multiple drugs when one covers all diagnoses (clarithromycin dual-coverage)
    - Combination brands when one component alone suffices (paracetamol+orphenadrine)
    - Expensive drug when cheaper equivalent exists
    - Polypharmacy where guideline regimen uses fewer drugs

    Never auto-denies. Always escalates to PENDING_REVIEW if flagged.
    """
    active = [p for p in procedures if not p.get("from_basket")
              and p.get("individual_decision") != "DENY"]
    basket = [p for p in procedures if p.get("from_basket")]

    if not active:
        return {"cost_effective": True, "confidence": 100,
                "reasoning": "No active procedures to assess.",
                "recommendations": [], "requires_review": False}

    context = "INPATIENT admission" if encounter_type == "INPATIENT" else "outpatient visit"

    def _fmt_active(p: Dict) -> str:
        diag_str = ", ".join(f"{d['code']} ({d['name']})" for d in p.get("diagnoses", []))
        status = ("[PENDING REVIEW]" if p.get("individual_decision") == "PENDING_REVIEW"
                  else "[CAN BE DENIED BY RULE 17]")
        return f"- {p['code']}: {p['name']}  {status}" + (f"  [for: {diag_str}]" if diag_str else "")

    def _fmt_basket(p: Dict) -> str:
        diag_str = ", ".join(f"{d['code']} ({d['name']})" for d in p.get("diagnoses", []))
        return (f"- {p['code']}: {p['name']}  [ALREADY APPROVED TODAY — CANNOT AUTO-DENY — "
                "agent must call provider to return/exchange]"
                + (f"  [for: {diag_str}]" if diag_str else ""))

    active_lines = "\n".join(_fmt_active(p) for p in active)
    basket_section = (
        "\nALREADY APPROVED TODAY — CANNOT BE AUTO-DENIED (agent must act if these should change):\n"
        + "\n".join(_fmt_basket(p) for p in basket)
    ) if basket else ""

    basket_instruction = (
        "\nBASKET RULE: Drugs marked [ALREADY APPROVED TODAY] cannot be denied by this system. "
        "If the basket drug + current-batch drugs together are sub-optimal:\n"
        "  → Deny the redundant CURRENT REQUEST drug(s)\n"
        "  → Set basket_action: 'Provider should return [basket drug] and substitute [alternative]'\n"
        "  The agent will call the provider to arrange the basket exchange."
    ) if basket else ""

    prompt = f"""You are a senior HMO clinical pharmacist in Nigeria ({context}) conducting a cost-effectiveness review.

Your role: think like a health insurer. Assess the ENTIRE regimen and identify whether any drug in the CURRENT REQUEST is redundant or replaceable with a cheaper, equally effective option.

KEY DISTINCTION:
- ACTIVE PROCEDURES IN THIS REQUEST: drugs labelled [CAN BE DENIED BY RULE 17] — you MAY and SHOULD deny these if redundant.
- ALREADY APPROVED TODAY: drugs labelled [ALREADY APPROVED TODAY — CANNOT AUTO-DENY] — you CANNOT deny these; use basket_action instead.

ACTIVE PROCEDURES IN THIS REQUEST (deny these if not cost-effective):
{active_lines}
{basket_section}
{basket_instruction}

CLINICAL RULES TO APPLY:

H. PYLORI + URTI PATTERN (most important):
- If a patient has BOTH H. pylori AND URTI, the optimal regimen is Clarithromycin triple therapy:
  Amoxicillin 1g BD + Clarithromycin 500mg BD + PPI (e.g. Omeprazole 20mg BD) x 14 days.
- Clarithromycin covers URTI pathogens (S. pneumoniae, H. influenzae via 14-OH metabolite, M. catarrhalis) AND H. pylori simultaneously.
- THEREFORE: if amoxicillin (H. pylori) + cefuroxime/azithromycin (URTI) + metronidazole (H. pylori) are ALL in the current request → DENY BOTH cefuroxime AND metronidazole. Keep amoxicillin. Recommend adding clarithromycin + PPI instead.
- EXAMPLE: Current request has [amoxicillin for H. pylori] + [metronidazole for H. pylori] + [cefuroxime for URTI] → deny metronidazole AND deny cefuroxime → recommended_alternative: "Clarithromycin 500mg BD x 14 days (replaces both metronidazole and cefuroxime — covers H. pylori as triple therapy and URTI via macrolide spectrum). Also add PPI Omeprazole 20mg BD x 14 days."

PAIN PATTERN:
- Paracetamol alone (500-1000mg QDS) is sufficient for mild-moderate pain. Orphenadrine addition rarely justified without myospasm diagnosis.
- Diclofenac alone (50mg TDS) covers analgesia + inflammation — paracetamol addition rarely additive for mild pain.

URTI ALONE:
- Simple uncomplicated URTI: amoxicillin 500mg TDS is cheaper first-line. Cefuroxime/azithromycin only if allergy or documented failure.

GENERAL:
- ONLY flag with strong clinical evidence. Do not second-guess clinically complex decisions.

Respond in JSON only (no markdown):
{{
  "cost_effective": true or false,
  "confidence": 0-100,
  "reasoning": "One concise sentence on overall cost-effectiveness of the full regimen.",
  "recommendations": [
    {{
      "procedure_code": "code of CURRENT REQUEST drug to deny (must be from the ACTIVE list)",
      "procedure_name": "name of that drug",
      "reason": "Why this drug is not cost-effective given the full regimen",
      "recommended_alternative": "Specific cheaper alternative with dose, duration, guideline. Be explicit — name the drug to add instead.",
      "basket_action": "Only if a basket drug needs to be returned: 'Provider should return [drug name] and substitute [alternative with dose]'. Empty string if no basket action needed.",
      "deny": true or false
    }}
  ]
}}

STRICT RULES:
- recommendations MUST ONLY include drugs from ACTIVE PROCEDURES IN THIS REQUEST. NEVER put basket drugs in recommendations.
- Set deny=true for drugs clearly redundant or replaceable with strong evidence.
- Set deny=false for concerns only.
- If cost_effective=true, recommendations must be empty array.
- basket_action must always be a string (empty string if not applicable, never null).
- When multiple current-batch drugs become redundant due to the same alternative, include ALL of them as separate recommendation entries."""

    ai = _call_claude(prompt)
    cost_effective  = bool(ai.get("cost_effective", True))
    confidence      = int(ai.get("confidence", 0))
    reasoning       = ai.get("reasoning", "")
    recommendations = ai.get("recommendations", [])

    # Only keep deny=true recommendations for valid active procedure codes
    active_codes = {p["code"].upper() for p in active}
    deny_recs = [
        r for r in recommendations
        if r.get("deny") and (r.get("procedure_code") or "").upper() in active_codes
    ]

    return {
        "cost_effective":  cost_effective,
        "confidence":      confidence,
        "reasoning":       reasoning,
        "recommendations": deny_recs,
        "requires_review": not cost_effective and bool(deny_recs),
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
    session_basket: Optional[List[Dict]] = None,
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
            session_basket=session_basket,
            batch_items=items,
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

    # Single engine for all request-level checks (post-parallel, same thread)
    from .comprehensive import ComprehensiveVettingEngine
    engine = ComprehensiveVettingEngine(db_path)

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

    combo_check    = check_disease_combination(all_diags_for_combo, encounter_type)
    mismatch_check = check_diagnosis_encounter_mismatch(all_diags_for_combo, encounter_type)

    # ── Rule 15 — Post-discharge overlap (per procedure) ──────────────────────
    # Run once per unique procedure. Flags labs/investigations submitted within
    # 14 days of a prior admission discharge — likely already covered by the admission.
    post_discharge_flags: List[Dict] = []
    for item, res in zip(items, results):
        pc = item["procedure_code"].strip().upper()
        pn = res.get("procedure_name", pc)
        pd_check = check_post_discharge_overlap(
            enrollee_id=enrollee_id,
            encounter_date=encounter_date,
            proc_code=pc,
            proc_name=pn,
            conn=engine.conn,
        )
        if pd_check.get("triggered"):
            post_discharge_flags.append({"procedure_code": pc, "procedure_name": pn, **pd_check})
            # Escalate this individual result to PENDING_REVIEW
            for r in results:
                if r.get("procedure_code", "").upper() == pc and r.get("decision") != "DENY":
                    r["decision"] = "PENDING_REVIEW"
                    r["requires_agent_review"] = True
                    r.setdefault("review_reasons", []).append(pd_check["reason"])

    # ── Procedure Combination Necessity Check (request-level) ─────────────────
    # Build procedure list: current request items + session basket (already approved).
    # Basket items are marked from_basket=True — they provide context but cannot be denied.
    proc_combo_input: List[Dict] = []
    seen_proc_codes: set = set()
    for item, res in zip(items, results):
        pc = item["procedure_code"].strip().upper()
        if pc in seen_proc_codes:
            continue
        seen_proc_codes.add(pc)
        diag_names_map = item.get("diagnosis_names", {})
        all_diag_codes = item.get("diagnosis_codes", [])
        proc_combo_input.append({
            "code": pc,
            "name": res.get("procedure_name", pc),
            "individual_decision": res.get("decision", "APPROVE"),
            "from_basket": False,
            "diagnoses": [
                {"code": dc, "name": diag_names_map.get(dc, dc)}
                for dc in all_diag_codes
            ],
        })

    # Append basket items (approved earlier this session) as context
    for bitem in (session_basket or []):
        bpc = (bitem.get("procedure_code") or "").strip().upper()
        if not bpc or bpc in seen_proc_codes:
            continue
        seen_proc_codes.add(bpc)
        bdiag_codes = bitem.get("diagnosis_codes", [])
        bdiag_names = bitem.get("diagnosis_names", {})
        proc_combo_input.append({
            "code": bpc,
            "name": bitem.get("procedure_name", bpc),
            "individual_decision": "APPROVE",
            "from_basket": True,
            "diagnoses": [
                {"code": dc, "name": bdiag_names.get(dc, dc)}
                for dc in bdiag_codes
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
            # Build a specific reason naming the conflicting drug(s)
            _pair_reason = None
            for fi in proc_combo_check.get("flagged_items", []):
                ca = (fi.get("code_a") or "").upper()
                cb = (fi.get("code_b") or "").upper()
                if ca == pc or cb == pc:
                    conflict_code = cb if ca == pc else ca
                    conflict_name = fi.get("name_b") if ca == pc else fi.get("name_a")
                    _pair_reason = (
                        f"Procedure combination check (Rule 11): {res.get('procedure_name', pc)} "
                        f"conflicts with {conflict_name} ({conflict_code}) — "
                        f"{fi.get('reason', 'redundant given other approved items in this request')}"
                    )
                    break
            _flag_reason = _pair_reason or (
                f"Procedure combination check (Rule 11): {res.get('procedure_name', pc)} "
                f"is redundant given other items in this request — AI recommends denial."
            )
            _escalate_to_review(
                res,
                escalated_by="combo_check",
                flag_reason=_flag_reason,
                ai_recommendation="DENY",
            )

    # ── Rule 17 — Cost-Effectiveness & Therapeutic Substitution ─────────────────
    # Run after all other checks so it has the final individual_decision per item.
    # Update proc_combo_input individual_decision to reflect post-escalation state.
    for entry in proc_combo_input:
        ec = entry["code"].upper()
        for r in results:
            if r.get("procedure_code", "").upper() == ec:
                entry["individual_decision"] = r.get("decision", entry["individual_decision"])
                break

    cost_check = check_cost_effectiveness(proc_combo_input, encounter_type)

    for rec in cost_check.get("recommendations", []):
        deny_code      = (rec.get("procedure_code") or "").upper()
        alternative    = rec.get("recommended_alternative", "")
        deny_reason    = rec.get("reason", "")
        basket_action  = (rec.get("basket_action") or "").strip()

        flag_reason = (
            f"Cost-effectiveness check (Rule 17): {rec.get('procedure_name', deny_code)} "
            f"is not the most cost-effective option for this regimen. {deny_reason}"
            + (f" Recommended alternative: {alternative}." if alternative else "")
            + (f" ACTION REQUIRED — {basket_action}" if basket_action else "")
        )
        for res in results:
            if res.get("procedure_code", "").upper() == deny_code and res.get("decision") != "DENY":
                _escalate_to_review(
                    res,
                    escalated_by="cost_check",
                    flag_reason=flag_reason,
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

    # Combo/cost checks failure escalates overall to PENDING_REVIEW
    if combo_check["requires_review"] and overall not in ("DENY", "PENDING_REVIEW"):
        overall = "PENDING_REVIEW"
    if proc_combo_check["requires_review"] and overall not in ("DENY", "PENDING_REVIEW"):
        overall = "PENDING_REVIEW"
    if mismatch_check["requires_review"] and overall not in ("DENY", "PENDING_REVIEW"):
        overall = "PENDING_REVIEW"
    if cost_check["requires_review"] and overall not in ("DENY", "PENDING_REVIEW"):
        overall = "PENDING_REVIEW"
    if post_discharge_flags and overall not in ("DENY", "PENDING_REVIEW"):
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
        "cost_effectiveness":      cost_check,
        "diagnosis_encounter_mismatch": mismatch_check,
        "post_discharge_flags":    post_discharge_flags,
        "requires_agent_review":   any(r.get("requires_agent_review") for r in results)
                                   or combo_check["requires_review"]
                                   or proc_combo_check["requires_review"]
                                   or cost_check["requires_review"]
                                   or mismatch_check["requires_review"]
                                   or bool(post_discharge_flags),
    }
