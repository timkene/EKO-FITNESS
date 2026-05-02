"""
CLEARLINE — Clinical Necessity Engine
======================================

Checks whether a procedure is clinically appropriate given:
  1. Drug route vs patient admission status (oral for sepsis = ❌)
  2. Required diagnostic tests before treatment (H. pylori test, FBC, malaria RDT ...)
  3. Step-down therapy (prior IV → oral is acceptable)
  4. Clinical appropriateness of drug for diagnosis severity

Admission detection:
  ADM/ICU codes in PA DATA; `granted` column = number of days on admission.
  requestdate + granted days = expected discharge date.

Test detection:
  All non-drug, non-admission codes are fetched with their procedure names
  (joined from PROCEDURE DATA) so the AI can read "HELICOBACTER PYLORI RAPID TEST",
  "FULL BLOOD COUNT", "MALARIA PARASITE TEST" etc. directly.
"""

import re
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any

import anthropic

from .drug_apis import rxclass_get_drug_classes, who_eml_lookup
from .bnf_client import bnf_get_guidance

logger = logging.getLogger(__name__)

# ── Drug route keywords ────────────────────────────────────────────────────────
INJECTABLE_KEYWORDS = ["INJ", "INJECTION", "IV ", "INTRAVENOUS", "INFUSION",
                        "AMPOULE", "VIAL", "IM ", "INTRAMUSCULAR"]
ORAL_KEYWORDS       = ["TAB", "TABLET", "CAP", "CAPSULE", "SYRUP", "SYRP",
                        "SUSP", "SUSPENSION", "ORAL", "ELIXIR", "SACHET",
                        "GRANULE", "POWDER", "DROPS", "SOLUTION"]
TOPICAL_KEYWORDS    = ["CREAM", "OINTMENT", "GEL", "LOTION", "PATCH",
                        "SPRAY", "EYE DROP", "EAR DROP", "PESSARY", "SUPPOSITORY"]

# Formulation values from PROCEDURE_MASTER that map to a route
INJECTABLE_FORMULATIONS = {"INJECTION", "IV", "INFUSION", "AMPOULE", "VIAL", "IM"}
ORAL_FORMULATIONS       = {"TABLET", "CAPSULE", "SYRUP", "SUSPENSION", "ORAL",
                            "DROPS", "SOLUTION", "ELIXIR", "SACHET", "POWDER"}
TOPICAL_FORMULATIONS    = {"CREAM", "OINTMENT", "GEL", "LOTION", "PATCH",
                            "SPRAY", "PESSARY", "SUPPOSITORY"}

# Codes to exclude from test history (drugs, admission, feeding, consultations)
NON_TEST_PREFIXES = {"DRG", "ADM", "FEE", "CON", "OPT", "ICU", "INP", "OUT",
                     "ANN", "GYM", "PHO", "PRO", "NHI", "BRG", "MIN", "DEN",
                     "MAJ", "MED", "SUR"}


@dataclass
class AdmissionStatus:
    is_admitted: bool
    admission_code: Optional[str] = None
    admission_date: Optional[str] = None
    discharge_date: Optional[str] = None
    days_granted: int = 0
    is_icu: bool = False


@dataclass
class ClinicalNecessityResult:
    passed: bool
    confidence: int
    reasoning: str
    severity: str = "unknown"
    route: str = "unknown"
    route_appropriate: bool = True
    tests_required: List[str] = field(default_factory=list)
    tests_found: List[str] = field(default_factory=list)
    step_down_applicable: bool = False
    concerns: List[str] = field(default_factory=list)
    source: str = "ai"


# ============================================================================
# MAIN ENGINE
# ============================================================================

class ClinicalNecessityEngine:

    def __init__(self, conn, anthropic_client: Optional[anthropic.Anthropic] = None):
        self.conn = conn
        self.ai   = anthropic_client or anthropic.Anthropic()

    # ── Public entry point ────────────────────────────────────────────────────

    def check(
        self,
        procedure_code: str,
        procedure_name: str,
        procedure_class: Optional[str],
        diagnosis_code: str,
        diagnosis_name: str,
        enrollee_id: str,
        encounter_date: str,
        all_request_procedures: Optional[List[Dict]] = None,
        session_basket: Optional[List[Dict]] = None,
    ) -> ClinicalNecessityResult:
        route      = self._extract_route(procedure_name, procedure_code)
        admission  = self._get_admission_status(enrollee_id, encounter_date)
        tests      = self._get_recent_tests(enrollee_id, encounter_date, days=7)
        prior_meds = self._get_recent_medications(enrollee_id, encounter_date, days=30)

        # Merge session basket drugs into prior_meds (these were approved this session
        # but are not yet in the live DB — treat them as same-day history)
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

        return self._evaluate_with_ai(
            procedure_code=procedure_code,
            procedure_name=procedure_name,
            procedure_class=procedure_class or "",
            diagnosis_code=diagnosis_code,
            diagnosis_name=diagnosis_name,
            route=route,
            admission=admission,
            recent_tests=tests,
            recent_meds=prior_meds,
            all_request_procedures=all_request_procedures,
        )

    # ── Route extraction ──────────────────────────────────────────────────────

    def _extract_route(self, procedure_name: str, procedure_code: Optional[str] = None) -> str:
        name = procedure_name.upper()

        # 1. Keywords in procedure name (fastest, most reliable)
        for kw in INJECTABLE_KEYWORDS:
            if kw in name:
                return "INJECTABLE"
        for kw in TOPICAL_KEYWORDS:
            if kw in name:
                return "TOPICAL"

        # 2. Formulation field from PROCEDURE_MASTER in MongoDB
        if procedure_code:
            try:
                from . import mongo_db
                doc = mongo_db.get_procedure_master(procedure_code)
                if doc:
                    formulation = doc.get("formulation", "").upper().strip()
                    if formulation in INJECTABLE_FORMULATIONS:
                        return "INJECTABLE"
                    if formulation in TOPICAL_FORMULATIONS:
                        return "TOPICAL"
                    if formulation in ORAL_FORMULATIONS:
                        return "ORAL"
            except Exception:
                pass

        # 3. Oral keywords in name
        for kw in ORAL_KEYWORDS:
            if kw in name:
                return "ORAL"

        # 4. Dosage-only name (e.g. "AMOXICILLIN 500MG") → assume oral tablet
        if re.search(r'\d+\s*(?:MG|MCG|G\b)', name):
            return "ORAL"

        return "UNKNOWN"

    # ── Admission status ──────────────────────────────────────────────────────

    def _get_admission_status(self, enrollee_id: str, encounter_date: str) -> AdmissionStatus:
        """
        Query PA DATA for ADM/ICU codes.
        `granted` column = number of admission days.
        requestdate + granted_days >= encounter_date → still admitted.
        """
        try:
            enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
            lookback = (enc_dt - timedelta(days=30)).strftime("%Y-%m-%d")

            rows = self.conn.execute("""
                SELECT UPPER(TRIM(code)) as code,
                       CAST(requestdate AS DATE) as adm_date,
                       COALESCE(TRY_CAST(quantity AS INTEGER), 1) as days_granted
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE IID = ?
                  AND UPPER(LEFT(TRIM(code), 3)) IN ('ADM', 'ICU')
                  AND CAST(requestdate AS DATE) >= ?
                  AND CAST(requestdate AS DATE) <= ?
                ORDER BY requestdate DESC
            """, [enrollee_id, lookback, encounter_date]).fetchdf()

            for _, row in rows.iterrows():
                adm_date     = row["adm_date"]
                days_granted = int(row["days_granted"]) if row["days_granted"] else 1

                if isinstance(adm_date, str):
                    adm_date = datetime.strptime(adm_date[:10], "%Y-%m-%d").date()

                discharge_dt = adm_date + timedelta(days=days_granted)
                if adm_date <= enc_dt <= discharge_dt:
                    return AdmissionStatus(
                        is_admitted=True,
                        admission_code=row["code"],
                        admission_date=str(adm_date),
                        discharge_date=str(discharge_dt),
                        days_granted=days_granted,
                        is_icu="ICU" in str(row["code"]).upper()
                    )

        except Exception as e:
            logger.warning(f"Admission check error: {e}")

        return AdmissionStatus(is_admitted=False)

    # ── Recent tests (3-day window) ───────────────────────────────────────────

    def _get_recent_tests(self, enrollee_id: str, encounter_date: str, days: int = 3) -> List[Dict]:
        """
        Fetch all non-drug, non-admission codes from PA DATA in the last `days` days,
        joined with PROCEDURE DATA for procedure names so the AI can read
        "HELICOBACTER PYLORI RAPID TEST", "FULL BLOOD COUNT", "MALARIA PARASITE TEST" etc.
        """
        tests = []
        try:
            enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
            lookback = (enc_dt - timedelta(days=days)).strftime("%Y-%m-%d")

            pa_rows = self.conn.execute("""
                SELECT
                    UPPER(TRIM(p.code)) as code,
                    COALESCE(TRIM(pd.proceduredesc), '') as name,
                    CAST(p.requestdate AS DATE) as test_date,
                    'PA' as source
                FROM "AI DRIVEN DATA"."PA DATA" p
                LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd
                    ON LOWER(TRIM(pd.procedurecode)) = LOWER(TRIM(p.code))
                WHERE p.IID = ?
                  AND CAST(p.requestdate AS DATE) >= ?
                  AND CAST(p.requestdate AS DATE) <= ?
                  AND UPPER(LEFT(TRIM(p.code), 3)) NOT IN (
                      'DRG','ADM','FEE','CON','OPT','ICU','INP','OUT',
                      'ANN','GYM','PHO','NHI','BRG','MIN','DEN','MAJ',
                      'MED','SUR','PRO'
                  )
                  AND p.code IS NOT NULL AND TRIM(p.code) != ''
            """, [enrollee_id, lookback, encounter_date]).fetchdf()

            for _, row in pa_rows.iterrows():
                tests.append({
                    "code": row["code"],
                    "name": row["name"] or row["code"],
                    "date": str(row["test_date"]),
                    "source": row["source"]
                })

        except Exception as e:
            logger.warning(f"Test history error: {e}")

        return tests

    # ── Recent medications (30-day window, for step-down check) ──────────────

    def _get_recent_medications(self, enrollee_id: str, encounter_date: str, days: int = 30) -> List[Dict]:
        """
        Fetch DRG codes from PA DATA + CLAIMS in recent history.
        Used to detect prior injectable therapy (step-down eligibility).
        """
        meds = []
        try:
            enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
            lookback = (enc_dt - timedelta(days=days)).strftime("%Y-%m-%d")

            pa_rows = self.conn.execute("""
                SELECT
                    UPPER(TRIM(p.code)) as code,
                    COALESCE(TRIM(pd.proceduredesc), UPPER(TRIM(p.code))) as name,
                    CAST(p.requestdate AS DATE) as med_date,
                    'PA' as source
                FROM "AI DRIVEN DATA"."PA DATA" p
                LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd
                    ON LOWER(TRIM(pd.procedurecode)) = LOWER(TRIM(p.code))
                WHERE p.IID = ?
                  AND CAST(p.requestdate AS DATE) >= ?
                  AND CAST(p.requestdate AS DATE) <= ?
                  AND UPPER(LEFT(TRIM(p.code), 3)) = 'DRG'
            """, [enrollee_id, lookback, encounter_date]).fetchdf()

            for _, row in pa_rows.iterrows():
                meds.append({
                    "code": row["code"],
                    "name": row["name"],
                    "date": str(row["med_date"]),
                    "source": row["source"]
                })

            try:
                cl_rows = self.conn.execute("""
                    SELECT
                        UPPER(TRIM(code)) as code,
                        UPPER(TRIM(code)) as name,
                        CAST(encounterdatefrom AS DATE) as med_date,
                        'Claims' as source
                    FROM "AI DRIVEN DATA"."CLAIMS DATA"
                    WHERE enrollee_id = ?
                      AND CAST(encounterdatefrom AS DATE) >= ?
                      AND CAST(encounterdatefrom AS DATE) <= ?
                      AND UPPER(LEFT(TRIM(code), 3)) = 'DRG'
                """, [enrollee_id, lookback, encounter_date]).fetchdf()

                for _, row in cl_rows.iterrows():
                    meds.append({
                        "code": row["code"],
                        "name": row["name"],
                        "date": str(row["med_date"]),
                        "source": row["source"]
                    })
            except Exception:
                pass

        except Exception as e:
            logger.warning(f"Medication history error: {e}")

        return meds

    # ── AI evaluation ─────────────────────────────────────────────────────────

    def _evaluate_with_ai(
        self,
        procedure_code: str,
        procedure_name: str,
        procedure_class: str,
        diagnosis_code: str,
        diagnosis_name: str,
        route: str,
        admission: AdmissionStatus,
        recent_tests: List[Dict],
        recent_meds: List[Dict],
        all_request_procedures: Optional[List[Dict]] = None,
    ) -> ClinicalNecessityResult:

        admission_ctx = (
            f"ADMITTED (since {admission.admission_date}, discharge expected "
            f"{admission.discharge_date}, {admission.days_granted} days granted, "
            f"{'ICU' if admission.is_icu else 'Ward'})"
            if admission.is_admitted
            else "OUTPATIENT (not currently admitted)"
        )

        tests_ctx = (
            "\n".join(
                f"  - {t['code']}: {t['name']} [{t['date']}]"
                for t in recent_tests
            ) if recent_tests else "  None found in last 7 days"
        )

        meds_ctx = (
            "\n".join(
                f"  - {m['code']}: {m['name']} [{m['date']}]"
                for m in recent_meds[-20:]
            ) if recent_meds else "  None found"
        )

        # Build full-regimen context if this is part of a bulk submission
        regimen_ctx = ""
        if all_request_procedures and len(all_request_procedures) > 1:
            approved_lines = []
            dropped_lines  = []
            for idx, p in enumerate(all_request_procedures, 1):
                p_code      = p.get("procedure_code", "")
                p_name      = p.get("procedure_name", p_code)
                p_class     = p.get("procedure_class", "")
                d_code      = p.get("diagnosis_code", "")
                d_name      = p.get("diagnosis_name", d_code)
                drop_reason = p.get("drop_reason", "")
                marker      = " ◄ THIS ITEM" if p_code == procedure_code else ""
                line        = f"  {idx}. {p_code}: {p_name} [{p_class or 'Unknown'}] → {d_name} ({d_code}){marker}"
                if drop_reason:
                    dropped_lines.append(line + f"  [DROPPED — {drop_reason}]")
                else:
                    approved_lines.append(line)

            regimen_ctx = (
                "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "FULL PA REQUEST REGIMEN (same enrollee, same day)\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            )
            if approved_lines:
                regimen_ctx += "Approved/remaining procedures:\n" + "\n".join(approved_lines) + "\n"
            if dropped_lines:
                regimen_ctx += (
                    "\nDropped procedures (denied by earlier rules — provided for clinical context only):\n"
                    + "\n".join(dropped_lines) + "\n"
                )
            regimen_ctx += (
                "\nNOTE: Evaluate ◄ THIS ITEM in the context of the COMPLETE regimen above. "
                "Consider the dropped procedures as part of the intended treatment plan "
                "when assessing whether the remaining procedures are clinically necessary "
                "(e.g. if triple therapy was intended but one component was dropped, "
                "assess whether the remaining components are still appropriate).\n"
            )

        # ── Pattern-based clinical advisory flags injected into prompt ──────────
        # These are pre-computed from request data so the AI doesn't have to
        # infer patterns it consistently misses.
        _clinical_flags: list[str] = []

        # Flag: Topical analgesic + unspecified myalgia + concurrent systemic infection
        # A topical gel/cream covers a localised area. Generalised infection myalgia
        # (malaria, typhoid, pneumonia, influenza) requires systemic analgesia, not topical.
        # Unspecified myalgia (M79.10/M7910) in the context of a systemic infection
        # almost certainly represents generalised infection myalgia, not a distinct local complaint.
        _diag_normalised = diagnosis_code.replace(".", "").upper()
        _is_unspecified_msk = (
            _diag_normalised in {"M7910", "M791", "M793", "M7930", "M797", "M7970"}
            or _diag_normalised.startswith(("M791", "M793"))
        )
        _SYSTEMIC_INFECTION_PREFIXES = (
            "B50", "B51", "B52", "B53", "B54",  # Malaria
            "J18", "J10", "J11", "J12", "J13", "J14", "J15", "J16", "J17",  # Pneumonia/influenza
            "A01", "A02", "A09", "A40", "A41",  # Typhoid, GI infections, Sepsis
            "B00", "B01", "B02",                # Viral infections
        )
        if route == "TOPICAL" and _is_unspecified_msk and all_request_procedures:
            _infection_in_regimen: list[str] = []
            _systemic_analgesics_in_regimen: list[str] = []
            _ORAL_ANALGESIC_KEYWORDS = [
                "PARACETAMOL", "ACETAMINOPHEN", "IBUPROFEN", "DICLOFENAC TAB",
                "NAPROXEN", "TRAMADOL", "CODEINE", "ASPIRIN",
            ]
            for _p in all_request_procedures:
                _pd = _p.get("diagnosis_code", "").replace(".", "").upper()
                if any(_pd.startswith(pfx) for pfx in _SYSTEMIC_INFECTION_PREFIXES):
                    _infection_in_regimen.append(
                        f"{_p.get('diagnosis_code', _pd)} ({_p.get('diagnosis_name', _pd)})"
                    )
                _pname = _p.get("procedure_name", "").upper()
                if (
                    _p.get("procedure_code") != procedure_code
                    and any(kw in _pname for kw in _ORAL_ANALGESIC_KEYWORDS)
                ):
                    _systemic_analgesics_in_regimen.append(_p.get("procedure_name", ""))

            if _infection_in_regimen:
                _analgesic_note = (
                    f" A systemic oral analgesic is already in this regimen "
                    f"({', '.join(_systemic_analgesics_in_regimen)}), which covers "
                    f"generalised pain systemically."
                    if _systemic_analgesics_in_regimen else ""
                )
                _clinical_flags.append(
                    f"⚠️ CLINICAL FLAG — TOPICAL ANALGESIC + UNSPECIFIED MYALGIA + SYSTEMIC INFECTION:\n"
                    f"  The diagnosis is '{diagnosis_name}' ({diagnosis_code}) — UNSPECIFIED SITE — "
                    f"in the context of a concurrent systemic infection in this regimen: "
                    f"{'; '.join(_infection_in_regimen)}.\n"
                    f"  Systemic infections such as malaria, pneumonia, and typhoid routinely cause "
                    f"GENERALISED body aches and myalgia as a symptom. A topical gel or cream "
                    f"applied to a surface area cannot relieve generalised infection myalgia — "
                    f"it only works on a localised anatomical site.{_analgesic_note}\n"
                    f"  KEY QUESTION: Is this a DISTINCT localised musculoskeletal complaint "
                    f"(e.g. knee arthralgia, low back pain, shoulder strain) that is SEPARATE "
                    f"from the systemic infection? If it were genuinely localised, the provider "
                    f"would typically use a site-specific ICD-10 code (e.g. M54.5 for low back pain, "
                    f"M79.11 for neck myalgia, M25.5x for specific joint pain) rather than the "
                    f"UNSPECIFIED code {diagnosis_code}. The unspecified code combined with an active "
                    f"systemic infection strongly suggests this is generalised infection myalgia "
                    f"being separately coded to justify the topical NSAID.\n"
                    f"  YOU MUST address this in your reasoning. If you approve, explicitly confirm "
                    f"why you believe this is a distinct localised complaint despite the unspecified code."
                )

        _clinical_flags_ctx = (
            "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "PRE-COMPUTED CLINICAL FLAGS (review carefully)\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            + "\n\n".join(_clinical_flags)
            if _clinical_flags else ""
        )

        # WHO EML + RxClass enrichment for AI prompt
        _who = who_eml_lookup(procedure_name)
        _who_ctx = (
            f"✅ WHO Essential Medicine (ATC: {_who.get('atc_code', 'N/A')})"
            if _who.get("essential")
            else "⚠️ Not on WHO Essential Medicines List (may be non-essential or brand-specific)"
        )

        _rx_classes = rxclass_get_drug_classes(procedure_name)
        _rx_class_ctx = (
            ", ".join(_rx_classes[:3]) if _rx_classes
            else procedure_class or "Unknown"
        )

        # RxClass step-therapy confirmation: check if any prior med is in same RxClass
        _same_class_priors: List[str] = []
        if _rx_classes and recent_meds:
            _rx_lower = {c.lower() for c in _rx_classes}
            for m in recent_meds[-20:]:
                m_classes = rxclass_get_drug_classes(m.get("name", ""))
                if any(mc.lower() in _rx_lower for mc in m_classes):
                    _same_class_priors.append(f"{m['name']} [{m['date']}]")

        _step_therapy_ctx = (
            "Prior same-class medications confirmed (RxClass): " + ", ".join(_same_class_priors[:3])
            if _same_class_priors
            else "No prior same-class medications found in RxClass"
        )

        # BNF clinical guidance (supplementary — silently absent if not found)
        _bnf_text = bnf_get_guidance(procedure_name, diagnosis_name)
        _bnf_ctx  = (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"BNF 80 CLINICAL GUIDANCE (supplementary)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{_bnf_text}"
            if _bnf_text else ""
        )

        prompt = f"""You are a senior clinical pharmacist reviewing a pre-authorization request for a Nigerian HMO.
Determine if this treatment request is clinically necessary and appropriate.
{regimen_ctx}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TREATMENT REQUESTED (current item)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Drug/Procedure  : {procedure_name} (code: {procedure_code})
Drug Class (RxClass): {_rx_class_ctx}
WHO EML Status  : {_who_ctx}
Route/Formulation: {route}
Diagnosis       : {diagnosis_name} (ICD-10: {diagnosis_code})

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PATIENT CONTEXT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Admission Status : {admission_ctx}

Tests/procedures done (last 7 days):
{tests_ctx}

Prior medications (last 30 days):
{meds_ctx}

Step-therapy (RxClass same-class check):
{_step_therapy_ctx}
{_bnf_ctx}{_clinical_flags_ctx}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CLINICAL NECESSITY EVALUATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Answer ALL of the following:

1. SEVERITY: How severe is this diagnosis? (mild/moderate/severe/critical)

2. ROUTE APPROPRIATENESS:
   - Is the route ({route}) clinically appropriate for this severity and setting?
   - Critical/severe conditions (sepsis, meningitis, severe pneumonia, severe malaria):
     injectable is required as first-line. Oral is only acceptable as step-down
     (i.e. patient already received injectable of same class — check prior medications).
   - Mild/moderate outpatient conditions: oral is generally acceptable.

3. PREREQUISITE TESTS:
   - For this diagnosis, what confirmatory tests are clinically expected before prescribing?
   - Examples:
       * H. pylori treatment (e.g. triple therapy, clarithromycin, metronidazole for PUD/H. pylori):
         → expect H. pylori test (stool antigen, rapid test, urea breath test)
       * Antibiotic for URTI: → expect FBC or throat swab/culture
       * Antibiotic for UTI: → expect urine M/C/S (microscopy, culture & sensitivity)
       * Antimalarial: → expect malaria parasite test (RDT, microscopy, blood film)
       * Antifungal: → expect microscopy or culture
       * TB drugs: → expect AFB smear or GeneXpert
   - Check the "Tests/procedures done" list above — read the NAME of each test carefully.
     The names are the actual procedure descriptions (e.g. "HELICOBACTER PYLORI RAPID TEST - SERUM",
     "FULL BLOOD COUNT", "MALARIA PARASITE TEST").
   - Were the required tests found? List what was found that satisfies requirements.
   - IMPORTANT: Missing tests are a FLAG (soft concern), not always a hard deny.
     Some presentations are clinically obvious (e.g. classic malaria symptoms in endemic area).

4. STEP-DOWN CHECK:
   - If route is ORAL and condition is severe, check prior medications for injectable
     of the same or similar class. If found → step-down is appropriate → approve route.
   - The "Step-therapy (RxClass)" field above gives authoritative confirmation of whether
     a same-class drug was used recently. If it lists prior medications → step-down confirmed.
   - WHO EML status above indicates if this is an essential first-line medicine.

5. OVERALL DECISION:
   - APPROVE (true): Clinically necessary and appropriate
   - DENY (false): Clinically inappropriate (wrong route for severity with no step-down evidence,
     or treatment has no clinical basis for this diagnosis)
   - Missing tests alone should rarely cause a DENY — flag as a concern instead.

Respond ONLY in valid JSON (no markdown):
{{
  "approved": true or false,
  "confidence": 0-100,
  "severity": "mild|moderate|severe|critical",
  "route_appropriate": true or false,
  "step_down_applicable": true or false,
  "tests_required": ["plain English description of each required test type"],
  "tests_found": ["names of tests from the list above that satisfy requirements"],
  "concerns": ["specific clinical concerns — be concise"],
  "reasoning": "2-3 sentence clinical reasoning"
}}"""

        try:
            response = self.ai.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = response.content[0].text.strip()
            raw = re.sub(r'^```(?:json)?\s*', '', raw)
            raw = re.sub(r'\s*```$', '', raw)

            data = json.loads(raw)

            approved          = bool(data.get("approved", True))
            confidence        = int(data.get("confidence", 80))
            severity          = data.get("severity", "unknown")
            route_appropriate = bool(data.get("route_appropriate", True))
            step_down         = bool(data.get("step_down_applicable", False))
            tests_required    = data.get("tests_required", [])
            tests_found       = data.get("tests_found", [])
            concerns          = data.get("concerns", [])
            reasoning         = data.get("reasoning", "")

            parts = [reasoning]
            if not route_appropriate and not step_down:
                parts.insert(0, f"⚠️ ROUTE: {route} is not appropriate for {severity} {diagnosis_name}.")
            if step_down:
                parts.append("✅ Step-down: prior injectable found — oral is appropriate.")
            if tests_required and not tests_found:
                parts.append(f"⚠️ NO TESTS: {', '.join(tests_required)} not found in last 7 days.")
            elif tests_required and tests_found:
                parts.append(f"✅ Tests confirmed: {', '.join(tests_found)}.")
            if concerns:
                parts.append("Concerns: " + "; ".join(concerns))

            full_reason = " | ".join(p for p in parts if p)

            logger.info(
                f"CLINICAL_NECESSITY [{procedure_code}+{diagnosis_code}] → "
                f"{'APPROVE' if approved else 'DENY'} ({confidence}%) "
                f"severity={severity} route={route} route_ok={route_appropriate} "
                f"tests_required={tests_required} tests_found={tests_found}"
            )

            return ClinicalNecessityResult(
                passed=approved,
                confidence=confidence,
                reasoning=full_reason,
                severity=severity,
                route=route,
                route_appropriate=route_appropriate,
                tests_required=tests_required,
                tests_found=tests_found,
                step_down_applicable=step_down,
                concerns=concerns,
                source="ai"
            )

        except json.JSONDecodeError as e:
            logger.error(f"Clinical necessity JSON parse error: {e}")
            return ClinicalNecessityResult(
                passed=True, confidence=50,
                reasoning="⚠️ Clinical necessity check could not complete — manual review advised.",
                source="ai"
            )
        except Exception as e:
            logger.error(f"Clinical necessity AI error: {e}")
            return ClinicalNecessityResult(
                passed=True, confidence=50,
                reasoning=f"⚠️ Clinical necessity check unavailable: {e}",
                source="ai"
            )
