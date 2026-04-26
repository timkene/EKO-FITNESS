"""
KLAIRE Admission Pre-Checks
============================
Rule-based pre-screening for inpatient admission requests.

Runs BEFORE the AI advisory call. Handles clear-cut cases without querying
the AI, reducing cost and latency.

Decision tiers:
  APPROVE        — diagnosis clearly warrants inpatient admission, room type
                   and duration within expected bounds, no readmission flag.
  DENY           — no submitted diagnosis justifies inpatient admission.
  PENDING_REVIEW — borderline severity, room/duration mismatch, or readmission
                   within 30 days — agent must decide.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Severity constants ────────────────────────────────────────────────────────

_SEV_CRITICAL   = "CRITICAL"
_SEV_SERIOUS    = "SERIOUS"
_SEV_OUTPATIENT = "OUTPATIENT"

# ── Severity catalogue ────────────────────────────────────────────────────────
# ICD-10 3–4 char prefix → (severity, max_days_general, max_days_private)
# max_days: beyond which the requested duration is flagged for agent review.

ADMISSION_SEVERITY: Dict[str, Tuple[str, int, int]] = {
    # ── Cerebrovascular / Neurological ───────────────────────────────────────
    "I60":  (_SEV_CRITICAL, 21, 21),   # Subarachnoid haemorrhage
    "I61":  (_SEV_CRITICAL, 21, 21),   # Intracerebral haemorrhage
    "I62":  (_SEV_CRITICAL, 14, 21),   # Other intracranial haemorrhage
    "I63":  (_SEV_CRITICAL, 14, 21),   # Cerebral infarction (stroke)
    "I64":  (_SEV_CRITICAL, 14, 21),   # Stroke, not specified
    "G00":  (_SEV_CRITICAL, 21, 21),   # Bacterial meningitis
    "G01":  (_SEV_CRITICAL, 21, 21),   # Meningitis in bacterial diseases
    "G03":  (_SEV_CRITICAL, 14, 21),   # Meningitis, other
    "G04":  (_SEV_CRITICAL, 14, 21),   # Encephalitis, myelitis
    "G35":  (_SEV_SERIOUS,   7, 14),   # Multiple sclerosis
    "G40":  (_SEV_SERIOUS,   5,  7),   # Epilepsy
    "G41":  (_SEV_CRITICAL,  7, 14),   # Status epilepticus
    "G51":  (_SEV_SERIOUS,   3,  5),   # Facial nerve / Bell's palsy
    # ── Cardiac ──────────────────────────────────────────────────────────────
    "I21":  (_SEV_CRITICAL, 10, 14),   # Acute MI
    "I22":  (_SEV_CRITICAL, 10, 14),   # Subsequent MI
    "I24":  (_SEV_CRITICAL,  7, 10),   # Other acute ischaemic heart disease
    "I26":  (_SEV_CRITICAL, 10, 14),   # Pulmonary embolism
    "I33":  (_SEV_CRITICAL, 14, 21),   # Acute endocarditis
    "I46":  (_SEV_CRITICAL,  7, 14),   # Cardiac arrest
    "I48":  (_SEV_SERIOUS,   5,  7),   # Atrial fibrillation/flutter
    "I50":  (_SEV_SERIOUS,   7, 10),   # Heart failure
    "I51":  (_SEV_SERIOUS,   5,  7),   # Complications of heart disease
    # ── Respiratory ──────────────────────────────────────────────────────────
    "J13":  (_SEV_SERIOUS,   7, 10),   # Streptococcal pneumonia
    "J14":  (_SEV_SERIOUS,   7, 10),   # H. influenzae pneumonia
    "J15":  (_SEV_SERIOUS,   7, 10),   # Bacterial pneumonia
    "J18":  (_SEV_SERIOUS,   7, 10),   # Pneumonia, unspecified
    "J44":  (_SEV_SERIOUS,   7, 10),   # COPD exacerbation
    "J45":  (_SEV_SERIOUS,   3,  5),   # Asthma, acute
    "J46":  (_SEV_CRITICAL,  5,  7),   # Status asthmaticus
    "J81":  (_SEV_CRITICAL,  5, 10),   # Pulmonary oedema
    "J96":  (_SEV_CRITICAL, 10, 14),   # Respiratory failure
    # ── Abdominal / GI ───────────────────────────────────────────────────────
    "K25":  (_SEV_CRITICAL,  5,  7),   # Gastric ulcer, acute
    "K26":  (_SEV_CRITICAL,  5,  7),   # Duodenal ulcer, acute
    "K35":  (_SEV_CRITICAL,  5,  7),   # Acute appendicitis
    "K72":  (_SEV_CRITICAL, 10, 14),   # Hepatic failure
    "K80":  (_SEV_CRITICAL,  5,  7),   # Cholelithiasis, acute
    "K85":  (_SEV_SERIOUS,   7, 10),   # Acute pancreatitis
    "K57":  (_SEV_SERIOUS,   5,  7),   # Diverticular disease, acute
    "K52":  (_SEV_SERIOUS,   5,  7),   # Gastroenteritis, non-infectious
    "A09":  (_SEV_SERIOUS,   3,  5),   # Diarrhoea, infectious
    # ── Renal ────────────────────────────────────────────────────────────────
    "N10":  (_SEV_SERIOUS,   7, 10),   # Acute tubulointerstitial nephritis
    "N17":  (_SEV_CRITICAL, 10, 14),   # Acute kidney failure
    "N20":  (_SEV_SERIOUS,   3,  5),   # Renal calculus
    "N39":  (_SEV_SERIOUS,   5,  7),   # UTI (pyelonephritis)
    # ── Sepsis / Infection ───────────────────────────────────────────────────
    "A39":  (_SEV_CRITICAL, 10, 14),   # Meningococcal infection
    "A40":  (_SEV_CRITICAL, 10, 14),   # Streptococcal sepsis
    "A41":  (_SEV_CRITICAL, 10, 14),   # Other sepsis
    "L03":  (_SEV_SERIOUS,   7, 10),   # Cellulitis
    # ── Malaria (Nigeria-specific) ───────────────────────────────────────────
    "B50":  (_SEV_SERIOUS,   5,  7),   # Malaria, P. falciparum
    "B51":  (_SEV_SERIOUS,   5,  7),   # Malaria, P. vivax
    "B54":  (_SEV_SERIOUS,   5,  7),   # Unspecified malaria
    # ── Diabetes complications ───────────────────────────────────────────────
    "E10":  (_SEV_SERIOUS,   5,  7),   # Type 1 DM with acute complication
    "E11":  (_SEV_SERIOUS,   5,  7),   # Type 2 DM with acute complication
    "E13":  (_SEV_SERIOUS,   5,  7),   # Other specified DM
    "E14":  (_SEV_SERIOUS,   5,  7),   # Unspecified DM
    # ── Obstetric ────────────────────────────────────────────────────────────
    "O00":  (_SEV_CRITICAL,  5,  7),   # Ectopic pregnancy
    "O08":  (_SEV_CRITICAL,  5,  7),   # Post-abortal complications
    "O20":  (_SEV_SERIOUS,   3,  5),   # Haemorrhage in early pregnancy
    "O44":  (_SEV_SERIOUS,   5,  7),   # Placenta praevia
    "O62":  (_SEV_CRITICAL,  3,  5),   # Abnormal forces of labour
    "O67":  (_SEV_CRITICAL,  3,  5),   # Labour with haemorrhage
    "P07":  (_SEV_SERIOUS,  14, 21),   # Disorders related to prematurity
    # ── Trauma / Shock ───────────────────────────────────────────────────────
    "R57":  (_SEV_CRITICAL,  7, 14),   # Shock
    "S02":  (_SEV_CRITICAL,  7, 14),   # Fracture of skull
    "S06":  (_SEV_CRITICAL,  7, 14),   # Intracranial injury
    "S12":  (_SEV_CRITICAL,  7, 14),   # Fracture of cervical spine
    "S22":  (_SEV_SERIOUS,   5,  7),   # Fracture of rib/sternum
    "S32":  (_SEV_CRITICAL,  7, 14),   # Fracture of lumbar spine / pelvis
    # ── Clearly OUTPATIENT ───────────────────────────────────────────────────
    "J00":  (_SEV_OUTPATIENT, 0, 0),   # Common cold
    "J02":  (_SEV_OUTPATIENT, 0, 0),   # Acute pharyngitis
    "J03":  (_SEV_OUTPATIENT, 0, 0),   # Acute tonsillitis (uncomplicated)
    "J06":  (_SEV_OUTPATIENT, 0, 0),   # Acute upper respiratory infection
    "J20":  (_SEV_OUTPATIENT, 0, 0),   # Acute bronchitis
    "J30":  (_SEV_OUTPATIENT, 0, 0),   # Vasomotor / allergic rhinitis
    "H10":  (_SEV_OUTPATIENT, 0, 0),   # Conjunctivitis
    "K21":  (_SEV_OUTPATIENT, 0, 0),   # Gastro-oesophageal reflux disease
    "K29":  (_SEV_OUTPATIENT, 0, 0),   # Gastritis (uncomplicated)
    "M54":  (_SEV_OUTPATIENT, 0, 0),   # Dorsalgia / back pain
    "Z00":  (_SEV_OUTPATIENT, 0, 0),   # Routine health examination
    "Z13":  (_SEV_OUTPATIENT, 0, 0),   # Screening
    "Z71":  (_SEV_OUTPATIENT, 0, 0),   # Counselling / advice
    "Z76":  (_SEV_OUTPATIENT, 0, 0),   # Encounter for administrative purposes
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _code_severity(icd_code: str) -> Optional[Tuple[str, int, int]]:
    """Return (severity, max_days_general, max_days_private) for an ICD-10 code.
    Tries 4-char prefix then 3-char. Returns None if not catalogued."""
    code = icd_code.strip().upper()
    for length in (4, 3):
        prefix = code[:length]
        if prefix in ADMISSION_SEVERITY:
            return ADMISSION_SEVERITY[prefix]
    return None


def score_diagnoses(diagnosis_codes: List[str]) -> Tuple[str, int, int]:
    """Return overall (severity, max_days_general, max_days_private).
    Most severe diagnosis wins; ties keep the more generous duration."""
    _ORDER = [_SEV_CRITICAL, _SEV_SERIOUS, _SEV_OUTPATIENT, "UNKNOWN"]

    best_sev    = "UNKNOWN"
    best_days_g = 0
    best_days_p = 0

    for code in diagnosis_codes:
        result = _code_severity(code)
        if result is None:
            continue
        sev, days_g, days_p = result
        if _ORDER.index(sev) < _ORDER.index(best_sev):
            best_sev    = sev
            best_days_g = days_g
            best_days_p = days_p
        elif sev == best_sev:
            best_days_g = max(best_days_g, days_g)
            best_days_p = max(best_days_p, days_p)

    return best_sev, best_days_g, best_days_p


def check_prior_admission(
    enrollee_id: str,
    encounter_date: str,
    conn,
    lookback_days: int = 30,
) -> Optional[Dict]:
    """Check PA DATA for a prior ADM code within lookback_days.
    Returns detail dict or None if no prior admission found."""
    try:
        enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
        lookback = (enc_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        enc_str  = enc_dt.strftime("%Y-%m-%d")

        row = conn.execute("""
            SELECT MAX(CAST(requestdate AS DATE))                    AS last_adm,
                   COALESCE(MAX(TRY_CAST(quantity AS INTEGER)), 1)  AS days_granted
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE IID = ?
              AND UPPER(LEFT(TRIM(code), 3)) = 'ADM'
              AND CAST(requestdate AS DATE) >= ?
              AND CAST(requestdate AS DATE) <= ?
        """, [enrollee_id, lookback, enc_str]).fetchone()

        if not (row and row[0]):
            return None

        adm_date     = (row[0] if hasattr(row[0], "date")
                        else datetime.strptime(str(row[0])[:10], "%Y-%m-%d").date())
        days_granted = int(row[1]) if row[1] else 1
        discharge_dt = adm_date + timedelta(days=days_granted)

        return {
            "last_admission_date":  str(adm_date),
            "days_granted":         days_granted,
            "discharge_date":       str(discharge_dt),
            "days_since_admission": (enc_dt - adm_date).days,
        }
    except Exception as e:
        logger.warning(f"check_prior_admission DB error: {e}")
        return None


# ── Main entry point ──────────────────────────────────────────────────────────

def run_admission_prechecks(
    enrollee_id: str,
    encounter_date: str,
    admission_code: str,
    days: int,
    diagnosis_codes: List[str],
    diagnosis_names: Dict[str, str],
    conn,
) -> Dict:
    """
    Rule-based admission pre-screening.

    Only auto-DENY when all diagnoses are outpatient-appropriate.
    All other cases route to PENDING_REVIEW with pre-check advice for the agent.

    Returns:
        decision        — "DENY" | "PENDING_REVIEW"
        auto_decided    — True only for DENY
        severity        — "CRITICAL" | "SERIOUS" | "OUTPATIENT" | "UNKNOWN"
        triggered_rules — list of rule dicts that fired
        summary         — human-readable advisory for the agent
        readmission     — prior admission dict or None
        duration_flag   — bool
    """
    triggered: List[Dict] = []

    # Rule 1: Severity scoring
    severity, max_days_g, max_days_p = score_diagnoses(diagnosis_codes)

    # Rule 2: Prior admission — readmission flag
    prior            = check_prior_admission(enrollee_id, encounter_date, conn)
    readmission_flag = prior is not None

    if readmission_flag:
        triggered.append({
            "rule":   "READMISSION_30_DAYS",
            "detail": (
                f"Enrollee had a prior admission {prior['days_since_admission']} day(s) ago "
                f"(admitted {prior['last_admission_date']}, discharge {prior['discharge_date']}, "
                f"{prior['days_granted']} day(s) granted). "
                "Readmission within 30 days requires clinical justification."
            ),
        })

    # Rule 3: Duration appropriateness
    duration_flag = False
    max_days      = max_days_p if admission_code == "ADM01" else max_days_g

    if severity not in (_SEV_OUTPATIENT, "UNKNOWN") and max_days > 0 and days > max_days:
        duration_flag = True
        triggered.append({
            "rule":   "DURATION_EXCEEDS_EXPECTED",
            "detail": (
                f"Requested {days} day(s) exceeds the expected ceiling of {max_days} day(s) "
                f"for {severity}-severity diagnoses. "
                "Agent to verify if the extended stay is clinically justified."
            ),
        })

    # Rule 4: All diagnoses are outpatient-appropriate → DENY (only auto-decision)
    if severity == _SEV_OUTPATIENT:
        named = [f"{c} ({diagnosis_names.get(c, c)})" for c in diagnosis_codes]
        triggered.append({
            "rule":   "OUTPATIENT_DIAGNOSIS_ONLY",
            "detail": (
                f"All submitted diagnoses — {', '.join(named)} — are outpatient-appropriate "
                "conditions that do not meet the clinical threshold for inpatient admission "
                "per standard HMO care guidelines."
            ),
        })
        return {
            "decision":        "DENY",
            "auto_decided":    True,
            "severity":        severity,
            "triggered_rules": triggered,
            "summary":         "All diagnoses are outpatient-appropriate — inpatient admission not warranted.",
            "readmission":     prior,
            "duration_flag":   duration_flag,
        }

    # All other cases → PENDING_REVIEW with advisory
    if severity == "UNKNOWN":
        triggered.append({
            "rule":   "SEVERITY_UNKNOWN",
            "detail": (
                f"Could not determine clinical severity for ICD-10 code(s): "
                f"{', '.join(diagnosis_codes)}. Agent to confirm clinical necessity."
            ),
        })

    sev_label = {"CRITICAL": "critical", "SERIOUS": "serious"}.get(severity, "unverified")
    if triggered:
        summary = (
            f"{severity} severity — admission may be clinically warranted; "
            f"{len(triggered)} advisory flag(s) for agent review."
        )
    else:
        summary = (
            f"Inpatient admission appears clinically justified — {sev_label} diagnosis. "
            f"{days} day(s) within the expected range."
        )

    return {
        "decision":        "PENDING_REVIEW",
        "auto_decided":    False,
        "severity":        severity,
        "triggered_rules": triggered,
        "summary":         summary,
        "readmission":     prior,
        "duration_flag":   duration_flag,
    }
