"""
CLEARLINE — Drug API Utilities
================================
Shared helpers for external drug reference APIs:
  - RxNorm/RxClass  (NLM) — therapeutic class lookup & CUI resolution
  - WHO Essential Medicines List — formulary status
  - OpenFDA drug label — indications_and_usage check

All calls are cached per-process in module-level dicts.  Network failures
are silently swallowed and return safe empty defaults so callers can proceed
without the enrichment data.
"""

import re
import json
import logging
import urllib.request
import urllib.parse
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Module-level caches (per-process, no expiry) ─────────────────────────────
_rxnorm_cache: Dict[str, Optional[Dict]]  = {}   # cleaned_name → {rxcui, class, ...} | None
_who_eml_cache: Dict[str, Dict]           = {}   # cleaned_name → {found, essential, atc, ...}
_openfda_ind_cache: Dict[str, str]        = {}   # cleaned_name → indications text

# ── High-risk drug classes: trigger DDI check ─────────────────────────────────
# These are RxClass/ATC class substrings that warrant checking for interactions.
HIGH_RISK_CLASSES = {
    # Coagulation
    "anticoagulant", "coumarin", "warfarin", "heparin",
    # NSAIDs / Analgesics
    "nonsteroidal", "nsaid", "anti-inflammatory",
    # Antifungals (potent CYP3A4 inhibitors)
    "antifungal", "azole antifungal", "fluconazole", "ketoconazole", "itraconazole",
    # Antiepileptics (CYP inducers / narrow TI)
    "antiepileptic", "anticonvulsant", "carbamazepine", "phenytoin", "valproic",
    # Macrolide antibiotics (CYP3A4 inhibitors)
    "macrolide", "clarithromycin", "erythromycin",
    # Antiretrovirals
    "antiretroviral", "protease inhibitor", "hiv",
    # Antidiabetics (hypoglycaemia risk)
    "antidiabetic", "sulfonylurea", "glibenclamide", "metformin",
    # Cardiac (narrow TI)
    "digoxin", "cardiac glycoside", "antiarrhythmic",
    # Immunosuppressants
    "immunosuppressant", "ciclosporin", "tacrolimus",
}


# ══════════════════════════════════════════════════════════════════════════════
# RxNorm / RxClass
# ══════════════════════════════════════════════════════════════════════════════

def _clean_drug_name(name: str) -> str:
    """
    Strip dosage/route/pack from a drug description so RxNorm can find it.
    'ARTEMETHER 80MG INJ X5' → 'artemether'
    """
    name = re.sub(r'\s+\d+[\s./]*(mg|mcg|g|ml|iu|units?|miu)\b.*', '', name, flags=re.I)
    name = re.sub(
        r'\b(tab|tabs|tablet|tablets|cap|caps|capsule|capsules|'
        r'syrup|syrp|susp|suspension|inj|injection|iv|oral|solution|'
        r'sachet|drops|cream|ointment|gel|x\s*\d+|ampoule|vial)\b.*',
        '', name, flags=re.I
    )
    return re.sub(r'\s+', ' ', name).strip().lower()


def rxnorm_lookup(drug_name: str, timeout: int = 8) -> Optional[Dict]:
    """
    Resolve drug name to RxCUI and therapeutic class via NLM RxNorm + RxClass.

    Returns dict with keys: rxcui, drug_name_normalized, therapeutic_class (str or None)
    Returns None if drug not found or network unavailable.
    """
    cleaned = _clean_drug_name(drug_name)
    if not cleaned or len(cleaned) < 3:
        return None

    if cleaned in _rxnorm_cache:
        return _rxnorm_cache[cleaned]

    try:
        # Step 1: approximate match → RxCUI
        url = (
            f"https://rxnav.nlm.nih.gov/REST/approximateTerm.json"
            f"?term={urllib.parse.quote(cleaned)}&maxEntries=3"
        )
        req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                   "User-Agent": "Clearline-KLAIRE/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())

        candidates = data.get("approximateGroup", {}).get("candidate", [])
        if not candidates:
            _rxnorm_cache[cleaned] = None
            return None

        best  = candidates[0]
        rxcui = best.get("rxcui")
        score = int(best.get("score", 0))
        rx_name = best.get("name", drug_name)

        if not rxcui or score < 60:
            _rxnorm_cache[cleaned] = None
            return None

        # Step 2: RxClass — try ATC, then MED-RT
        therapeutic_class = _rxclass_for_cui(rxcui, timeout=timeout)

        result = {
            "rxcui":                rxcui,
            "drug_name_normalized": rx_name,
            "therapeutic_class":    therapeutic_class,
            "score":                score,
        }
        _rxnorm_cache[cleaned] = result
        return result

    except Exception as e:
        logger.debug(f"RxNorm lookup failed for '{drug_name}': {e}")
        _rxnorm_cache[cleaned] = None
        return None


def _rxclass_for_cui(rxcui: str, timeout: int = 8) -> Optional[str]:
    """Return therapeutic class name for an RxCUI from RxClass (ATC → MED-RT fallback)."""
    sources = [
        ("ATC1-4",  "isa"),
        ("MEDRT",   "has_EPC"),
        ("FDASPL",  "has_EPC"),
    ]
    for rela_source, rela in sources:
        try:
            url = (
                f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json"
                f"?rxcui={rxcui}&relaSource={rela_source}&rela={rela}"
            )
            req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                       "User-Agent": "Clearline-KLAIRE/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read())

            classes = data.get("rxclassDrugInfoList", {}).get("rxclassDrugInfo", [])
            if classes:
                class_name = classes[0].get("rxclassMinConceptItem", {}).get("className", "")
                if class_name:
                    return class_name
        except Exception:
            continue

    return None


def rxclass_get_drug_classes(drug_name: str) -> List[str]:
    """
    Return a list of all RxClass names for a drug (ATC + MED-RT).
    Returns empty list if not found.
    """
    result = rxnorm_lookup(drug_name)
    if not result:
        return []
    classes = []
    if result.get("therapeutic_class"):
        classes.append(result["therapeutic_class"])
    # Also try byDrugName endpoint for additional classes
    cleaned = _clean_drug_name(drug_name)
    try:
        url = (
            f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byDrugName.json"
            f"?drugName={urllib.parse.quote(cleaned)}&relaSource=MEDRT"
        )
        req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                   "User-Agent": "Clearline-KLAIRE/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
        items = data.get("rxclassDrugInfoList", {}).get("rxclassDrugInfo", [])
        for item in items:
            cn = item.get("rxclassMinConceptItem", {}).get("className", "")
            if cn and cn not in classes:
                classes.append(cn)
    except Exception:
        pass

    return classes


def is_high_risk_class(drug_name: str) -> bool:
    """
    Return True if this drug belongs to a high-risk interaction class.
    Used as a pre-filter before expensive DDI checks.
    """
    classes = rxclass_get_drug_classes(drug_name)
    classes_lower = " ".join(c.lower() for c in classes)
    return any(marker in classes_lower for marker in HIGH_RISK_CLASSES)


# ══════════════════════════════════════════════════════════════════════════════
# WHO Essential Medicines List
# ══════════════════════════════════════════════════════════════════════════════

def who_eml_lookup(drug_name: str) -> Dict:
    """
    Check if a drug is on the WHO Essential Medicines List.

    Returns:
        {
            "found": bool,
            "essential": bool,      # True if listed on EML
            "description": str,     # WHO description if found
            "atc_code": str,        # ATC code if available
        }
    """
    cleaned = _clean_drug_name(drug_name)
    if not cleaned:
        return {"found": False, "essential": False, "description": "", "atc_code": ""}

    if cleaned in _who_eml_cache:
        return _who_eml_cache[cleaned]

    try:
        url = f"https://list.essentialmeds.org/api/medicines?query={urllib.parse.quote(cleaned)}"
        req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                   "User-Agent": "Clearline-KLAIRE/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())

        results = data if isinstance(data, list) else data.get("results", data.get("medicines", []))

        if results:
            first  = results[0]
            desc   = first.get("name", "") or first.get("generic_name", "")
            atc    = first.get("atc_code", "") or first.get("atcCode", "")
            result = {"found": True, "essential": True, "description": desc, "atc_code": atc}
        else:
            result = {"found": False, "essential": False, "description": "", "atc_code": ""}

        _who_eml_cache[cleaned] = result
        return result

    except Exception as e:
        logger.debug(f"WHO EML lookup failed for '{drug_name}': {e}")
        result = {"found": False, "essential": False, "description": "", "atc_code": ""}
        _who_eml_cache[cleaned] = result
        return result


# ══════════════════════════════════════════════════════════════════════════════
# OpenFDA — drug label indications_and_usage
# ══════════════════════════════════════════════════════════════════════════════

_OPENFDA_LABEL_URL = "https://api.fda.gov/drug/label.json"


def openfda_get_indications(drug_name: str) -> str:
    """
    Fetch the indications_and_usage section from the FDA drug label.
    Returns the text (capped at 1500 chars) or empty string on failure.
    """
    cleaned = _clean_drug_name(drug_name)
    if not cleaned or len(cleaned) < 3:
        return ""

    if cleaned in _openfda_ind_cache:
        return _openfda_ind_cache[cleaned]

    try:
        q   = urllib.parse.quote(f'"{cleaned}"')
        url = f"{_OPENFDA_LABEL_URL}?search=openfda.generic_name:{q}&limit=1"
        req = urllib.request.Request(url, headers={"User-Agent": "Clearline-KLAIRE/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())

        results = data.get("results", [])
        if not results:
            # Fallback: text search
            url = f"{_OPENFDA_LABEL_URL}?search=indications_and_usage:{urllib.parse.quote(cleaned)}&limit=1"
            req = urllib.request.Request(url, headers={"User-Agent": "Clearline-KLAIRE/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            results = data.get("results", [])

        if not results:
            _openfda_ind_cache[cleaned] = ""
            return ""

        ind_sections = results[0].get("indications_and_usage", [])
        text = " ".join(ind_sections[:2])[:1500]
        _openfda_ind_cache[cleaned] = text
        return text

    except Exception as e:
        logger.debug(f"OpenFDA indications lookup failed for '{drug_name}': {e}")
        _openfda_ind_cache[cleaned] = ""
        return ""
