#!/usr/bin/env python3
"""
30-Day Duplicate Validation Engine - PROCEDURES ONLY
=====================================================

Prevents duplicate procedures within 30-day rolling window by checking:
1. Exact duplicates (same code used again)  
2. Therapeutic class duplicates (different code, same class)

Classification Hierarchy (v3.6):
1. PROCEDURE_MASTER (curated, instant)
2. Learning table (previously verified pairs)
3. RxNorm + RxClass (NLM authority for DRUGS only)
4. Haiku AI (labs, surgeries, and drugs not in RxNorm)
5. PubMed (second opinion for unrecognized items)
6. Unrecognized → agent verifies

Author: Casey's AI Assistant
Date: February 2026
Version: 3.6 - Single Source of Truth + RxNorm/RxClass + PubMed
"""

import os
import re
import anthropic
import duckdb
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from . import mongo_db

@dataclass
class HistoryItem:
    """Single procedure from 30-day history"""
    code: str
    description: str
    therapeutic_class: str
    source: str  # "PA" or "Claims"
    date: str
    classification_source: str  # "master" or "ai"


@dataclass
class ThirtyDayValidation:
    """Result of 30-day validation"""
    validation_type: str  # "PROCEDURE_30DAY"
    input_code: str
    input_description: str
    input_therapeutic_class: str
    history_items: List[HistoryItem]
    has_exact_duplicate: bool
    exact_duplicate_items: List[HistoryItem]
    has_class_duplicate: bool
    class_duplicate_items: List[HistoryItem]
    passed: bool
    reasoning: str
    used_ai_for_classification: bool
    ai_confidence: Optional[float] = None
    used_web_search: bool = False
    
    def get_denial_reason(self) -> str:
        """Generate detailed denial reason with clear INPUT vs PRIOR labels and source"""
        if not self.passed:
            reasons = []
            
            if self.has_exact_duplicate:
                for item in self.exact_duplicate_items:
                    reasons.append(
                        f"❌ EXACT DUPLICATE: INPUT {self.input_code} ({self.input_description}) "
                        f"was already used on {item.date} ({item.source})"
                    )
            
            if self.has_class_duplicate:
                for item in self.class_duplicate_items:
                    source_label = {"master": "📖 Master", "rxnorm": "💊 RxNorm", "ai": "🤖 AI", "pubmed": "🔬 PubMed", "learning_table": "📚 Learned"}.get(item.classification_source, item.classification_source)
                    reasons.append(
                        f"❌ THERAPEUTIC CLASS DUPLICATE ({source_label}): "
                        f"INPUT {self.input_code} ({self.input_description}) "
                        f"is class [{self.input_therapeutic_class}] — "
                        f"PRIOR {item.code} ({item.description}) is also [{item.therapeutic_class}], "
                        f"used on {item.date} ({item.source})"
                    )
            
            return "\n".join(reasons)
        else:
            NON_CLASSES = {"Unknown", "Unrecognized", "Non-Medical Product"}
            if len(self.history_items) == 0:
                if self.input_therapeutic_class in NON_CLASSES:
                    return f"⚠️ APPROVED (30-day): No procedures in last 30 days, but INPUT {self.input_code} ({self.input_description}) is {self.input_therapeutic_class} — agent should verify"
                return "✅ APPROVED: No procedures found in last 30 days"
            else:
                class_list = ", ".join([
                    f"{item.therapeutic_class}" 
                    for item in self.history_items
                ])
                # Flag any unrecognized items
                unrecognized = [
                    item for item in self.history_items
                    if item.therapeutic_class in NON_CLASSES
                ]
                msg = (
                    f"✅ APPROVED: {self.input_therapeutic_class} is different from "
                    f"previously used classes ({class_list})"
                )
                if self.input_therapeutic_class in NON_CLASSES:
                    msg += f" ⚠️ INPUT is {self.input_therapeutic_class} — agent should verify"
                if unrecognized:
                    unrec_list = ", ".join([f"{i.code} ({i.description})" for i in unrecognized])
                    msg += f" ⚠️ Unrecognized history items: {unrec_list}"
                return msg


class ThirtyDayValidationEngine:
    """Engine for 30-day duplicate validation (PROCEDURES ONLY)"""
    
    def __init__(self, db_path: str = "ai_driven_data.duckdb", conn=None, learning_engine=None):
        """
        Initialize engine
        
        Parameters:
        -----------
        db_path : str
            Path to DuckDB database
        conn : duckdb.Connection, optional
            Existing connection to reuse (prevents multiple connection conflicts)
        """
        self.db_path = db_path
        
        # Reuse existing connection if provided
        if conn is not None:
            self.conn = conn
            self._owns_connection = False
        else:
            self.conn = duckdb.connect(db_path, read_only=True)
            self._owns_connection = True
        
        # Learning engine for checking previously learned class relationships
        self.learning_engine = learning_engine
        
        # Initialize Anthropic client
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment")
        self.client = anthropic.Anthropic(api_key=api_key, max_retries=5, timeout=120.0)
        
        # RxNorm cache — avoids re-querying NLM for the same drug in one session
        # Key: cleaned drug name → Value: {'rxcui': str, 'class': str} or None
        self._rxnorm_cache: Dict[str, Optional[Dict]] = {}
    
    def close(self):
        """Close connection if we own it"""
        if self._owns_connection and self.conn:
            self.conn.close()
    
    def __enter__(self):
        """Context manager support"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection on context exit"""
        self.close()
        return False
    
    def _get_window_days(self, procedure_code: str) -> int:
        """Return the lookback window in days for this procedure from PROCEDURE_MASTER.
        Defaults to 30 if the procedure is not in master."""
        try:
            from apis.vetting import mongo_db as _mdb
            doc = _mdb.get_procedure_master(procedure_code)
            if doc and doc.get("thirty_day_check"):
                return int(doc["thirty_day_check"])
        except Exception:
            pass
        return 30

    def _get_30_day_procedures(self, enrollee_id: str, encounter_date: str, window_days: int = 30) -> List[Dict]:
        """
        Get all procedures for this enrollee in the last `window_days` days.

        Queries both PA DATA and CLAIMS DATA

        Returns list of dicts: [{'code': ..., 'date': ..., 'source': ...}, ...]
        """
        try:
            encounter_dt = datetime.fromisoformat(encounter_date)
        except:
            encounter_dt = datetime.strptime(encounter_date, '%Y-%m-%d')

        date_30_days_ago = (encounter_dt - timedelta(days=window_days)).strftime('%Y-%m-%d')
        
        # Query PA DATA
        pa_query = f"""
        SELECT DISTINCT 
            LOWER(TRIM(code)) as code,
            requestdate as date,
            'PA' as source
        FROM "AI DRIVEN DATA"."PA DATA"
        WHERE IID = '{enrollee_id}'
          AND requestdate >= '{date_30_days_ago}'
          AND requestdate < '{encounter_date}'
          AND code IS NOT NULL
          AND TRIM(code) != ''
        """
        
        # Query CLAIMS DATA
        claims_query = f"""
        SELECT DISTINCT 
            LOWER(TRIM(code)) as code,
            encounterdatefrom as date,
            'Claims' as source
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE enrollee_id = '{enrollee_id}'
          AND encounterdatefrom >= '{date_30_days_ago}'
          AND encounterdatefrom < '{encounter_date}'
          AND code IS NOT NULL
          AND TRIM(code) != ''
        """
        
        procedures = []
        
        try:
            pa_results = self.conn.execute(pa_query).fetchdf()
            for _, row in pa_results.iterrows():
                procedures.append({
                    'code': row['code'],
                    'date': str(row['date']),
                    'source': row['source'],
                    'description': ''  # Populated below
                })
        except Exception as e:
            print(f"Warning: Could not query PA DATA: {e}")
        
        try:
            claims_results = self.conn.execute(claims_query).fetchdf()
            for _, row in claims_results.iterrows():
                procedures.append({
                    'code': row['code'],
                    'date': str(row['date']),
                    'source': row['source'],
                    'description': ''  # Populated below
                })
        except Exception as e:
            print(f"Warning: Could not query CLAIMS DATA: {e}")
        
        # Resolve descriptions for each procedure code
        # (needed for RxNorm lookup and display)
        desc_cache = {}
        for proc in procedures:
            code = proc['code']
            if code not in desc_cache:
                master_info = self._get_procedure_from_master(code)
                desc_cache[code] = master_info['description'] if master_info else "Unknown"
            proc['description'] = desc_cache[code]
        
        return procedures
    
    def _get_procedure_from_master(self, code: str) -> Optional[Dict]:
        """
        Get procedure info from PROCEDURE_MASTER, falling back to PROCEDURE DATA
        
        Returns dict with keys: code, description, therapeutic_class
        
        FIXED v3.3: Also checks "AI DRIVEN DATA"."PROCEDURE DATA" table
        when code isn't found in PROCEDURE_MASTER. This resolves the "Unknown"
        therapeutic class issue for codes like DRG1106 (Amlodipine 10mg) that
        exist in PROCEDURE DATA but not PROCEDURE_MASTER.
        """
        # First try PROCEDURE_MASTER (has therapeutic class)
        try:
            doc = mongo_db.get_procedure_master(code)
            if doc:
                return {
                    'code': doc.get('procedure_code', code),
                    'description': doc.get('procedure_name'),
                    'therapeutic_class': doc.get('procedure_class'),
                }
        except Exception as e:
            print(f"Warning: Could not fetch procedure from master: {e}")
        
        # Fallback: Check PROCEDURE DATA (has description but no class)
        try:
            fallback_query = f"""
            SELECT procedurecode, proceduredesc
            FROM "AI DRIVEN DATA"."PROCEDURE DATA"
            WHERE LOWER(TRIM(procedurecode)) = '{code.lower().strip()}'
            LIMIT 1
            """
            result = self.conn.execute(fallback_query).fetchdf()
            if len(result) > 0:
                desc = str(result.iloc[0]['proceduredesc']).strip()
                return {
                    'code': result.iloc[0]['procedurecode'],
                    'description': desc,
                    'therapeutic_class': None  # Will trigger AI classification
                }
        except Exception as e:
            print(f"Warning: Could not fetch procedure from PROCEDURE DATA: {e}")
        
        return None
    
    # ===================================================================
    # RxNorm + RxClass — NLM Authority for Drug Classification
    # ===================================================================
    
    def _clean_drug_name(self, description: str) -> str:
        """
        Clean a drug description for RxNorm lookup.
        
        Strips dosages, forms, quantities to get the generic drug name.
        'AMOXICILLIN 500MG CAPS X 10' → 'amoxicillin'
        'ARTEMETHER INJ 80MG' → 'artemether'
        'VITAMIN C 100MG TABS' → 'vitamin c'
        'MOKO MIST ALBA 200ML' → 'moko mist alba'
        """
        name = description.upper().strip()
        
        # Remove quantity patterns like "X 10", "X10", "x 20"
        name = re.sub(r'\bX\s*\d+\b', '', name, flags=re.IGNORECASE)
        
        # Remove dosage patterns: 500MG, 100 MG, 80mg, 10ML, 200ml, 5mg/5ml
        name = re.sub(r'\d+\.?\d*\s*(?:MG|MCG|ML|G|IU|MG/ML|MG/\d+ML|%)\b', '', name, flags=re.IGNORECASE)
        
        # Remove dosage forms
        forms = r'\b(?:TABS?|TABLETS?|CAPS?|CAPSULES?|INJ|INJECTION|SYRUP|SYRP|SUSP|SUSPENSION|CREAM|OINTMENT|GEL|DROPS?|SOLUTION|SOL|INHALER|SUPPOSITORY|PATCH|LOTION|SPRAY|POWDER|GRANULES?|SACHETS?|VIALS?|AMPOULES?|AMP|PESSARY|ENEMA|MIST|ELIXIR)\b'
        name = re.sub(forms, '', name, flags=re.IGNORECASE)
        
        # Remove parenthetical content like (FBC), (PCM)
        name = re.sub(r'\([^)]*\)', '', name)
        
        # Clean up multiple spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name.lower()
    
    def _rxnorm_lookup(self, drug_name: str, timeout: int = 8) -> Optional[Dict]:
        """
        Look up a drug in RxNorm to get RxCUI and then therapeutic class from RxClass.
        
        Returns:
            dict with keys: rxcui, drug_name_normalized, therapeutic_class, atc_code
            None if not found in RxNorm (meaning it's not a recognized drug)
        """
        cleaned = self._clean_drug_name(drug_name)
        
        # Check cache first
        if cleaned in self._rxnorm_cache:
            cached = self._rxnorm_cache[cleaned]
            if cached:
                print(f"  💊 RxNorm cache hit: {cleaned} → {cached['therapeutic_class']}")
            return cached
        
        try:
            # Step 1: Search RxNorm for approximate match
            encoded_term = urllib.parse.quote(cleaned)
            url = f"https://rxnav.nlm.nih.gov/REST/approximateTerm.json?term={encoded_term}&maxEntries=3"
            
            req = urllib.request.Request(url, headers={'Accept': 'application/json'})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            
            # Extract candidates
            candidates = data.get('approximateGroup', {}).get('candidate', [])
            if not candidates:
                print(f"  💊 RxNorm: '{cleaned}' not found — not a recognized drug")
                self._rxnorm_cache[cleaned] = None
                return None
            
            # Get the best match (highest score)
            best = candidates[0]
            rxcui = best.get('rxcui')
            rx_name = best.get('name', '')
            score = int(best.get('score', 0))
            
            # Require reasonable match score (RxNorm scores 0-100)
            if score < 60:
                print(f"  💊 RxNorm: '{cleaned}' best match '{rx_name}' score {score} too low — skipping")
                self._rxnorm_cache[cleaned] = None
                return None
            
            if not rxcui:
                self._rxnorm_cache[cleaned] = None
                return None
            
            print(f"  💊 RxNorm: '{cleaned}' → RxCUI {rxcui} ({rx_name}, score: {score})")
            
            # Step 2: Get therapeutic class from RxClass
            therapeutic_class = self._rxclass_lookup(rxcui, timeout=timeout)
            
            if therapeutic_class:
                result = {
                    'rxcui': rxcui,
                    'drug_name_normalized': rx_name,
                    'therapeutic_class': therapeutic_class,
                    'score': score
                }
                self._rxnorm_cache[cleaned] = result
                return result
            else:
                # RxNorm found it but RxClass has no class — still a real drug
                # Use the RxNorm name for AI classification (better than raw description)
                result = {
                    'rxcui': rxcui,
                    'drug_name_normalized': rx_name,
                    'therapeutic_class': None,  # AI will classify
                    'score': score
                }
                self._rxnorm_cache[cleaned] = result
                return result
                
        except urllib.error.URLError as e:
            print(f"  💊 RxNorm network error: {e}")
            return None
        except Exception as e:
            print(f"  💊 RxNorm error for '{drug_name}': {e}")
            return None
    
    def _rxclass_lookup(self, rxcui: str, timeout: int = 8) -> Optional[str]:
        """
        Get therapeutic class for an RxCUI from RxClass.
        
        Tries ATC first (WHO standard), then MED-RT (VA), then FDA SPL.
        Returns the therapeutic class name or None.
        """
        # Try multiple class sources in order of preference
        sources = [
            ('ATC', 'has_ATC'),          # WHO ATC classification — best for international use
            ('MEDRT', 'has_MoA'),         # MED-RT Mechanism of Action
            ('MEDRT', 'may_treat'),       # MED-RT therapeutic intent
        ]
        
        for rela_source, rela in sources:
            try:
                url = (
                    f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json"
                    f"?rxcui={rxcui}&relaSource={rela_source}&rela={rela}"
                )
                req = urllib.request.Request(url, headers={'Accept': 'application/json'})
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                
                classes = data.get('rxclassDrugInfoList', {}).get('rxclassDrugInfo', [])
                if classes:
                    # Get the most specific class (prefer level closest to drug)
                    class_name = classes[0].get('rxclassMinConceptItem', {}).get('className', '')
                    class_id = classes[0].get('rxclassMinConceptItem', {}).get('classId', '')
                    
                    if class_name:
                        # Normalize common ATC class names to match our system
                        normalized = self._normalize_rxclass(class_name)
                        print(f"  💊 RxClass ({rela_source}): RxCUI {rxcui} → {normalized} (raw: {class_name})")
                        return normalized
                        
            except Exception as e:
                continue
        
        print(f"  💊 RxClass: No therapeutic class found for RxCUI {rxcui}")
        return None
    
    def _normalize_rxclass(self, class_name: str) -> str:
        """
        Normalize RxClass/ATC class names to match our system's therapeutic classes.
        
        ATC classes can be very specific ('Beta-lactam antibacterials, penicillins')
        but our system uses broader groups ('Antibiotics'). This maps ATC → our classes.
        """
        name_lower = class_name.lower()
        
        # Antibiotics / Antibacterials
        if any(term in name_lower for term in ['antibacter', 'antibiotic', 'penicillin', 'cephalosporin', 
               'macrolide', 'quinolone', 'tetracycline', 'sulfonamide', 'aminoglycoside',
               'beta-lactam', 'carbapenem', 'glycopeptide']):
            return "Antibiotics"
        
        # Antimalarials
        if any(term in name_lower for term in ['antimalar', 'antiprotozoal']):
            return "Antimalarials"
        
        # Analgesics / Pain relief
        if any(term in name_lower for term in ['analgesic', 'antipyretic', 'anilide', 'paracetamol', 'acetaminophen']):
            return "Analgesics/Antipyretics"
        
        # NSAIDs
        if any(term in name_lower for term in ['anti-inflammatory', 'nsaid', 'propionic acid', 'acetic acid derivative',
               'oxicam', 'fenamate', 'coxib']):
            return "NSAIDs"
        
        # Antihypertensives
        if any(term in name_lower for term in ['antihypertensive', 'calcium channel', 'ace inhibitor', 
               'angiotensin', 'beta blocking', 'diuretic']):
            return "Antihypertensives"
        
        # Antidiabetics
        if any(term in name_lower for term in ['antidiabet', 'blood glucose', 'insulin', 'biguanide',
               'sulfonylurea', 'metformin']):
            return "Antidiabetics"
        
        # Antihistamines
        if 'antihistamine' in name_lower or 'histamine' in name_lower:
            return "Antihistamines"
        
        # Corticosteroids
        if 'corticosteroid' in name_lower or 'glucocorticoid' in name_lower:
            return "Corticosteroids"
        
        # Antacids / PPI / GI drugs
        if any(term in name_lower for term in ['antacid', 'proton pump', 'h2 receptor', 'antiulcer']):
            return "Antacids/Antiulcer"
        
        # Vitamins / Supplements
        if any(term in name_lower for term in ['vitamin', 'ascorbic', 'mineral', 'supplement', 'multivitamin']):
            return "Vitamin Supplements"
        
        # Antifungals
        if 'antifungal' in name_lower or 'antimycotic' in name_lower:
            return "Antifungals"
        
        # Antivirals
        if 'antiviral' in name_lower:
            return "Antivirals"
        
        # Anthelmintics (dewormers)
        if 'anthelmintic' in name_lower or 'anthelminthic' in name_lower:
            return "Anthelmintics"
        
        # Opioids
        if 'opioid' in name_lower or 'narcotic' in name_lower:
            return "Opioids"
        
        # Bronchodilators / Respiratory
        if any(term in name_lower for term in ['bronchodilat', 'adrenergic', 'respiratory', 'antiasthmatic']):
            return "Bronchodilators"
        
        # Antidepressants / Psychotropics
        if any(term in name_lower for term in ['antidepress', 'psycho', 'anxiolytic', 'ssri', 'benzodiazepine']):
            return "Psychotropics"
        
        # Anticonvulsants
        if 'antiepilep' in name_lower or 'anticonvuls' in name_lower:
            return "Anticonvulsants"
        
        # Muscle relaxants
        if 'muscle relax' in name_lower:
            return "Muscle Relaxants"
        
        # Antispasmodics
        if 'antispasmodic' in name_lower or 'spasmolytic' in name_lower:
            return "Antispasmodics"
        
        # Laxatives
        if 'laxative' in name_lower:
            return "Laxatives"
        
        # Antiemetics
        if 'antiemetic' in name_lower:
            return "Antiemetics"
        
        # Iron preparations / Haematinics
        if any(term in name_lower for term in ['iron prep', 'antianemic', 'haematinic', 'hematini']):
            return "Haematinics"
        
        # If no mapping found, use the ATC name directly (title case, cleaned)
        return class_name.title().strip()
    
    def _rxnorm_classify_batch(self, procedures: List[Dict]) -> Tuple[Dict[str, str], List[Dict]]:
        """
        Try to classify a batch of procedures using RxNorm/RxClass.
        
        Returns:
            (classifications, remaining)
            - classifications: {code: therapeutic_class} for items found in RxNorm
            - remaining: list of procedures NOT found in RxNorm (labs, surgeries, unknown)
        """
        classifications = {}
        remaining = []
        
        for proc in procedures:
            code = proc['code']
            description = proc.get('description', 'Unknown')
            
            if description in ('Unknown', '', None):
                remaining.append(proc)
                continue
            
            rxnorm_result = self._rxnorm_lookup(description)
            
            if rxnorm_result and rxnorm_result.get('therapeutic_class'):
                # RxNorm found it AND RxClass gave us a class → done
                classifications[code] = rxnorm_result['therapeutic_class']
                print(f"  💊 {code} ({description}) → {rxnorm_result['therapeutic_class']} (via RxNorm/RxClass)")
            elif rxnorm_result and not rxnorm_result.get('therapeutic_class'):
                # RxNorm found it (it IS a drug) but RxClass had no class
                # Send to AI with the normalized drug name for better classification
                proc_copy = dict(proc)
                proc_copy['description'] = rxnorm_result['drug_name_normalized']
                proc_copy['is_confirmed_drug'] = True
                remaining.append(proc_copy)
                print(f"  💊 {code}: RxNorm confirmed drug ({rxnorm_result['drug_name_normalized']}) but no RxClass → AI will classify")
            else:
                # Not in RxNorm → not a recognized drug → could be lab, surgery, or non-medical
                remaining.append(proc)
        
        return classifications, remaining
    
    def _classify_procedures_with_ai(
        self, 
        procedures: List[Dict],
        use_web_search: bool = False
    ) -> tuple[Dict[str, str], float]:
        """
        Use AI to classify therapeutic classes for procedures
        
        Parameters:
        -----------
        procedures : List[Dict]
            List of procedures with 'code' and 'description'
        use_web_search : bool
            Whether to use web search tool for better accuracy
        
        Returns:
        --------
        tuple: (classification_dict, confidence_score)
            - classification_dict: {procedure_code: therapeutic_class}
            - confidence_score: float 0-100
        """
        if not procedures:
            return {}, 100.0
        
        # Build prompt
        procedure_list = "\n".join([
            f"- {p['code']}: {p.get('description', 'Unknown')}"
            for p in procedures
        ])
        
        prompt = f"""You are a medical classification expert. Classify each procedure into its functional class.

IMPORTANT: "Procedure" in our system covers THREE types:
1. MEDICATIONS (drugs) → classify by therapeutic class
2. LABORATORY TESTS → classify by diagnostic category
3. SURGICAL PROCEDURES → classify by surgical category

PROCEDURES TO CLASSIFY:
{procedure_list}

INSTRUCTIONS:
1. First determine the TYPE of each procedure (medication, lab test, or surgery)
2. Classify into the appropriate functional class:

   MEDICATIONS: Use standard therapeutic classes
   Examples: "Antibiotics", "Antimalarials", "Analgesics/Antipyretics", "Antihypertensives", 
   "Vitamin Supplements", "NSAIDs", "Antihistamines", "Corticosteroids"

   LABORATORY TESTS: Use diagnostic category
   Examples: "Hematology Lab" (FBC, CBC, PCV, blood film), 
   "Parasitology Lab" (malaria thick/thin film, stool microscopy),
   "Clinical Chemistry" (LFT, RFT, electrolytes, lipid profile),
   "Microbiology Lab" (wound culture, urine MCS, blood culture),
   "Diagnostic Imaging" (X-ray, ultrasound, CT scan, MRI),
   "Urinalysis Lab" (urine analysis, urine dipstick),
   "Serology Lab" (HIV test, Hepatitis B, Widal test)

   SURGICAL PROCEDURES: Use surgical category
   Examples: "Gynecological Surgery" (myomectomy, hysterectomy, C-section),
   "General Surgery" (appendectomy, hernia repair, cholecystectomy),
   "Orthopedic Surgery" (fracture fixation, joint replacement),
   "Ophthalmic Surgery" (cataract extraction, trabeculectomy),
   "ENT Surgery" (tonsillectomy, adenoidectomy, septoplasty)

3. Group similar items into the SAME class:
   - Different antibiotics → ALL "Antibiotics"
   - FBC and CBC → BOTH "Hematology Lab"
   - Malaria thick film and thin film → BOTH "Parasitology Lab"
   - Myomectomy and hysterectomy → BOTH "Gynecological Surgery"
   - X-ray chest and X-ray pelvis → BOTH "Diagnostic Imaging"

4. CRITICAL — UNRECOGNIZED PRODUCTS:
   If you do NOT recognize a product as a standard pharmaceutical drug, lab test,
   or surgical procedure, classify it as "Unrecognized".
   Do NOT guess. Do NOT assume it belongs to the same class as other items in the list.
   
   Examples of unrecognized products:
   - Cosmetic/skincare products (MOKO MIST, body lotion, face cream)
   - Household items submitted fraudulently
   - Brand names you cannot identify as pharmaceutical
   
   "Unrecognized" → better than a wrong guess that creates false duplicate matches

5. Return a JSON object:

{{
  "classifications": {{
    "drg1234": "Antibiotics",
    "drg5678": "Hematology Lab",
    "drg9012": "Unrecognized"
  }},
  "confidence": 95
}}

Return ONLY the JSON object, no other text."""

        try:
            # Build message with or without web search
            if use_web_search:
                response = self.client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=2000,
                    temperature=0,
                    tools=[{
                        "type": "web_search_20250305",
                        "name": "web_search"
                    }],
                    messages=[{"role": "user", "content": prompt}]
                )
            else:
                response = self.client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=2000,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}]
                )
            
            # Extract text from response (handle tool use blocks)
            response_text = ""
            for block in response.content:
                if block.type == "text":
                    response_text += block.text
            
            response_text = response_text.strip()
            
            # Parse JSON
            # Remove markdown code blocks if present
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]
                response_text = response_text.strip()
            
            result = json.loads(response_text)
            
            # Extract classifications and confidence
            classifications = result.get("classifications", {})
            confidence = result.get("confidence", 80.0)
            
            # Normalize keys to lowercase
            classifications = {k.lower().strip(): v for k, v in classifications.items()}
            
            return classifications, float(confidence)
            
        except Exception as e:
            print(f"AI classification error: {e}")
            return {}, 0.0
    
    def _pubmed_classify_procedure(
        self,
        procedure_code: str,
        procedure_description: str,
        input_therapeutic_class: str
    ) -> Optional[str]:
        """
        Use PubMed to help classify an unrecognized procedure.
        
        Called when Haiku couldn't identify a procedure's therapeutic class.
        Searches PubMed for the procedure name, then asks Haiku to classify
        using the evidence.
        
        Parameters:
        -----------
        procedure_code : str
            The procedure code (e.g., drg2399)
        procedure_description : str  
            The procedure name (e.g., MOKO MIST ALBA 200ML)
        input_therapeutic_class : str
            The input procedure's known class, for context
            
        Returns:
        --------
        str or None: Therapeutic class if identified, None if still unknown
        """
        if not self.learning_engine:
            return None
        
        try:
            # Search PubMed for the procedure itself
            evidence = self.learning_engine.search_pubmed_evidence(
                procedure_description, 
                "pharmacology therapeutic class",  # generic medical context
                max_results=3,
                timeout=8
            )
            
            if not evidence.get('articles'):
                # Try with just the cleaned name + "drug" or "medication"
                clean_name = self.learning_engine._clean_for_pubmed(procedure_description)
                evidence = self.learning_engine.search_pubmed_evidence(
                    clean_name,
                    "drug medication",
                    max_results=3,
                    timeout=8
                )
            
            if not evidence.get('articles'):
                return None
            
            # Format evidence for Haiku
            evidence_text = self.learning_engine.format_pubmed_for_prompt(evidence)
            
            prompt = f"""Based on the PubMed evidence below, determine the THERAPEUTIC CLASS of this product.

Product: {procedure_description} (code: {procedure_code})

{evidence_text}

INSTRUCTIONS:
- If the evidence shows this is a PHARMACEUTICAL DRUG, classify by therapeutic class
  (e.g., "Antibiotics", "Antimalarials", "Analgesics/Antipyretics", "Vitamin Supplements", etc.)
- If the evidence shows this is a LABORATORY TEST, classify by diagnostic category
  (e.g., "Hematology Lab", "Clinical Chemistry", etc.)
- If the evidence shows this is a COSMETIC, SKINCARE, or NON-MEDICAL product, classify as "Non-Medical Product"
- If the evidence is insufficient or unclear, classify as "Unrecognized"

Return ONLY a JSON object:
{{
  "therapeutic_class": "the class name",
  "confidence": 0-100,
  "reasoning": "brief explanation"
}}"""
            
            response = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                temperature=0,
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = ""
            for block in response.content:
                if block.type == "text":
                    response_text += block.text
            response_text = response_text.strip()
            
            # Parse JSON
            if not response_text.startswith("{"):
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                if start >= 0 and end > start:
                    response_text = response_text[start:end]
                else:
                    return None
            
            result = json.loads(response_text)
            therapeutic_class = result.get("therapeutic_class", "Unrecognized")
            pm_confidence = result.get("confidence", 0)
            reasoning = result.get("reasoning", "")
            
            print(f"    PubMed+Haiku: {procedure_description} → {therapeutic_class} ({pm_confidence}%) — {reasoning[:80]}")
            
            if therapeutic_class in ("Unrecognized", "Unknown", "") or pm_confidence < 60:
                return None
            
            return therapeutic_class
            
        except Exception as e:
            print(f"    PubMed classification error for {procedure_description}: {e}")
            return None
    
    def validate_procedure_30_day(
        self,
        procedure_code: str,
        enrollee_id: str,
        encounter_date: str
    ) -> ThirtyDayValidation:
        """
        Validate if a procedure can be used (check 30-day duplicates)
        
        v3.6 CLASSIFICATION HIERARCHY (per comparison pair):
        
        1. PROCEDURE_MASTER — Both sides in master? Use master taxonomy.
        2. Learning table — Pair previously verified? Use stored decision.
        3. RxNorm/RxClass — Drug recognized by NLM? Use ATC class.
        4. Haiku AI — Labs, surgeries, or drugs not in RxNorm.
        5. PubMed — Haiku says "Unrecognized"? Search for evidence.
        6. Unrecognized — Nothing found? Agent verifies.
        
        CRITICAL RULE: If EITHER side of a comparison is not in master,
        BOTH go through the same non-master pipeline (RxNorm → AI → PubMed).
        Never compare master-classified vs AI-classified.
        """
        # Normalize code
        procedure_code = procedure_code.lower().strip()
        
        # Step 1: Input procedure from PROCEDURE_MASTER (always present)
        input_proc = self._get_procedure_from_master(procedure_code)
        
        if input_proc:
            input_description = input_proc['description']
            input_class_from_master = input_proc['therapeutic_class']
        else:
            input_description = "Unknown"
            input_class_from_master = None
        
        # If input found in PROCEDURE DATA (has name but no class), try RxNorm then AI
        if input_description != "Unknown" and not input_class_from_master:
            # Try RxNorm first — authoritative for drugs
            rxnorm_result = self._rxnorm_lookup(input_description)
            if rxnorm_result and rxnorm_result.get('therapeutic_class'):
                input_class_from_master = rxnorm_result['therapeutic_class']
                print(f"💊 Input {procedure_code} pre-classified by RxNorm: {input_class_from_master}")
            else:
                # RxNorm couldn't classify — defer to Step 4 (AI batch with history)
                # Don't classify input alone here; let it go through the full pipeline
                # where it's classified alongside history items for consistency
                pass
        
        input_therapeutic_class = input_class_from_master or "Unknown"
        
        # Step 2: Get history within procedure-specific window
        window_days = self._get_window_days(procedure_code)
        history_procedures = self._get_30_day_procedures(enrollee_id, encounter_date, window_days=window_days)
        
        if not history_procedures:
            return ThirtyDayValidation(
                validation_type="PROCEDURE_30DAY",
                input_code=procedure_code,
                input_description=input_description,
                input_therapeutic_class=input_therapeutic_class,
                history_items=[],
                has_exact_duplicate=False,
                exact_duplicate_items=[],
                has_class_duplicate=False,
                class_duplicate_items=[],
                passed=True,
                reasoning="✅ APPROVED: No procedures found in last 30 days",
                used_ai_for_classification=False
            )
        
        # ===============================================================
        # CONSULTATION PAIR RULE (CONS021 / CONS022)
        # ===============================================================
        # CONS021 = General Practitioner Consultation (initial visit)
        # CONS022 = Follow-up Consultation
        #
        # Business rule:
        #   - Max 1 CONS021 per 30-day window
        #   - Max 1 CONS022 per 30-day window
        #   - CONS022 must follow a CONS021 (no follow-up without initial)
        #   - After 30 days from CONS021, the cycle resets
        #
        # These are handled BEFORE general duplicate logic because they
        # are a linked pair — CONS021 + CONS022 are NOT class duplicates
        # of each other, they are complementary.
        # ===============================================================
        
        CONS_CODES = {'cons021', 'cons022'}
        input_upper = procedure_code.strip().upper()
        
        if input_upper in {'CONS021', 'CONS022'}:
            # Build minimal history items for CONS codes in the 30-day window
            cons_history = []
            for proc in history_procedures:
                code = proc['code'].strip().lower()
                if code in CONS_CODES:
                    cons_history.append(HistoryItem(
                        code=code,
                        description=proc['description'],
                        therapeutic_class="CONSULTATION",
                        source=proc['source'],
                        date=proc['date'],
                        classification_source="master"
                    ))
            
            has_cons021 = any(h.code == 'cons021' for h in cons_history)
            has_cons022 = any(h.code == 'cons022' for h in cons_history)
            cons021_items = [h for h in cons_history if h.code == 'cons021']
            cons022_items = [h for h in cons_history if h.code == 'cons022']
            
            if input_upper == 'CONS021':
                if has_cons021:
                    # Already had a GP consultation in 30 days → DENY
                    dates_str = ", ".join(h.date for h in cons021_items)
                    return ThirtyDayValidation(
                        validation_type="PROCEDURE_30DAY",
                        input_code=procedure_code,
                        input_description=input_description or "GENERAL PRACTITIONER CONSULTATION",
                        input_therapeutic_class="CONSULTATION",
                        history_items=cons_history,
                        has_exact_duplicate=True,
                        exact_duplicate_items=cons021_items,
                        has_class_duplicate=False,
                        class_duplicate_items=[],
                        passed=False,
                        reasoning=f"❌ DENIED: GP Consultation (CONS021) already used on {dates_str}. Only 1 allowed per 30-day window. Use CONS022 (Follow-up) instead.",
                        used_ai_for_classification=False
                    )
                else:
                    # No prior CONS021 → APPROVE
                    return ThirtyDayValidation(
                        validation_type="PROCEDURE_30DAY",
                        input_code=procedure_code,
                        input_description=input_description or "GENERAL PRACTITIONER CONSULTATION",
                        input_therapeutic_class="CONSULTATION",
                        history_items=cons_history,
                        has_exact_duplicate=False,
                        exact_duplicate_items=[],
                        has_class_duplicate=False,
                        class_duplicate_items=[],
                        passed=True,
                        reasoning="✅ APPROVED: No prior GP Consultation in last 30 days",
                        used_ai_for_classification=False
                    )
            
            elif input_upper == 'CONS022':
                if not has_cons021:
                    # No initial consultation → DENY follow-up
                    return ThirtyDayValidation(
                        validation_type="PROCEDURE_30DAY",
                        input_code=procedure_code,
                        input_description=input_description or "FOLLOW-UP CONSULTATION",
                        input_therapeutic_class="CONSULTATION",
                        history_items=cons_history,
                        has_exact_duplicate=False,
                        exact_duplicate_items=[],
                        has_class_duplicate=False,
                        class_duplicate_items=[],
                        passed=False,
                        reasoning="❌ DENIED: Follow-up Consultation (CONS022) requires a prior GP Consultation (CONS021) within the same 30-day window. No CONS021 found.",
                        used_ai_for_classification=False
                    )
                elif has_cons022:
                    # Already had a follow-up → DENY
                    dates_str = ", ".join(h.date for h in cons022_items)
                    return ThirtyDayValidation(
                        validation_type="PROCEDURE_30DAY",
                        input_code=procedure_code,
                        input_description=input_description or "FOLLOW-UP CONSULTATION",
                        input_therapeutic_class="CONSULTATION",
                        history_items=cons_history,
                        has_exact_duplicate=True,
                        exact_duplicate_items=cons022_items,
                        has_class_duplicate=False,
                        class_duplicate_items=[],
                        passed=False,
                        reasoning=f"❌ DENIED: Follow-up Consultation (CONS022) already used on {dates_str}. Only 1 follow-up allowed per 30-day window after a GP Consultation.",
                        used_ai_for_classification=False
                    )
                else:
                    # Has CONS021 but no CONS022 yet → APPROVE
                    cons021_date = cons021_items[0].date if cons021_items else "?"
                    return ThirtyDayValidation(
                        validation_type="PROCEDURE_30DAY",
                        input_code=procedure_code,
                        input_description=input_description or "FOLLOW-UP CONSULTATION",
                        input_therapeutic_class="CONSULTATION",
                        history_items=cons_history,
                        has_exact_duplicate=False,
                        exact_duplicate_items=[],
                        has_class_duplicate=False,
                        class_duplicate_items=[],
                        passed=True,
                        reasoning=f"✅ APPROVED: Valid follow-up to GP Consultation (CONS021) on {cons021_date}",
                        used_ai_for_classification=False
                    )
        
        # Step 3: Resolve each history procedure's class
        # 
        # CRITICAL LOGIC (v3.6 — single source of truth per comparison):
        #
        # For EACH history item, the rule is:
        #   - If BOTH input AND history item are in PROCEDURE_MASTER (with class)
        #     → use master for both (same taxonomy, safe to compare)
        #   - If EITHER side is NOT in master
        #     → check learning table (stores pairwise decisions)
        #     → if not in learning, BOTH go to AI batch together
        #       (AI classifies input + history item in one call = consistent)
        #
        # This prevents comparing master-classified input vs AI-classified history
        # which could use different class boundaries.
        
        input_in_master = bool(input_proc and input_proc.get('therapeutic_class'))
        
        print(f"30-DAY: Input {procedure_code} {'IS' if input_in_master else 'NOT'} in master (class: {input_therapeutic_class})")
        
        history_items = []
        needs_ai = []  # Procedures that need AI classification
        used_ai = False
        used_web_search = False
        confidence = 100.0
        
        for hist_proc in history_procedures:
            code = hist_proc['code']
            hist_desc = hist_proc['description']  # Pre-fetched in _get_30_day_procedures
            
            # Check if history item is in master WITH a therapeutic class
            hist_master = self._get_procedure_from_master(code)
            hist_in_master = bool(hist_master and hist_master.get('therapeutic_class'))
            
            # RULE: Both in master → use master taxonomy
            if input_in_master and hist_in_master:
                history_items.append(HistoryItem(
                    code=code,
                    description=hist_desc,
                    therapeutic_class=hist_master['therapeutic_class'],
                    source=hist_proc['source'],
                    date=hist_proc['date'],
                    classification_source="master"
                ))
                continue
            
            # RULE: Either side NOT in master → check learning table first
            # (learning stores pairwise relationships, always consistent)
            if self.learning_engine:
                learning_result = self.learning_engine.check_procedure_class_learning(
                    procedure_code, code
                )
                if learning_result:
                    therapeutic_class = learning_result['class_name'] if learning_result['same_class'] else "Different class"
                    history_items.append(HistoryItem(
                        code=code,
                        description=hist_desc,
                        therapeutic_class=therapeutic_class if learning_result['same_class'] else f"Not {input_therapeutic_class}",
                        source=hist_proc['source'],
                        date=hist_proc['date'],
                        classification_source="learning_table"
                    ))
                    # If input class is unknown but learning says they share a class,
                    # the shared class IS the input's class — set it now so the
                    # comparison step doesn't fail when needs_ai is empty.
                    if learning_result['same_class'] and input_therapeutic_class in ("Unknown", None, ""):
                        input_therapeutic_class = learning_result['class_name']
                        print(f"📚 Input {procedure_code} class inferred from learning: {input_therapeutic_class}")
                    continue
            
            # RULE: Not in master or learning → RxNorm/AI batch (both sides classified together)
            needs_ai.append({
                'code': code,
                'description': hist_desc,
                'source': hist_proc['source'],
                'date': hist_proc['date']
            })
        
        # Step 4: Classify items NOT resolved by master or learning
        # 
        # Pipeline: RxNorm/RxClass (drugs) → Haiku AI (labs/surgery/remaining) → PubMed (unrecognized)
        #
        if needs_ai:
            used_ai = True
            
            # ---------------------------------------------------------------
            # Step 4a: RxNorm/RxClass for drug classification
            # ---------------------------------------------------------------
            # RxNorm identifies real drugs and RxClass gives therapeutic class.
            # Items NOT in RxNorm are labs, surgeries, or non-medical products.
            # Include input in the batch if it wasn't resolved by master.
            
            rxnorm_batch = list(needs_ai)  # history items needing classification
            input_needs_rxnorm = not input_in_master or input_therapeutic_class in ("Unknown", None)
            
            if input_needs_rxnorm:
                rxnorm_batch.insert(0, {'code': procedure_code, 'description': input_description})
            
            print(f"💊 Step 4a: Trying RxNorm for {len(rxnorm_batch)} items...")
            rxnorm_classifications, remaining_after_rxnorm = self._rxnorm_classify_batch(rxnorm_batch)
            
            # Track which items RxNorm resolved
            rxnorm_resolved = set(rxnorm_classifications.keys())
            
            # Update input class if RxNorm resolved it
            if procedure_code in rxnorm_classifications:
                if input_needs_rxnorm:
                    input_therapeutic_class = rxnorm_classifications[procedure_code]
                    print(f"💊 Input {procedure_code} classified by RxNorm: {input_therapeutic_class}")
                # Remove input from remaining list
                remaining_after_rxnorm = [p for p in remaining_after_rxnorm if p['code'] != procedure_code]
            
            # Build history items for RxNorm-resolved items
            for proc in needs_ai:
                if proc['code'] in rxnorm_resolved:
                    history_items.append(HistoryItem(
                        code=proc['code'],
                        description=proc['description'],
                        therapeutic_class=rxnorm_classifications[proc['code']],
                        source=proc['source'],
                        date=proc['date'],
                        classification_source="rxnorm"
                    ))
            
            # Filter needs_ai to only items NOT resolved by RxNorm
            needs_ai_after_rxnorm = [
                p for p in needs_ai if p['code'] not in rxnorm_resolved
            ]
            
            print(f"💊 RxNorm resolved {len(rxnorm_resolved)} items, {len(needs_ai_after_rxnorm)} remaining for AI")
            
            # ---------------------------------------------------------------
            # Step 4b: Haiku AI for remaining items (labs, surgeries, non-drugs)
            # ---------------------------------------------------------------
            classifications = dict(rxnorm_classifications)  # Start with RxNorm results
            
            if needs_ai_after_rxnorm:
                # Build AI batch — include input for context
                ai_procedures = [
                    {'code': procedure_code, 'description': input_description}
                ] + [
                    {'code': p['code'], 'description': p['description']}
                    for p in needs_ai_after_rxnorm
                ]
                
                ai_classifications, confidence = self._classify_procedures_with_ai(
                    ai_procedures, use_web_search=False
                )
                
                if confidence < 85:
                    print(f"⚠️  AI confidence {confidence:.1f}% < 85%, retrying with web search...")
                    ai_classifications, confidence = self._classify_procedures_with_ai(
                        ai_procedures, use_web_search=True
                    )
                    used_web_search = True
                
                # Merge AI classifications (don't overwrite RxNorm results)
                for code, cls in ai_classifications.items():
                    if code not in classifications:
                        classifications[code] = cls
            else:
                confidence = 100.0
            
            # ---------------------------------------------------------------
            # Step 4c: PubMed for "Unrecognized" items
            # ---------------------------------------------------------------
            unrecognized_items = [
                p for p in needs_ai_after_rxnorm
                if classifications.get(p['code'], 'Unknown') in ('Unrecognized', 'Unknown', '')
            ]
            
            # Check if input itself is unrecognized (only if not resolved by RxNorm/master)
            input_unrecognized = (
                procedure_code not in rxnorm_resolved
                and not input_in_master
                and classifications.get(procedure_code, '') in ('Unrecognized', 'Unknown', '')
            )
            
            if (unrecognized_items or input_unrecognized) and self.learning_engine:
                print(f"🔬 Step 4c: {len(unrecognized_items)} unrecognized item(s) + input={'yes' if input_unrecognized else 'no'} — consulting PubMed...")
                
                pubmed_reclassified = set()
                
                items_to_lookup = []
                if input_unrecognized:
                    items_to_lookup.append({'code': procedure_code, 'description': input_description})
                items_to_lookup.extend(unrecognized_items)
                
                for item in items_to_lookup:
                    pubmed_class = self._pubmed_classify_procedure(
                        item['code'], item['description'], input_therapeutic_class
                    )
                    if pubmed_class and pubmed_class not in ('Unrecognized', 'Unknown'):
                        print(f"  🔬 PubMed identified {item['code']} ({item['description']}) → {pubmed_class}")
                        classifications[item['code']] = pubmed_class
                        pubmed_reclassified.add(item['code'])
                    else:
                        print(f"  🔬 PubMed could not identify {item['code']} ({item['description']}) — stays Unrecognized")
            else:
                pubmed_reclassified = set()
            
            # Store AI's classification of the input SEPARATELY
            # - Master-classified history items compare against master's input class
            # - AI/RxNorm-classified history items compare against AI's input class
            # This ensures SAME SOURCE on both sides of every comparison
            ai_input_class = classifications.get(procedure_code, None)
            
            if ai_input_class and not input_in_master and procedure_code not in rxnorm_resolved:
                # Input NOT in master or RxNorm — AI class becomes the primary
                input_therapeutic_class = ai_input_class
            
            # Build history items for AI/PubMed-classified items
            # (RxNorm-resolved items were already built above)
            for proc in needs_ai_after_rxnorm:
                code = proc['code']
                therapeutic_class = classifications.get(code, "Unknown")
                
                # Determine classification source
                if code in pubmed_reclassified:
                    cls_source = "pubmed"
                elif therapeutic_class in ('Unrecognized', 'Unknown', 'Non-Medical Product'):
                    cls_source = "unrecognized"
                else:
                    cls_source = "ai"
                
                history_items.append(HistoryItem(
                    code=code,
                    description=proc['description'],
                    therapeutic_class=therapeutic_class,
                    source=proc['source'],
                    date=proc['date'],
                    classification_source=cls_source
                ))
        else:
            ai_input_class = None
        
        # Step 5: Check for exact duplicates
        exact_duplicates = [
            item for item in history_items
            if item.code.lower() == procedure_code.lower()
        ]
        has_exact = len(exact_duplicates) > 0
        
        # Step 6: Check for class duplicates (same therapeutic class, different code)
        #
        # CRITICAL (v3.6): Source-aware comparison
        # - Master-classified history → compare against input's MASTER class
        # - RxNorm/AI/PubMed-classified history → compare against input's non-master class
        #   (RxNorm class names are normalized to match AI's taxonomy via _normalize_rxclass)
        # This prevents cross-taxonomy false matches.
        #
        NON_CLASSES = {"Unknown", "Unrecognized", "Non-Medical Product"}
        class_duplicates = []
        
        for item in history_items:
            # Skip exact duplicates (handled in Step 5)
            if item.code.lower() == procedure_code.lower():
                continue
            # Skip non-classifications
            if item.therapeutic_class in NON_CLASSES:
                continue
            if item.therapeutic_class.startswith("Not ") or item.therapeutic_class.startswith("Different"):
                continue
            
            # Pick the RIGHT input class based on source
            if item.classification_source in ("master", "learning_table"):
                # Master or learning-resolved → compare against master's input class
                # (most reliable, avoids AI taxonomy mismatch)
                compare_class = input_therapeutic_class
            else:
                # AI/RxNorm/PubMed-classified → compare against AI's input class
                compare_class = ai_input_class or input_therapeutic_class

            if compare_class in NON_CLASSES:
                continue

            # Case-insensitive comparison to handle taxonomy differences
            # e.g. "ANTIBIOTIC" vs "Antibiotic" vs "Antibiotics"
            if item.therapeutic_class.lower() == compare_class.lower():
                class_duplicates.append(item)
        
        has_class = len(class_duplicates) > 0
        
        # Create validation result
        validation = ThirtyDayValidation(
            validation_type="PROCEDURE_30DAY",
            input_code=procedure_code,
            input_description=input_description,
            input_therapeutic_class=input_therapeutic_class,
            history_items=history_items,
            has_exact_duplicate=has_exact,
            exact_duplicate_items=exact_duplicates,
            has_class_duplicate=has_class,
            class_duplicate_items=class_duplicates,
            passed=(not has_exact and not has_class),
            used_ai_for_classification=used_ai,
            ai_confidence=confidence if used_ai else None,
            used_web_search=used_web_search,
            reasoning=""
        )
        
        validation.reasoning = validation.get_denial_reason()
        
        return validation


if __name__ == "__main__":
    # Test the engine
    with ThirtyDayValidationEngine() as engine:
        
        print("\n" + "="*80)
        print("TESTING PROCEDURE 30-DAY VALIDATION")
        print("="*80)
        
        # Test with DRG3188 (VITAMIN C - should be in master as VITAMIN_SUPPLEMENT)
        proc_validation = engine.validate_procedure_30_day(
            procedure_code="DRG3188",
            enrollee_id="CL/OCTA/723449/2023-A",
            encounter_date="2024-12-01"
        )
        
        print(f"\nInput: {proc_validation.input_code} - {proc_validation.input_description}")
        print(f"Class: {proc_validation.input_therapeutic_class}")
        print(f"Used AI: {proc_validation.used_ai_for_classification}")
        if proc_validation.ai_confidence:
            print(f"AI Confidence: {proc_validation.ai_confidence:.1f}%")
        if proc_validation.used_web_search:
            print(f"Used Web Search: ✅")
        
        print(f"\n30-Day History ({len(proc_validation.history_items)} items):")
        
        for item in proc_validation.history_items:
            print(f"  - {item.code} ({item.description}) - Class: {item.therapeutic_class} - {item.source} on {item.date}")
        
        print(f"\nValidation Result: {'✅ PASSED' if proc_validation.passed else '❌ DENIED'}")
        print(f"Reasoning:\n{proc_validation.reasoning}")