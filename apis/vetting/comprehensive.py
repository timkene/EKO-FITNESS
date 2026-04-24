#!/usr/bin/env python3
"""
COMPREHENSIVE VALIDATION ENGINE - FIXED
=========================================

CRITICAL FIX: All AI validation prompts now resolve procedure/diagnosis codes
to their actual names from the database BEFORE calling the AI. This prevents
the AI from hallucinating what "DRG1106" means (e.g., confusing it with
standard DRG codes instead of recognizing it as "Amlodipine 10mg").

The fix applies the same name-resolution pattern already used in
vetting_learning_engine.py's validate_procedure_diagnosis() method,
but extends it to all 5 individual rule validators.

Changes from v2.0:
- Added _resolve_procedure_info() helper
- Added _resolve_diagnosis_info() helper  
- Rewrote all 5 AI prompt methods to include resolved names
- Added system prompt establishing internal code context
- AI now receives: code + resolved name + category + patient context

Runs ALL validation rules and provides complete report showing:
- Which rules passed/failed
- Which source validated each (master vs AI)
- Overall decision (ALL must pass for APPROVE)
- Stores AI-approved rules in learning tables

New Learning Tables:
- ai_human_procedure_age
- ai_human_procedure_gender
- ai_human_diagnosis_age
- ai_human_diagnosis_gender

Author: Casey
Date: February 2026
Version: 3.0 - Fixed AI Code Resolution
"""

import os
import duckdb
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from . import mongo_db

logger = logging.getLogger(__name__)


def _anthropic_create_with_retry(client, **kwargs):
    """Call client.messages.create with exponential backoff on 529 overload errors."""
    import time
    last_err = None
    for attempt in range(3):  # delays: 0, 2, 4 s
        try:
            return client.messages.create(**kwargs)
        except Exception as e:
            last_err = e
            if "529" in str(e) or "overloaded" in str(e).lower():
                wait = 2 ** attempt
                logger.warning(f"Anthropic overloaded, retry {attempt+1}/3 in {wait}s")
                time.sleep(wait)
            else:
                raise
    raise last_err

# Import 30-day validation engine
try:
    from .thirty_day import ThirtyDayValidationEngine, ThirtyDayValidation
    THIRTY_DAY_AVAILABLE = True
except ImportError:
    THIRTY_DAY_AVAILABLE = False
    print("Warning: 30-day validation engine not available")

@dataclass
class RuleResult:
    """Individual rule validation result"""
    rule_name: str  # "PROCEDURE_AGE", "DIAGNOSIS_GENDER", etc.
    passed: bool
    source: str  # "master_table" or "ai"
    confidence: int  # 0-100
    reasoning: str
    details: Dict = field(default_factory=dict)

@dataclass
class ComprehensiveValidation:
    """Complete validation report"""
    overall_decision: str  # "APPROVE" or "DENY"
    overall_confidence: int
    overall_reasoning: str
    rule_results: List[RuleResult]
    requires_human_review: bool
    can_store_ai_approvals: bool  # True if any AI approvals to store
    auto_deny: bool = False  # True if all failed rules are high-confidence learned denials
    auto_deny_rules: List[RuleResult] = field(default_factory=list)  # Which rules triggered it
    
    def get_failed_rules(self) -> List[RuleResult]:
        """Get all failed rules"""
        return [r for r in self.rule_results if not r.passed]
    
    def get_ai_approved_rules(self) -> List[RuleResult]:
        """Get AI-approved rules for storage (DEPRECATED - use get_ai_validated_rules)"""
        return [r for r in self.rule_results if r.passed and r.source == "ai"]
    
    def get_ai_validated_rules(self) -> List[RuleResult]:
        """
        Get ALL AI-validated rules for learning (both passed AND failed)
        
        This is critical for learning from both approvals AND denials:
        - PASSED rules: Learn what IS valid (e.g., DRG9999 IS valid for 45yo)
        - FAILED rules: Learn what IS NOT valid (e.g., DRG2216+DRG1081 ARE same class)
        """
        return [r for r in self.rule_results if r.source == "ai"]
    
    def get_summary(self) -> Dict:
        """Get summary statistics"""
        total = len(self.rule_results)
        passed = sum(1 for r in self.rule_results if r.passed)
        failed = total - passed
        master_validated = sum(1 for r in self.rule_results if r.source == "master_table")
        # Count both fresh AI validations AND learning table hits (past AI approvals)
        ai_validated = sum(1 for r in self.rule_results if r.source in ["ai", "learning_table"])
        
        return {
            'total_rules': total,
            'passed': passed,
            'failed': failed,
            'master_validated': master_validated,
            'ai_validated': ai_validated,
            'pass_rate': round(passed / total * 100, 1) if total > 0 else 0
        }


class ComprehensiveVettingEngine:
    """
    Validation engine that runs ALL rules and provides complete report
    """
    
    # =====================================================================
    # SYSTEM PROMPT: Establishes context for ALL AI validation calls
    # This prevents the AI from misinterpreting internal codes like DRG1106
    # =====================================================================
    AI_SYSTEM_PROMPT = """You are a medical validation AI for Clearline International, a Health Maintenance Organization (HMO) in Nigeria.

CRITICAL CONTEXT:
- Procedure codes starting with "DRG" are INTERNAL codes mapped to specific drugs/procedures in our database.
- They are NOT standard DRG (Diagnosis Related Group) codes. Never interpret them as DRG classifications.
- When a procedure name is provided alongside a code, ALWAYS use the procedure name for your medical assessment.
- Diagnosis codes follow ICD-10 standards.

YOUR ROLE:
- Validate medical appropriateness based on the RESOLVED NAME provided, not the code format.
- Never comment on code format or validity — codes have already been verified in our database.
- Focus purely on clinical/medical judgment.

Respond ONLY in valid JSON format. No markdown, no backticks, no extra text."""

    def __init__(self, db_path: str = "ai_driven_data.duckdb"):
        """Initialize engine"""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path, read_only=True)
        
        # Import from existing engine
        from .learning_engine import LearningVettingEngine
        self.base_engine = LearningVettingEngine(db_path)
        
        # Initialize 30-day validation engine
        if THIRTY_DAY_AVAILABLE:
            # Pass our connection to prevent multiple connection conflicts
            self.thirty_day_engine = ThirtyDayValidationEngine(db_path, conn=self.conn, learning_engine=self.base_engine)
        else:
            self.thirty_day_engine = None

        # Initialize clinical necessity engine
        try:
            from .clinical_necessity import ClinicalNecessityEngine
            self.clinical_necessity_engine = ClinicalNecessityEngine(conn=self.conn)
            logger.info("✅ Clinical necessity engine initialized")
        except Exception as e:
            logger.warning(f"Clinical necessity engine unavailable: {e}")
            self.clinical_necessity_engine = None

        # Ensure MongoDB indexes for learning tables
        mongo_db.ensure_indexes()

    def _create_learning_tables(self):
        """No-op: learning tables now live in MongoDB."""
        pass
    
    # ==================================================================
    # CODE RESOLUTION HELPERS (THE CRITICAL FIX)
    # ==================================================================
    # These methods resolve internal codes to their actual names before
    # the AI ever sees them. This is what was missing in v2.0.
    # ==================================================================
    
    def _resolve_procedure_info(self, procedure_code: str) -> Dict:
        """
        Resolve a procedure code to its actual name and category.
        
        Checks: PROCEDURE_MASTER → PROCEDURE DATA (comprehensive) → returns raw code
        
        This is THE critical fix — without this, the AI sees "DRG1106" and 
        hallucinates it as a standard DRG code instead of recognizing it as 
        Amlodipine 10mg.
        
        Returns:
            Dict with keys: code, name, category, source, found
        """
        # Try master table first (has curated clinical info)
        proc_master = self.base_engine.check_procedure_master(procedure_code)
        if proc_master:
            return {
                'code': procedure_code,
                'name': proc_master.get('name', procedure_code),
                'category': proc_master.get('class', 'Unknown'),
                'source': 'master_table',
                'found': True
            }
        
        # Try comprehensive table (PROCEDURE DATA)
        proc_comp = self.base_engine._get_procedure_from_comprehensive(procedure_code)
        if proc_comp:
            return {
                'code': procedure_code,
                'name': proc_comp.get('name', procedure_code),
                'category': proc_comp.get('category', 'Unknown'),
                'source': 'comprehensive_table',
                'found': True
            }
        
        # Not found anywhere — return raw code with warning
        return {
            'code': procedure_code,
            'name': procedure_code,  # Raw code as fallback
            'category': 'Unknown',
            'source': 'not_found',
            'found': False
        }
    
    def _resolve_diagnosis_info(self, diagnosis_code: str) -> Dict:
        """
        Resolve a diagnosis code to its actual name and category.
        
        Checks: DIAGNOSIS_MASTER → DIAGNOSIS (comprehensive) → returns raw code
        
        Returns:
            Dict with keys: code, name, category, source, found
        """
        # Try master table first
        diag_master = self.base_engine.check_diagnosis_master(diagnosis_code)
        if diag_master:
            return {
                'code': diagnosis_code,
                'name': diag_master.get('name', diagnosis_code),
                'category': diag_master.get('class', 'Unknown'),
                'source': 'master_table',
                'found': True
            }
        
        # Try comprehensive table
        diag_comp = self.base_engine._get_diagnosis_from_comprehensive(diagnosis_code)
        if diag_comp:
            return {
                'code': diagnosis_code,
                'name': diag_comp.get('name', diagnosis_code),
                'category': diag_comp.get('category', 'Unknown'),
                'source': 'comprehensive_table',
                'found': True
            }
        
        # Try MotherDuck DIAGNOSIS table (covers internal codes like MX1429)
        try:
            row = self.conn.execute("""
                SELECT diagnosisdesc
                FROM "AI DRIVEN DATA"."DIAGNOSIS"
                WHERE UPPER(TRIM(diagnosiscode)) = UPPER(TRIM(?))
                LIMIT 1
            """, [diagnosis_code]).fetchone()
            if row and row[0]:
                return {
                    'code': diagnosis_code,
                    'name': str(row[0]).strip(),
                    'category': 'Unknown',
                    'source': 'diagnosis_table',
                    'found': True
                }
        except Exception:
            pass

        # Not found anywhere
        return {
            'code': diagnosis_code,
            'name': diagnosis_code,
            'category': 'Unknown',
            'source': 'not_found',
            'found': False
        }
    
    # ==================================================================
    # AI VALIDATION METHODS (ALL FIXED WITH NAME RESOLUTION)
    # ==================================================================
    
    def _ai_validate_procedure_age(self, procedure_code: str, age: int, 
                                    proc_info: Optional[Dict] = None) -> Dict:
        """
        Call AI to validate procedure age appropriateness.
        
        FIXED: Now resolves procedure code to actual name before calling AI.
        """
        import json
        
        # Resolve procedure name if not already provided
        if not proc_info:
            proc_info = self._resolve_procedure_info(procedure_code)
        
        proc_name = proc_info['name']
        proc_category = proc_info['category']
        
        prompt = f"""Validate if this procedure/medication is appropriate for this patient's age.

Procedure Code: {procedure_code}
Procedure Name: {proc_name}
Therapeutic Category: {proc_category}
Patient Age: {age} years

IMPORTANT: Base your assessment on the PROCEDURE NAME "{proc_name}", not the code format.

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "Brief explanation referencing the actual drug/procedure name"
}}

Rules:
- APPROVE if {proc_name} is safe and appropriate for a {age}-year-old patient
- DENY only if there is a clear age contraindication (e.g., adult-only drug for a child, pediatric formulation for elderly)
- Age restrictions: pediatric formulations <12yo, adult dosing standard, geriatric caution >65yo
- If age is within normal adult range and no specific restriction exists → APPROVE"""

        try:
            result = self.base_engine._call_claude_for_validation(prompt)
            return {
                'is_valid': result.get('suggested_action') == 'APPROVE',
                'confidence': result.get('confidence', 70),
                'reasoning': result.get('reasoning', 'AI validation completed')
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}'
            }
    
    def _ai_validate_procedure_gender(self, procedure_code: str, gender: str,
                                       proc_info: Optional[Dict] = None) -> Dict:
        """
        Call AI to validate procedure gender appropriateness.
        
        FIXED: Now resolves procedure code to actual name before calling AI.
        """
        if not proc_info:
            proc_info = self._resolve_procedure_info(procedure_code)
        
        proc_name = proc_info['name']
        proc_category = proc_info['category']
        
        prompt = f"""Validate if this procedure/medication is appropriate for this patient's gender.

Procedure Code: {procedure_code}
Procedure Name: {proc_name}
Therapeutic Category: {proc_category}
Patient Gender: {gender}

IMPORTANT: Base your assessment on the PROCEDURE NAME "{proc_name}", not the code format.

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "Brief explanation referencing the actual drug/procedure name"
}}

Rules:
- APPROVE if {proc_name} can be used by a {gender} patient
- DENY only if there is an ANATOMICAL impossibility (e.g., prostate medication for female, pregnancy drug for male)
- Most medications are appropriate for both genders → APPROVE
- Only flag gender-specific drugs: pregnancy/ovarian/cervical/uterine = female only, prostate/testicular = male only"""

        try:
            result = self.base_engine._call_claude_for_validation(prompt)
            return {
                'is_valid': result.get('suggested_action') == 'APPROVE',
                'confidence': result.get('confidence', 70),
                'reasoning': result.get('reasoning', 'AI validation completed')
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}'
            }
    
    def _ai_validate_diagnosis_age(self, diagnosis_code: str, age: int,
                                    diag_info: Optional[Dict] = None) -> Dict:
        """
        Call AI to validate diagnosis age appropriateness.
        
        FIXED: Now resolves diagnosis code to actual name before calling AI.
        Also handles unresolved codes by instructing AI to interpret ICD-10.
        """
        if not diag_info:
            diag_info = self._resolve_diagnosis_info(diagnosis_code)
        
        diag_name = diag_info['name']
        diag_category = diag_info['category']
        code_resolved = diag_info.get('found', False)
        
        # Build name context
        if code_resolved and diag_name != diagnosis_code:
            name_instruction = f"""Diagnosis Code: {diagnosis_code}
Diagnosis Name: {diag_name}
Category: {diag_category}

IMPORTANT: Base your assessment on the DIAGNOSIS NAME "{diag_name}", not the code format."""
        else:
            name_instruction = f"""Diagnosis Code: {diagnosis_code}

NOTE: This code was not found in our internal database.
You MUST interpret this as an ICD-10 diagnosis code and identify the condition it represents.
First state what condition {diagnosis_code} represents, then assess age appropriateness.
If you cannot identify the ICD-10 code, DENY with low confidence."""
        
        prompt = f"""Validate if this diagnosis is medically plausible for this patient's age.

{name_instruction}
Patient Age: {age} years

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "First identify the condition, then explain age appropriateness"
}}

Rules:
- APPROVE if the condition can plausibly occur in a {age}-year-old patient
- Even if a condition is UNCOMMON at this age, APPROVE unless medically virtually impossible
- DENY only if the condition is near-impossible at this age (e.g., Alzheimer's in a 5-year-old, menopause in a child)
- Age-unlikely ≠ Age-impossible. Unlikely conditions should still APPROVE"""

        try:
            result = self.base_engine._call_claude_for_validation(prompt)
            return {
                'is_valid': result.get('suggested_action') == 'APPROVE',
                'confidence': result.get('confidence', 70),
                'reasoning': result.get('reasoning', 'AI validation completed')
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}'
            }
    
    def _ai_validate_diagnosis_gender(self, diagnosis_code: str, gender: str,
                                       diag_info: Optional[Dict] = None) -> Dict:
        """
        Call AI to validate diagnosis gender appropriateness.
        
        FIXED: Now resolves diagnosis code to actual name before calling AI.
        Also handles unresolved codes by instructing AI to interpret ICD-10.
        """
        if not diag_info:
            diag_info = self._resolve_diagnosis_info(diagnosis_code)
        
        diag_name = diag_info['name']
        diag_category = diag_info['category']
        code_resolved = diag_info.get('found', False)
        
        # Build name context - if name wasn't resolved, tell AI to interpret the code
        if code_resolved and diag_name != diagnosis_code:
            name_instruction = f"""Diagnosis Code: {diagnosis_code}
Diagnosis Name: {diag_name}
Category: {diag_category}

IMPORTANT: Base your assessment on the DIAGNOSIS NAME "{diag_name}", not the code format."""
        else:
            name_instruction = f"""Diagnosis Code: {diagnosis_code}

NOTE: This code was not found in our internal database. 
You MUST interpret this as an ICD-10 diagnosis code and identify the condition it represents.
First state what condition {diagnosis_code} represents, then assess gender appropriateness.
If you cannot identify the ICD-10 code, DENY with low confidence."""
        
        prompt = f"""Validate if this diagnosis is appropriate for this patient's gender.

{name_instruction}
Patient Gender: {gender}

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "First identify the condition, then explain gender appropriateness"
}}

STRICT GENDER RULES:
- Female-only conditions (DENY if patient is Male): pregnancy, ovarian cancer, cervical cancer, uterine conditions, vaginitis, endometriosis
- Male-only conditions (DENY if patient is Female): prostate cancer, testicular cancer, penile conditions
- A {gender} patient diagnosed with a condition of the OPPOSITE gender's anatomy = DENY (anatomically impossible)
- Most conditions can occur in both genders → APPROVE
- Only DENY for anatomical impossibilities"""

        try:
            result = self.base_engine._call_claude_for_validation(prompt)
            return {
                'is_valid': result.get('suggested_action') == 'APPROVE',
                'confidence': result.get('confidence', 70),
                'reasoning': result.get('reasoning', 'AI validation completed')
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}'
            }
    
    def _ai_validate_proc_diag_compatibility(self, procedure_code: str, diagnosis_code: str,
                                            age: Optional[int] = None, gender: Optional[str] = None,
                                            proc_info: Optional[Dict] = None,
                                            diag_info: Optional[Dict] = None,
                                            encounter_type: str = "OUTPATIENT") -> Dict:
        """
        Two-phase AI validation for procedure-diagnosis compatibility.
        
        Phase 1: Fast Haiku call (no PubMed) — handles ~80% of cases
        Phase 2: If Haiku confidence < 75%, search PubMed and re-evaluate
                 with real clinical evidence — handles edge cases
        """
        if not proc_info:
            proc_info = self._resolve_procedure_info(procedure_code)
        if not diag_info:
            diag_info = self._resolve_diagnosis_info(diagnosis_code)
        
        proc_name = proc_info['name']
        proc_category = proc_info['category']
        diag_name = diag_info['name']
        diag_category = diag_info['category']
        proc_resolved = proc_info.get('found', False)
        diag_resolved = diag_info.get('found', False)
        
        context_str = f"\nEncounter Setting: {encounter_type}"
        if age:
            context_str += f"\nPatient Age: {age} years"
        if gender:
            context_str += f"\nPatient Gender: {gender}"
        if encounter_type == "INPATIENT":
            context_str += (
                "\n\nINPATIENT CONTEXT: This patient has been admitted. "
                "IV/injectable medications, room charges, continuous monitoring, "
                "and higher-acuity interventions are clinically appropriate. "
                "Do NOT deny solely because a drug is injectable or a procedure "
                "seems 'too intensive' — inpatient care warrants these."
            )
        else:
            context_str += (
                "\n\nOUTPATIENT CONTEXT: This patient is NOT admitted. "
                "Evaluate whether the procedure is proportionate to an outpatient setting. "
                "Injectable/IV medications require justification (e.g., patient vomiting, "
                "drug not available orally) — but do not deny solely on route if the drug "
                "itself is clinically indicated."
            )
        
        # Build procedure section
        if proc_resolved and proc_name != procedure_code:
            proc_section = f"""Procedure Code: {procedure_code}
Procedure Name: {proc_name}
Therapeutic Category: {proc_category}"""
        else:
            proc_section = f"""Procedure Code: {procedure_code}
(Not found in internal database - interpret as standard procedure/drug code)"""
        
        # Build diagnosis section
        if diag_resolved and diag_name != diagnosis_code:
            diag_section = f"""Diagnosis Code: {diagnosis_code}
Diagnosis Name: {diag_name}
Diagnosis Category: {diag_category}"""
        else:
            diag_section = f"""Diagnosis Code: {diagnosis_code}
(Not found in internal database - interpret as ICD-10 diagnosis code. You MUST identify the condition first.)"""
        
        # Use resolved names in the prompt, fall back to codes
        proc_display = proc_name if (proc_resolved and proc_name != procedure_code) else procedure_code
        diag_display = diag_name if (diag_resolved and diag_name != diagnosis_code) else diagnosis_code
        
        # Build the core validation prompt (shared by both phases)
        validation_instructions = f"""
IMPORTANT: "Procedure" covers THREE types — each has different validation logic:

1. MEDICATION → Is it used IN THE TREATMENT of this diagnosis?
   This includes ALL of the following:
   - Primary treatment (directly targets the disease)
   - Symptomatic relief (treats symptoms that accompany the condition)
   - Supportive care (aids recovery, boosts immunity, prevents complications)
   - Adjunctive therapy (commonly co-prescribed alongside primary treatment)

   ✅ Artemether for Malaria (primary — kills the parasite)
   ✅ Paracetamol for Malaria (symptomatic — treats fever/body pain)
   ✅ Vitamin C for Malaria (supportive — immune support during recovery)
   ✅ Omeprazole for Peptic Ulcer (primary — reduces acid)
   ✅ Antacid for Gastritis (symptomatic — neutralizes acid)
   ❌ Amlodipine for Ovarian Cancer (not part of cancer treatment at all)
   ❌ Metformin for Fracture (diabetes drug, no role in fracture treatment)

   ROUTE OF ADMINISTRATION RULE:
   Route (tablet, injection, syrup, IV, IM, topical) is a separate clinical judgment from
   drug-diagnosis compatibility. For this check, evaluate whether the DRUG is indicated —
   not whether the injectable route is the most appropriate delivery method.
   ✅ Omeprazole Injection for Gastroenteritis — Omeprazole IS indicated; patient may be
      vomiting and unable to swallow. Route is a clinical call, not a compatibility failure.
   ✅ Metronidazole IV for abdominal infection — drug is indicated regardless of route.
   ❌ Ceftriaxone Injection for mild conjunctivitis — deny because systemic antibiotics
      are NOT indicated for conjunctivitis at all (not because it is injectable).
   ❌ Paracetamol Injection for mild conjunctivitis — deny because inpatient-grade
      medication is inappropriate for a condition that does not warrant admission,
      AND because the dose/route is disproportionate to the severity of the diagnosis.
   RULE: Only deny on route if (a) the drug itself is inappropriate for the diagnosis,
   OR (b) injectable administration is clearly disproportionate to a mild/outpatient-only
   diagnosis where no reasonable clinical scenario would require IV/IM delivery.

2. LABORATORY TEST → Does it INVESTIGATE, CONFIRM, or MONITOR this diagnosis?
   ✅ Full Blood Count for Anaemia (confirms with haemoglobin levels)
   ✅ Malaria Thick/Thin Film for Plasmodium Falciparum (confirms parasites)
   ✅ Full Blood Count for URTI (checks WBC for infection)
   ✅ Full Blood Count for Malaria (monitors haemoglobin, platelet count)
   ✅ Liver Function Test for Hepatitis (confirms liver damage)
   ❌ Malaria Film for Fracture (irrelevant investigation)

3. SURGICAL PROCEDURE → Is it an appropriate INTERVENTION for this condition?
   ✅ Myomectomy for Uterine Fibroids (removes the fibroids)
   ✅ Appendectomy for Acute Appendicitis (removes inflamed appendix)
   ❌ Appendectomy for Malaria (wrong intervention entirely)

COMBINATION DRUGS — SPECIAL RULE:
If the procedure name describes a combination drug (e.g. "Diclofenac 75mg and Misoprostol 200mcg",
"Co-amoxiclav", "Arthrotec"), evaluate it based on the PRIMARY active ingredient, not the
GI-protective or adjunct component:
   - GI-protective agents included in combinations (Misoprostol, Omeprazole, Pantoprazole,
     Ranitidine, Esomeprazole) are adjuncts co-formulated to protect the stomach when an
     NSAID or other primary drug is taken — they do NOT require a separate GI diagnosis.
   - Evaluate whether the PRIMARY drug (the NSAID, antibiotic, etc.) is appropriate for
     the diagnosis. If it is, APPROVE the combination as a whole.
   ✅ Diclofenac + Misoprostol for Mononeuropathy — Diclofenac is an NSAID that treats
      pain/inflammation in mononeuropathy; Misoprostol is the GI protectant co-formulation.
      APPROVE based on the Diclofenac component.
   ✅ Co-amoxiclav for Pneumonia — Amoxicillin is the antibiotic; Clavulanic acid prevents
      bacterial resistance. Evaluate on Amoxicillin's role.

ANTIHISTAMINES FOR PRURITIC CONDITIONS — SPECIAL RULE:
Antihistamines (Cetirizine, Loratadine, Chlorphenamine, Promethazine, Hydroxyzine, etc.)
are routinely co-prescribed for ANY condition that causes significant itching (pruritus):
   ✅ Cetirizine for Tinea (ringworm, tinea capitis, tinea barbae) — fungal infections cause
      intense pruritus; antihistamines control itch and reduce scratching/secondary infection.
   ✅ Cetirizine for Urticaria, Eczema, Scabies, Insect bites, Drug reactions — all pruritic.
   ✅ Chlorphenamine for Chickenpox — symptomatic itch relief.
   - If the diagnosis causes pruritus as a common symptom, APPROVE antihistamines as
     symptomatic/supportive therapy, even though they do not treat the underlying cause.

VALIDATION RULES:
- First identify the PROCEDURE TYPE (medication, lab, or surgery)
- For COMBINATION DRUGS: identify the primary active ingredient, then evaluate on that ingredient
- For MEDICATIONS: APPROVE if it plays ANY role in treating the patient with this diagnosis — primary treatment, symptom management, supportive care, or standard co-prescription. Ask: "Would a doctor reasonably prescribe this for a patient with this diagnosis?"
- For LAB TESTS: APPROVE if it is a clinically relevant investigation that helps diagnose, monitor, or rule out the condition
- For SURGICAL PROCEDURES: APPROVE if it is an appropriate intervention for the condition
- DENY only if there is NO clinical reason to use this procedure for a patient with this diagnosis"""

        # ==================================================================
        # PHASE 1: Fast Haiku call (no PubMed)
        # ==================================================================
        phase1_prompt = f"""Validate if this procedure is CLINICALLY APPROPRIATE for a patient diagnosed with this condition.

{proc_section}

{diag_section}{context_str}
{validation_instructions}

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "procedure_type": "medication" or "lab_test" or "surgical",
  "reasoning": "State the procedure type and whether {proc_display} is clinically appropriate for a patient with {diag_display}. If approving, state the role (primary/symptomatic/supportive). If denying, state what {proc_display} is actually used for."
}}"""

        try:
            phase1_result = self.base_engine._call_claude_for_validation(phase1_prompt, model="claude-opus-4-6")
            phase1_confidence = phase1_result.get('confidence', 0)
            phase1_action = phase1_result.get('suggested_action', 'DENY')
            
            # ==============================================================
            # PHASE 2: PubMed second opinion (ONLY if confidence < 75%)
            # ==============================================================
            PUBMED_THRESHOLD = 75
            pubmed_evidence = {'articles': [], 'count': 0, 'query': ''}
            evidence_summary = []
            
            if phase1_confidence < PUBMED_THRESHOLD:
                print(f"🔬 Phase 1 confidence {phase1_confidence}% < {PUBMED_THRESHOLD}% — searching PubMed for evidence...")
                
                pubmed_evidence = self.base_engine.search_pubmed_evidence(proc_name, diag_name)
                evidence_section = self.base_engine.format_pubmed_for_prompt(pubmed_evidence)
                
                if pubmed_evidence.get('count', 0) > 0 and evidence_section:
                    # Re-ask Haiku WITH evidence
                    phase2_prompt = f"""Validate if this procedure is CLINICALLY APPROPRIATE for a patient diagnosed with this condition.

{proc_section}

{diag_section}{context_str}

{evidence_section}
{validation_instructions}
- If PubMed evidence supports the combination, cite the PMID(s) in your reasoning
- If the evidence contradicts the combination, explain why

Respond in JSON format:
{{
  "action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "procedure_type": "medication" or "lab_test" or "surgical",
  "reasoning": "State the procedure type and whether {proc_display} is clinically appropriate for a patient with {diag_display}. Cite any relevant PubMed PMID(s)."
}}"""
                    
                    phase2_result = self.base_engine._call_claude_for_validation(phase2_prompt, model="claude-opus-4-6")
                    
                    # Use Phase 2 result (evidence-informed)
                    final_result = phase2_result
                    final_reasoning = phase2_result.get('reasoning', '') + f" [PubMed: {pubmed_evidence['count']} article(s) reviewed]"
                    print(f"🔬 Phase 2 complete — confidence now {phase2_result.get('confidence', 0)}% (was {phase1_confidence}%)")
                else:
                    # No PubMed results found — stick with Phase 1
                    final_result = phase1_result
                    final_reasoning = phase1_result.get('reasoning', '') + " [PubMed: no articles found]"
                    print(f"🔬 PubMed returned no results — keeping Phase 1 decision")
            else:
                # Phase 1 was confident enough — no PubMed needed
                final_result = phase1_result
                final_reasoning = phase1_result.get('reasoning', '')
            
            # Build evidence summary for UI
            for article in pubmed_evidence.get('articles', []):
                evidence_summary.append({
                    'pmid': article['pmid'],
                    'title': article['title'],
                    'year': article['year'],
                    'authors': article['authors']
                })
            
            return {
                'is_valid': final_result.get('suggested_action') == 'APPROVE',
                'confidence': final_result.get('confidence', 70),
                'reasoning': final_reasoning,
                'pubmed_evidence': evidence_summary,
                'pubmed_query': pubmed_evidence.get('query', ''),
                'pubmed_count': pubmed_evidence.get('count', 0),
                'pubmed_triggered': phase1_confidence < PUBMED_THRESHOLD
            }
        except Exception as e:
            return {
                'is_valid': False,
                'confidence': 0,
                'reasoning': f'AI validation failed: {str(e)}',
                'pubmed_evidence': [],
                'pubmed_count': 0,
                'pubmed_triggered': False
            }
    
    def check_excess_care_frequency(
        self,
        enrollee_id: str,
        encounter_date: str,
        days: int = 14,
        pa_number: Optional[str] = None,
    ) -> Dict:
        """
        Returns {'triggered': bool, 'last_date': str, 'days_since': int, 'source': str}
        Checks PA DATA (requestdate) and CLAIMS DATA (encounterdatefrom) for any
        encounter within `days` days before encounter_date.

        pa_number — if provided, the PA DATA row with that exact panumber is excluded
        so that the pre-auth record for the current visit does not count as a prior visit.
        """
        from datetime import datetime as _dt, timedelta
        result = {"triggered": False, "last_date": None, "days_since": None, "source": None}
        try:
            enc_dt   = _dt.strptime(encounter_date[:10], "%Y-%m-%d").date()
            lookback = (enc_dt - timedelta(days=days)).strftime("%Y-%m-%d")
            enc_str  = enc_dt.strftime("%Y-%m-%d")

            # Check PA DATA — exclude the pre-auth record for this very PA number
            if pa_number:
                row = self.conn.execute("""
                    SELECT MAX(CAST(requestdate AS DATE)) as last_date
                    FROM "AI DRIVEN DATA"."PA DATA"
                    WHERE IID = ?
                      AND CAST(requestdate AS DATE) >= ?
                      AND CAST(requestdate AS DATE) < ?
                      AND panumber != ?
                """, [enrollee_id, lookback, enc_str, str(pa_number)]).fetchone()
            else:
                row = self.conn.execute("""
                    SELECT MAX(CAST(requestdate AS DATE)) as last_date
                    FROM "AI DRIVEN DATA"."PA DATA"
                    WHERE IID = ?
                      AND CAST(requestdate AS DATE) >= ?
                      AND CAST(requestdate AS DATE) < ?
                """, [enrollee_id, lookback, enc_str]).fetchone()
            if row and row[0]:
                last = row[0]
                if hasattr(last, 'strftime'):
                    last_str = last.strftime("%Y-%m-%d")
                else:
                    last_str = str(last)[:10]
                last_dt   = _dt.strptime(last_str, "%Y-%m-%d").date()
                days_ago  = (enc_dt - last_dt).days
                result = {"triggered": True, "last_date": last_str,
                          "days_since": days_ago, "source": "PA DATA"}
                return result
        except Exception as e:
            logger.warning(f"PA DATA frequency check error: {e}")

        try:
            enc_dt   = _dt.strptime(encounter_date[:10], "%Y-%m-%d").date()
            lookback = (enc_dt - timedelta(days=days)).strftime("%Y-%m-%d")
            enc_str  = enc_dt.strftime("%Y-%m-%d")

            # Check CLAIMS DATA
            row = self.conn.execute("""
                SELECT MAX(CAST(encounterdatefrom AS DATE)) as last_date
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE enrollee_id = ?
                  AND CAST(encounterdatefrom AS DATE) >= ?
                  AND CAST(encounterdatefrom AS DATE) < ?
            """, [enrollee_id, lookback, enc_str]).fetchone()
            if row and row[0]:
                last = row[0]
                if hasattr(last, 'strftime'):
                    last_str = last.strftime("%Y-%m-%d")
                else:
                    last_str = str(last)[:10]
                last_dt  = _dt.strptime(last_str, "%Y-%m-%d").date()
                days_ago = (enc_dt - last_dt).days
                result = {"triggered": True, "last_date": last_str,
                          "days_since": days_ago, "source": "CLAIMS DATA"}
        except Exception as e:
            logger.warning(f"CLAIMS DATA frequency check error: {e}")

        return result

    # ==================================================================
    # CLINICAL CONSTANTS (shared by all batch / per-line checks below)
    # ==================================================================

    _GP_CODES = frozenset({"CONS021", "CONS022"})

    _IV_FLUID_PATTERNS = (
        "NORMAL SALINE", "N/SALINE", "0.9% NACL", "NACL 0.9",
        "DEXTROSE SALINE", "DEXTROSE 5%", "D5W", "D5NS",
        "RINGER", "RINGER'S LACTATE", "HARTMANN",
        "IV FLUID", "IVF ", "INFUSION FLUID", "INTRAVENOUS FLUID",
    )

    _SHOTGUN_LAB_PATTERNS = (
        "FULL BLOOD COUNT", "FBC", "COMPLETE BLOOD COUNT", "CBC",
        "WIDAL",
        "MALARIA PARASITE", "MALARIA FILM", "MALARIA RDT", "MP TEST",
        "URINALYSIS", "URINE MCS", "URINE M/C/S",
        "RANDOM BLOOD SUGAR", "RBS", "FASTING BLOOD SUGAR", "FBS",
        "BLOOD GLUCOSE",
    )

    _VITAMIN_PATTERNS = (
        "VITAMIN B COMPLEX", "VIT B COMPLEX", "B COMPLEX",
        "VITAMIN C", "VIT C", "ASCORBIC ACID",
        "FOLIC ACID", "FOLATE",
        "VITAMIN B12", "CYANOCOBALAMIN",
        "VITAMIN D", "CHOLECALCIFEROL",
        "VITAMIN E", "TOCOPHEROL",
        "MULTIVITAMIN", "MULTI VITAMIN",
        "VITAMIN B6", "PYRIDOXINE",
    )

    _VITAMIN_INDICATIONS = (
        "ANAEMIA", "ANEMIA",
        "PREGNAN", "ANTENATAL", "MATERNAL",
        "MALNUTRITION", "DEFICIENCY", "KWASHIORKOR", "MARASMUS",
        "SCURVY", "RICKETS", "PELLAGRA", "BERIBERI",
        "NEUROPATHY",
        "SICKLE CELL",
        "HIV", "AIDS",
        "HEPATITIS",
        "RENAL FAILURE", "DIALYSIS",
        "CHRONIC LIVER",
    )

    _LAB_INVESTIGATION_KW = (
        "BLOOD COUNT", "FBC", "CBC",
        "WIDAL", "MALARIA", "PARASITE",
        "URINALYSIS", "URINE",
        "GLUCOSE", "RBS", "FBS",
        "CULTURE", "SENSITIVITY",
        "LIVER FUNCTION", "RENAL FUNCTION", "KIDNEY FUNCTION",
        "THYROID", "HIV", "HEPATITIS",
        "CHOLESTEROL", "LIPID PROFILE",
        "ELECTROLYTE", "UREA", "CREATININE",
        "X-RAY", "XRAY", "ULTRASOUND", "SCAN", "ECG",
        "TEST", "LEVEL", "PROFILE", "SCREEN",
    )

    # ICD-10 prefixes for conditions manageable in outpatient / daycare
    _MILD_ICD10_PREFIXES = (
        "B50", "B51", "B52", "B53",   # Uncomplicated malaria
        "J00", "J06",                   # Common cold / AURI
        "J02", "J03",                   # Pharyngitis, tonsillitis
        "J20", "J22",                   # Acute bronchitis (mild)
        "N30", "N39",                   # UTI uncomplicated
        "I10",                          # Stable hypertension
        "E11", "E14",                   # T2DM without complications
        "K29",                          # Gastritis
        "A09",                          # Viral diarrhoea
        "K52",                          # Non-infective gastroenteritis
    )

    @classmethod
    def _is_mild_diagnosis(cls, diagnosis_code: str) -> bool:
        c = diagnosis_code.strip().upper()
        return any(c.startswith(p) for p in cls._MILD_ICD10_PREFIXES)

    # ==================================================================
    # BATCH CHECK 1: GP CONSULTATION FREQUENCY
    # >1 GP consult at same provider within 7 days → flag extras
    # ==================================================================

    def check_gp_consult_frequency(
        self,
        procedures: List[Dict],
        enrollee_id: str,
        encounter_date: str,
        provider_id: str,
    ) -> Dict:
        """
        Flags GP consultations (CONS021/022) when one already occurred at the
        same provider within 7 days, or when the batch itself carries >1.

        Returns: {triggered, flagged_codes, reason}
        """
        from datetime import timedelta as _td

        gp_in_batch = [
            rp["procedure_code"] for rp in procedures
            if rp["procedure_code"].upper() in self._GP_CODES
        ]
        if not gp_in_batch:
            return {"triggered": False, "flagged_codes": [], "reason": ""}

        if provider_id:
            try:
                enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
                lookback = (enc_dt - _td(days=7)).strftime("%Y-%m-%d")
                enc_str  = enc_dt.strftime("%Y-%m-%d")
                row = self.conn.execute("""
                    SELECT COUNT(*) as cnt, MAX(CAST(requestdate AS DATE)) as last_date
                    FROM "AI DRIVEN DATA"."PA DATA"
                    WHERE IID = ?
                      AND UPPER(TRIM(code)) IN ('CONS021', 'CONS022')
                      AND CAST(requestdate AS DATE) >= ?
                      AND CAST(requestdate AS DATE) < ?
                      AND providerid = ?
                """, [enrollee_id, lookback, enc_str, provider_id]).fetchone()
                if row and row[0] and int(row[0]) > 0:
                    last_date = str(row[1])[:10] if row[1] else "recently"
                    days_ago  = (enc_dt - datetime.strptime(last_date, "%Y-%m-%d").date()).days
                    return {
                        "triggered": True,
                        "flagged_codes": gp_in_batch,
                        "reason": (
                            f"❌ GP_CONSULT_FREQUENCY: GP consultation already performed at this "
                            f"provider {days_ago} day(s) ago ({last_date}). Only 1 GP consult per "
                            f"7 days at the same facility is allowed."
                        ),
                    }
            except Exception as e:
                logger.warning(f"check_gp_consult_frequency DB error: {e}")

        if len(gp_in_batch) > 1:
            return {
                "triggered": True,
                "flagged_codes": gp_in_batch[1:],
                "reason": (
                    f"❌ GP_CONSULT_FREQUENCY: Batch contains {len(gp_in_batch)} GP consultations. "
                    f"Only 1 GP consult is allowed per encounter."
                ),
            }

        return {"triggered": False, "flagged_codes": [], "reason": ""}

    # ==================================================================
    # BATCH CHECK 2: SPECIALIST WITHOUT REFERRAL
    # Specialist CONS without a GP CONS in same batch or 7-day history
    # ==================================================================

    def check_specialist_without_referral(
        self,
        procedures: List[Dict],
        enrollee_id: str,
        encounter_date: str,
        provider_id: str,
    ) -> Dict:
        """
        Returns flagged specialist CONS codes when no GP referral exists in the
        batch or within 7 days at the same provider.

        Returns: {triggered, flagged_codes, reason}
        """
        from datetime import timedelta as _td

        specialist_in_batch = [
            rp["procedure_code"] for rp in procedures
            if (rp["procedure_code"].upper().startswith("CONS")
                and rp["procedure_code"].upper() not in self._GP_CODES)
        ]
        if not specialist_in_batch:
            return {"triggered": False, "flagged_codes": [], "reason": ""}

        # GP present in same batch → automatic referral
        if any(rp["procedure_code"].upper() in self._GP_CODES for rp in procedures):
            return {"triggered": False, "flagged_codes": [], "reason": ""}

        # Check history
        if provider_id:
            try:
                enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
                lookback = (enc_dt - _td(days=7)).strftime("%Y-%m-%d")
                enc_str  = enc_dt.strftime("%Y-%m-%d")
                row = self.conn.execute("""
                    SELECT COUNT(*) as cnt
                    FROM "AI DRIVEN DATA"."PA DATA"
                    WHERE IID = ?
                      AND UPPER(TRIM(code)) IN ('CONS021', 'CONS022')
                      AND CAST(requestdate AS DATE) >= ?
                      AND CAST(requestdate AS DATE) < ?
                      AND providerid = ?
                """, [enrollee_id, lookback, enc_str, provider_id]).fetchone()
                if row and row[0] and int(row[0]) > 0:
                    return {"triggered": False, "flagged_codes": [], "reason": ""}
            except Exception as e:
                logger.warning(f"check_specialist_without_referral DB error: {e}")

        return {
            "triggered": True,
            "flagged_codes": specialist_in_batch,
            "reason": (
                f"❌ SPECIALIST_WITHOUT_REFERRAL: Specialist consultation(s) "
                f"({', '.join(specialist_in_batch)}) submitted without a GP referral "
                f"(CONS021/CONS022) in this batch or within 7 days at this provider."
            ),
        }

    # ==================================================================
    # BATCH CHECK 3: SAME DIAGNOSIS DIFFERENT PROVIDER (double-billing)
    # ==================================================================

    def check_same_diagnosis_different_provider(
        self,
        enrollee_id: str,
        encounter_date: str,
        diagnosis_codes: List[str],
        provider_id: str,
        lookback_days: int = 30,
    ) -> Dict:
        """
        Checks whether any diagnosis in this batch was already treated at a
        *different* provider within the last `lookback_days` days.

        Returns: {triggered, detail, reason}
        """
        from datetime import timedelta as _td
        result = {"triggered": False, "detail": [], "reason": ""}
        if not provider_id or not diagnosis_codes:
            return result

        try:
            enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
            lookback = (enc_dt - _td(days=lookback_days)).strftime("%Y-%m-%d")
            enc_str  = enc_dt.strftime("%Y-%m-%d")

            for dcode in diagnosis_codes:
                row = self.conn.execute("""
                    SELECT COUNT(*) as cnt, MAX(providerid) as other_prov,
                           MAX(CAST(requestdate AS DATE)) as last_date
                    FROM "AI DRIVEN DATA"."PA DATA"
                    WHERE IID = ?
                      AND UPPER(TRIM(diagnosiscode)) = UPPER(TRIM(?))
                      AND CAST(requestdate AS DATE) >= ?
                      AND CAST(requestdate AS DATE) < ?
                      AND providerid != ?
                """, [enrollee_id, dcode, lookback, enc_str, provider_id]).fetchone()
                if row and row[0] and int(row[0]) > 0:
                    other_prov = str(row[1]) if row[1] else "another provider"
                    last_date  = str(row[2])[:10] if row[2] else "recently"
                    result["triggered"] = True
                    result["detail"].append({
                        "diagnosis_code": dcode,
                        "other_provider": other_prov,
                        "last_date": last_date,
                    })
        except Exception as e:
            logger.warning(f"check_same_diagnosis_different_provider DB error: {e}")
            return result

        if result["triggered"]:
            items = "; ".join(
                f"{d['diagnosis_code']} at Provider {d['other_provider']} on {d['last_date']}"
                for d in result["detail"]
            )
            result["reason"] = (
                f"❌ SAME_DIAGNOSIS_DIFFERENT_PROVIDER: Diagnosis already treated at a different "
                f"provider within {lookback_days} days — possible double-billing. ({items})"
            )
        return result

    # ==================================================================
    # BATCH CHECK 4: POST-DISCHARGE INVESTIGATION OVERLAP (14 days)
    # ==================================================================

    def check_post_discharge(
        self,
        enrollee_id: str,
        encounter_date: str,
        procedures: List[Dict],
        lookback_days: int = 14,
    ) -> Dict:
        """
        If the enrollee was recently admitted (inpatient / ADM code within
        `lookback_days` days), investigation procedures in the current batch
        are likely already covered by the admission.

        Returns: {triggered, flagged_codes, discharge_date, reason}
        """
        from datetime import timedelta as _td
        result = {"triggered": False, "flagged_codes": [], "discharge_date": None, "reason": ""}

        try:
            enc_dt   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
            lookback = (enc_dt - _td(days=lookback_days)).strftime("%Y-%m-%d")
            enc_str  = enc_dt.strftime("%Y-%m-%d")

            row = self.conn.execute("""
                SELECT MAX(CAST(requestdate AS DATE)) as last_adm
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE IID = ?
                  AND UPPER(LEFT(TRIM(code), 3)) = 'ADM'
                  AND CAST(requestdate AS DATE) >= ?
                  AND CAST(requestdate AS DATE) < ?
            """, [enrollee_id, lookback, enc_str]).fetchone()

            if not (row and row[0]):
                return result

            last_adm = row[0]
            discharge_date = (
                last_adm.strftime("%Y-%m-%d")
                if hasattr(last_adm, "strftime")
                else str(last_adm)[:10]
            )
            days_ago = (enc_dt - datetime.strptime(discharge_date, "%Y-%m-%d").date()).days

            flagged = [
                rp["procedure_code"] for rp in procedures
                if any(kw in rp["procedure_name"].upper() for kw in self._LAB_INVESTIGATION_KW)
            ]
            if flagged:
                result["triggered"]     = True
                result["flagged_codes"] = flagged
                result["discharge_date"] = discharge_date
                result["reason"] = (
                    f"❌ POST_DISCHARGE_CHECK: Enrollee was admitted (inpatient) {days_ago} day(s) "
                    f"ago (last admission: {discharge_date}). Investigation(s) "
                    f"({', '.join(flagged)}) were likely performed during that admission and cannot "
                    f"be billed separately within {lookback_days} days of discharge."
                )
        except Exception as e:
            logger.warning(f"check_post_discharge DB error: {e}")

        return result

    # ==================================================================
    # BATCH CHECK 5: DIAGNOSIS STACKING (AI + learning table)
    # Are all diagnoses simultaneously plausible?
    # ==================================================================

    def check_diagnosis_stacking(self, diagnoses: List[Dict]) -> Dict:
        """
        AI check: are all submitted diagnoses simultaneously plausible?

        Never learns — same combination can be valid or invalid depending on
        clinical context and patient history, so it is always evaluated fresh.

        diagnoses: list of {code, name}
        Returns: {triggered, confidence, reason}
        """
        import json as _json
        import re  as _re

        result = {"triggered": False, "confidence": 0, "reason": ""}
        if len(diagnoses) < 2:
            return result

        diag_list = "\n".join(f"  - {d['code']}: {d['name']}" for d in diagnoses)
        prompt = f"""You are a Nigerian HMO medical officer reviewing a pre-authorization.

The following diagnoses were submitted TOGETHER for one patient in a single visit:
{diag_list}

Determine whether ALL of these diagnoses can be simultaneously present in the same patient.

Consider:
1. Are these conditions that can plausibly co-exist? (e.g., malaria + fever: fine)
2. Is there a pattern of diagnosis padding to inflate claim value?
3. Are any diagnoses mutually exclusive?
4. Could a single underlying condition explain multiple codes (fragmented billing)?

Respond ONLY in JSON:
{{
  "verdict": "PLAUSIBLE" or "IMPLAUSIBLE",
  "confidence": 0-100,
  "reasoning": "Brief clinical explanation. If IMPLAUSIBLE, name the problematic combination."
}}"""

        try:
            import anthropic as _ant
            client = _ant.Anthropic(max_retries=0)
            resp = _anthropic_create_with_retry(client,
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                temperature=0,
                system=self.AI_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}]
            )
            raw  = _re.sub(r'^```(?:json)?\s*|\s*```$', '', resp.content[0].text.strip())
            data = _json.loads(raw)

            verdict    = data.get("verdict", "PLAUSIBLE")
            confidence = int(data.get("confidence", 70))
            reasoning  = data.get("reasoning", "")

            if verdict == "IMPLAUSIBLE":
                result["triggered"]  = True
                result["confidence"] = confidence
                result["reason"]     = f"❌ DIAGNOSIS_STACKING: {reasoning}"
        except Exception as e:
            logger.warning(f"check_diagnosis_stacking AI error: {e}")

        return result

    # ==================================================================
    # BATCH CHECK 6: POLYPHARMACY (>5 DRG drugs — AI evaluates)
    # ==================================================================

    def check_polypharmacy(self, procedures: List[Dict]) -> Dict:
        """
        Count DRG (drug) codes in the batch. If >5, call AI to evaluate
        necessity → AUTO_DENY whole batch with AI reasoning if inappropriate.

        Returns: {triggered, drug_count, reason}
        """
        import json as _json
        import re  as _re

        THRESHOLD = 5
        drg_items = [rp for rp in procedures if rp["procedure_code"].upper().startswith("DRG")]
        drug_count = len(drg_items)
        result = {"triggered": False, "drug_count": drug_count, "reason": ""}

        if drug_count <= THRESHOLD:
            return result

        drug_list = "\n".join(
            f"  - {d['procedure_code']}: {d['procedure_name']} "
            f"(dx: {d['diagnosis_name']} [{d['diagnosis_code']}])"
            for d in drg_items
        )
        diag_set  = {d["diagnosis_code"]: d["diagnosis_name"] for d in procedures}
        diag_list = "\n".join(f"  - {c}: {n}" for c, n in diag_set.items())

        prompt = f"""You are a Nigerian HMO medical officer. This PA has {drug_count} drugs — exceeding the 5-drug polypharmacy threshold.

DIAGNOSES:
{diag_list}

DRUGS REQUESTED ({drug_count} total):
{drug_list}

Are ALL {drug_count} drugs genuinely necessary? Flag as INAPPROPRIATE if any drug:
- Lacks a clear indication for the listed diagnoses
- Duplicates another drug in the same class
- Suggests claim padding

Respond ONLY in JSON:
{{
  "verdict": "APPROPRIATE" or "INAPPROPRIATE",
  "confidence": 0-100,
  "reasoning": "State which drugs are justified and which are not.",
  "problematic_drugs": ["procedure_code_list"]
}}"""

        try:
            import anthropic as _ant
            client = _ant.Anthropic(max_retries=0)
            resp = _anthropic_create_with_retry(client,
                model="claude-haiku-4-5-20251001",
                max_tokens=600,
                system=self.AI_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}]
            )
            raw  = _re.sub(r'^```(?:json)?\s*|\s*```$', '', resp.content[0].text.strip())
            data = _json.loads(raw)

            verdict    = data.get("verdict", "APPROPRIATE")
            ai_reason  = data.get("reasoning", "")
            problematic = data.get("problematic_drugs", [])

            if verdict == "INAPPROPRIATE":
                prob_str = f" Flagged: {', '.join(problematic)}." if problematic else ""
                result["triggered"] = True
                result["reason"] = (
                    f"❌ POLYPHARMACY: {drug_count} drugs exceed the {THRESHOLD}-drug threshold. "
                    f"AI assessment: {ai_reason}{prob_str} Auto-denied for medical officer review."
                )
            else:
                result["reason"] = (
                    f"⚠️ POLYPHARMACY WARNING: {drug_count} drugs (>{THRESHOLD}) but AI found all "
                    f"clinically justified. {ai_reason}"
                )
        except Exception as e:
            logger.warning(f"check_polypharmacy AI error: {e}")
            result["triggered"] = True
            result["reason"] = (
                f"❌ POLYPHARMACY: {drug_count} drugs exceed the {THRESHOLD}-drug threshold. "
                f"AI assessment unavailable — auto-denied for medical officer review."
            )

        return result

    # ==================================================================
    # PER-LINE CHECK: LEVEL OF CARE (ADM with mild diagnosis)
    # ==================================================================

    def check_level_of_care(
        self,
        procedure_code: str,
        procedure_name: str,
        diagnosis_code: str,
        diagnosis_name: str,
    ) -> Dict:
        """
        Denies inpatient admission charges (ADM codes) for mild diagnoses that
        do not require hospitalisation per WHO / standard clinical guidelines.

        Returns: {triggered, reason}
        """
        pn = procedure_name.upper()
        is_adm = (
            procedure_code.upper().startswith("ADM")
            or "ADMISSION" in pn
            or "BED CHARGE" in pn
            or "WARD CHARGE" in pn
            or "DAILY CHARGE" in pn
        )
        if not is_adm or not self._is_mild_diagnosis(diagnosis_code):
            return {"triggered": False, "reason": ""}

        return {
            "triggered": True,
            "reason": (
                f"❌ LEVEL_OF_CARE: Inpatient admission ({procedure_code} — {procedure_name}) "
                f"is not clinically justified for '{diagnosis_name}' ({diagnosis_code}). "
                f"This condition is manageable in outpatient/daycare per WHO care guidelines."
            ),
        }

    # ==================================================================
    # PER-LINE CHECK: IV FLUID PADDING (outpatient + mild diagnosis)
    # ==================================================================

    def check_iv_fluid_padding(
        self,
        procedure_code: str,
        procedure_name: str,
        diagnosis_code: str,
        diagnosis_name: str,
        has_adm_in_batch: bool,
    ) -> Dict:
        """
        Denies IV fluid line items for outpatient encounters with mild diagnoses.

        Returns: {triggered, reason}
        """
        if not any(p in procedure_name.upper() for p in self._IV_FLUID_PATTERNS):
            return {"triggered": False, "reason": ""}
        if has_adm_in_batch:                          # inpatient — IV justified
            return {"triggered": False, "reason": ""}
        if not self._is_mild_diagnosis(diagnosis_code):
            return {"triggered": False, "reason": ""}

        return {
            "triggered": True,
            "reason": (
                f"❌ IV_FLUID_PADDING: IV fluid ({procedure_code} — {procedure_name}) is not "
                f"clinically justified for outpatient management of '{diagnosis_name}' "
                f"({diagnosis_code}). Oral rehydration/medication is standard first-line care."
            ),
        }

    # ==================================================================
    # PER-LINE CHECK: VITAMIN / SUPPLEMENT PADDING
    # ==================================================================

    def check_vitamin_padding(
        self,
        procedure_code: str,
        procedure_name: str,
        diagnosis_code: str,
        diagnosis_name: str,
    ) -> Dict:
        """
        Flags vitamin/supplement requests without a specific clinical indication.

        Returns: {triggered, reason}
        """
        pn = procedure_name.upper()
        if not any(v in pn for v in self._VITAMIN_PATTERNS):
            return {"triggered": False, "reason": ""}

        combined = (diagnosis_name + " " + diagnosis_code).upper()
        if any(ind in combined for ind in self._VITAMIN_INDICATIONS):
            return {"triggered": False, "reason": ""}

        return {
            "triggered": True,
            "reason": (
                f"❌ VITAMIN_PADDING: {procedure_name} ({procedure_code}) has no documented "
                f"clinical indication for '{diagnosis_name}' ({diagnosis_code}). "
                f"Vitamins are covered only for anaemia, pregnancy, nutritional deficiency, "
                f"sickle cell disease, or chronic disease states."
            ),
        }

    # ==================================================================
    # BATCH CHECK: SHOTGUN LABS (≥4 non-specific panel labs in one PA)
    # ==================================================================

    def check_shotgun_labs(self, procedures: List[Dict]) -> Dict:
        """
        Detects 'shotgun' lab ordering: ≥4 of (FBC, Widal, Malaria parasite,
        Urinalysis, RBS) ordered together without targeted clinical rationale.

        Returns: {triggered, flagged_codes, reason}
        """
        THRESHOLD = 4
        hits = [
            rp["procedure_code"] for rp in procedures
            if any(p in rp["procedure_name"].upper() for p in self._SHOTGUN_LAB_PATTERNS)
        ]
        if len(hits) < THRESHOLD:
            return {"triggered": False, "flagged_codes": [], "reason": ""}

        return {
            "triggered": True,
            "flagged_codes": hits,
            "reason": (
                f"❌ SHOTGUN_LABS: {len(hits)} non-specific panel labs submitted together "
                f"({', '.join(hits)}). Ordering FBC, Widal, Malaria parasite, Urinalysis, "
                f"and RBS as a blanket panel is not evidence-based targeted practice. "
                f"Investigations must be linked to a specific clinical working diagnosis."
            ),
        }

    # ==================================================================
    # BATCH CHECK: SYMPTOM-ONLY INVESTIGATION (R-code + ≥3 labs)
    # ==================================================================

    def check_symptom_only_investigations(
        self,
        procedures: List[Dict],
        primary_diagnosis_code: str,
    ) -> Dict:
        """
        When the primary diagnosis is a symptom/sign-only R-code and ≥3 lab
        investigations are requested, the labs are flagged — investigations
        should follow a working diagnosis, not a vague symptom code.

        Returns: {triggered, flagged_codes, reason}
        """
        THRESHOLD = 3
        if not primary_diagnosis_code.strip().upper().startswith("R"):
            return {"triggered": False, "flagged_codes": [], "reason": ""}

        lab_codes = [
            rp["procedure_code"] for rp in procedures
            if (not rp["procedure_code"].upper().startswith("CONS")
                and any(kw in rp["procedure_name"].upper() for kw in self._LAB_INVESTIGATION_KW))
        ]
        if len(lab_codes) < THRESHOLD:
            return {"triggered": False, "flagged_codes": [], "reason": ""}

        return {
            "triggered": True,
            "flagged_codes": lab_codes,
            "reason": (
                f"❌ SYMPTOM_ONLY_INVESTIGATION: Primary diagnosis '{primary_diagnosis_code}' "
                f"is a symptom/sign-only ICD-10 R-code (non-specific finding). "
                f"{len(lab_codes)} lab investigations ({', '.join(lab_codes)}) cannot be "
                f"approved against a vague symptom code alone. A working clinical diagnosis "
                f"is required before investigations are authorised."
            ),
        }

    # ==================================================================
    # PER-LINE CHECK: DIAGNOSIS ACUITY MISMATCH
    # High-acuity procedure (IV antimalarial) + mild diagnosis (uncomplicated malaria)
    # ==================================================================

    def check_diagnosis_acuity_mismatch(
        self,
        procedure_code: str,
        procedure_name: str,
        diagnosis_code: str,
        diagnosis_name: str,
    ) -> Dict:
        """
        Denies high-acuity procedures paired with a mild diagnosis that doesn't
        warrant them — specifically IV/injectable antimalarials for uncomplicated malaria.

        Returns: {triggered, reason}
        """
        pn = procedure_name.upper()
        IV_ANTIMALARIAL = (
            "ARTESUNATE INJ", "ARTESUNATE IV",
            "ARTEMETHER INJ", "ARTEMETHER IV",
            "QUININE INJ", "QUININE IV", "QUININE IM", "QUININE INFUSION",
        )
        SEVERE_MALARIA_KW = ("SEVERE MALARIA", "CEREBRAL MALARIA", "COMPLICATED MALARIA")
        UNCOMPLICATED_PREFIXES = (
            "B500", "B501", "B509",
            "B510", "B519",
            "B520", "B521", "B528", "B529",
            "B530", "B531", "B538",
        )

        if not any(kw in pn for kw in IV_ANTIMALARIAL):
            return {"triggered": False, "reason": ""}

        dcode = diagnosis_code.strip().upper()
        is_uncomplicated = any(dcode.startswith(p) for p in UNCOMPLICATED_PREFIXES)
        if not is_uncomplicated:
            return {"triggered": False, "reason": ""}

        # If the name itself says "severe" / "cerebral" — allow it despite the mild code
        if any(kw in diagnosis_name.upper() for kw in SEVERE_MALARIA_KW):
            return {"triggered": False, "reason": ""}

        return {
            "triggered": True,
            "reason": (
                f"❌ DIAGNOSIS_ACUITY_MISMATCH: IV/injectable antimalarial "
                f"({procedure_code} — {procedure_name}) prescribed for uncomplicated malaria "
                f"({diagnosis_code} — {diagnosis_name}). WHO guidelines: uncomplicated malaria "
                f"is treated with oral ACT (e.g., artemether-lumefantrine). IV artesunate/quinine "
                f"is reserved for severe or cerebral malaria only."
            ),
        }

    # ==================================================================
    # TARIFF & QUANTITY HELPERS
    # ==================================================================

    def get_tariff_price(self, provider_id: str, procedure_code: str) -> Optional[float]:
        """Look up contracted tariff price for a provider + procedure via MotherDuck."""
        try:
            result = self.conn.execute("""
                SELECT t.tariffamount
                FROM   "AI DRIVEN DATA".PROVIDERS p
                JOIN   "AI DRIVEN DATA".PROVIDERS_TARIFF pt
                       ON p.protariffid = pt.protariffid
                JOIN   "AI DRIVEN DATA".TARIFF t
                       ON CAST(pt.tariffid AS VARCHAR) = t.tariffid
                WHERE  p.providerid = ?
                  AND  UPPER(TRIM(t.procedurecode)) = UPPER(TRIM(?))
                  AND  t.tariffamount > 0
                LIMIT 1
            """, [provider_id, procedure_code]).fetchone()
            return float(result[0]) if result else None
        except Exception as e:
            logger.warning(f"Tariff lookup error ({provider_id}/{procedure_code}): {e}")
            return None

    def get_max_quantity(self, procedure_code: str) -> Optional[int]:
        """Get max allowed quantity from PROCEDURE_MASTER. Returns None if not defined."""
        try:
            doc = mongo_db.get_procedure_master(procedure_code)
            if doc:
                qty = doc.get("Quantity_limit") or doc.get("quantity")
                if qty is not None and str(qty).strip() not in ("", "None", "nan"):
                    qty_int = int(float(str(qty)))
                    if qty_int > 0:
                        return qty_int
        except Exception:
            pass
        return None

    def ai_check_quantity(
        self, procedure_name: str, procedure_class: str, quantity: int
    ) -> Dict:
        """Use Claude Haiku to determine if the requested quantity is clinically reasonable."""
        import anthropic as _ant, json as _json, re as _re
        try:
            client = _ant.Anthropic(max_retries=0)
            prompt = f"""You are a clinical pharmacist reviewing a Nigerian HMO pre-authorization.
Assess whether the requested quantity is clinically reasonable for a single prescription/visit.

Procedure : {procedure_name}
Drug Class : {procedure_class or "Unknown"}
Requested  : {quantity} unit(s)

Return ONLY valid JSON (no markdown):
{{
  "is_reasonable": true or false,
  "max_reasonable_quantity": <integer — maximum clinically appropriate for one prescription>,
  "reasoning": "brief explanation"
}}"""
            resp = _anthropic_create_with_retry(client,
                model="claude-haiku-4-5-20251001", max_tokens=200,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = resp.content[0].text.strip()
            raw = _re.sub(r'^```(?:json)?\s*', '', raw)
            raw = _re.sub(r'\s*```$', '', raw)
            data = _json.loads(raw)
            return {
                "is_reasonable": bool(data.get("is_reasonable", True)),
                "max_reasonable_quantity": max(1, int(data.get("max_reasonable_quantity", quantity))),
                "reasoning": data.get("reasoning", ""),
                "source": "ai",
            }
        except Exception as e:
            logger.warning(f"AI quantity check error: {e}")
            return {"is_reasonable": True, "max_reasonable_quantity": quantity,
                    "reasoning": "", "source": "ai_error"}

    # ==================================================================
    # RULE 7: CLINICAL NECESSITY (standalone — callable from pipeline)
    # ==================================================================

    def run_clinical_necessity(
        self,
        procedure_code: str,
        diagnosis_code: str,
        enrollee_id: Optional[str],
        encounter_date: Optional[str],
        all_request_procedures: Optional[List[Dict]] = None,
    ) -> Optional[Dict]:
        """
        Run Rule 7 (Clinical Necessity) for a single procedure.
        Activation criteria are checked first; returns None if not applicable.

        Returns:
            {"rule": RuleResult, "dual_antibiotic_override": bool}
            or None if activation criteria not met / engine unavailable.
        """
        if not (enrollee_id and encounter_date and self.clinical_necessity_engine):
            return None

        try:
            proc_info_cn  = self._resolve_procedure_info(procedure_code)
            proc_name_cn  = proc_info_cn.get("name", procedure_code)
            proc_class_cn = proc_info_cn.get("category", proc_info_cn.get("class", "")).upper()

            # ── Criterion 1: severe diagnosis ────────────────────────────────
            SEVERE_KEYWORDS = {
                "SEPSIS", "SEPTICAEMIA", "SEPTICEMIA", "MENINGITIS",
                "PERITONITIS", "GANGRENE", "NECROTISING", "NECROTIZING",
                "MAJOR SURGERY", "MAJOR WOUND", "SEVERE PNEUMONIA",
                "SEVERE MALARIA", "CEREBRAL MALARIA", "ECLAMPSIA",
                "PULMONARY EMBOLISM", "MYOCARDIAL INFARCTION",
                "STROKE", "ENCEPHALITIS", "ORGAN FAILURE",
                "DIABETIC KETOACIDOSIS", "ANAPHYLAXIS", "SHOCK",
            }
            diag_name_upper = self._resolve_diagnosis_info(diagnosis_code).get("name", "").upper()
            criterion_severe = any(kw in diag_name_upper for kw in SEVERE_KEYWORDS)

            # ── Criterion 2: injectable medication ───────────────────────────
            INJECTABLE_KW = {"INJ", "INJECTION", "IV ", "INTRAVENOUS",
                              "INFUSION", "AMPOULE", "AMP", "VIAL", "IM ",
                              "INTRAMUSCULAR"}
            INJECTABLE_FORMULATIONS = {"INJECTION", "IV", "INFUSION", "AMPOULE", "VIAL", "IM"}
            proc_name_upper = proc_name_cn.upper()
            criterion_injectable = any(kw in proc_name_upper for kw in INJECTABLE_KW)
            if not criterion_injectable:
                try:
                    _doc = mongo_db.get_procedure_master(procedure_code)
                    if _doc and _doc.get("formulation", "").upper() in INJECTABLE_FORMULATIONS:
                        criterion_injectable = True
                except Exception:
                    pass

            ANTIBIOTIC_CLASSES_SET = {
                "ANTIBIOTIC", "ANTIBACTERIAL", "ANTIMICROBIAL",
                "ANTIBIOTICS", "ANTI-INFECTIVE"
            }

            # ── Criterion 3: mandatory confirmatory test ──────────────────────
            HPYLORI_KEYWORDS = {
                "HELICOBACTER", "H. PYLORI", "H.PYLORI", "PYLORI",
                "PEPTIC ULCER", "GASTRIC ULCER", "DUODENAL ULCER",
                "GASTRITIS", "PEPTIC"
            }
            UTI_KEYWORDS = {
                "URINARY TRACT INFECTION", "UTI", "CYSTITIS",
                "PYELONEPHRITIS", "URETHRITIS", "BACTERIURIA"
            }
            criterion_test_required = False
            if proc_class_cn in ANTIBIOTIC_CLASSES_SET:
                if any(kw in diag_name_upper for kw in HPYLORI_KEYWORDS):
                    criterion_test_required = True
                elif any(kw in diag_name_upper for kw in UTI_KEYWORDS):
                    criterion_test_required = True

            # ── Criterion 4: dual antibiotic within 3 days ───────────────────
            criterion_dual_antibiotic = False
            if proc_class_cn in ANTIBIOTIC_CLASSES_SET:
                try:
                    from datetime import timedelta as _td
                    enc_dt3   = datetime.strptime(encounter_date[:10], "%Y-%m-%d").date()
                    lookback3 = (enc_dt3 - _td(days=3)).strftime("%Y-%m-%d")
                    prior_abx = self.conn.execute("""
                        SELECT UPPER(TRIM(p.code)) as code
                        FROM "AI DRIVEN DATA"."PA DATA" p
                        WHERE p.IID = ?
                          AND CAST(p.requestdate AS DATE) >= ?
                          AND CAST(p.requestdate AS DATE) < ?
                          AND UPPER(LEFT(TRIM(p.code), 3)) = 'DRG'
                    """, [enrollee_id, lookback3, encounter_date]).fetchdf()
                    for _, row in prior_abx.iterrows():
                        code3 = str(row["code"]).upper()
                        if code3 == procedure_code.upper():
                            continue
                        doc3 = mongo_db.get_procedure_master(code3)
                        if doc3 and doc3.get("procedure_class", "").upper() in ANTIBIOTIC_CLASSES_SET:
                            criterion_dual_antibiotic = True
                            break
                except Exception as e:
                    logger.warning(f"Dual antibiotic criterion check error: {e}")

            activate_rule7 = (
                criterion_severe or criterion_injectable
                or criterion_dual_antibiotic or criterion_test_required
            )

            if not activate_rule7:
                logger.info(f"Rule 7 SKIPPED [{procedure_code}] — no activation criteria met")
                return None

            logger.info(
                f"Rule 7 ACTIVATED [{procedure_code}] — severe={criterion_severe} "
                f"injectable={criterion_injectable} dual_abx={criterion_dual_antibiotic} "
                f"test_required={criterion_test_required}"
            )

            cn = self.clinical_necessity_engine.check(
                procedure_code=procedure_code,
                procedure_name=proc_name_cn,
                procedure_class=proc_class_cn,
                diagnosis_code=diagnosis_code,
                diagnosis_name=diag_name_upper,
                enrollee_id=enrollee_id,
                encounter_date=encounter_date,
                all_request_procedures=all_request_procedures,
            )

            cn_rule = RuleResult(
                rule_name="CLINICAL_NECESSITY",
                passed=cn.passed,
                source=cn.source,
                confidence=cn.confidence,
                reasoning=cn.reasoning,
                details={
                    "severity": cn.severity,
                    "route": cn.route,
                    "route_appropriate": cn.route_appropriate,
                    "step_down_applicable": cn.step_down_applicable,
                    "tests_required": cn.tests_required,
                    "tests_found": cn.tests_found,
                    "concerns": cn.concerns,
                    "criteria_triggered": {
                        "severe_diagnosis": criterion_severe,
                        "injectable": criterion_injectable,
                        "test_required": criterion_test_required,
                        "dual_antibiotic_3day": criterion_dual_antibiotic,
                    }
                }
            )

            dual_antibiotic_override = (
                criterion_dual_antibiotic
                and not criterion_severe
                and not criterion_injectable
                and not criterion_test_required
            )

            return {"rule": cn_rule, "dual_antibiotic_override": dual_antibiotic_override}

        except Exception as e:
            logger.error(f"run_clinical_necessity error [{procedure_code}]: {e}")
            return None

    def validate_comprehensive(
        self,
        procedure_code: str,
        diagnosis_code: str,
        enrollee_id: Optional[str] = None,
        encounter_date: Optional[str] = None,
        all_request_procedures: Optional[List] = None,
        skip_rule7: bool = False,
        encounter_type: str = "OUTPATIENT",
        # Pre-fetched values to avoid redundant DB round-trips in bulk mode
        prefetched_enrollee_context=None,
        prefetched_proc_info: Optional[Dict] = None,
        prefetched_diag_info: Optional[Dict] = None,
    ) -> ComprehensiveValidation:
        """
        Run ALL validation rules and return comprehensive report
        
        FIXED: Resolves procedure and diagnosis names ONCE upfront,
        then passes them to all individual rule validators.
        
        Validation Rules:
        1. Procedure Age (if enrollee age known)
        2. Procedure Gender (if enrollee gender known)
        3. Diagnosis Age (if enrollee age known)
        4. Diagnosis Gender (if enrollee gender known)
        5. Procedure-Diagnosis Compatibility
        6. Procedure 30-Day Duplicate Check (exact + therapeutic class)
        
        Returns comprehensive report with all rule results
        """
        rule_results = []
        short_circuit = False  # stop evaluating rules after first failure

        # Use pre-fetched enrollee context if provided (avoids redundant DB round-trip in bulk mode)
        if prefetched_enrollee_context is not None:
            enrollee_context = prefetched_enrollee_context
        elif enrollee_id:
            enrollee_context = self.base_engine.get_enrollee_context(enrollee_id, encounter_date)
        else:
            enrollee_context = None

        # Get master table entries
        proc_master = self.base_engine.check_procedure_master(procedure_code)
        diag_master = self.base_engine.check_diagnosis_master(diagnosis_code)

        # Use pre-fetched name resolutions if provided (avoids redundant DB round-trips in bulk mode)
        proc_info = prefetched_proc_info or self._resolve_procedure_info(procedure_code)
        diag_info = prefetched_diag_info or self._resolve_diagnosis_info(diagnosis_code)
        
        # ===================================================================
        # RULE 1: PROCEDURE AGE VALIDATION
        # ===================================================================
        if enrollee_context and enrollee_context.age is not None:
            if proc_master:
                # Master table validation
                proc_age_result = self.base_engine.validate_age_for_procedure(
                    procedure_code, enrollee_context.age
                )
                rule_results.append(RuleResult(
                    rule_name="PROCEDURE_AGE",
                    passed=proc_age_result.get("is_valid", True),
                    source="master_table",
                    confidence=100,
                    reasoning=proc_age_result.get("reasoning", "Age validated by master table"),
                    details=proc_age_result
                ))
            else:
                # Check learning table first
                age_learning = self.base_engine.check_procedure_age_learning(
                    procedure_code, enrollee_context.age
                )
                if age_learning:
                    # Format reasoning based on whether it's approval or denial
                    is_valid = age_learning['is_valid']
                    stored_reason = age_learning.get('reason', '')
                    min_age = age_learning.get('min_age')
                    max_age = age_learning.get('max_age')
                    
                    if is_valid:
                        reasoning = f"✅ Valid for age {enrollee_context.age} (learned approval: {stored_reason})"
                    else:
                        reasoning = f"❌ AGE RESTRICTION: {procedure_code} is invalid for age {enrollee_context.age} (learned denial: {stored_reason})"
                    
                    rule_results.append(RuleResult(
                        rule_name="PROCEDURE_AGE",
                        passed=is_valid,
                        source="learning_table",
                        confidence=age_learning.get('confidence', 95),
                        reasoning=reasoning,
                        details={
                            **age_learning,
                            "enrollee_age": enrollee_context.age,
                            "learned_from_previous": True
                        }
                    ))
                else:
                    # Not in master or learning - CALL AI with RESOLVED NAME
                    ai_result = self._ai_validate_procedure_age(
                        procedure_code, enrollee_context.age, proc_info=proc_info
                    )
                    
                    rule_results.append(RuleResult(
                        rule_name="PROCEDURE_AGE",
                        passed=ai_result['is_valid'],
                        source="ai",
                        confidence=ai_result['confidence'],
                        reasoning=ai_result['reasoning'],
                        details={
                            "enrollee_age": enrollee_context.age,
                            "ai_validated": True,
                            "validation_type": "procedure_age",
                            "resolved_name": proc_info['name'],
                            "resolved_category": proc_info['category']
                        }
                    ))
        
        # Short-circuit: stop after first rule failure
        if rule_results and not rule_results[-1].passed:
            short_circuit = True

        # ===================================================================
        # RULE 2: PROCEDURE GENDER VALIDATION
        # ===================================================================
        if not short_circuit and enrollee_context and enrollee_context.gender:
            if proc_master:
                # Master table validation
                proc_gender_result = self.base_engine.validate_gender_for_procedure(
                    procedure_code, enrollee_context.gender
                )
                rule_results.append(RuleResult(
                    rule_name="PROCEDURE_GENDER",
                    passed=proc_gender_result.get("is_valid", True),
                    source="master_table",
                    confidence=100 if not proc_gender_result.get("is_valid", True) else 95,
                    reasoning=proc_gender_result.get("reasoning", "Gender validated by master table"),
                    details=proc_gender_result
                ))
            else:
                # Check learning table first
                gender_learning = self.base_engine.check_procedure_gender_learning(
                    procedure_code, enrollee_context.gender
                )
                if gender_learning:
                    is_valid = gender_learning['is_valid']
                    stored_reason = gender_learning.get('reason', '')
                    
                    if is_valid:
                        reasoning = f"✅ Valid for {enrollee_context.gender} (learned approval: {stored_reason})"
                    else:
                        reasoning = f"❌ GENDER RESTRICTION: {procedure_code} is invalid for {enrollee_context.gender} (learned denial: {stored_reason})"
                    
                    rule_results.append(RuleResult(
                        rule_name="PROCEDURE_GENDER",
                        passed=is_valid,
                        source="learning_table",
                        confidence=gender_learning.get('confidence', 95),
                        reasoning=reasoning,
                        details={
                            **gender_learning,
                            "enrollee_gender": enrollee_context.gender,
                            "learned_from_previous": True
                        }
                    ))
                else:
                    # Not in master or learning - CALL AI with RESOLVED NAME
                    ai_result = self._ai_validate_procedure_gender(
                        procedure_code, enrollee_context.gender, proc_info=proc_info
                    )
                    
                    rule_results.append(RuleResult(
                        rule_name="PROCEDURE_GENDER",
                        passed=ai_result['is_valid'],
                        source="ai",
                        confidence=ai_result['confidence'],
                        reasoning=ai_result['reasoning'],
                        details={
                            "enrollee_gender": enrollee_context.gender,
                            "ai_validated": True,
                            "validation_type": "procedure_gender",
                            "resolved_name": proc_info['name'],
                            "resolved_category": proc_info['category']
                        }
                    ))
        
        # Short-circuit: stop after first rule failure
        if rule_results and not rule_results[-1].passed:
            short_circuit = True

        # ===================================================================
        # RULE 3: DIAGNOSIS AGE VALIDATION
        # ===================================================================
        if not short_circuit and enrollee_context and enrollee_context.age is not None:
            if diag_master:
                # Master table validation
                diag_age_result = self.base_engine.validate_age_for_diagnosis(
                    diagnosis_code, enrollee_context.age
                )
                rule_results.append(RuleResult(
                    rule_name="DIAGNOSIS_AGE",
                    passed=diag_age_result.get("is_valid", True),
                    source="master_table",
                    confidence=95,
                    reasoning=diag_age_result.get("reasoning", "Age validated by master table"),
                    details=diag_age_result
                ))
            else:
                # Check learning table first
                age_learning = self.base_engine.check_diagnosis_age_learning(
                    diagnosis_code, enrollee_context.age
                )
                if age_learning:
                    is_valid = age_learning['is_valid']
                    stored_reason = age_learning.get('reason', '')
                    
                    if is_valid:
                        reasoning = f"✅ Valid for age {enrollee_context.age} (learned approval: {stored_reason})"
                    else:
                        reasoning = f"❌ AGE RESTRICTION: {diagnosis_code} is invalid for age {enrollee_context.age} (learned denial: {stored_reason})"
                    
                    rule_results.append(RuleResult(
                        rule_name="DIAGNOSIS_AGE",
                        passed=is_valid,
                        source="learning_table",
                        confidence=age_learning.get('confidence', 90),
                        reasoning=reasoning,
                        details={
                            **age_learning,
                            "enrollee_age": enrollee_context.age,
                            "learned_from_previous": True
                        }
                    ))
                else:
                    # Not in master or learning - CALL AI with RESOLVED NAME
                    ai_result = self._ai_validate_diagnosis_age(
                        diagnosis_code, enrollee_context.age, diag_info=diag_info
                    )
                    
                    rule_results.append(RuleResult(
                        rule_name="DIAGNOSIS_AGE",
                        passed=ai_result['is_valid'],
                        source="ai",
                        confidence=ai_result['confidence'],
                        reasoning=ai_result['reasoning'],
                        details={
                            "enrollee_age": enrollee_context.age,
                            "ai_validated": True,
                            "validation_type": "diagnosis_age",
                            "resolved_name": diag_info['name'],
                            "resolved_category": diag_info['category']
                        }
                    ))
        
        # Short-circuit: stop after first rule failure
        if rule_results and not rule_results[-1].passed:
            short_circuit = True

        # ===================================================================
        # RULE 4: DIAGNOSIS GENDER VALIDATION
        # ===================================================================
        if not short_circuit and enrollee_context and enrollee_context.gender:
            if diag_master:
                # Master table validation
                diag_gender_result = self.base_engine.validate_gender_for_diagnosis(
                    diagnosis_code, enrollee_context.gender
                )
                rule_results.append(RuleResult(
                    rule_name="DIAGNOSIS_GENDER",
                    passed=diag_gender_result.get("is_valid", True),
                    source="master_table",
                    confidence=100 if not diag_gender_result.get("is_valid", True) else 95,
                    reasoning=diag_gender_result.get("reasoning", "Gender validated by master table"),
                    details=diag_gender_result
                ))
            else:
                # Check learning table first
                gender_learning = self.base_engine.check_diagnosis_gender_learning(
                    diagnosis_code, enrollee_context.gender
                )
                if gender_learning:
                    is_valid = gender_learning['is_valid']
                    stored_reason = gender_learning.get('reason', '')
                    
                    if is_valid:
                        reasoning = f"✅ Valid for {enrollee_context.gender} (learned approval: {stored_reason})"
                    else:
                        reasoning = f"❌ GENDER RESTRICTION: {diagnosis_code} is invalid for {enrollee_context.gender} (learned denial: {stored_reason})"
                    
                    rule_results.append(RuleResult(
                        rule_name="DIAGNOSIS_GENDER",
                        passed=is_valid,
                        source="learning_table",
                        confidence=gender_learning.get('confidence', 90),
                        reasoning=reasoning,
                        details={
                            **gender_learning,
                            "enrollee_gender": enrollee_context.gender,
                            "learned_from_previous": True
                        }
                    ))
                else:
                    # Not in master or learning - CALL AI with RESOLVED NAME
                    ai_result = self._ai_validate_diagnosis_gender(
                        diagnosis_code, enrollee_context.gender, diag_info=diag_info
                    )
                    
                    rule_results.append(RuleResult(
                        rule_name="DIAGNOSIS_GENDER",
                        passed=ai_result['is_valid'],
                        source="ai",
                        confidence=ai_result['confidence'],
                        reasoning=ai_result['reasoning'],
                        details={
                            "enrollee_gender": enrollee_context.gender,
                            "ai_validated": True,
                            "validation_type": "diagnosis_gender",
                            "resolved_name": diag_info['name'],
                            "resolved_category": diag_info['category']
                        }
                    ))
        
        # ===================================================================
        # RULE 5: PROCEDURE-DIAGNOSIS COMPATIBILITY
        # ===================================================================
        # NEW FLOW (v2.0):
        #   1. Learning table → if found, use it
        #   2. Universal consultation codes (CONS021/CONS022) → auto-approve
        #   3. PROCEDURE_DIAGNOSIS_COMP table → if pair exists, approve
        #   4. Pair NOT in COMP → AI validates (human confirms → learning)
        #
        # REMOVED: typical_diagnoses matching (columns now empty)
        # REMOVED: typical_symptoms / implied_symptoms overlap matching
        #          (flawed: shared symptoms like "fever" caused false approvals)
        # ===================================================================
        
        # Short-circuit: stop after first rule failure
        if rule_results and not rule_results[-1].passed:
            short_circuit = True

        # Step 1: Check learning table
        learning_result = (
            None if short_circuit
            else self.base_engine.check_procedure_diagnosis_learning(procedure_code, diagnosis_code)
        )

        # Discard untrusted learning entries — fall through to AI
        if learning_result and not mongo_db.is_learning_trusted(learning_result):
            learning_result = None

        if learning_result:
            is_valid = learning_result.get("is_valid", False)
            stored_reason = learning_result.get("reasoning", '')

            if is_valid:
                reasoning = f"✅ Valid combination (learned approval: {stored_reason})"
            else:
                reasoning = f"❌ INCOMPATIBLE: {procedure_code} + {diagnosis_code} mismatch (learned denial: {stored_reason})"

            rule_results.append(RuleResult(
                rule_name="PROC_DIAG_COMPATIBILITY",
                passed=is_valid,
                source="learning_table",
                confidence=learning_result.get("confidence", 100),
                reasoning=reasoning,
                details={
                    **learning_result,
                    "learned_from_previous": True
                }
            ))

        # Step 2: Universal consultation codes
        elif not short_circuit and self.base_engine.is_universal_procedure(procedure_code):
            proc_name = proc_info.get('name', procedure_code)
            diag_name = diag_info.get('name', diagnosis_code)
            rule_results.append(RuleResult(
                rule_name="PROC_DIAG_COMPATIBILITY",
                passed=True,
                source="master_table",
                confidence=100,
                reasoning=f"✅ {proc_name} is a universal consultation — valid for all diagnoses including {diag_name}",
                details={"match_type": "UNIVERSAL_CONSULTATION"}
            ))
        
        # Step 3: PROCEDURE_DIAGNOSIS_COMP table lookup
        elif not short_circuit:
            comp_result = self.base_engine.check_procedure_diagnosis_comp(
                procedure_code, diagnosis_code
            )
            
            if comp_result:
                rule_results.append(RuleResult(
                    rule_name="PROC_DIAG_COMPATIBILITY",
                    passed=True,
                    source="master_table",
                    confidence=100,
                    reasoning=f"✅ {comp_result['procedure_name']} is a validated match for {comp_result['diagnosis_name']}",
                    details={"match_type": "COMP_TABLE_MATCH", "comp_result": comp_result}
                ))
            else:
                # Step 4: Pair NOT in COMP → AI validates
                # NOT being in COMP does NOT mean invalid — just not curated yet
                ai_result = self._ai_validate_proc_diag_compatibility(
                    procedure_code,
                    diagnosis_code,
                    age=enrollee_context.age if enrollee_context else None,
                    gender=enrollee_context.gender if enrollee_context else None,
                    proc_info=proc_info,
                    diag_info=diag_info,
                    encounter_type=encounter_type,
                )
                
                rule_results.append(RuleResult(
                    rule_name="PROC_DIAG_COMPATIBILITY",
                    passed=ai_result['is_valid'],
                    source="ai",
                    confidence=ai_result['confidence'],
                    reasoning=ai_result['reasoning'],
                    details={
                        "ai_validated": True,
                        "not_in_comp_table": True,
                        "enrollee_age": enrollee_context.age if enrollee_context else None,
                        "enrollee_gender": enrollee_context.gender if enrollee_context else None,
                        "resolved_procedure": proc_info['name'],
                        "resolved_diagnosis": diag_info['name'],
                        "pubmed_triggered": ai_result.get('pubmed_triggered', False),
                        "pubmed_count": ai_result.get('pubmed_count', 0),
                        "pubmed_query": ai_result.get('pubmed_query', ''),
                        "pubmed_evidence": ai_result.get('pubmed_evidence', [])
                    }
                ))
        
        # Short-circuit: stop after first rule failure
        if rule_results and not rule_results[-1].passed:
            short_circuit = True

        # ===================================================================
        # RULE 6: PROCEDURE 30-DAY DUPLICATE CHECK
        # ===================================================================
        if not short_circuit and enrollee_id and encounter_date and self.thirty_day_engine:
            proc_30day = self.thirty_day_engine.validate_procedure_30_day(
                procedure_code=procedure_code,
                enrollee_id=enrollee_id,
                encounter_date=encounter_date
            )
            
            # Source label: only mark as AI if the actual duplicate items
            # were AI/RxNorm/PubMed classified. Non-duplicate items going
            # through AI (e.g. NORMAL SALINE) should not pollute the label.
            duplicate_items = proc_30day.exact_duplicate_items + proc_30day.class_duplicate_items
            ai_sources = {"ai", "rxnorm", "pubmed"}
            duplicate_needed_ai = any(
                getattr(item, 'classification_source', '') in ai_sources
                for item in duplicate_items
            )
            rule_source = "ai" if duplicate_needed_ai else "master_table"

            rule_results.append(RuleResult(
                rule_name="PROCEDURE_30DAY_DUPLICATE",
                passed=proc_30day.passed,
                source=rule_source,
                confidence=100 if not proc_30day.passed else 95,
                reasoning=proc_30day.reasoning if not proc_30day.passed else f"✅ No duplicate procedures in last 30 days (checked {len(proc_30day.history_items)} items)",
                details={
                    "input_code": proc_30day.input_code,
                    "input_class": proc_30day.input_therapeutic_class,
                    "history_count": len(proc_30day.history_items),
                    "exact_duplicates": len(proc_30day.exact_duplicate_items),
                    "class_duplicates": len(proc_30day.class_duplicate_items),
                    "history_items": [
                        {
                            "code": item.code,
                            "description": item.description,
                            "class": item.therapeutic_class,
                            "source": item.source,
                            "date": item.date,
                            "classification_source": item.classification_source,
                            "is_class_duplicate": item.code in {d.code for d in proc_30day.class_duplicate_items}
                        }
                        for item in proc_30day.history_items
                    ]
                }
            ))
        
        # ===================================================================
        # RULE 7: CLINICAL NECESSITY — delegates to run_clinical_necessity()
        # ===================================================================
        cn_rule          = None
        dual_antibiotic_override = False

        if not skip_rule7:
            cn_result = self.run_clinical_necessity(
                procedure_code=procedure_code,
                diagnosis_code=diagnosis_code,
                enrollee_id=enrollee_id,
                encounter_date=encounter_date,
                all_request_procedures=all_request_procedures,
            )
            if cn_result:
                cn_rule = cn_result["rule"]
                dual_antibiotic_override = cn_result["dual_antibiotic_override"]
                if cn_rule:
                    rule_results.append(cn_rule)

        # ===================================================================
        # DETERMINE OVERALL DECISION
        # ===================================================================
        core_rules    = [r for r in rule_results if r.rule_name != "CLINICAL_NECESSITY"]
        core_failed   = [r for r in core_rules if not r.passed]
        core_all_pass = len(core_failed) == 0 and len(core_rules) > 0
        core_has_ai   = any(r.source == "ai" for r in core_rules)

        # ── Special case: dual antibiotic override ───────────────────────────
        # If the ONLY failing core rule is PROCEDURE_30DAY_DUPLICATE triggered
        # by an antibiotic within 3 days, and Rule 7 approves → PENDING_REVIEW
        # instead of AUTO_DENY. Gives the agent a chance to review dual therapy.
        if (
            dual_antibiotic_override
            and cn_rule is not None and cn_rule.passed
            and len(core_failed) == 1
            and core_failed[0].rule_name == "PROCEDURE_30DAY_DUPLICATE"
        ):
            overall_decision   = "APPROVE"   # agent can see both sides
            overall_confidence = cn_rule.confidence
            overall_reasoning  = (
                "⚠️ 30-day duplicate (antibiotic) but Rule 7 finds dual therapy "
                f"clinically justified: {cn_rule.reasoning}"
            )
            requires_review    = True         # still goes to agent
            can_store          = False
            auto_deny          = False
            auto_deny_rules    = []

        elif core_all_pass:
            # Rules 1-6 all pass → Rule 7 decides
            if cn_rule is None or cn_rule.passed:
                overall_decision   = "APPROVE"
                overall_confidence = min(r.confidence for r in rule_results) if rule_results else 100
                overall_reasoning  = "✅ All validation rules passed"
            else:
                overall_decision   = "DENY"
                overall_confidence = cn_rule.confidence
                overall_reasoning  = f"⚠️ Clinical necessity concern: {cn_rule.reasoning}"
        else:
            # Rules 1-6 deny/review → Rule 7 has no impact
            overall_decision   = "DENY"
            overall_confidence = max(r.confidence for r in core_failed) if core_failed else 100
            failed_names       = [r.rule_name for r in core_failed]
            overall_reasoning  = f"❌ Failed rules: {', '.join(failed_names)}"

        if not dual_antibiotic_override:
            # AI validations: exclude CLINICAL_NECESSITY (not learnable)
            ai_validated_rules = [r for r in core_rules if r.source == "ai"]
            can_store          = len(ai_validated_rules) > 0
            cn_denied          = cn_rule is not None and not cn_rule.passed
            requires_review    = core_has_ai or cn_denied
        
        # ===================================================================
        # AUTO-DENY DETECTION
        # ===================================================================
        # If ALL failed rules come from learning table with:
        #   - is_valid = False (learned denial)
        #   - usage_count >= 3 (confirmed enough times to trust)
        #   - approved_by exists (was human-approved at least once)
        # Then auto-deny without human review.
        #
        # Safe because: human validated → used 3+ times without correction
        # → represents trusted institutional medical knowledge.
        # Override button still available in UI for edge cases.
        # ===================================================================
        AUTO_DENY_MIN_USAGE = 3

        if not dual_antibiotic_override:
            auto_deny      = False
            auto_deny_rules = []

        if not dual_antibiotic_override and overall_decision == "DENY" and core_failed:
            all_failed_qualify = True
            qualifying_rules   = []

            for rule in core_failed:
                is_learned_denial = rule.source == "learning_table" and not rule.passed
                if is_learned_denial and mongo_db.is_learning_trusted(rule.details):
                    qualifying_rules.append(rule)
                else:
                    all_failed_qualify = False
                    break

            if all_failed_qualify and qualifying_rules:
                auto_deny       = True
                auto_deny_rules = qualifying_rules
                requires_review = False
                overall_reasoning = (
                    f"🚫 AUTO-DENIED: {len(qualifying_rules)} learned denial(s) — " +
                    ", ".join(
                        f"{r.rule_name} (used {r.details.get('usage_count', 0)}x)"
                        for r in qualifying_rules
                    )
                )
        
        return ComprehensiveValidation(
            overall_decision=overall_decision,
            overall_confidence=overall_confidence,
            overall_reasoning=overall_reasoning,
            rule_results=rule_results,
            requires_human_review=requires_review,
            can_store_ai_approvals=can_store,
            auto_deny=auto_deny,
            auto_deny_rules=auto_deny_rules
        )
    
    def store_ai_validated_rules(
        self,
        procedure_code: str,
        diagnosis_code: str,
        validation: ComprehensiveValidation,
        approved_by: str = "Casey"
    ) -> Dict[str, bool]:
        """
        Store AI-validated rules (BOTH approvals AND denials) for learning
        
        This is the core of the learning system:
        - PASSED rules: Store as is_valid=TRUE
        - FAILED rules: Store as is_valid=FALSE
        
        Examples:
        - ✅ PROCEDURE_AGE passed: Store min_age=45, max_age=45, is_valid=TRUE
        - ❌ PROCEDURE_AGE failed: Store min_age=5, max_age=5, is_valid=FALSE
        - ❌ 30DAY_DUPLICATE failed: Extract pairwise class relationships
        
        Returns dict indicating which rules were stored
        """
        stored = {}
        
        if not validation.can_store_ai_approvals:
            return stored
        
        # Get ALL AI-validated rules (both passed and failed)
        ai_rules = validation.get_ai_validated_rules()
        
        for rule in ai_rules:
            try:
                # Guard: Never store AI error results as learned decisions
                if rule.confidence == 0 or (rule.reasoning and "AI error" in rule.reasoning):
                    logger.warning(f"Skipping storage of {rule.rule_name} - AI error/zero confidence")
                    continue
                
                if rule.rule_name == "PROCEDURE_AGE":
                    enrollee_age = rule.details.get("enrollee_age")
                    if enrollee_age is not None:
                        success = self.base_engine.store_procedure_age_decision(
                            procedure_code=procedure_code,
                            min_age=enrollee_age,
                            max_age=enrollee_age,
                            is_valid=rule.passed,
                            reason=rule.reasoning,
                            confidence=rule.confidence,
                            ai_reasoning=rule.reasoning,
                            approved_by=approved_by
                        )
                        if success:
                            stored["PROCEDURE_AGE"] = True
                
                elif rule.rule_name == "PROCEDURE_GENDER":
                    enrollee_gender = rule.details.get("enrollee_gender", "Unknown")
                    success = self.base_engine.store_procedure_gender_decision(
                        procedure_code=procedure_code,
                        allowed_gender=enrollee_gender,
                        is_valid=rule.passed,
                        reason=rule.reasoning,
                        confidence=rule.confidence,
                        ai_reasoning=rule.reasoning,
                        approved_by=approved_by
                    )
                    if success:
                        stored["PROCEDURE_GENDER"] = True
                
                elif rule.rule_name == "DIAGNOSIS_AGE":
                    enrollee_age = rule.details.get("enrollee_age")
                    if enrollee_age is not None:
                        success = self.base_engine.store_diagnosis_age_decision(
                            diagnosis_code=diagnosis_code,
                            min_age=enrollee_age,
                            max_age=enrollee_age,
                            is_valid=rule.passed,
                            reason=rule.reasoning,
                            confidence=rule.confidence,
                            ai_reasoning=rule.reasoning,
                            approved_by=approved_by
                        )
                        if success:
                            stored["DIAGNOSIS_AGE"] = True
                
                elif rule.rule_name == "DIAGNOSIS_GENDER":
                    enrollee_gender = rule.details.get("enrollee_gender", "Unknown")
                    success = self.base_engine.store_diagnosis_gender_decision(
                        diagnosis_code=diagnosis_code,
                        allowed_gender=enrollee_gender,
                        is_valid=rule.passed,
                        reason=rule.reasoning,
                        confidence=rule.confidence,
                        ai_reasoning=rule.reasoning,
                        approved_by=approved_by
                    )
                    if success:
                        stored["DIAGNOSIS_GENDER"] = True
                
                elif rule.rule_name == "PROC_DIAG_COMPATIBILITY":
                    # Guard: Don't store AI errors as learned decisions
                    if rule.confidence == 0 or (rule.reasoning and rule.reasoning.startswith("AI error")):
                        logger.warning(f"Skipping storage of PROC_DIAG_COMPATIBILITY - AI error result")
                        continue
                    
                    success = self.base_engine.store_approved_decision(
                        procedure_code=procedure_code,
                        diagnosis_code=diagnosis_code,
                        is_valid=rule.passed,
                        reason=rule.reasoning,
                        confidence=rule.confidence,
                        ai_reasoning=rule.reasoning,
                        approved_by=approved_by
                    )
                    if success:
                        stored["PROC_DIAG_COMPATIBILITY"] = True
                
                elif rule.rule_name == "PROCEDURE_30DAY_DUPLICATE":
                    # Store ALL AI-classified pairs so future lookups skip AI
                    if rule.details:
                        input_code = rule.details.get('input_code', '').lower()
                        input_class = rule.details.get('input_class', '')
                        history_items = rule.details.get('history_items', [])
                        
                        for item in history_items:
                            item_code = item.get('code', '').lower()
                            item_class = item.get('class', '')
                            classification_source = item.get('classification_source', '')
                            
                            # Only store AI-classified items (master/learning already known)
                            if classification_source != 'ai':
                                continue
                            
                            # Skip exact duplicates (same code)
                            if item_code == input_code:
                                continue
                            
                            # Determine if same class — use the flag set by the 30-day engine
                            # (avoids string-mismatch bugs like "ANTIBIOTIC" vs "Antibiotics")
                            is_same_class = item.get('is_class_duplicate', False)
                            
                            success = self.base_engine.store_procedure_class_decision(
                                procedure_code_1=input_code.upper(),
                                procedure_code_2=item_code.upper(),
                                shared_class=input_class if is_same_class else "",
                                same_class=is_same_class,
                                ai_confidence=rule.confidence,
                                ai_reasoning=f"Both classified as {input_class}" if is_same_class else f"Input: {input_class}, History: {item_class}",
                                approved_by=approved_by
                            )
                            if success:
                                if "PROCEDURE_30DAY_DUPLICATE" not in stored:
                                    stored["PROCEDURE_30DAY_DUPLICATE"] = []
                                stored["PROCEDURE_30DAY_DUPLICATE"].append(
                                    f"{input_code.upper()}+{item_code.upper()} ({'SAME' if is_same_class else 'DIFF'})"
                                )
            
            except Exception as e:
                print(f"Error storing {rule.rule_name}: {e}")
                stored[rule.rule_name] = False
        
        return stored
    
    # Backward compatibility
    def store_ai_approved_rules(self, *args, **kwargs):
        """Deprecated: Use store_ai_validated_rules instead"""
        return self.store_ai_validated_rules(*args, **kwargs)


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    print("Testing Comprehensive Validation Engine (FIXED v3.0)...\n")
    
    engine = ComprehensiveVettingEngine()
    
    # Test case: DRG1106 + C593 + Male (44yo) — the case that exposed the bug
    print("TEST CASE: DRG1106 (Amlodipine 10mg) + C593 (Malignant neoplasm of ovary)")
    print("Patient: Male, 44yo")
    print("Expected: PROCEDURE_AGE=PASS, PROCEDURE_GENDER=PASS, DIAGNOSIS_AGE=PASS,")
    print("          DIAGNOSIS_GENDER=FAIL, PROC_DIAG_COMPATIBILITY=FAIL")
    print()
    
    # First, show what the codes resolve to
    proc_info = engine._resolve_procedure_info("DRG1106")
    diag_info = engine._resolve_diagnosis_info("C593")
    print(f"DRG1106 resolves to: {proc_info['name']} ({proc_info['source']})")
    print(f"C593 resolves to: {diag_info['name']} ({diag_info['source']})")
    print()
    
    validation = engine.validate_comprehensive(
        procedure_code="DRG1106",
        diagnosis_code="C593",
        enrollee_id="CL/OCTA/723449/2023-A"
    )
    
    print("="*70)
    print(f"OVERALL DECISION: {validation.overall_decision}")
    print(f"Confidence: {validation.overall_confidence}%")
    print(f"Reasoning: {validation.overall_reasoning}")
    print("="*70)
    
    print("\nRULE RESULTS:")
    print("-"*70)
    for rule in validation.rule_results:
        status = "✅ PASS" if rule.passed else "❌ FAIL"
        print(f"{status} | {rule.rule_name:<30} | {rule.source:<15} | {rule.confidence}%")
        print(f"     {rule.reasoning}")
        if rule.details.get('resolved_name'):
            print(f"     [Resolved: {rule.details.get('resolved_name')}]")
        print()
    
    summary = validation.get_summary()
    print("="*70)
    print("SUMMARY:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    print(f"\nCan store AI approvals: {validation.can_store_ai_approvals}")
    print(f"Requires human review: {validation.requires_human_review}")