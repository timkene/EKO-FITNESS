#!/usr/bin/env python3
"""
NO-AUTH VETTING SYSTEM - LEARNING ENGINE (FIXED)
=================================================

Human-in-Loop learning system with:
- Correct schema names (AI DRIVEN DATA for master tables)
- Correct column names (procedure_code with underscore)
- Age/gender lookup from MEMBERS table
- Full enrollee context support

Author: Casey's AI Assistant
Date: February 2026
Version: 1.1 - FIXED
"""

import os
import re
import duckdb
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from anthropic import Anthropic
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from . import mongo_db

# Setup logger
logger = logging.getLogger(__name__)


def _anthropic_create_with_retry(client, **kwargs):
    """Call client.messages.create with exponential backoff on 529 overload errors."""
    import time
    last_err = None
    for attempt in range(4):
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
logging.basicConfig(level=logging.INFO)

# Load .env so ANTHROPIC_API_KEY is available when run via Streamlit or CLI
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_PATH = "ai_driven_data.duckdb"
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class VettingSuggestion:
    """AI suggestion for human review"""
    suggested_action: str  # 'APPROVE' or 'DENY'
    confidence: int  # 0-100
    reasoning: str
    source: str  # 'learning_table', 'master_table', 'ai_suggestion'
    requires_human_review: bool
    rule_details: Optional[Dict] = None

@dataclass
class EnrolleeContext:
    """Enrollee context for validation"""
    enrollee_id: str
    age: Optional[int] = None
    gender: Optional[str] = None
    encounter_date: Optional[str] = None

# ============================================================================
# LEARNING ENGINE CLASS
# ============================================================================

class LearningVettingEngine:
    """
    Vetting engine with learning capabilities
    
    Workflow:
    1. Check learning tables (human-approved)
    2. Check master tables
    3. Call AI for suggestion (if needed)
    4. Return suggestion for human approval
    """
    
    def __init__(self, db_path: str = DB_PATH):
        """Initialize engine with database connection"""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path, read_only=True)
        self.anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY, max_retries=0, timeout=30.0) if ANTHROPIC_API_KEY else None
        
        # Performance tracking
        self.stats = {
            'learning_hits': 0,
            'master_hits': 0,
            'ai_calls': 0,
            'total_requests': 0
        }
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    # ========================================================================
    # ENROLLEE CONTEXT LOOKUP
    # ========================================================================
    
    def get_enrollee_context(self, enrollee_id: str, encounter_date: Optional[str] = None) -> EnrolleeContext:
        """
        Get enrollee age and gender from MEMBERS table.
        Uses correct columns: genderid (1=Male, 2=Female), dob.
        """
        query = """
        SELECT 
            enrollee_id,
            genderid,
            dob
        FROM "AI DRIVEN DATA"."MEMBERS"
        WHERE enrollee_id = ?
        LIMIT 1
        """
        
        result = self.conn.execute(query, [enrollee_id]).fetchone()
        
        if not result:
            print(f"WARNING: Enrollee {enrollee_id} not found in MEMBERS table")
            return EnrolleeContext(
                enrollee_id=enrollee_id,
                age=None,
                gender=None,
                encounter_date=encounter_date
            )
        
        # Decode gender (1 = Male, 2 = Female)
        try:
            g = int(result[1]) if result[1] is not None else None
        except (TypeError, ValueError):
            g = None
        gender = 'Male' if g == 1 else 'Female' if g == 2 else 'Unknown'
        
        # Calculate age from dob
        dob = result[2]
        age = None
        
        if dob:
            reference_date = datetime.strptime(encounter_date, '%Y-%m-%d').date() if encounter_date else date.today()
            dob_date = dob if isinstance(dob, date) else datetime.strptime(str(dob), '%Y-%m-%d').date()
            age = reference_date.year - dob_date.year - ((reference_date.month, reference_date.day) < (dob_date.month, dob_date.day))
        
        return EnrolleeContext(
            enrollee_id=enrollee_id,
            age=age,
            gender=gender,
            encounter_date=encounter_date
        )
    
    # ========================================================================
    # AGE VALIDATION (AGE_RANGE lookup)
    # ========================================================================
    
    def get_age_range_bounds(self, age_type: str) -> Optional[Tuple[int, int]]:
        """
        Get min and max age for a given age_type from AGE_RANGE table.
        Returns (min_age, max_age) or None if not found. 'ALL' → (0, 120).
        """
        if not age_type or age_type.strip().upper() == 'ALL':
            return (0, 120)
        return mongo_db.get_age_range(age_type)
    
    def validate_age_for_procedure(
        self, procedure_code: str, enrollee_age: int
    ) -> Dict:
        """
        Validate if enrollee's age is appropriate for the procedure.
        Returns dict with is_valid, procedure_age_range, allowed_ages, enrollee_age, reasoning.
        """
        doc = mongo_db.get_procedure_master(procedure_code)
        if not doc:
            return {
                'is_valid': None,
                'procedure_age_range': None,
                'allowed_ages': 'Unknown',
                'enrollee_age': enrollee_age,
                'reasoning': f'Procedure {procedure_code} not found in master table'
            }
        proc_name = doc.get('procedure_name')
        proc_class = doc.get('procedure_class')
        age_range = doc.get('age_range')
        if not age_range or str(age_range).strip().upper() == 'ALL':
            return {
                'is_valid': True,
                'procedure_age_range': 'ALL',
                'allowed_ages': 'All ages (0-120)',
                'enrollee_age': enrollee_age,
                'reasoning': f'{proc_name} is appropriate for all ages'
            }
        age_bounds = self.get_age_range_bounds(str(age_range))
        if not age_bounds:
            return {
                'is_valid': None,
                'procedure_age_range': str(age_range),
                'allowed_ages': 'Unknown range',
                'enrollee_age': enrollee_age,
                'reasoning': f'Age range "{age_range}" not found in AGE_RANGE table'
            }
        min_age, max_age = age_bounds
        is_valid = min_age <= enrollee_age <= max_age
        return {
            'is_valid': is_valid,
            'procedure_age_range': str(age_range),
            'allowed_ages': f'{min_age}-{max_age} years',
            'enrollee_age': enrollee_age,
            'reasoning': (
                f'{proc_name} is for {age_range} ({min_age}-{max_age} years). '
                f'Enrollee is {enrollee_age} years old - {"VALID" if is_valid else "INVALID"}'
            )
        }
    
    def validate_age_for_diagnosis(
        self, diagnosis_code: str, enrollee_age: int
    ) -> Dict:
        """
        Validate if enrollee's age is appropriate for the diagnosis.
        Returns dict with is_valid, diagnosis_age_range, allowed_ages, enrollee_age, reasoning.
        """
        doc = mongo_db.get_diagnosis_master(diagnosis_code)
        if not doc:
            return {
                'is_valid': None,
                'diagnosis_age_range': None,
                'allowed_ages': 'Unknown',
                'enrollee_age': enrollee_age,
                'reasoning': f'Diagnosis {diagnosis_code} not found in master table'
            }
        diag_name = doc.get('diagnosis_name')
        diag_class = doc.get('diagnosis_class')
        age_range = doc.get('age_range')
        if not age_range or str(age_range).strip().upper() == 'ALL':
            return {
                'is_valid': True,
                'diagnosis_age_range': 'ALL',
                'allowed_ages': 'All ages (0-120)',
                'enrollee_age': enrollee_age,
                'reasoning': f'{diag_name} can occur at all ages'
            }
        age_bounds = self.get_age_range_bounds(str(age_range))
        if not age_bounds:
            return {
                'is_valid': None,
                'diagnosis_age_range': str(age_range),
                'allowed_ages': 'Unknown range',
                'enrollee_age': enrollee_age,
                'reasoning': f'Age range "{age_range}" not found in AGE_RANGE table'
            }
        min_age, max_age = age_bounds
        is_valid = min_age <= enrollee_age <= max_age
        return {
            'is_valid': is_valid,
            'diagnosis_age_range': str(age_range),
            'allowed_ages': f'{min_age}-{max_age} years',
            'enrollee_age': enrollee_age,
            'reasoning': (
                f'{diag_name} typically occurs in {age_range} ({min_age}-{max_age} years). '
                f'Enrollee is {enrollee_age} years old - {"VALID" if is_valid else "INVALID"}'
            )
        }

    def validate_gender_for_procedure(
        self, procedure_code: str, enrollee_gender: str
    ) -> Dict:
        """
        Validate if enrollee's gender is appropriate for the procedure.
        Returns dict with is_valid, gender_applicable, enrollee_gender, reasoning.
        """
        doc = mongo_db.get_procedure_master(procedure_code)
        if not doc:
            return {
                'is_valid': None,
                'gender_applicable': None,
                'enrollee_gender': enrollee_gender,
                'reasoning': f'Procedure {procedure_code} not found in master table'
            }
        proc_name = doc.get('procedure_name')
        proc_class = doc.get('procedure_class')
        gender_code = doc.get('gender_applicable')

        # If no gender restriction or applies to all
        if not gender_code or str(gender_code).strip().upper() in ['ALL', 'BOTH']:
            return {
                'is_valid': True,
                'gender_applicable': gender_code or 'ALL',
                'enrollee_gender': enrollee_gender,
                'reasoning': f'{proc_name} is appropriate for all genders'
            }

        # Normalize enrollee gender
        enrollee_gender_upper = enrollee_gender.upper().strip() if enrollee_gender else 'UNKNOWN'
        gender_code_upper = str(gender_code).strip().upper()

        # Get gender description from GENDER_TYPE table
        gender_description = mongo_db.get_gender_type(gender_code) or gender_code

        # ANATOMICAL KEYWORD DETECTION - Auto-upgrade gender restrictions
        # If procedure name contains anatomically-specific terms, upgrade to _ONLY
        proc_name_lower = proc_name.lower() if proc_name else ''
        
        # Female-only anatomical terms
        female_only_keywords = ['vulva', 'vagina', 'uterus', 'uterine', 'ovary', 'ovarian', 
                                 'cervix', 'cervical', 'pregnancy', 'pregnant', 'delivery', 
                                 'maternal', 'menstrual', 'menopause', 'endometrial', 'hysterectomy',
                                 'episiotomy', 'cesarean', 'caesarean']
        
        # Male-only anatomical terms
        male_only_keywords = ['prostate', 'prostatic', 'testicular', 'testis', 'testes',
                               'penis', 'penile', 'scrotum', 'scrotal', 'epididym', 
                               'vasectomy', 'circumcision']
        
        # Check for female-only anatomy
        if any(keyword in proc_name_lower for keyword in female_only_keywords):
            if gender_code_upper in ['FEMALE', 'BOTH', 'ALL']:
                # Auto-upgrade to FEMALE_ONLY
                gender_code_upper = 'FEMALE_ONLY'
                gender_description = 'Female-only (anatomical)'
        
        # Check for male-only anatomy
        elif any(keyword in proc_name_lower for keyword in male_only_keywords):
            if gender_code_upper in ['MALE', 'BOTH', 'ALL']:
                # Auto-upgrade to MALE_ONLY
                gender_code_upper = 'MALE_ONLY'
                gender_description = 'Male-only (anatomical)'

        # Validation logic
        if gender_code_upper == 'MALE_ONLY':
            is_valid = enrollee_gender_upper == 'MALE'
            return {
                'is_valid': is_valid,
                'gender_applicable': gender_code,
                'gender_description': gender_description,
                'enrollee_gender': enrollee_gender,
                'reasoning': (
                    f'{proc_name} is male-only. '
                    f'Enrollee is {enrollee_gender} - {"VALID" if is_valid else "INVALID (GENDER MISMATCH)"}'
                )
            }

        elif gender_code_upper == 'FEMALE_ONLY':
            is_valid = enrollee_gender_upper == 'FEMALE'
            return {
                'is_valid': is_valid,
                'gender_applicable': gender_code,
                'gender_description': gender_description,
                'enrollee_gender': enrollee_gender,
                'reasoning': (
                    f'{proc_name} is female-only. '
                    f'Enrollee is {enrollee_gender} - {"VALID" if is_valid else "INVALID (GENDER MISMATCH)"}'
                )
            }

        elif gender_code_upper == 'MALE':
            if enrollee_gender_upper == 'MALE':
                return {
                    'is_valid': True,
                    'gender_applicable': gender_code,
                    'gender_description': gender_description,
                    'enrollee_gender': enrollee_gender,
                    'reasoning': f'{proc_name} predominantly affects males. Enrollee is male - VALID'
                }
            else:
                return {
                    'is_valid': True,
                    'is_warning': True,
                    'gender_applicable': gender_code,
                    'gender_description': gender_description,
                    'enrollee_gender': enrollee_gender,
                    'reasoning': f'{proc_name} predominantly affects males. Enrollee is {enrollee_gender} - UNCOMMON (requires review)'
                }

        elif gender_code_upper == 'FEMALE':
            if enrollee_gender_upper == 'FEMALE':
                return {
                    'is_valid': True,
                    'gender_applicable': gender_code,
                    'gender_description': gender_description,
                    'enrollee_gender': enrollee_gender,
                    'reasoning': f'{proc_name} predominantly affects females. Enrollee is female - VALID'
                }
            else:
                return {
                    'is_valid': True,
                    'is_warning': True,
                    'gender_applicable': gender_code,
                    'gender_description': gender_description,
                    'enrollee_gender': enrollee_gender,
                    'reasoning': f'{proc_name} predominantly affects females. Enrollee is {enrollee_gender} - UNCOMMON (requires review)'
                }

        # Default: valid
        return {
            'is_valid': True,
            'gender_applicable': gender_code,
            'enrollee_gender': enrollee_gender,
            'reasoning': f'{proc_name} gender validation passed'
        }

    def validate_gender_for_diagnosis(
        self, diagnosis_code: str, enrollee_gender: str
    ) -> Dict:
        """
        Validate if enrollee's gender is appropriate for the diagnosis.
        Returns dict with is_valid, gender_applicable, enrollee_gender, reasoning.
        """
        doc = mongo_db.get_diagnosis_master(diagnosis_code)
        if not doc:
            return {
                'is_valid': None,
                'gender_applicable': None,
                'enrollee_gender': enrollee_gender,
                'reasoning': f'Diagnosis {diagnosis_code} not found in master table'
            }
        diag_name = doc.get('diagnosis_name')
        diag_class = doc.get('diagnosis_class')
        gender_code = doc.get('gender_applicable')

        # If no gender restriction or applies to all
        if not gender_code or str(gender_code).strip().upper() in ['ALL', 'BOTH']:
            return {
                'is_valid': True,
                'gender_applicable': gender_code or 'ALL',
                'enrollee_gender': enrollee_gender,
                'reasoning': f'{diag_name} can occur in all genders'
            }

        # Normalize enrollee gender
        enrollee_gender_upper = enrollee_gender.upper().strip() if enrollee_gender else 'UNKNOWN'
        gender_code_upper = str(gender_code).strip().upper()

        # Get gender description from GENDER_TYPE table
        gender_description = mongo_db.get_gender_type(gender_code) or gender_code

        # ANATOMICAL KEYWORD DETECTION - Auto-upgrade gender restrictions
        # If diagnosis name contains anatomically-specific terms, upgrade to _ONLY
        diag_name_lower = diag_name.lower() if diag_name else ''
        
        # Female-only anatomical terms
        female_only_keywords = ['vulva', 'vagina', 'uterus', 'uterine', 'ovary', 'ovarian', 
                                 'cervix', 'cervical cancer', 'pregnancy', 'pregnant', 'delivery', 
                                 'maternal', 'menstrual', 'menopause', 'endometrial']
        
        # Male-only anatomical terms
        male_only_keywords = ['prostate', 'prostatic', 'testicular', 'testis', 'testes',
                               'penis', 'penile', 'scrotum', 'scrotal', 'epididym']
        
        # Check for female-only anatomy
        if any(keyword in diag_name_lower for keyword in female_only_keywords):
            if gender_code_upper in ['FEMALE', 'BOTH', 'ALL']:
                # Auto-upgrade to FEMALE_ONLY
                gender_code_upper = 'FEMALE_ONLY'
                gender_description = 'Female-only (anatomical)'
        
        # Check for male-only anatomy
        elif any(keyword in diag_name_lower for keyword in male_only_keywords):
            if gender_code_upper in ['MALE', 'BOTH', 'ALL']:
                # Auto-upgrade to MALE_ONLY
                gender_code_upper = 'MALE_ONLY'
                gender_description = 'Male-only (anatomical)'

        # Validation logic
        if gender_code_upper == 'MALE_ONLY':
            is_valid = enrollee_gender_upper == 'MALE'
            return {
                'is_valid': is_valid,
                'gender_applicable': gender_code,
                'gender_description': gender_description,
                'enrollee_gender': enrollee_gender,
                'reasoning': (
                    f'{diag_name} is a male-only condition. '
                    f'Enrollee is {enrollee_gender} - {"VALID" if is_valid else "INVALID (GENDER MISMATCH)"}'
                )
            }

        elif gender_code_upper == 'FEMALE_ONLY':
            is_valid = enrollee_gender_upper == 'FEMALE'
            return {
                'is_valid': is_valid,
                'gender_applicable': gender_code,
                'gender_description': gender_description,
                'enrollee_gender': enrollee_gender,
                'reasoning': (
                    f'{diag_name} is a female-only condition. '
                    f'Enrollee is {enrollee_gender} - {"VALID" if is_valid else "INVALID (GENDER MISMATCH)"}'
                )
            }

        elif gender_code_upper == 'MALE':
            if enrollee_gender_upper == 'MALE':
                return {
                    'is_valid': True,
                    'gender_applicable': gender_code,
                    'gender_description': gender_description,
                    'enrollee_gender': enrollee_gender,
                    'reasoning': f'{diag_name} predominantly occurs in males. Enrollee is male - VALID'
                }
            else:
                return {
                    'is_valid': True,
                    'is_warning': True,
                    'gender_applicable': gender_code,
                    'gender_description': gender_description,
                    'enrollee_gender': enrollee_gender,
                    'reasoning': f'{diag_name} predominantly occurs in males. Enrollee is {enrollee_gender} - UNCOMMON (requires review)'
                }

        elif gender_code_upper == 'FEMALE':
            if enrollee_gender_upper == 'FEMALE':
                return {
                    'is_valid': True,
                    'gender_applicable': gender_code,
                    'gender_description': gender_description,
                    'enrollee_gender': enrollee_gender,
                    'reasoning': f'{diag_name} predominantly occurs in females. Enrollee is female - VALID'
                }
            else:
                return {
                    'is_valid': True,
                    'is_warning': True,
                    'gender_applicable': gender_code,
                    'gender_description': gender_description,
                    'enrollee_gender': enrollee_gender,
                    'reasoning': f'{diag_name} predominantly occurs in females. Enrollee is {enrollee_gender} - UNCOMMON (requires review)'
                }

        # Default: valid
        return {
            'is_valid': True,
            'gender_applicable': gender_code,
            'enrollee_gender': enrollee_gender,
            'reasoning': f'{diag_name} gender validation passed'
        }
    
    # ========================================================================
    # LEARNING TABLE LOOKUPS
    # ========================================================================
    
    def check_procedure_diagnosis_learning(
        self, 
        procedure_code: str, 
        diagnosis_code: str
    ) -> Optional[Dict]:
        """
        Check if this procedure-diagnosis pair has been learned
        
        Returns:
        --------
        Dict with is_valid_match, reasoning, confidence if found
        None if not in learning table
        """
        doc = mongo_db.get_procedure_diagnosis_learning(procedure_code, diagnosis_code)
        if doc:
            mongo_db.inc_usage(
                "ai_human_procedure_diagnosis",
                {"procedure_code": procedure_code, "diagnosis_code": diagnosis_code},
                timestamp_field="last_used_date"
            )
            self.stats['learning_hits'] += 1
            return {
                'source': 'learning_table',
                'is_valid': doc.get('is_valid_match'),
                'reasoning': doc.get('match_reason'),
                'confidence': doc.get('ai_confidence'),
                'ai_reasoning': doc.get('ai_reasoning'),
                'usage_count': doc.get('usage_count'),
                'approved_by': doc.get('approved_by'),
                'admin_approved': doc.get('admin_approved', False),
            }
        return None
    
    def check_procedure_class_learning(
        self, 
        procedure_code_1: str,
        procedure_code_2: str
    ) -> Optional[Dict]:
        """Check if these procedures belong to same class (learned)
        
        FIXED v3.3: Column names now match actual table schema:
        procedure_code_1/2, shared_class, ai_reasoning (not code_1/2, class_name, reason)
        """
        # Ensure ordered pair for consistent lookups
        code_1, code_2 = sorted([procedure_code_1.upper().strip(), procedure_code_2.upper().strip()])
        
        try:
            doc = mongo_db.get_procedure_class_learning(code_1, code_2)
            if doc:
                mongo_db.inc_usage(
                    "ai_human_procedure_class",
                    {"procedure_code_1": code_1, "procedure_code_2": code_2},
                    timestamp_field="last_used_date"
                )
                self.stats['learning_hits'] += 1
                return {
                    'source': 'learning_table',
                    'same_class': doc.get('same_class'),
                    'class_name': doc.get('shared_class'),
                    'reasoning': doc.get('ai_reasoning'),
                    'confidence': doc.get('ai_confidence'),
                    'usage_count': doc.get('usage_count'),
                    'approved_by': doc.get('approved_by'),
                    'admin_approved': doc.get('admin_approved', False),
                }
        except Exception as e:
            logger.error(f"Error checking procedure class learning: {e}")
        
        return None
    
    def check_diagnosis_class_learning(
        self, 
        diagnosis_code_1: str,
        diagnosis_code_2: str
    ) -> Optional[Dict]:
        """Check if these diagnoses belong to same class (learned)"""
        # Ensure ordered pair
        code_1, code_2 = sorted([diagnosis_code_1, diagnosis_code_2])
        
        doc = mongo_db.get_diagnosis_class_learning(code_1, code_2)
        if doc:
            mongo_db.inc_usage(
                "ai_human_diagnosis_class",
                {"code_1": code_1, "code_2": code_2},
                timestamp_field="last_used_date"
            )
            self.stats['learning_hits'] += 1
            return {
                'source': 'learning_table',
                'same_class': doc.get('same_class'),
                'class_name': doc.get('class_name'),
                'reasoning': doc.get('reason'),
                'confidence': doc.get('ai_confidence'),
                'usage_count': doc.get('usage_count'),
                'approved_by': doc.get('approved_by'),
                'admin_approved': doc.get('admin_approved', False),
            }
        
        return None
    
    def check_procedure_age_learning(
        self,
        procedure_code: str,
        enrollee_age: int
    ) -> Optional[Dict]:
        """
        Check if procedure age validation learned from previous AI decisions
        
        Returns:
        --------
        Dict with validation result if found, None otherwise
        """
        try:
            doc = mongo_db.get_procedure_age_learning(procedure_code, enrollee_age)
            if doc:
                mongo_db.inc_usage(
                    "ai_human_procedure_age",
                    {"procedure_code": {"$regex": f"^{re.escape(procedure_code.strip())}$", "$options": "i"},
                     "min_age": {"$lte": enrollee_age}, "max_age": {"$gte": enrollee_age}},
                    timestamp_field="last_used"
                )
                self.stats['learning_hits'] += 1
                return {
                    'min_age': doc.get('min_age'),
                    'max_age': doc.get('max_age'),
                    'is_valid': doc.get('is_valid_for_age'),
                    'reason': doc.get('reason'),
                    'confidence': doc.get('confidence'),
                    'ai_reasoning': doc.get('ai_reasoning'),
                    'usage_count': doc.get('usage_count'),
                    'last_used': doc.get('last_used'),
                    'admin_approved': doc.get('admin_approved', False),
                    'source': 'learning_table'
                }
            return None
            
        except Exception as e:
            logger.error(f"Error checking procedure age learning: {e}")
            return None
    
    def check_procedure_gender_learning(
        self,
        procedure_code: str,
        enrollee_gender: str
    ) -> Optional[Dict]:
        """
        Check if procedure gender validation learned from previous AI decisions
        
        Returns:
        --------
        Dict with validation result if found, None otherwise
        """
        try:
            doc = mongo_db.get_procedure_gender_learning(procedure_code, enrollee_gender)
            if doc:
                mongo_db.inc_usage(
                    "ai_human_procedure_gender",
                    {"procedure_code": {"$regex": f"^{re.escape(procedure_code.strip())}$", "$options": "i"},
                     "gender": {"$regex": f"^{re.escape(enrollee_gender.strip())}$", "$options": "i"}},
                    timestamp_field="last_used"
                )
                self.stats['learning_hits'] += 1
                return {
                    'allowed_gender': doc.get('gender'),
                    'is_valid': doc.get('is_valid_for_gender'),
                    'reason': doc.get('reason'),
                    'confidence': doc.get('confidence'),
                    'ai_reasoning': doc.get('ai_reasoning'),
                    'usage_count': doc.get('usage_count'),
                    'last_used': doc.get('last_used'),
                    'admin_approved': doc.get('admin_approved', False),
                    'source': 'learning_table'
                }
            return None

        except Exception as e:
            logger.error(f"Error checking procedure gender learning: {e}")
            return None
    
    def check_diagnosis_age_learning(
        self,
        diagnosis_code: str,
        enrollee_age: int
    ) -> Optional[Dict]:
        """
        Check if diagnosis age validation learned from previous AI decisions
        
        Returns:
        --------
        Dict with validation result if found, None otherwise
        """
        try:
            doc = mongo_db.get_diagnosis_age_learning(diagnosis_code, enrollee_age)
            if doc:
                mongo_db.inc_usage(
                    "ai_human_diagnosis_age",
                    {"diagnosis_code": {"$regex": f"^{re.escape(diagnosis_code.strip())}$", "$options": "i"},
                     "min_age": {"$lte": enrollee_age}, "max_age": {"$gte": enrollee_age}},
                    timestamp_field="last_used"
                )
                self.stats['learning_hits'] += 1
                return {
                    'min_age': doc.get('min_age'),
                    'max_age': doc.get('max_age'),
                    'is_valid': doc.get('is_valid_for_age'),
                    'reason': doc.get('reason'),
                    'confidence': doc.get('confidence'),
                    'ai_reasoning': doc.get('ai_reasoning'),
                    'usage_count': doc.get('usage_count'),
                    'last_used': doc.get('last_used'),
                    'admin_approved': doc.get('admin_approved', False),
                    'source': 'learning_table'
                }
            return None

        except Exception as e:
            logger.error(f"Error checking diagnosis age learning: {e}")
            return None
    
    def check_diagnosis_gender_learning(
        self,
        diagnosis_code: str,
        enrollee_gender: str
    ) -> Optional[Dict]:
        """
        Check if diagnosis gender validation learned from previous AI decisions
        
        Returns:
        --------
        Dict with validation result if found, None otherwise
        """
        try:
            doc = mongo_db.get_diagnosis_gender_learning(diagnosis_code, enrollee_gender)
            if doc:
                mongo_db.inc_usage(
                    "ai_human_diagnosis_gender",
                    {"diagnosis_code": {"$regex": f"^{re.escape(diagnosis_code.strip())}$", "$options": "i"},
                     "gender": {"$regex": f"^{re.escape(enrollee_gender.strip())}$", "$options": "i"}},
                    timestamp_field="last_used"
                )
                self.stats['learning_hits'] += 1
                return {
                    'allowed_gender': doc.get('gender'),
                    'is_valid': doc.get('is_valid_for_gender'),
                    'reason': doc.get('reason'),
                    'confidence': doc.get('confidence'),
                    'ai_reasoning': doc.get('ai_reasoning'),
                    'usage_count': doc.get('usage_count'),
                    'last_used': doc.get('last_used'),
                    'admin_approved': doc.get('admin_approved', False),
                    'source': 'learning_table'
                }
            return None

        except Exception as e:
            logger.error(f"Error checking diagnosis gender learning: {e}")
        return None
    
    # ========================================================================
    # MASTER TABLE LOOKUPS (FIXED SCHEMA AND COLUMN NAMES)
    # ========================================================================
    
    def check_procedure_master(self, procedure_code: str) -> Optional[Dict]:
        """
        Check if procedure exists in master table.
        
        NOTE: typical_diagnoses and typical_symptoms columns are DEPRECATED.
        Procedure-diagnosis matching now uses PROCEDURE_DIAGNOSIS_COMP table.
        """
        try:
            doc = mongo_db.get_procedure_master(procedure_code)
            if not doc:
                return None
            self.stats['master_hits'] += 1
            return {
                'source': 'master_table',
                'name': doc.get('procedure_name'),
                'class': doc.get('procedure_class'),
                'frequency': doc.get('frequency_category'),
                'clinical_notes': doc.get('clinical_notes') or '',
                'confidence': 100
            }
        except Exception as e:
            print(f"Error checking procedure master: {e}")
        return None
    
    def check_diagnosis_master(self, diagnosis_code: str) -> Optional[Dict]:
        """Check if diagnosis exists in master table."""
        try:
            doc = mongo_db.get_diagnosis_master(diagnosis_code)
            if not doc:
                return None
            self.stats['master_hits'] += 1
            implied_sx_str = doc.get('implied_symptoms') or ""
            implied_sx_list = [sx.strip().upper() for sx in str(implied_sx_str).split(',') if sx.strip()]
            return {
                'source': 'master_table',
                'name': doc.get('diagnosis_name'),
                'class': doc.get('diagnosis_class'),
                'implied_symptoms': implied_sx_list,
                'confidence': 100
            }
        except Exception as e:
            print(f"Error checking diagnosis master: {e}")
        return None
    
    # ========================================================================
    # PROCEDURE-DIAGNOSIS COMPATIBILITY TABLE (replaces symptom matching)
    # ========================================================================
    
    # Universal consultation codes — valid with ANY diagnosis
    UNIVERSAL_PROCEDURE_CODES = {
        'CONS021', 'CONS022',
        # Administrative / registration charges — not clinical procedures,
        # no diagnosis compatibility check required
        'REGISTRATION', 'REG', 'REG01', 'REG02', 'REG1', 'REG2',
    }

    # Procedure name keywords that are always universal (admin/registration)
    _ADMIN_PROCEDURE_KW = ("REGISTRATION", "ADMIN FEE", "CARD FEE", "FOLDER FEE", "FORM FEE")
    
    def check_procedure_diagnosis_comp(self, procedure_code: str, diagnosis_code: str) -> Optional[Dict]:
        """
        Check if procedure-diagnosis pair exists in PROCEDURE_DIAGNOSIS_COMP table.
        
        This is the NEW authoritative source for procedure-diagnosis matching,
        replacing the old typical_diagnoses + typical_symptoms logic.
        
        Returns:
            Dict with match info if found, None if pair not in table.
            None does NOT mean invalid — it means AI should validate.
        """
        try:
            doc = mongo_db.get_procedure_diagnosis_comp(procedure_code, diagnosis_code)
            if doc:
                self.stats['master_hits'] += 1
                return {
                    'source': 'comp_table',
                    'procedure_code': doc.get('procedure_code'),
                    'procedure_name': doc.get('procedure_name'),
                    'diagnosis_code': doc.get('diagnosis_code'),
                    'diagnosis_name': doc.get('diagnosis_name'),
                    'is_valid': True,
                    'confidence': 100
                }
            return None
        except Exception as e:
            print(f"Error checking PROCEDURE_DIAGNOSIS_COMP: {e}")
            return None
    
    def is_universal_procedure(self, procedure_code: str) -> bool:
        """
        Check if procedure is a universal code (valid with any diagnosis).

        CONS021/022 = Consultation codes.
        REGISTRATION / admin charges = administrative, no clinical compatibility needed.
        """
        code = procedure_code.strip().upper()
        if code in self.UNIVERSAL_PROCEDURE_CODES:
            return True
        # Also catch registration/admin by name keyword (claims files pass description as code)
        return any(kw in code for kw in self._ADMIN_PROCEDURE_KW)
    
    # ========================================================================
    # COMPREHENSIVE TABLE LOOKUPS (Fallback when not in master)
    # ========================================================================
    
    def _get_procedure_from_comprehensive(self, procedure_code: str) -> Optional[Dict]:
        """
        Get procedure from comprehensive PROCEDURE DATA table.
        Fallback when procedure is not in PROCEDURE_MASTER.
        This database uses: procedurecode, proceduredesc (AI DRIVEN DATA.PROCEDURE DATA).
        """
        # Try multiple query variants for robustness (primary = this DB's column names)
        queries = [
            # Primary: This DB uses procedurecode, proceduredesc
            '''
            SELECT procedurecode, proceduredesc
            FROM "AI DRIVEN DATA"."PROCEDURE DATA"
            WHERE LOWER(TRIM(procedurecode)) = LOWER(TRIM(?))
            LIMIT 1
            ''',
            # Fallback: Some schemas use code, procedurename
            '''
            SELECT code, procedurename, category, description
            FROM "AI DRIVEN DATA"."PROCEDURE DATA"
            WHERE LOWER(TRIM(code)) = LOWER(TRIM(?))
            LIMIT 1
            ''',
            '''
            SELECT code, procedurename, category
            FROM "AI DRIVEN DATA"."PROCEDURE DATA"
            WHERE LOWER(TRIM(code)) = LOWER(TRIM(?))
            LIMIT 1
            ''',
            '''
            SELECT code, procedurename
            FROM "AI DRIVEN DATA"."PROCEDURE DATA"
            WHERE LOWER(TRIM(code)) = LOWER(TRIM(?))
            LIMIT 1
            '''
        ]

        for query in queries:
            try:
                result = self.conn.execute(query, [procedure_code]).fetchone()
                if result:
                    # Successfully found the procedure
                    return {
                        'code': result[0],
                        'name': result[1] if len(result) > 1 else 'Unknown',
                        'category': result[2] if len(result) > 2 else None,
                        'description': result[3] if len(result) > 3 else None,
                        'source': 'PROCEDURE_DATA',
                        'has_age_rules': False,
                        'has_gender_rules': False
                    }
            except Exception:
                # This query variant failed, try next one
                continue

        # All query variants failed - procedure not found
        return None
    
    def _get_diagnosis_from_comprehensive(self, diagnosis_code: str) -> Optional[Dict]:
        """
        Get diagnosis from comprehensive DIAGNOSIS table.
        Fallback when diagnosis is not in DIAGNOSIS_MASTER.
        Uses correct column names: diagnosiscode, diagnosisdesc
        """
        # Try multiple query variants for robustness
        queries = [
            # Primary: Try with category
            '''
            SELECT diagnosiscode, diagnosisdesc, category
            FROM "AI DRIVEN DATA"."DIAGNOSIS"
            WHERE LOWER(TRIM(diagnosiscode)) = LOWER(TRIM(?))
            LIMIT 1
            ''',
            # Fallback: Just code and description
            '''
            SELECT diagnosiscode, diagnosisdesc
            FROM "AI DRIVEN DATA"."DIAGNOSIS"
            WHERE LOWER(TRIM(diagnosiscode)) = LOWER(TRIM(?))
            LIMIT 1
            '''
        ]

        for query in queries:
            try:
                result = self.conn.execute(query, [diagnosis_code]).fetchone()
                if result:
                    # Successfully found the diagnosis
                    return {
                        'code': result[0],
                        'name': result[1] if len(result) > 1 else 'Unknown',
                        'category': result[2] if len(result) > 2 else None,
                        'source': 'DIAGNOSIS',
                        'has_age_rules': False,
                        'has_gender_rules': False
                    }
            except Exception:
                # This query variant failed, try next one
                continue

        # All query variants failed - diagnosis not found
        return None
    
    # ========================================================================
    # PUBMED EVIDENCE SEARCH (E-utilities API)
    # ========================================================================
    
    PUBMED_SEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    PUBMED_FETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    
    def search_pubmed_evidence(
        self,
        procedure_name: str,
        diagnosis_name: str,
        max_results: int = 3,
        timeout: int = 10
    ) -> Dict:
        """
        Search PubMed for clinical evidence supporting a procedure-diagnosis pair.
        
        Works for all procedure types:
        - Medications: "Vitamin C AND malaria treatment"
        - Lab tests: "Full Blood Count AND anaemia diagnosis"
        - Surgical: "appendectomy AND appendicitis"
        
        Returns:
            Dict with 'articles' list, 'query' used, 'count' of results.
            Each article: {title, abstract, pmid, year, authors}
        """
        try:
            # Clean names for search (remove dosage info, special chars)
            proc_clean = self._clean_for_pubmed(procedure_name)
            diag_clean = self._clean_for_pubmed(diagnosis_name)
            
            # Build query — search for clinical relationship
            query = f'({proc_clean}) AND ({diag_clean}) AND (treatment OR diagnosis OR therapy OR management OR adjunct OR supportive)'
            
            # Step 1: Search for PMIDs
            pmids = self._pubmed_search(query, max_results, timeout)
            
            if not pmids:
                # Broader fallback: just procedure + diagnosis
                query = f'({proc_clean}) AND ({diag_clean})'
                pmids = self._pubmed_search(query, max_results, timeout)
            
            if not pmids:
                return {
                    'articles': [],
                    'query': query,
                    'count': 0,
                    'error': None
                }
            
            # Step 2: Fetch abstracts
            articles = self._pubmed_fetch_abstracts(pmids, timeout)
            
            return {
                'articles': articles,
                'query': query,
                'count': len(articles),
                'error': None
            }
            
        except Exception as e:
            print(f"PubMed search failed: {e}")
            return {
                'articles': [],
                'query': f'{procedure_name} AND {diagnosis_name}',
                'count': 0,
                'error': str(e)
            }
    
    def _clean_for_pubmed(self, name: str) -> str:
        """
        Clean a procedure/diagnosis name for PubMed search.
        Remove dosage info, dosage forms, special characters, internal codes.
        """
        import re
        
        # Replace slashes with spaces first (THICK/THIN → THICK THIN)
        cleaned = name.replace('/', ' ')
        
        # Remove dosage patterns: "100mg", "500 mg", "10ml"
        cleaned = re.sub(r'\d+\s*(mg|ml|mcg|iu|%)\b', '', cleaned, flags=re.IGNORECASE)
        
        # Remove standalone dosage form words
        cleaned = re.sub(r'\b(tabs?|caps?|capsules?|tablets?|inj|injection|syrup|susp|suspension|cream|ointment|drops|solution)\b', '', cleaned, flags=re.IGNORECASE)
        
        # Remove parenthetical codes
        cleaned = re.sub(r'\([^)]*\)', '', cleaned)
        
        # Remove special chars but keep spaces and hyphens
        cleaned = re.sub(r'[^\w\s\-]', '', cleaned)
        
        # Collapse whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # If too short after cleaning, use original
        if len(cleaned) < 3:
            cleaned = re.sub(r'[^\w\s\-]', '', name).strip()
        
        return cleaned
    
    def _pubmed_search(self, query: str, max_results: int, timeout: int) -> List[str]:
        """Search PubMed and return list of PMIDs."""
        params = urllib.parse.urlencode({
            'db': 'pubmed',
            'term': query,
            'retmode': 'json',
            'retmax': max_results,
            'sort': 'relevance'
        })
        url = f"{self.PUBMED_SEARCH_URL}?{params}"
        
        req = urllib.request.Request(url, headers={'User-Agent': 'ClearlineHMO-VettingEngine/1.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            import json
            data = json.loads(resp.read().decode('utf-8'))
        
        id_list = data.get('esearchresult', {}).get('idlist', [])
        return id_list
    
    def _pubmed_fetch_abstracts(self, pmids: List[str], timeout: int) -> List[Dict]:
        """Fetch article details (title, abstract, year, authors) for given PMIDs."""
        params = urllib.parse.urlencode({
            'db': 'pubmed',
            'id': ','.join(pmids),
            'retmode': 'xml',
            'rettype': 'abstract'
        })
        url = f"{self.PUBMED_FETCH_URL}?{params}"
        
        req = urllib.request.Request(url, headers={'User-Agent': 'ClearlineHMO-VettingEngine/1.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            xml_data = resp.read().decode('utf-8')
        
        articles = []
        try:
            root = ET.fromstring(xml_data)
            
            for article_elem in root.findall('.//PubmedArticle'):
                try:
                    # Title
                    title_elem = article_elem.find('.//ArticleTitle')
                    title = title_elem.text if title_elem is not None and title_elem.text else "No title"
                    
                    # Abstract — combine all AbstractText elements
                    abstract_parts = []
                    for abs_text in article_elem.findall('.//AbstractText'):
                        label = abs_text.get('Label', '')
                        text = ''.join(abs_text.itertext()) if abs_text is not None else ''
                        if text:
                            if label:
                                abstract_parts.append(f"{label}: {text}")
                            else:
                                abstract_parts.append(text)
                    abstract = ' '.join(abstract_parts)
                    
                    # Truncate long abstracts (keep under ~300 words to save tokens)
                    words = abstract.split()
                    if len(words) > 300:
                        abstract = ' '.join(words[:300]) + '...'
                    
                    # PMID
                    pmid_elem = article_elem.find('.//PMID')
                    pmid = pmid_elem.text if pmid_elem is not None else "Unknown"
                    
                    # Year
                    year_elem = article_elem.find('.//PubDate/Year')
                    if year_elem is None:
                        year_elem = article_elem.find('.//PubDate/MedlineDate')
                    year = year_elem.text[:4] if year_elem is not None and year_elem.text else "N/A"
                    
                    # Authors (first 3)
                    authors = []
                    for author in article_elem.findall('.//Author')[:3]:
                        last = author.find('LastName')
                        first = author.find('Initials')
                        if last is not None and last.text:
                            name = last.text
                            if first is not None and first.text:
                                name += f" {first.text}"
                            authors.append(name)
                    author_str = ', '.join(authors)
                    if len(article_elem.findall('.//Author')) > 3:
                        author_str += ' et al.'
                    
                    articles.append({
                        'title': title,
                        'abstract': abstract,
                        'pmid': pmid,
                        'year': year,
                        'authors': author_str
                    })
                except Exception:
                    continue
                    
        except ET.ParseError as e:
            print(f"PubMed XML parse error: {e}")
        
        return articles
    
    def format_pubmed_for_prompt(self, evidence: Dict) -> str:
        """
        Format PubMed evidence into a string section for the AI prompt.
        Returns empty string if no evidence found.
        """
        articles = evidence.get('articles', [])
        if not articles:
            return ""
        
        lines = [
            f"\n{'='*60}",
            f"PUBMED CLINICAL EVIDENCE ({len(articles)} article(s) found)",
            f"Search query: {evidence.get('query', 'N/A')}",
            f"{'='*60}"
        ]
        
        for i, article in enumerate(articles, 1):
            lines.append(f"\n--- Paper {i} (PMID: {article['pmid']}, {article['year']}) ---")
            lines.append(f"Title: {article['title']}")
            lines.append(f"Authors: {article['authors']}")
            if article['abstract']:
                lines.append(f"Abstract: {article['abstract']}")
            else:
                lines.append("Abstract: Not available")
        
        lines.append(f"\n{'='*60}")
        lines.append("Use the above evidence to inform your decision.")
        lines.append("If the evidence supports the procedure-diagnosis pair, cite the PMID(s).")
        lines.append("If no relevant evidence is found in the abstracts, rely on your medical knowledge.")
        lines.append(f"{'='*60}\n")
        
        return '\n'.join(lines)
    
    # ========================================================================
    # AI SUGGESTION METHODS
    # ========================================================================
    
    def get_ai_suggestion_procedure_diagnosis(
        self, 
        procedure_code: str,
        diagnosis_code: str,
        context: Optional[EnrolleeContext] = None
    ) -> Dict:
        """
        Get AI suggestion for procedure-diagnosis match
        
        Returns suggestion with confidence and reasoning for human review
        """
        if not self.anthropic_client:
            return {
                'suggested_action': 'DENY',
                'confidence': 0,
                'reasoning': 'API key not configured',
                'requires_human_review': True
            }
        
        # Build context string
        context_str = ""
        if context:
            if context.age:
                context_str += f"Patient age: {context.age} years\n"
            if context.gender:
                context_str += f"Patient gender: {context.gender}\n"
            if context.encounter_date:
                context_str += f"Encounter date: {context.encounter_date}\n"
        
        prompt = f"""Analyze if this procedure is appropriate for this diagnosis.

Procedure Code: {procedure_code}
Diagnosis Code: {diagnosis_code}
{context_str}

Respond in JSON format:
{{
  "suggested_action": "APPROVE" or "DENY",
  "confidence": 0-100,
  "reasoning": "One sentence explanation"
}}

Consider:
- Medical appropriateness
- Age/gender appropriateness (if provided)
- Standard treatment protocols
- Clinical guidelines
"""
        
        try:
            self.stats['ai_calls'] += 1
            
            response = _anthropic_create_with_retry(self.anthropic_client,
                model="claude-haiku-4-5-20251001",
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Collect text from ALL content blocks (not just [0])
            import json
            text = ""
            for block in response.content:
                if hasattr(block, 'text') and block.text:
                    text += block.text
            text = text.strip()
            
            if not text:
                raise ValueError("AI returned empty response")
            
            # Extract JSON robustly
            if not text.startswith("{"):
                start = text.find("{")
                end = text.rfind("}") + 1
                if start >= 0 and end > start:
                    text = text[start:end]
            
            result = json.loads(text)
            
            result['requires_human_review'] = (
                result['confidence'] < 80 or 
                result['suggested_action'] == 'DENY'
            )
            
            return result
            
        except Exception as e:
            print(f"AI call failed: {e}")
            return {
                'suggested_action': 'DENY',
                'confidence': 0,
                'reasoning': f'AI error: {str(e)}',
                'requires_human_review': True
            }
    
    def _call_claude_for_validation(self, prompt: str, model: str = "claude-haiku-4-5-20251001") -> Dict:
        """
        Send validation prompt to Claude and parse JSON response.
        Returns dict with suggested_action, confidence, reasoning, requires_human_review.

        model — defaults to Haiku for speed/cost. Pass "claude-opus-4-6" for
                 medically complex checks (e.g. PROC_DIAG_COMPATIBILITY).
        """
        if not self.anthropic_client:
            return {
                'suggested_action': 'DENY',
                'confidence': 0,
                'reasoning': 'API key not configured',
                'requires_human_review': True
            }
        try:
            self.stats['ai_calls'] += 1
            response = _anthropic_create_with_retry(self.anthropic_client,
                model=model,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}]
            )

            import json

            # Collect text from ALL content blocks
            text = ""
            for block in response.content:
                if hasattr(block, 'text') and block.text:
                    text += block.text

            text = text.strip()

            # If empty, one more attempt with stronger instruction
            if not text:
                print("⚠️  Empty AI response, retrying with explicit JSON instruction...")
                self.stats['ai_calls'] += 1
                response = _anthropic_create_with_retry(self.anthropic_client,
                    model="claude-haiku-4-5-20251001",
                    max_tokens=800,
                    messages=[{"role": "user", "content": prompt + "\n\nRespond with ONLY the JSON object, nothing else."}]
                )
                text = ""
                for block in response.content:
                    if hasattr(block, 'text') and block.text:
                        text += block.text
                text = text.strip()

            if not text:
                raise ValueError("AI returned empty response after retry")

            # Extract JSON robustly
            if "```" in text:
                start = text.find("{")
                end = text.rfind("}") + 1
                if start >= 0 and end > start:
                    text = text[start:end]
            elif text.startswith("{"):
                pass
            else:
                start = text.find("{")
                end = text.rfind("}") + 1
                if start >= 0 and end > start:
                    text = text[start:end]
                else:
                    raise ValueError(f"No JSON found in response: {text[:200]}")

            data = json.loads(text)
            action = (data.get("action") or data.get("suggested_action") or "DENY").upper()
            suggested_action = "APPROVE" if "APPROVE" in action else "DENY"
            confidence = int(data.get("confidence", 50))
            reasoning = data.get("reasoning", "")
            return {
                'suggested_action': suggested_action,
                'confidence': min(100, max(0, confidence)),
                'reasoning': reasoning or "No reasoning provided.",
                'requires_human_review': True
            }
        except Exception as e:
            print(f"AI validation call failed: {e}")
            return {
                'suggested_action': 'DENY',
                'confidence': 0,
                'reasoning': f'AI error: {str(e)}',
                'requires_human_review': True
            }
    
    # ========================================================================
    # STORAGE METHODS
    # ========================================================================
    
    def store_approved_decision(
        self,
        procedure_code: str,
        diagnosis_code: str,
        is_valid: bool,
        reason: str,
        confidence: int,
        ai_reasoning: str,
        approved_by: str
    ) -> bool:
        """
        Store human-approved decision in learning table
        
        Returns True if successful
        """
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            mongo_db.upsert(
                "ai_human_procedure_diagnosis",
                {"procedure_code": procedure_code, "diagnosis_code": diagnosis_code},
                {
                    "procedure_code": procedure_code, "diagnosis_code": diagnosis_code,
                    "is_valid_match": is_valid, "match_reason": reason,
                    "ai_confidence": confidence, "ai_reasoning": ai_reasoning,
                    "approved_by": approved_by, "approved_date": now,
                    "usage_count": 0, "last_used_date": now,
                }
            )
            return True
        except Exception as e:
            print(f"Failed to store decision: {e}")
            return False

    def store_procedure_age_decision(
        self,
        procedure_code: str,
        min_age: int,
        max_age: int,
        is_valid: bool,
        reason: str,
        confidence: int,
        ai_reasoning: str,
        approved_by: str
    ) -> bool:
        """
        Store human-approved procedure age validation decision
        
        Returns True if successful
        """
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            mongo_db.upsert(
                "ai_human_procedure_age",
                {"procedure_code": procedure_code, "min_age": min_age, "max_age": max_age},
                {
                    "procedure_code": procedure_code, "min_age": min_age, "max_age": max_age,
                    "is_valid_for_age": is_valid, "reason": reason,
                    "confidence": confidence, "ai_reasoning": ai_reasoning,
                    "approved_by": approved_by, "approved_date": now,
                    "usage_count": 0, "last_used": now,
                }
            )
            return True
        except Exception as e:
            logger.error(f"Failed to store procedure age decision: {e}")
            return False

    def store_procedure_gender_decision(
        self,
        procedure_code: str,
        allowed_gender: str,
        is_valid: bool,
        reason: str,
        confidence: int,
        ai_reasoning: str,
        approved_by: str
    ) -> bool:
        """
        Store human-approved procedure gender validation decision
        
        Returns True if successful
        """
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            mongo_db.upsert(
                "ai_human_procedure_gender",
                {"procedure_code": procedure_code, "gender": allowed_gender},
                {
                    "procedure_code": procedure_code, "gender": allowed_gender,
                    "is_valid_for_gender": is_valid, "reason": reason,
                    "confidence": confidence, "ai_reasoning": ai_reasoning,
                    "approved_by": approved_by, "approved_date": now,
                    "usage_count": 0, "last_used": now,
                }
            )
            return True
        except Exception as e:
            logger.error(f"Failed to store procedure gender decision: {e}")
            return False

    def store_diagnosis_age_decision(
        self,
        diagnosis_code: str,
        min_age: int,
        max_age: int,
        is_valid: bool,
        reason: str,
        confidence: int,
        ai_reasoning: str,
        approved_by: str
    ) -> bool:
        """
        Store human-approved diagnosis age validation decision
        
        Returns True if successful
        """
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            mongo_db.upsert(
                "ai_human_diagnosis_age",
                {"diagnosis_code": diagnosis_code, "min_age": min_age, "max_age": max_age},
                {
                    "diagnosis_code": diagnosis_code, "min_age": min_age, "max_age": max_age,
                    "is_valid_for_age": is_valid, "reason": reason,
                    "confidence": confidence, "ai_reasoning": ai_reasoning,
                    "approved_by": approved_by, "approved_date": now,
                    "usage_count": 0, "last_used": now,
                }
            )
            return True
        except Exception as e:
            logger.error(f"Failed to store diagnosis age decision: {e}")
            return False

    def store_diagnosis_gender_decision(
        self,
        diagnosis_code: str,
        allowed_gender: str,
        is_valid: bool,
        reason: str,
        confidence: int,
        ai_reasoning: str,
        approved_by: str
    ) -> bool:
        """
        Store human-approved diagnosis gender validation decision
        
        Returns True if successful
        """
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            mongo_db.upsert(
                "ai_human_diagnosis_gender",
                {"diagnosis_code": diagnosis_code, "gender": allowed_gender},
                {
                    "diagnosis_code": diagnosis_code, "gender": allowed_gender,
                    "is_valid_for_gender": is_valid, "reason": reason,
                    "confidence": confidence, "ai_reasoning": ai_reasoning,
                    "approved_by": approved_by, "approved_date": now,
                    "usage_count": 0, "last_used": now,
                }
            )
            return True
        except Exception as e:
            logger.error(f"Failed to store diagnosis gender decision: {e}")
            return False

    def store_procedure_class_decision(
        self,
        procedure_code_1: str,
        procedure_code_2: str,
        shared_class: str,
        same_class: bool,
        ai_confidence: int,
        ai_reasoning: str,
        approved_by: str
    ) -> bool:
        """
        Store human-approved procedure class relationship
        
        Used when AI determines two procedures share (or don't share) a therapeutic class.
        Critical for 30-day duplicate checking.
        
        Args:
            procedure_code_1: First procedure code
            procedure_code_2: Second procedure code  
            shared_class: Therapeutic class name (e.g., "Antimalarials")
            same_class: True if they share the class, False if they don't
            ai_confidence: AI confidence score
            ai_reasoning: AI's explanation
            approved_by: Who approved this decision
            
        Returns True if successful
        """
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            code_1, code_2 = sorted([procedure_code_1.upper().strip(), procedure_code_2.upper().strip()])
            mongo_db.upsert(
                "ai_human_procedure_class",
                {"procedure_code_1": code_1, "procedure_code_2": code_2},
                {
                    "procedure_code_1": code_1, "procedure_code_2": code_2,
                    "shared_class": shared_class, "same_class": same_class,
                    "ai_confidence": ai_confidence, "ai_reasoning": ai_reasoning,
                    "approved_by": approved_by, "approved_date": now,
                    "usage_count": 0, "last_used_date": now,
                }
            )
            return True

        except Exception as e:
            logger.error(f"Failed to store procedure class decision: {e}")
            return False
    
    # ========================================================================
    # MAIN VALIDATION WORKFLOW
    # ========================================================================
    
    def validate_procedure_diagnosis(
        self,
        procedure_code: str,
        diagnosis_code: str,
        enrollee_context: Optional[EnrolleeContext] = None
    ) -> VettingSuggestion:
        """
        Validate procedure-diagnosis pair with age validation.
        1. Check learning table.
        2. Get master table entries.
        3. Age validation only if in master (DENY if invalid, no AI).
        4. If NOT in master → AI validates age in TIER 3.
        5. Procedure-diagnosis compatibility (direct or symptom match).
        """
        self.stats["total_requests"] += 1
        
        # TIER 1: Learning table
        learning_result = self.check_procedure_diagnosis_learning(procedure_code, diagnosis_code)
        if learning_result:
            action = "APPROVE" if learning_result["is_valid"] else "DENY"
            return VettingSuggestion(
                suggested_action=action,
                confidence=learning_result.get("confidence") or 100,
                reasoning=learning_result.get("reasoning") or learning_result.get("reason", "") or "From learning table",
                source="learning_table",
                requires_human_review=False,
                rule_details=learning_result
            )
        
        # TIER 2: Master tables
        proc_master = self.check_procedure_master(procedure_code)
        diag_master = self.check_diagnosis_master(diagnosis_code)
        
        # Age validation (only if in master and enrollee age available)
        age_validation_results = {}
        if enrollee_context and enrollee_context.age is not None:
            if proc_master:
                proc_age_result = self.validate_age_for_procedure(procedure_code, enrollee_context.age)
                age_validation_results["procedure"] = proc_age_result
                if proc_age_result.get("is_valid") is False:
                    return VettingSuggestion(
                        suggested_action="DENY",
                        confidence=100,
                        reasoning=f"❌ AGE RESTRICTION: {proc_age_result['reasoning']}",
                        source="master_table",
                        requires_human_review=True,
                        rule_details={
                            "rule": "AGE_VALIDATION",
                            "violation_type": "PROCEDURE_AGE_MISMATCH",
                            "procedure_age": proc_age_result,
                            "enrollee_age": enrollee_context.age,
                            "master_table_authority": True,
                            "ai_override": False
                        }
                    )
            if diag_master:
                diag_age_result = self.validate_age_for_diagnosis(diagnosis_code, enrollee_context.age)
                age_validation_results["diagnosis"] = diag_age_result
                if diag_age_result.get("is_valid") is False:
                    return VettingSuggestion(
                        suggested_action="DENY",
                        confidence=95,
                        reasoning=f"⚠️ AGE WARNING: {diag_age_result['reasoning']}",
                        source="master_table",
                        requires_human_review=True,
                        rule_details={
                            "rule": "AGE_VALIDATION",
                            "violation_type": "DIAGNOSIS_AGE_MISMATCH",
                            "diagnosis_age": diag_age_result,
                            "enrollee_age": enrollee_context.age,
                            "master_table_authority": True,
                            "ai_override": False
                        }
                    )

        # TIER 2C: Gender validation from master tables (after age validation)
        gender_validation_results = {}
        if enrollee_context and enrollee_context.gender:
            if proc_master:
                proc_gender_result = self.validate_gender_for_procedure(procedure_code, enrollee_context.gender)
                gender_validation_results["procedure"] = proc_gender_result
                # Hard DENY for MALE_ONLY/FEMALE_ONLY mismatches
                if proc_gender_result.get("is_valid") is False:
                    return VettingSuggestion(
                        suggested_action="DENY",
                        confidence=100,
                        reasoning=f"❌ GENDER RESTRICTION: {proc_gender_result['reasoning']}",
                        source="master_table",
                        requires_human_review=False,
                        rule_details={
                            "rule": "GENDER_VALIDATION",
                            "violation_type": "PROCEDURE_GENDER_MISMATCH",
                            "procedure_gender": proc_gender_result,
                            "enrollee_gender": enrollee_context.gender,
                            "master_table_authority": True,
                            "ai_override": False
                        }
                    )

            if diag_master:
                diag_gender_result = self.validate_gender_for_diagnosis(diagnosis_code, enrollee_context.gender)
                gender_validation_results["diagnosis"] = diag_gender_result
                # Hard DENY for MALE_ONLY/FEMALE_ONLY mismatches
                if diag_gender_result.get("is_valid") is False:
                    return VettingSuggestion(
                        suggested_action="DENY",
                        confidence=100,
                        reasoning=f"❌ GENDER RESTRICTION: {diag_gender_result['reasoning']}",
                        source="master_table",
                        requires_human_review=False,
                        rule_details={
                            "rule": "GENDER_VALIDATION",
                            "violation_type": "DIAGNOSIS_GENDER_MISMATCH",
                            "diagnosis_gender": diag_gender_result,
                            "enrollee_gender": enrollee_context.gender,
                            "master_table_authority": True,
                            "ai_override": False
                        }
                    )
                # Note: Warnings (MALE/FEMALE predominant) don't block, they continue validation

        # TIER 2A: Procedure-Diagnosis Compatibility
        # ============================================================
        # NEW FLOW: CONS universal → COMP table → AI
        # OLD (removed): typical_diagnoses + symptom overlap matching
        # ============================================================
        
        # Step 1: Universal consultation codes (CONS021, CONS022) 
        if self.is_universal_procedure(procedure_code):
            proc_name = proc_master['name'] if proc_master else procedure_code
            diag_name = diag_master['name'] if diag_master else diagnosis_code
            match_reasoning = f"✅ APPROVED: {proc_name} is a universal consultation — valid for all diagnoses including {diag_name}"
            
            if enrollee_context:
                notes = []
                if enrollee_context.age is not None and age_validation_results:
                    notes.append(f"Age: {enrollee_context.age} years")
                if enrollee_context.gender and gender_validation_results:
                    notes.append(f"Gender: {enrollee_context.gender}")
                if notes:
                    match_reasoning += f" ({', '.join(notes)} - valid)"
            
            return VettingSuggestion(
                suggested_action="APPROVE",
                confidence=100,
                reasoning=match_reasoning,
                source="master_table",
                requires_human_review=False,
                rule_details={
                    "procedure": proc_master,
                    "diagnosis": diag_master,
                    "match_type": "UNIVERSAL_CONSULTATION",
                    "age_validation": age_validation_results,
                    "gender_validation": gender_validation_results
                }
            )
        
        # Step 2: PROCEDURE_DIAGNOSIS_COMP table lookup
        comp_result = self.check_procedure_diagnosis_comp(procedure_code, diagnosis_code)
        
        if comp_result:
            match_reasoning = f"✅ APPROVED: {comp_result['procedure_name']} is a validated match for {comp_result['diagnosis_name']}"
            
            if enrollee_context:
                notes = []
                if enrollee_context.age is not None and age_validation_results:
                    notes.append(f"Age: {enrollee_context.age} years")
                if enrollee_context.gender and gender_validation_results:
                    notes.append(f"Gender: {enrollee_context.gender}")
                if notes:
                    match_reasoning += f" ({', '.join(notes)} - valid)"
            
            return VettingSuggestion(
                suggested_action="APPROVE",
                confidence=100,
                reasoning=match_reasoning,
                source="master_table",
                requires_human_review=False,
                rule_details={
                    "procedure": proc_master,
                    "diagnosis": diag_master,
                    "match_type": "COMP_TABLE_MATCH",
                    "comp_result": comp_result,
                    "age_validation": age_validation_results,
                    "gender_validation": gender_validation_results
                }
            )
        
        # Step 3: Not in COMP table → AI validates
        # (pair not in COMP does NOT mean invalid — just means we haven't curated it yet)
        
        # AI VALIDATION: Pair not in PROCEDURE_DIAGNOSIS_COMP table
        # Could be because: procedure and/or diagnosis not curated, or pair just not mapped yet
        missing_from_master = []
        not_in_comp = True  # We got here because COMP lookup returned None
        if not proc_master:
            missing_from_master.append(f"Procedure {procedure_code}")
        if not diag_master:
            missing_from_master.append(f"Diagnosis {diagnosis_code}")
        
        proc_comprehensive = self._get_procedure_from_comprehensive(procedure_code) if not proc_master else None
        diag_comprehensive = self._get_diagnosis_from_comprehensive(diagnosis_code) if not diag_master else None
        
        # Build AI prompt with actual information when available
        ai_prompt = "Medical Validation - Item(s) Not in Curated Master Table\n\n"
        
        # Procedure information
        if proc_master:
            ai_prompt += f"Procedure: {proc_master['name']} ({proc_master['class']})\n"
            ai_prompt += "  Status: In curated master table\n"
            if proc_master.get("age_range"):
                ai_prompt += f"  Age Range: {proc_master['age_range']}\n"
        elif proc_comprehensive:
            ai_prompt += f"Procedure Code: {procedure_code}\n"
            ai_prompt += f"Procedure Name: {proc_comprehensive['name']}\n"
            ai_prompt += f"Category: {proc_comprehensive.get('category') or 'Unknown'}\n"
            ai_prompt += "  Status: ⚠️ NOT in curated master table (no age/gender rules defined)\n"
            if proc_comprehensive.get("description"):
                ai_prompt += f"  Description: {proc_comprehensive['description']}\n"
        else:
            ai_prompt += f"Procedure Code: {procedure_code}\n"
            ai_prompt += "  Status: ❌ NOT FOUND in any database table\n"
            ai_prompt += "  ⚠️ This code may be invalid, misspelled, or custom\n"
        ai_prompt += "\n"
        
        # Diagnosis information
        if diag_master:
            ai_prompt += f"Diagnosis: {diag_master['name']} ({diag_master['class']})\n"
            ai_prompt += "  Status: In curated master table\n"
            if diag_master.get("age_range"):
                ai_prompt += f"  Typical Age: {diag_master['age_range']}\n"
        elif diag_comprehensive:
            ai_prompt += f"Diagnosis Code: {diagnosis_code}\n"
            ai_prompt += f"Diagnosis Name: {diag_comprehensive['name']}\n"
            if diag_comprehensive.get("category"):
                ai_prompt += f"Category: {diag_comprehensive['category']}\n"
            ai_prompt += "  Status: ⚠️ NOT in curated master table (no age/gender rules defined)\n"
        else:
            ai_prompt += f"Diagnosis Code: {diagnosis_code}\n"
            ai_prompt += "  Status: ❌ NOT FOUND in any database table\n"
            ai_prompt += "  ⚠️ This code may be invalid, misspelled, or custom\n"
        
        # Patient context
        if enrollee_context:
            ai_prompt += "\nPatient Information:\n"
            if enrollee_context.age is not None:
                ai_prompt += f"  Age: {enrollee_context.age} years old\n"
            if enrollee_context.gender:
                ai_prompt += f"  Gender: {enrollee_context.gender}\n"
        
        # Validation requirements
        ai_prompt += f"\n{'='*60}\n"
        ai_prompt += "VALIDATION REQUIRED:\n"
        ai_prompt += f"Missing from curated master table: {', '.join(missing_from_master)}\n\n"
        
        age_str = f"{enrollee_context.age}-year-old" if enrollee_context and enrollee_context.age is not None else "patient"
        
        if not proc_master and proc_comprehensive:
            ai_prompt += f"""For Procedure {proc_comprehensive['name']}:
1. Is this procedure/medication medically appropriate for a {age_str}?
2. Is it safe at this age?
3. Any age-specific contraindications?

NOTE: This procedure has been verified in the database. Focus only on medical appropriateness, not code format.

"""
        elif not proc_master and not proc_comprehensive:
            ai_prompt += f"""For Procedure {procedure_code}:
❌ CODE NOT FOUND IN DATABASE
1. Does this procedure code exist in any standard coding system?
2. Could this be a typo or custom facility code?
3. If you recognize the code from context, what is it for?

"""
        
        if not diag_master and diag_comprehensive:
            ai_prompt += f"""For Diagnosis {diag_comprehensive['name']}:
1. Is this diagnosis clinically reasonable for a {age_str}?
2. Does this condition typically occur at this age?
3. Any age-specific concerns?

"""
        elif not diag_master and not diag_comprehensive:
            ai_prompt += f"""For Diagnosis {diagnosis_code}:
❌ CODE NOT FOUND IN DATABASE
1. Is this a valid ICD-10 diagnosis code?
2. Could this be a typo or obsolete code?
3. If you recognize it, what condition does it represent?

"""
        
        # Gender validation questions (if not in master, let AI assess)
        if enrollee_context and enrollee_context.gender:
            gender_str = enrollee_context.gender.lower()

            if not proc_master and proc_comprehensive:
                ai_prompt += f"""Gender Appropriateness for Procedure:
1. Is {proc_comprehensive['name']} appropriate for a {gender_str} patient?
2. Is this procedure gender-specific (e.g., pregnancy tests for females, prostate exams for males)?
3. Any gender-specific contraindications?

"""

            if not diag_master and diag_comprehensive:
                ai_prompt += f"""Gender Appropriateness for Diagnosis:
1. Does {diag_comprehensive['name']} occur in {gender_str} patients?
2. Is this condition gender-specific (e.g., ovarian cancer, testicular cancer)?
3. Any concerns about this diagnosis for a {gender_str}?

"""

        ai_prompt += "Overall Assessment:\n"
        # Check if both items are identified from ANY source (master OR comprehensive)
        proc_known = proc_master or proc_comprehensive
        diag_known = diag_master or diag_comprehensive
        if proc_known or diag_known:
            if proc_comprehensive or diag_comprehensive:
                ai_prompt += "- Some items exist in the database but lack curated age/gender rules\n"
            if proc_known and diag_known:
                p_name = proc_known["name"]
                d_name = diag_known["name"]
                ai_prompt += "\nCRITICAL QUESTION:\n"
                ai_prompt += f"Is {p_name} CLINICALLY APPROPRIATE for {d_name}?\n\n"
                ai_prompt += "⚠️ IMPORTANT — 'Procedure' covers THREE types:\n"
                ai_prompt += "1. MEDICATION → Is it used IN THE TREATMENT of this diagnosis? (primary, symptomatic relief, supportive care, or standard co-prescription)\n"
                ai_prompt += "2. LAB TEST → Does it INVESTIGATE, CONFIRM, or MONITOR this diagnosis?\n"
                ai_prompt += "3. SURGICAL PROCEDURE → Is it an appropriate INTERVENTION?\n\n"
                ai_prompt += "For medications, ask: 'Would a doctor reasonably prescribe this for a patient with this diagnosis?'\n"
                ai_prompt += "This includes drugs that treat symptoms accompanying the condition or support recovery.\n"
                ai_prompt += "Example: Vitamin C for Malaria = APPROVE (supportive care during recovery)\n"
                ai_prompt += "Example: Paracetamol for Malaria = APPROVE (symptomatic — treats fever/pain)\n\n"
                ai_prompt += "- IMPORTANT: Both items verified in database - do NOT comment on code format or validity\n"
            ai_prompt += "- Consider the patient's age in your recommendation\n"
        # Check if both items are identified from ANY source (master OR comprehensive)
        proc_identified = proc_master or proc_comprehensive
        diag_identified = diag_master or diag_comprehensive

        if not proc_identified or not diag_identified:
            ai_prompt += "- Some codes were not found in any database table\n"
            ai_prompt += "- Verify code validity before approving\n"

        # Different instructions based on whether both items are identified
        if proc_identified and diag_identified:
            # Both items verified - focus on CLINICAL APPROPRIATENESS (medication/lab/surgery)
            proc_name = (proc_master or proc_comprehensive)["name"]
            diag_name = (diag_master or diag_comprehensive)["name"]
            ai_prompt += """
🚫 CRITICAL: CODE VALIDATION COMPLETE
=====================================
Both codes have been VERIFIED in the database:
✅ Procedure exists: """
            ai_prompt += proc_name + "\n"
            ai_prompt += "✅ Diagnosis exists: " + diag_name + """


🚫 DO NOT discuss:
   - Code format (DRG vs NDC vs HCPCS vs ICD-10)
   - Coding systems or standards
   - Whether code "should" be formatted differently
   - Code validity (already verified)

⚠️ Your ONLY job: Check if the procedure is CLINICALLY APPROPRIATE for a patient with this diagnosis

CRITICAL INSTRUCTIONS FOR PRE-AUTHORIZATION:

1. AGE: Is this safe for this patient's age?

2. GENDER: Is this appropriate for this patient's gender?
   - Gender-specific procedures/diagnoses must match (e.g., prostate exam = male only)

3. CLINICAL APPROPRIATENESS (MOST IMPORTANT):
   First identify the procedure type, then apply the right logic:

   FOR MEDICATIONS — Is it used IN THE TREATMENT of this diagnosis?
   This includes ALL of the following roles:
   - Primary treatment (directly targets the disease)
   - Symptomatic relief (treats symptoms that accompany the condition)
   - Supportive care (aids recovery, boosts immunity, prevents complications)
   - Standard co-prescription (commonly prescribed alongside primary treatment)
   
   Ask: "Would a doctor reasonably prescribe this for a patient with this diagnosis?"
   
   ✅ APPROVE: Artemether for Malaria (primary — kills the parasite)
   ✅ APPROVE: Paracetamol for Malaria (symptomatic — treats fever/body pain)
   ✅ APPROVE: Vitamin C for Malaria (supportive — immune support during recovery)
   ✅ APPROVE: Antibiotics for bacterial infections (primary treatment)
   ✅ APPROVE: Antacid for Gastritis (symptomatic relief)
   ❌ DENY: Antihypertensives for Pneumonia (no role in pneumonia treatment)
   ❌ DENY: Metformin for Fracture (diabetes drug, no role in fracture treatment)

   FOR LAB TESTS — Does it INVESTIGATE, CONFIRM, or MONITOR this diagnosis?
   ✅ APPROVE: Full Blood Count for Anaemia (checks haemoglobin)
   ✅ APPROVE: Malaria Thick/Thin Film for Plasmodium Falciparum (detects parasites)
   ✅ APPROVE: Full Blood Count for URTI (checks WBC for infection)
   ✅ APPROVE: Full Blood Count for Malaria (monitors haemoglobin, platelets)
   ✅ APPROVE: Liver Function Test for Hepatitis (checks liver enzymes)
   ✅ APPROVE: Chest X-ray for Pneumonia (visualizes lung infiltrates)
   ❌ DENY: Malaria Film for Fracture (irrelevant investigation)
   ❌ DENY: Pregnancy test for male patient

   FOR SURGICAL PROCEDURES — Is it an appropriate INTERVENTION?
   ✅ APPROVE: Myomectomy for Uterine Fibroids, Appendectomy for Appendicitis
   ❌ DENY: Appendectomy for Malaria (wrong intervention)

DECISION RULES:
- Age inappropriate/unsafe → DENY
- Gender inappropriate → DENY
- Medication has NO role in treating a patient with this diagnosis → DENY
- Lab test is NOT relevant to investigating this diagnosis → DENY
- Surgery is NOT an appropriate intervention for this condition → DENY
- Clinically appropriate (any role: primary/symptomatic/supportive) + age + gender → APPROVE

⚠️ DO NOT approve based only on "no contraindication"
⚠️ DO approve if medication treats SYMPTOMS of the condition or supports recovery

Respond in JSON format:
{"action": "APPROVE or DENY", "confidence": 0-100, "reasoning": "State: (1) Procedure type (medication/lab/surgery), (2) Age safety, (3) Gender appropriateness, (4) Clinical role (primary/symptomatic/supportive/none) for this diagnosis. DO NOT mention code format."}
"""
        else:
            # One or both items missing - validate codes
            ai_prompt += """
CRITICAL INSTRUCTIONS:
- If the procedure/diagnosis code is INVALID or NOT FOUND, recommend DENY
- If age makes this medically inappropriate or unsafe, recommend DENY
- If gender makes this medically inappropriate, recommend DENY
- If items exist but combination is inappropriate, recommend DENY
- Only approve if all validations pass

Respond in JSON format:
{"action": "APPROVE or DENY", "confidence": 0-100, "reasoning": "Include code validity, age, and gender considerations"}
"""
        self.stats["ai_calls"] += 1
        ai_response = self._call_claude_for_validation(ai_prompt)
        return VettingSuggestion(
            suggested_action=ai_response["suggested_action"],
            confidence=ai_response["confidence"],
            reasoning=ai_response["reasoning"],
            source="ai_suggestion",
            requires_human_review=True,
            rule_details={
                "procedure_master": proc_master,
                "procedure_comprehensive": proc_comprehensive,
                "diagnosis_master": diag_master,
                "diagnosis_comprehensive": diag_comprehensive,
                "missing_from_master": missing_from_master,
                "found_in_comprehensive": {
                    "procedure": proc_comprehensive is not None,
                    "diagnosis": diag_comprehensive is not None
                },
                "truly_invalid": {
                    "procedure": not proc_master and not proc_comprehensive,
                    "diagnosis": not diag_master and not diag_comprehensive
                },
                "enrollee_age": enrollee_context.age if enrollee_context else None,
                "enrollee_gender": enrollee_context.gender if enrollee_context else None,
                "ai_validation": "COMPREHENSIVE_CHECK_PERFORMED",
                "age_validation": age_validation_results,
                "gender_validation": gender_validation_results
            }
        )
    
    # ========================================================================
    # CONVENIENCE METHOD FOR SIMPLE USAGE
    # ========================================================================
    
    def vet_claim(
        self,
        procedure_code: str,
        diagnosis_code: str,
        enrollee_id: Optional[str] = None,
        encounter_date: Optional[str] = None
    ) -> VettingSuggestion:
        """
        Convenience method for vetting a claim
        
        Parameters:
        -----------
        procedure_code : str
        diagnosis_code : str
        enrollee_id : str, optional
            If provided, will fetch age/gender from MEMBERS table
        encounter_date : str, optional
            Date in YYYY-MM-DD format
        
        Returns:
        --------
        VettingSuggestion
        """
        # Get enrollee context if ID provided
        context = None
        if enrollee_id:
            context = self.get_enrollee_context(enrollee_id, encounter_date)
        
        return self.validate_procedure_diagnosis(
            procedure_code, diagnosis_code, context
        )
    
    # ========================================================================
    # STATISTICS AND REPORTING
    # ========================================================================
    
    def get_performance_stats(self) -> Dict:
        """Get performance statistics"""
        total = self.stats['total_requests']
        if total == 0:
            return {
                'learning_hit_rate': 0,
                'master_hit_rate': 0,
                'ai_call_rate': 0,
                'cost_saved_usd': 0,
                'cost_saved_ngn': 0
            }
        
        # Cost calculation: $0.015 per API call (Sonnet 4)
        ai_cost_per_call = 0.015
        calls_avoided = self.stats['learning_hits']
        cost_saved_usd = calls_avoided * ai_cost_per_call
        cost_saved_ngn = cost_saved_usd * 1650  # Approximate exchange rate
        
        return {
            'total_requests': total,
            'learning_hits': self.stats['learning_hits'],
            'master_hits': self.stats['master_hits'],
            'ai_calls': self.stats['ai_calls'],
            'learning_hit_rate': round(self.stats['learning_hits'] / total * 100, 2),
            'master_hit_rate': round(self.stats['master_hits'] / total * 100, 2),
            'ai_call_rate': round(self.stats['ai_calls'] / total * 100, 2),
            'cost_saved_usd': round(cost_saved_usd, 2),
            'cost_saved_ngn': round(cost_saved_ngn, 2)
        }
    
    def get_learning_stats(self) -> Dict:
        """Get statistics from learning tables"""
        stats = {}
        
        # Procedure-Diagnosis stats from MongoDB
        from pymongo import MongoClient
        pipeline = [{"$group": {
            "_id": None,
            "total_rules": {"$sum": 1},
            "total_reuses": {"$sum": "$usage_count"},
            "avg_reuses_per_rule": {"$avg": "$usage_count"},
            "valid_count": {"$sum": {"$cond": [{"$eq": ["$is_valid_match", True]}, 1, 0]}},
            "invalid_count": {"$sum": {"$cond": [{"$eq": ["$is_valid_match", False]}, 1, 0]}},
        }}]
        rows = list(mongo_db._col("ai_human_procedure_diagnosis").aggregate(pipeline))
        if rows:
            r = rows[0]
            stats['procedure_diagnosis'] = {
                'total_rules': r.get('total_rules', 0),
                'total_reuses': r.get('total_reuses') or 0,
                'avg_reuses_per_rule': round(r.get('avg_reuses_per_rule') or 0, 1),
                'valid_count': r.get('valid_count', 0),
                'invalid_count': r.get('invalid_count', 0),
            }
        else:
            stats['procedure_diagnosis'] = {
                'total_rules': 0, 'total_reuses': 0,
                'avg_reuses_per_rule': 0.0, 'valid_count': 0, 'invalid_count': 0
            }
        
        # Calculate total cost savings
        total_reuses = stats['procedure_diagnosis']['total_reuses']
        cost_saved_usd = total_reuses * 0.015
        stats['summary'] = {
            'total_reuses': total_reuses,
            'cost_savings_usd': round(cost_saved_usd, 2),
            'cost_savings_ngn': round(cost_saved_usd * 1650, 2)
        }
        
        return stats


# ============================================================================
# MAIN (for testing)
# ============================================================================

if __name__ == "__main__":
    print("Testing Learning Vetting Engine (FIXED VERSION)...")
    
    engine = LearningVettingEngine()
    
    # Test 1: With enrollee context
    print("\n=== TEST 1: With Enrollee Context ===")
    suggestion = engine.vet_claim(
        procedure_code="DRG1958",
        diagnosis_code="B373",
        enrollee_id="CL1234567",  # Replace with actual enrollee ID
        encounter_date="2025-01-15"
    )
    
    print(f"Action: {suggestion.suggested_action}")
    print(f"Confidence: {suggestion.confidence}%")
    print(f"Reasoning: {suggestion.reasoning}")
    print(f"Source: {suggestion.source}")
    print(f"Requires Review: {suggestion.requires_human_review}")
    
    # Test 2: Without enrollee context
    print("\n=== TEST 2: Without Enrollee Context ===")
    suggestion = engine.vet_claim(
        procedure_code="DRG1693",
        diagnosis_code="Z51"
    )
    
    print(f"Action: {suggestion.suggested_action}")
    print(f"Confidence: {suggestion.confidence}%")
    print(f"Reasoning: {suggestion.reasoning}")
    print(f"Source: {suggestion.source}")
    
    # Show stats
    print("\n=== Performance Stats ===")
    stats = engine.get_performance_stats()
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    engine.close()