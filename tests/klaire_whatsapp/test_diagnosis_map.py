import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from apis.klaire_whatsapp.diagnosis_map import icd10_to_plain


def test_malaria_b50():
    assert icd10_to_plain("B50") == "malaria treatment"

def test_malaria_b54():
    assert icd10_to_plain("B54") == "malaria treatment"

def test_malaria_with_suffix():
    assert icd10_to_plain("B50.9") == "malaria treatment"

def test_pregnancy():
    assert icd10_to_plain("O21") == "pregnancy care"

def test_blood_pressure():
    assert icd10_to_plain("I10") == "blood pressure management"

def test_uti():
    assert icd10_to_plain("N39") == "urinary tract infection"

def test_unknown_code_falls_back():
    assert icd10_to_plain("Z99") == "a recent health condition"

def test_empty_code_falls_back():
    assert icd10_to_plain("") == "a recent health condition"

def test_none_code_falls_back():
    assert icd10_to_plain(None) == "a recent health condition"
