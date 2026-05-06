import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from apis.klaire_whatsapp.front_desk import detect_intent, is_emergency, is_complaint


def test_detect_hospital_intent():
    assert detect_intent("what hospital am i mapped to") == "mapped_hospital"

def test_detect_plan_intent():
    assert detect_intent("is my plan still active") == "plan_status"

def test_detect_limit_intent():
    assert detect_intent("how much have I used") == "limit_used"

def test_detect_pa_intent():
    assert detect_intent("what is my PA status") == "pa_status"

def test_detect_benefits_intent():
    assert detect_intent("what is covered under my plan") == "benefits"

def test_detect_complaint_intent():
    assert detect_intent("I want to make a complaint") == "complaint"

def test_detect_emergency_intent():
    assert detect_intent("I am having chest pain") == "emergency"

def test_detect_general_intent_fallback():
    assert detect_intent("hello how are you") == "general"

def test_is_emergency_detects_keywords():
    assert is_emergency("I'm having chest pain") is True
    assert is_emergency("emergency help me") is True
    assert is_emergency("how do I renew my plan") is False

def test_is_complaint_detects_keywords():
    assert is_complaint("I want to complain about the hospital") is True
    assert is_complaint("the service was terrible") is True
    assert is_complaint("thank you") is False

def test_emergency_takes_priority_over_complaint():
    assert detect_intent("emergency the service here is terrible") == "emergency"

def test_complaint_takes_priority_over_hospital():
    assert detect_intent("complain about the hospital they sent me to") == "complaint"
