import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from apis.klaire_whatsapp.models import (
    TermiiInbound, EnrolleeIdentity, FeedbackEntry, OutreachEntry
)

def test_termii_inbound_parses_from_alias():
    payload = {"from": "2348012345678", "to": "2341234567890", "text": "Hello", "type": "incoming"}
    msg = TermiiInbound.model_validate(payload)
    assert msg.from_ == "2348012345678"
    assert msg.text == "Hello"

def test_enrollee_identity_fields():
    e = EnrolleeIdentity(memberid="123", legacycode="CL/ARIK/001/2020",
                         firstname="Amaka", lastname="Obi", genderid=2, dateofbirth="1990-03-15")
    assert e.firstname == "Amaka"

def test_feedback_entry_defaults():
    f = FeedbackEntry(enrollee_id="CL/ARIK/001/2020", panumber="123456", hospital="Kupa Medical")
    assert f.adherence_flag is False
    assert f.escalated is False
    assert f.rating is None
