import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from apis.vetting.klaire_pa import _is_injection_procedure


def test_iv_artemether_detected():
    assert _is_injection_procedure("IV Artemether 80mg") is True


def test_infusion_detected():
    assert _is_injection_procedure("Normal Saline Infusion 1L") is True


def test_ampoule_detected():
    assert _is_injection_procedure("Diclofenac Ampoule 75mg") is True


def test_im_detected():
    assert _is_injection_procedure("Benzylpenicillin IM 1.2MU") is True


def test_tablet_not_detected():
    assert _is_injection_procedure("Amoxicillin Tablet 500mg") is False


def test_capsule_not_detected():
    assert _is_injection_procedure("Doxycycline Capsule 100mg") is False


def test_syrup_not_detected():
    assert _is_injection_procedure("Paracetamol Syrup 120mg/5ml") is False


def test_class_field_used():
    assert _is_injection_procedure("Ceftriaxone 1g", "INJECTION") is True
