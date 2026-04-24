# Pre-Auth Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Pre-Auth (INPATIENT) flow with agent-driven decisions, admission sub-flow, and Rule 12 (Injection-Without-Admission).

**Architecture:** Add `admission_status` threading through `validate_pa_request → _validate_one_procedure`, force PENDING_REVIEW for all INPATIENT results in `klaire_pa.py`, add new admission endpoint in `main.py`, and extend the Streamlit UI with a two-path gate (Admitted vs Not Admitted) before the procedure form.

**Tech Stack:** Python/FastAPI, MongoDB (pymongo), Streamlit, Anthropic Claude (claude-opus-4-6), Pydantic v2

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `apis/vetting/klaire_pa.py` | Modify | Add `admission_status` param, force PENDING_REVIEW for INPATIENT, add Rule 12, set `review_type="PA_PREAUTH"` |
| `apis/vetting/main.py` | Modify | New `KlaireAdmissionRequest` model, two new endpoints, update `KlairePARequest`, update submit handler for `PA_PREAUTH` + `PA_ADMISSION` |
| `streamlit_vetting/klaire_app.py` | Modify | Pre-Auth sub-flow gate, admission form, Agent Review `PA_ADMISSION` card, `PA_PREAUTH` badge |
| `tests/test_preauth.py` | Create | Unit tests for Rule 12 detection, force-PENDING logic, admission endpoint |

---

## Task 1: Rule 12 detection helper + admission_status threading in klaire_pa.py

**Files:**
- Modify: `apis/vetting/klaire_pa.py`
- Create: `tests/test_preauth.py`

- [ ] **Step 1: Write failing tests for injection detection**

Create `tests/test_preauth.py`:

```python
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _is_injection(name, cls=""):
    """Mirror of the helper to test in isolation."""
    text = f"{name} {cls}".upper()
    keywords = [
        "IV ", "I.V.", "INFUSION", "INJECTION", "INJECTABLE",
        "AMPOULE", " AMP ", "AMP.", " IM ", "I.M.", "INJ ", "INJ.",
        "INTRAVENOUS", "INTRAMUSCULAR", "PARENTERAL",
    ]
    return any(kw in text for kw in keywords)


def test_iv_artemether_detected():
    assert _is_injection("IV Artemether 80mg") is True


def test_infusion_detected():
    assert _is_injection("Normal Saline Infusion 1L") is True


def test_ampoule_detected():
    assert _is_injection("Diclofenac Ampoule 75mg") is True


def test_im_detected():
    assert _is_injection("Benzylpenicillin IM 1.2MU") is True


def test_tablet_not_detected():
    assert _is_injection("Amoxicillin Tablet 500mg") is False


def test_capsule_not_detected():
    assert _is_injection("Doxycycline Capsule 100mg") is False


def test_syrup_not_detected():
    assert _is_injection("Paracetamol Syrup 120mg/5ml") is False


def test_class_field_used():
    assert _is_injection("Ceftriaxone 1g", cls="INJECTION") is True
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
cd /Users/kenechukwuchukwuka/Downloads/DLT
venv/bin/python -m pytest tests/test_preauth.py -v 2>&1 | head -30
```

Expected: `ModuleNotFoundError` or `NameError` — `_is_injection` not importable yet.

- [ ] **Step 3: Add `_is_injection_procedure` and `check_injection_without_admission` to klaire_pa.py**

Add after the `check_first_line_treatment` function (after line ~148), before `_r()`:

```python
# ── Rule 12 — Injection Without Admission ─────────────────────────────────────

_INJECTION_KEYWORDS = [
    "IV ", "I.V.", "INFUSION", "INJECTION", "INJECTABLE",
    "AMPOULE", " AMP ", "AMP.", " IM ", "I.M.", "INJ ", "INJ.",
    "INTRAVENOUS", "INTRAMUSCULAR", "PARENTERAL",
]


def _is_injection_procedure(proc_name: str, proc_class: str = "") -> bool:
    text = f"{proc_name} {proc_class}".upper()
    return any(kw in text for kw in _INJECTION_KEYWORDS)


def check_injection_without_admission(
    procedure_code: str,
    procedure_name: str,
    procedure_class: str,
    diagnosis_codes: List[str],
    diagnosis_names: Dict[str, str],
) -> Dict:
    """
    Rule 12 — fired for INPATIENT + NOT_ADMITTED when an IV/IM procedure is prescribed.
    AI assesses whether oral alternatives should have been tried first.
    Never auto-denies. Always PENDING_REVIEW if triggered.
    """
    if not _is_injection_procedure(procedure_name, procedure_class):
        return {"triggered": False}

    diag_list = ", ".join(
        f"{c} ({diagnosis_names.get(c, c)})" for c in diagnosis_codes
    )
    prompt = f"""You are a clinical reviewer for a Nigerian HMO cost-control unit.

A provider has prescribed a parenteral (injection/infusion) medication for a patient who is NOT admitted.

Procedure: {procedure_code} — {procedure_name}
Diagnoses: {diag_list}

Assess strictly:
1. Is there an oral/tablet equivalent that should normally be tried first in Nigerian HMO practice?
2. Is any of these diagnoses critical enough to justify skipping oral treatment entirely?
   (Examples of critical: severe malaria with vomiting, sepsis, status epilepticus, acute severe asthma, cerebral malaria)
3. Overall: is the direct parenteral route clinically justified for a non-admitted patient?

Respond in JSON only (no markdown):
{{
  "oral_alternative_exists": true or false,
  "diagnosis_critical": true or false,
  "justified": true or false,
  "confidence": 0-100,
  "reasoning": "One concise sentence."
}}"""

    ai = _call_claude(prompt)
    return {
        "triggered": True,
        "justified": bool(ai.get("justified", False)),
        "confidence": int(ai.get("confidence", 0)),
        "reasoning": ai.get("reasoning", ""),
        "oral_alternative_exists": bool(ai.get("oral_alternative_exists", True)),
        "diagnosis_critical": bool(ai.get("diagnosis_critical", False)),
    }
```

- [ ] **Step 4: Update test file to import from actual module**

Replace the `_is_injection` local copy in `tests/test_preauth.py` with the real import:

```python
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
    assert _is_injection_procedure("Ceftriaxone 1g", cls="INJECTION") is True
```

Wait — the helper signature is `_is_injection_procedure(proc_name, proc_class="")`, not `cls`. Fix the last test:

```python
def test_class_field_used():
    assert _is_injection_procedure("Ceftriaxone 1g", "INJECTION") is True
```

- [ ] **Step 5: Run tests — expect PASS**

```bash
venv/bin/python -m pytest tests/test_preauth.py -v
```

Expected output: `8 passed`

- [ ] **Step 6: Add `admission_status` param to `_validate_one_procedure` signature**

Find this line in `klaire_pa.py` (around line 166):
```python
def _validate_one_procedure(
    engine,
    proc_code: str,
    diag_codes: List[str],
    diag_names: Dict[str, str],
    enrollee_id: str,
    encounter_date: str,
    encounter_type: str,
    all_proc_codes: List[str],
    tariff_price: Optional[float] = None,
    provider_price: Optional[float] = None,
    comment: Optional[str] = None,
    quantity: int = 1,
    proc_name_hint: str = "",
) -> Dict:
```

Replace with:
```python
def _validate_one_procedure(
    engine,
    proc_code: str,
    diag_codes: List[str],
    diag_names: Dict[str, str],
    enrollee_id: str,
    encounter_date: str,
    encounter_type: str,
    all_proc_codes: List[str],
    tariff_price: Optional[float] = None,
    provider_price: Optional[float] = None,
    comment: Optional[str] = None,
    quantity: int = 1,
    proc_name_hint: str = "",
    admission_status: str = "NOT_ADMITTED",
) -> Dict:
```

- [ ] **Step 7: Commit**

```bash
git add apis/vetting/klaire_pa.py tests/test_preauth.py
git commit -m "feat: add Rule 12 injection-without-admission helper and admission_status param"
```

---

## Task 2: Apply Rule 12 and force PENDING_REVIEW for INPATIENT in _validate_one_procedure

**Files:**
- Modify: `apis/vetting/klaire_pa.py`

- [ ] **Step 1: Resolve proc_class from master table inside `_validate_one_procedure`**

Find the block in `_validate_one_procedure` where `proc_name` is resolved (around line 186):
```python
    if proc_name_hint:
        proc_name = proc_name_hint
    else:
        proc_info = engine._resolve_procedure_info(proc_code)
        proc_name = proc_info.get("name", proc_code) if proc_info else proc_code
```

Replace with:
```python
    if proc_name_hint:
        proc_name = proc_name_hint
    else:
        proc_info = engine._resolve_procedure_info(proc_code)
        proc_name = proc_info.get("name", proc_code) if proc_info else proc_code

    proc_master = mongo_db.get_procedure_master(proc_code)
    proc_class  = (proc_master or {}).get("procedure_class", "")
```

- [ ] **Step 2: Apply Rule 12 after the first-line check**

Find the quantity check block (after `first_line = check_first_line_treatment(...)`):
```python
    # ── Quantity check ────────────────────────────────────────────────────────
```

Insert Rule 12 immediately before the quantity check:
```python
    # ── Rule 12 — Injection Without Admission ─────────────────────────────────
    injection_check: Dict = {"triggered": False}
    if encounter_type == "INPATIENT" and admission_status == "NOT_ADMITTED":
        injection_check = check_injection_without_admission(
            proc_code, proc_name, proc_class, diag_codes, diag_names
        )

```

- [ ] **Step 3: Force PENDING_REVIEW for INPATIENT and include injection flag in review_reasons**

Find the end of the decision logic block, just before this line:
```python
    approved_diagnoses = passing_diags if not all_failed else []
```

Insert after the existing `if price_override` block:
```python
    # Rule 12 — injection advisory (non-admitted pre-auth)
    if injection_check.get("triggered"):
        requires_review = True
        if not injection_check.get("justified"):
            review_reasons.append(
                f"Injection-Without-Admission: oral alternative not documented — "
                f"agent to verify. ({injection_check.get('reasoning', '')})"
            )
        else:
            review_reasons.append(
                f"Injection-Without-Admission: diagnosis justifies direct parenteral route. "
                f"({injection_check.get('reasoning', '')})"
            )
        if decision != "DENY":
            decision = "PENDING_REVIEW"

    # Pre-Auth: force all non-DENY results to agent review — AI advises only
    if encounter_type == "INPATIENT" and decision != "DENY":
        decision = "PENDING_REVIEW"
        requires_review = True
        if "Pre-Auth: agent decision required." not in review_reasons:
            review_reasons.append("Pre-Auth: agent decision required.")

```

- [ ] **Step 4: Change review_type to PA_PREAUTH for INPATIENT**

Find in `_validate_one_procedure` the `mongo_db.insert_klaire_review({...})` call (around line 353). Change:
```python
            "review_type":         "PA_OUTPATIENT",
```
to:
```python
            "review_type":         "PA_PREAUTH" if encounter_type == "INPATIENT" else "PA_OUTPATIENT",
```

- [ ] **Step 5: Also include injection_check in the review doc**

In the same `insert_klaire_review({...})` call, add after `"first_line": first_line,`:
```python
            "injection_check":     injection_check,
```

- [ ] **Step 6: Thread admission_status through validate_pa_request**

Find `def validate_pa_request(` (around line 579). Add `admission_status: str = "NOT_ADMITTED"` to the signature:
```python
def validate_pa_request(
    items: List[Dict],
    enrollee_id: str,
    provider_id: str,
    hospital_name: str,
    encounter_date: str,
    encounter_type: str,
    db_path: str,
    admission_status: str = "NOT_ADMITTED",
) -> Dict:
```

Find the `_run_item` inner function (around line 605) and add `admission_status` to the `_validate_one_procedure` call:
```python
    def _run_item(item):
        thread_engine = ComprehensiveVettingEngine(db_path)
        return _validate_one_procedure(
            engine=thread_engine,
            proc_code=item["procedure_code"].strip().upper(),
            proc_name_hint=item.get("procedure_name") or "",
            diag_codes=[d.strip().upper() for d in item.get("diagnosis_codes", [])],
            diag_names=item.get("diagnosis_names", {}),
            enrollee_id=enrollee_id,
            encounter_date=encounter_date,
            encounter_type=encounter_type,
            all_proc_codes=all_proc_codes,
            tariff_price=item.get("tariff_price"),
            provider_price=item.get("provider_price"),
            comment=item.get("comment"),
            quantity=int(item.get("quantity") or 1),
            admission_status=admission_status,
        )
```

- [ ] **Step 7: Commit**

```bash
git add apis/vetting/klaire_pa.py
git commit -m "feat: force PENDING_REVIEW for INPATIENT and apply Rule 12 in pre-auth"
```

---

## Task 3: New models and endpoints in main.py (admission-codes, admission, PA model update)

**Files:**
- Modify: `apis/vetting/main.py`

- [ ] **Step 1: Add `admission_status` and `admission_approved_id` to `KlairePARequest`**

Find `KlairePARequest` (around line 1767):
```python
class KlairePARequest(BaseModel):
    enrollee_id:    str
    provider_id:    str
    hospital_name:  Optional[str] = None
    encounter_date: str
    encounter_type: Literal["OUTPATIENT", "INPATIENT"] = "OUTPATIENT"
    items:          List[KlairePAItem] = Field(..., min_length=1)
```

Replace with:
```python
class KlairePARequest(BaseModel):
    enrollee_id:          str
    provider_id:          str
    hospital_name:        Optional[str] = None
    encounter_date:       str
    encounter_type:       Literal["OUTPATIENT", "INPATIENT"] = "OUTPATIENT"
    admission_status:     Literal["ADMITTED", "NOT_ADMITTED"] = "NOT_ADMITTED"
    admission_approved_id: Optional[str] = None   # review_id of approved admission
    items:                List[KlairePAItem] = Field(..., min_length=1)
```

- [ ] **Step 2: Update `klaire_pa()` to pass `admission_status`**

Find the `klaire_pa` function (around line 1776):
```python
        result = validate_pa_request(
            items=[item.model_dump() for item in req.items],
            enrollee_id=req.enrollee_id,
            provider_id=req.provider_id,
            hospital_name=req.hospital_name or "",
            encounter_date=req.encounter_date,
            encounter_type=req.encounter_type,
            db_path=get_db_path(),
        )
```

Replace with:
```python
        result = validate_pa_request(
            items=[item.model_dump() for item in req.items],
            enrollee_id=req.enrollee_id,
            provider_id=req.provider_id,
            hospital_name=req.hospital_name or "",
            encounter_date=req.encounter_date,
            encounter_type=req.encounter_type,
            db_path=get_db_path(),
            admission_status=req.admission_status,
        )
```

- [ ] **Step 3: Add ADMISSION_CODES constant and KlaireAdmissionRequest model**

Add after the existing `KlairePARequest` model (before `@app.post("/api/v1/klaire/pa")`):

```python
ADMISSION_CODES = {
    "ADM01": "Private Room",
    "ADM02": "Semi-Private Room",
    "ADM03": "General Room",
}


class KlaireAdmissionRequest(BaseModel):
    enrollee_id:                str
    provider_id:                str
    hospital_name:              Optional[str] = None
    encounter_date:             str
    admission_code:             Literal["ADM01", "ADM02", "ADM03"]
    days:                       int = Field(..., ge=1)
    admitting_diagnosis_codes:  List[str] = Field(..., min_length=1)
    admitting_diagnosis_names:  Dict[str, str] = Field(default_factory=dict)
```

- [ ] **Step 4: Add `GET /api/v1/klaire/admission-codes` endpoint**

Add after the existing `@app.get("/api/v1/klaire/consultation-codes")` handler:

```python
@app.get("/api/v1/klaire/admission-codes")
def get_admission_codes():
    """Return the three room-type admission codes."""
    return {
        "codes": [
            {"code": k, "name": v} for k, v in ADMISSION_CODES.items()
        ]
    }
```

- [ ] **Step 5: Add `POST /api/v1/klaire/admission` endpoint**

Add after the `get_admission_codes` endpoint:

```python
@app.post("/api/v1/klaire/admission")
def klaire_admission(req: KlaireAdmissionRequest):
    """
    Submit an admission request for agent approval.
    AI advises on clinical appropriateness; agent makes the final call.
    Returns review_id — frontend polls or waits for agent to act in Agent Review tab.
    """
    from .klaire_pa import _call_claude
    room_name = ADMISSION_CODES[req.admission_code]
    diag_list = ", ".join(
        f"{c} ({req.admitting_diagnosis_names.get(c, c)})"
        for c in req.admitting_diagnosis_codes
    )
    prompt = f"""You are a clinical reviewer for a Nigerian HMO cost-control unit.

A provider is requesting inpatient admission for an enrollee.

Room type: {req.admission_code} — {room_name}
Expected duration: {req.days} day(s)
Admitting diagnoses: {diag_list}

Assess:
1. Is inpatient admission clinically necessary for these diagnoses?
2. Is the expected duration reasonable for this condition in Nigerian HMO practice?
3. Is the room type appropriate? (Private is for serious/critical cases; General for routine)

Respond in JSON only (no markdown):
{{
  "appropriate": true or false,
  "duration_reasonable": true or false,
  "room_appropriate": true or false,
  "confidence": 0-100,
  "reasoning": "One or two concise sentences."
}}"""

    try:
        ai = _call_claude(prompt)
    except Exception:
        ai = {"appropriate": False, "confidence": 0,
              "reasoning": "AI call failed — agent to assess independently."}

    review_id = str(uuid.uuid4())[:16]
    mongo_db.insert_klaire_review({
        "review_id":                 review_id,
        "review_type":               "PA_ADMISSION",
        "enrollee_id":               req.enrollee_id,
        "provider_id":               req.provider_id,
        "hospital_name":             req.hospital_name or "",
        "encounter_date":            req.encounter_date,
        "admission_code":            req.admission_code,
        "admission_name":            room_name,
        "days":                      req.days,
        "admitting_diagnosis_codes": req.admitting_diagnosis_codes,
        "admitting_diagnosis_names": req.admitting_diagnosis_names,
        "ai_appropriate":            ai.get("appropriate", False),
        "ai_duration_reasonable":    ai.get("duration_reasonable", True),
        "ai_room_appropriate":       ai.get("room_appropriate", True),
        "ai_confidence":             int(ai.get("confidence", 0)),
        "ai_reasoning":              ai.get("reasoning", ""),
        "status":                    "PENDING_REVIEW",
        "reviewed_by":               None,
        "review_notes":              None,
        "reviewed_at":               None,
        "created_at":                datetime.now().isoformat(),
    })

    return {
        "review_id": review_id,
        "status":    "PENDING_REVIEW",
        "ai_advice": {
            "appropriate":       ai.get("appropriate", False),
            "duration_reasonable": ai.get("duration_reasonable", True),
            "room_appropriate":  ai.get("room_appropriate", True),
            "confidence":        int(ai.get("confidence", 0)),
            "reasoning":         ai.get("reasoning", ""),
        },
    }
```

- [ ] **Step 6: Update `klaire_submit_review` to handle PA_PREAUTH and PA_ADMISSION**

Find (around line 1584):
```python
    if review_type == "PA_OUTPATIENT":
```

Replace with:
```python
    if review_type in ("PA_OUTPATIENT", "PA_PREAUTH"):
```

Then find the `else:` block that handles SPECIALIST:
```python
    else:
        # Specialist-diagnosis review — update ai_specialist_diagnosis
```

Replace with:
```python
    elif review_type == "PA_ADMISSION":
        # Admission approval is a one-time gate — no learning table written
        message = (
            f"Admission {'approved' if status == 'HUMAN_APPROVED' else 'denied'} "
            f"for {doc.get('admission_code', '')} "
            f"({doc.get('admission_name', '')}) — {doc.get('days', '?')} day(s)."
        )

    else:
        # Specialist-diagnosis review — update ai_specialist_diagnosis
```

- [ ] **Step 7: Restart API and verify new endpoints appear in docs**

```bash
pkill -f "uvicorn apis.vetting.main" 2>/dev/null; sleep 1
nohup venv/bin/uvicorn apis.vetting.main:app --host 0.0.0.0 --port 8000 --reload > /tmp/vetting_api.log 2>&1 &
sleep 4 && curl -s "http://localhost:8000/api/v1/klaire/admission-codes"
```

Expected:
```json
{"codes":[{"code":"ADM01","name":"Private Room"},{"code":"ADM02","name":"Semi-Private Room"},{"code":"ADM03","name":"General Room"}]}
```

- [ ] **Step 8: Commit**

```bash
git add apis/vetting/main.py
git commit -m "feat: add admission-codes and admission endpoints, update PA request model"
```

---

## Task 4: Streamlit — Pre-Auth sub-flow gate and admission form

**Files:**
- Modify: `streamlit_vetting/klaire_app.py`

- [ ] **Step 1: Add `load_admission_codes` cached loader**

Add after the existing `load_specialist_codes` function (around line 102):

```python
@st.cache_data(ttl=86400)
def load_admission_codes():
    try:
        r = requests.get(f"{API}/api/v1/klaire/admission-codes", timeout=10)
        r.raise_for_status()
        return {c["code"]: c["name"] for c in r.json().get("codes", [])}
    except Exception:
        return {"ADM01": "Private Room", "ADM02": "Semi-Private Room", "ADM03": "General Room"}
```

- [ ] **Step 2: Add Pre-Auth gate immediately after the encounter type radio in the PA Request tab**

Find in the PA Request tab (around line 779):
```python
    _enc_display = st.radio(
        "Encounter Type",
        options=["No-Auth (Outpatient)", "Pre-Auth (Inpatient)"],
        horizontal=True,
        key="pa_enc_type",
    )
    enc_type = "OUTPATIENT" if _enc_display.startswith("No-Auth") else "INPATIENT"

    st.markdown("---")
```

Replace with:
```python
    _enc_display = st.radio(
        "Encounter Type",
        options=["No-Auth (Outpatient)", "Pre-Auth (Inpatient)"],
        horizontal=True,
        key="pa_enc_type",
    )
    enc_type = "OUTPATIENT" if _enc_display.startswith("No-Auth") else "INPATIENT"

    st.markdown("---")

    # ── Pre-Auth sub-flow gate ────────────────────────────────────────────────
    if enc_type == "INPATIENT":
        st.markdown("### 🏥 Is the enrollee being admitted?")
        adm_col1, adm_col2, _ = st.columns([1, 1, 3])
        with adm_col1:
            if st.button("✅ Yes — Admitted", key="pa_adm_yes",
                         type="primary" if st.session_state.get("pa_admission_status") == "ADMITTED" else "secondary",
                         use_container_width=True):
                st.session_state["pa_admission_status"] = "ADMITTED"
                st.session_state.pop("admission_approved", None)
                st.session_state.pop("admission_review_id", None)
                st.rerun()
        with adm_col2:
            if st.button("❌ No — Not Admitted", key="pa_adm_no",
                         type="primary" if st.session_state.get("pa_admission_status") == "NOT_ADMITTED" else "secondary",
                         use_container_width=True):
                st.session_state["pa_admission_status"] = "NOT_ADMITTED"
                st.rerun()

        admission_status = st.session_state.get("pa_admission_status", "")
        if not admission_status:
            st.info("Select whether the enrollee is being admitted before adding procedures.")
            st.stop()

        if admission_status == "ADMITTED":
            _render_admission_form(enrollee_id, provider_id, hospital_name, encounter_date, diag_options=load_diagnoses())
            if not st.session_state.get("admission_approved"):
                st.warning("⏳ Admission request is pending agent approval. Go to **🛡️ Agent Review** tab to approve, then return here.")
                st.stop()
            else:
                st.success(f"✅ Admission approved — {st.session_state.get('admission_review_id', '')}. Add procedures below.")
        else:
            st.info("Not admitted — proceed directly to procedures. Rule 12 (Injection-Without-Admission) will apply.")

        st.markdown("---")
    else:
        admission_status = "NOT_ADMITTED"

```

- [ ] **Step 3: Add `_render_admission_form` helper function**

Add before the `call_api` function (around line 441):

```python
def _render_admission_form(enrollee_id, provider_id, hospital_name, encounter_date, diag_options):
    """Renders the admission room + days + diagnoses form and handles submission."""
    adm_codes = load_admission_codes()

    with st.container(border=True):
        st.markdown("#### 🏥 Admission Request")
        a1, a2 = st.columns([2, 1])
        with a1:
            room_display = st.selectbox(
                "Room Type",
                options=[f"{k} — {v}" for k, v in adm_codes.items()],
                key="adm_room",
            )
            adm_code = room_display.split(" — ")[0].strip()
        with a2:
            adm_days = st.number_input("Days", min_value=1, value=1, step=1, key="adm_days")

        if diag_options:
            adm_diags = st.multiselect(
                "Admitting Diagnoses",
                options=list(diag_options.keys()),
                placeholder="Select one or more admitting diagnoses…",
                key="adm_diags",
            )
            adm_diag_codes = [diag_options[d]["code"] for d in adm_diags]
            adm_diag_names = {diag_options[d]["code"]: diag_options[d]["name"] for d in adm_diags}
        else:
            raw = st.text_input("Admitting Diagnosis Codes (comma-separated)", key="adm_diags_raw")
            adm_diag_codes = [c.strip().upper() for c in raw.split(",") if c.strip()]
            adm_diag_names = {c: c for c in adm_diag_codes}

        if st.button("▶ Submit Admission Request", type="primary", key="adm_submit"):
            errs = []
            if not (enrollee_id or "").strip():
                errs.append("Enrollee ID required.")
            if not (provider_id or "").strip():
                errs.append("Provider ID required.")
            if not adm_diag_codes:
                errs.append("At least one admitting diagnosis required.")
            for e in errs:
                st.error(e)
            if not errs:
                payload = {
                    "enrollee_id":               enrollee_id.strip(),
                    "provider_id":               provider_id.strip(),
                    "hospital_name":             (hospital_name or "").strip() or None,
                    "encounter_date":            str(encounter_date),
                    "admission_code":            adm_code,
                    "days":                      int(adm_days),
                    "admitting_diagnosis_codes": adm_diag_codes,
                    "admitting_diagnosis_names": adm_diag_names,
                }
                with st.spinner("Submitting admission request…"):
                    try:
                        resp = requests.post(f"{API}/api/v1/klaire/admission", json=payload, timeout=60)
                        resp.raise_for_status()
                        data = resp.json()
                        st.session_state["admission_review_id"] = data["review_id"]
                        ai = data.get("ai_advice", {})
                        color = "#22c55e" if ai.get("appropriate") else "#ef4444"
                        st.markdown(
                            f'<div style="background:#1a1a2e;border:1px solid #7c3aed;border-radius:10px;padding:14px 18px;">'
                            f'<span style="color:#a78bfa;font-weight:700;">🔍 Admission Request Sent — Pending Agent Review</span><br>'
                            f'<span style="color:#c4b5fd;font-size:0.88em;">Review ID: <code>{data["review_id"]}</code></span><br><br>'
                            f'<strong style="color:{color};">AI Advisory: {"Admission appropriate" if ai.get("appropriate") else "Admission may not be appropriate"} ({ai.get("confidence", 0)}%)</strong><br>'
                            f'<span style="color:#cbd5e1;font-size:0.88em;">{ai.get("reasoning", "")}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                    except Exception as ex:
                        st.error(f"Submission failed: {ex}")
```

- [ ] **Step 4: Update payload in PA Request submission to include admission_status**

Find in the PA submit block (around line 1000):
```python
            payload = {
                "enrollee_id":    enrollee_id.strip(),
                "provider_id":    provider_id.strip(),
                "hospital_name":  (hospital_name or "").strip() or None,
                "encounter_date": str(encounter_date),
                "encounter_type": enc_type,
                "items":          items_payload,
            }
```

Replace with:
```python
            _adm_status = st.session_state.get("pa_admission_status", "NOT_ADMITTED") if enc_type == "INPATIENT" else "NOT_ADMITTED"
            payload = {
                "enrollee_id":          enrollee_id.strip(),
                "provider_id":          provider_id.strip(),
                "hospital_name":        (hospital_name or "").strip() or None,
                "encounter_date":       str(encounter_date),
                "encounter_type":       enc_type,
                "admission_status":     _adm_status,
                "admission_approved_id": st.session_state.get("admission_review_id"),
                "items":                items_payload,
            }
```

- [ ] **Step 5: Restart Streamlit and manually test Pre-Auth gate renders**

```bash
pkill -f "streamlit run streamlit_vetting" 2>/dev/null; sleep 1
nohup venv/bin/streamlit run streamlit_vetting/klaire_app.py --server.port 8501 > /tmp/streamlit.log 2>&1 &
sleep 4 && echo "Streamlit started"
```

Open http://localhost:8501 → PA Request tab → select Pre-Auth → verify Admitted/Not Admitted buttons appear.

- [ ] **Step 6: Commit**

```bash
git add streamlit_vetting/klaire_app.py
git commit -m "feat: add Pre-Auth sub-flow gate and admission request form to Streamlit"
```

---

## Task 5: Streamlit — Agent Review tab PA_ADMISSION card + PA_PREAUTH badge

**Files:**
- Modify: `streamlit_vetting/klaire_app.py`

- [ ] **Step 1: Add PA_ADMISSION card to Agent Review queue loop**

Find the `with rc1:` block inside the Agent Review loop (around line 1149). It currently has:
```python
                    if review_type == "PA_OUTPATIENT":
                        # ── PA Outpatient review card ──────────────────────
```

Add a new branch for `PA_ADMISSION` before the existing `if review_type == "PA_OUTPATIENT":`:

```python
                    if review_type == "PA_ADMISSION":
                        # ── Admission review card ──────────────────────────
                        adm_code   = rv.get("admission_code", "")
                        adm_name   = rv.get("admission_name", adm_code)
                        adm_days   = rv.get("days", 1)
                        adm_diags  = rv.get("admitting_diagnosis_codes", [])
                        adm_dnames = rv.get("admitting_diagnosis_names", {})
                        ai_conf    = rv.get("ai_confidence", 0)
                        ai_why     = rv.get("ai_reasoning", "")
                        ai_ok      = rv.get("ai_appropriate", False)

                        st.markdown(
                            f'<span style="background:#166534;color:#bbf7d0;border-radius:4px;'
                            f'padding:2px 8px;font-size:0.8em;">🏥 PA ADMISSION</span>',
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            f"**{adm_code} — {adm_name}** · {adm_days} day(s)  \n"
                            f"Enrollee: `{rv.get('enrollee_id','')}` · {rv.get('encounter_date','')}  \n"
                            f"Provider: {rv.get('hospital_name','')}"
                        )
                        if adm_diags:
                            pills = "  ".join(
                                f'`{c}` {adm_dnames.get(c, c)}' for c in adm_diags
                            )
                            st.markdown(f"Admitting diagnoses: {pills}")

                        adv_color = "#22c55e" if ai_ok else "#ef4444"
                        adv_label = "Admission clinically appropriate" if ai_ok else "Admission may not be appropriate"
                        st.markdown(
                            f'<div style="background:{"#052e16" if ai_ok else "#2d0a0a"};'
                            f'border-radius:6px;padding:8px 12px;margin-top:6px;">'
                            f'<strong style="color:{adv_color};">AI Advisory: {adv_label} ({ai_conf}%)</strong><br>'
                            f'<span style="color:#cbd5e1;font-size:0.88em;">{ai_why}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                    elif review_type == "PA_OUTPATIENT":
```

(Note: change `if review_type == "PA_OUTPATIENT":` to `elif review_type == "PA_OUTPATIENT":` and also change `elif review_type == "PA_PREAUTH":` if needed — but since PA_PREAUTH cards display identically to PA_OUTPATIENT cards, also add it to the `elif`:)

Change:
```python
                    elif review_type == "PA_OUTPATIENT":
```
to:
```python
                    elif review_type in ("PA_OUTPATIENT", "PA_PREAUTH"):
```

- [ ] **Step 2: Update badge label for PA_PREAUTH cards in the Agent Review tab**

Inside the `elif review_type in ("PA_OUTPATIENT", "PA_PREAUTH"):` block, find the badge:
```python
                        st.markdown(
                            f'<span style="background:#4c1d95;color:#ddd6fe;border-radius:4px;'
                            f'padding:2px 8px;font-size:0.8em;">💊 PA NO-AUTH</span>',
                            unsafe_allow_html=True,
                        )
```

Replace with:
```python
                        _rv_badge  = "💊 PA PRE-AUTH" if review_type == "PA_PREAUTH" else "💊 PA NO-AUTH"
                        st.markdown(
                            f'<span style="background:#4c1d95;color:#ddd6fe;border-radius:4px;'
                            f'padding:2px 8px;font-size:0.8em;">{_rv_badge}</span>',
                            unsafe_allow_html=True,
                        )
```

- [ ] **Step 3: Add Approve/Deny buttons for PA_ADMISSION in the `with rc2:` block**

Find (around line 1258):
```python
                    if review_type == "PA_OUTPATIENT":
                        if st.button("✅ Agree with AI", ...
```

Add `PA_ADMISSION` branch before it:
```python
                    if review_type == "PA_ADMISSION":
                        if st.button("✅ Approve Admission", key=f"rv_app_{rid}",
                                     use_container_width=True, type="primary"):
                            _submit_review(rid, "APPROVE", agent, notes)
                            # Mark admission as approved in session state if review_id matches
                            if st.session_state.get("admission_review_id") == rid:
                                st.session_state["admission_approved"] = True
                            st.rerun()
                        if st.button("❌ Deny Admission", key=f"rv_den_{rid}",
                                     use_container_width=True):
                            _submit_review(rid, "DENY", agent, notes)
                            st.rerun()
                    elif review_type in ("PA_OUTPATIENT", "PA_PREAUTH"):
```

(Change the existing `if review_type == "PA_OUTPATIENT":` to `elif review_type in ("PA_OUTPATIENT", "PA_PREAUTH"):`)

- [ ] **Step 4: Restart Streamlit and verify Agent Review renders PA_ADMISSION cards**

```bash
pkill -f "streamlit run streamlit_vetting" 2>/dev/null; sleep 1
nohup venv/bin/streamlit run streamlit_vetting/klaire_app.py --server.port 8501 > /tmp/streamlit.log 2>&1 &
sleep 4 && echo "Streamlit restarted"
```

Test manually: submit an admission request → go to Agent Review tab → verify card shows room type, days, diagnoses, AI advisory, Approve/Deny buttons.

- [ ] **Step 5: Commit**

```bash
git add streamlit_vetting/klaire_app.py
git commit -m "feat: add PA_ADMISSION review card and PA_PREAUTH badge to Agent Review tab"
```

---

## Task 6: Streamlit — injection_check advisory display in PA results

**Files:**
- Modify: `streamlit_vetting/klaire_app.py`

- [ ] **Step 1: Add injection_check banner inside `_render_pa_item`**

Find in `_render_pa_item` (around line 300):
```python
    quantity     = proc_res.get("quantity", 1)
    adjusted_qty = proc_res.get("adjusted_qty", quantity)
    max_qty      = proc_res.get("max_qty")
    qty_adjusted = proc_res.get("qty_adjusted", False)
    qty_reason   = proc_res.get("qty_reason", "")
```

Add after this block:
```python
    injection_check = proc_res.get("injection_check", {})
```

Then find the block that renders quantity (around line 357):
```python
        # Quantity check result
        if qty_adjusted:
```

After the quantity block (after the `else: st.caption(f"📦 Quantity: {quantity}")` line), add:

```python
        # Rule 12 — injection advisory
        inj = injection_check
        if inj.get("triggered"):
            if inj.get("justified"):
                st.markdown(
                    f'<div style="background:#052e16;border:1px solid #16a34a;border-radius:6px;'
                    f'padding:6px 12px;margin-top:6px;">'
                    f'<span style="color:#4ade80;font-size:0.85em;">💉 Injection-Without-Admission: '
                    f'Diagnosis justifies direct parenteral route ({inj.get("confidence",0)}%)</span><br>'
                    f'<span style="color:#86efac;font-size:0.82em;">{inj.get("reasoning","")}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div style="background:#1c0f00;border:1px solid #d97706;border-radius:6px;'
                    f'padding:6px 12px;margin-top:6px;">'
                    f'<span style="color:#fcd34d;font-size:0.85em;">💉 Injection-Without-Admission: '
                    f'Oral alternative not documented — agent to verify ({inj.get("confidence",0)}%)</span><br>'
                    f'<span style="color:#fde68a;font-size:0.82em;">{inj.get("reasoning","")}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
```

- [ ] **Step 2: Restart Streamlit and test end-to-end**

```bash
pkill -f "streamlit run streamlit_vetting" 2>/dev/null; sleep 1
nohup venv/bin/streamlit run streamlit_vetting/klaire_app.py --server.port 8501 > /tmp/streamlit.log 2>&1 &
sleep 4 && echo "Streamlit restarted"
```

Manual test — Pre-Auth, Not Admitted, with an IV procedure (e.g. "IV Artemether"):
- Verify Rule 12 banner appears on the PA result card
- Verify the overall decision is `PENDING_REVIEW`
- Verify Agent Review tab shows the `💊 PA PRE-AUTH` badge

- [ ] **Step 3: Commit**

```bash
git add streamlit_vetting/klaire_app.py
git commit -m "feat: display Rule 12 injection advisory banner in PA result cards"
```

---

## Self-Review Checklist

- [x] **Spec coverage:**
  - All decisions agent-driven → Task 2 forces PENDING_REVIEW for INPATIENT
  - IV/IM/infusions flagged for non-admitted → Task 1+2 Rule 12
  - Admission codes ADM01/02/03 → Task 3 `ADMISSION_CODES` constant + endpoint
  - Admission gate before procedures → Task 4 `_render_admission_form` + session state
  - Admission must be approved before proceeding → Task 4 `st.stop()` gate
  - Agent Review PA_ADMISSION card → Task 5
  - PA_PREAUTH badge (not PA_OUTPATIENT) → Task 5
  - Injection justified/not-justified advisory banner → Task 6
- [x] **No placeholders** — all code blocks are complete
- [x] **Type consistency** — `admission_status` is `str` throughout; `_is_injection_procedure(proc_name, proc_class)` matches usage in `check_injection_without_admission`
- [x] **`_call_claude` import** — used in the new admission endpoint via `from .klaire_pa import _call_claude`
