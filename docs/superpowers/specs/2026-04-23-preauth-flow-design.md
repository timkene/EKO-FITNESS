# Pre-Auth Flow Design — KLAIRE PA Vetting System
**Date:** 2026-04-23  
**Status:** Approved by product owner

---

## Overview

Pre-Auth (INPATIENT) is a new encounter type alongside No-Auth (OUTPATIENT). It shares the same rules engine and procedure/diagnosis framework but differs in three critical ways:

1. **All decisions are agent-driven.** AI advises only — no auto-approve, no auto-deny, even with trusted learning data. Every result surfaces as `PENDING_REVIEW`.
2. **Two distinct sub-paths** based on whether the enrollee is being admitted.
3. **Injection-without-admission check** (Rule 12) flags IV/IM prescriptions for non-admitted patients who haven't tried oral alternatives.

---

## Two Sub-Paths

### Path A — Admission

The enrollee is being admitted to the facility. Sequence:

1. **Agent selects room type + number of days**
   - ADM01 — Private Room
   - ADM02 — Semi-Private Room
   - ADM03 — General Room
   - Quantity = number of days
2. **Admission request sent for agent approval** — AI advises on appropriateness of admission vs diagnosis; agent approves or denies. Procedures cannot be submitted until this step is approved.
3. **Procedures & Diagnoses entered** — same UI as No-Auth (`PA Request` tab). All procedures treated as under admission; AI clinical reasoning tuned for inpatient complexity.
4. **Agent reviews each procedure** — AI advises, agent decides every item.

IV/IM/infusions are permitted freely on admission (patient is physically in-facility; daily injection visits are normal for admitted patients).

### Path B — Not Admitted

The enrollee is not being admitted but requires complex/expensive procedures (e.g. monthly IV infusions, high-cost diagnostics). Sequence:

1. **Procedures & Diagnoses entered** — same form as No-Auth.
2. **Rule 12 (Injection-Without-Admission)** fires for any IV/IM/infusion procedure — AI assesses whether oral/tablet alternatives were tried first. Exception: diagnosis is clinically critical (AI determines criticality from ICD-10 context). If flagged, shown as a prominent advisory on the review card.
3. **Agent reviews each procedure** — injection flag shown prominently; agent decides.

---

## Agent-Driven Decision Model

In Pre-Auth, the system **never** auto-decides regardless of:
- Master table hits
- Trusted learning entries (usage_count ≥ 3, admin_approved = True)
- Any combination of passing rule results

All results are forced to `PENDING_REVIEW`. Learning data and master table results are presented as AI reasoning/advice to help the agent decide faster — not as a final outcome.

In `klaire_pa.py`, when `encounter_type == "INPATIENT"`, override any `APPROVE` or `DENY` result to `PENDING_REVIEW` before returning, appending `"Pre-Auth: agent decision required"` to `review_reasons`.

---

## Rule 12 — Injection Without Admission

**Triggers:** Encounter is INPATIENT + sub-path is Not Admitted + procedure is IV/IM/infusion/ampoule.

**Detection:** Procedure class or name contains keywords: `IV`, `INFUSION`, `INJECTION`, `AMPOULE`, `IM `, `INJ`, `I.M.`, `I.V.`, `IV`, `IM`, `AMP`, `INF`. (Case-insensitive. Applied to `procedure_name` and `procedure_class` from PROCEDURE_MASTER.)

**AI assessment:** Given the procedure and all diagnoses in the request, Claude evaluates:
- Is there an oral/tablet equivalent that should have been tried first?
- Is the diagnosis critical enough to justify skipping oral step? (e.g. severe malaria, sepsis, status epilepticus)

**Outcome:** Always `PENDING_REVIEW`. Advisory shown on agent review card:
- 🟠 "Injection-Without-Admission: oral alternative not documented — agent to verify"
- ✅ "Injection-Without-Admission: diagnosis justifies direct parenteral route"

Rule 12 never auto-denies — it informs the agent.

---

## Admission Codes

Stored as a static lookup in the API (no separate DB collection needed initially):

| Code  | Name              |
|-------|-------------------|
| ADM01 | Private Room      |
| ADM02 | Semi-Private Room |
| ADM03 | General Room      |

Admission request payload:
```json
{
  "enrollee_id": "CL/OCTA/723449/2023-A",
  "provider_id": "118",
  "hospital_name": "General Hospital",
  "encounter_date": "2026-04-23",
  "admission_code": "ADM02",
  "days": 3,
  "admitting_diagnosis_codes": ["A01", "J189"]
}
```

Admission review type in `klaire_reviews`: `"PA_ADMISSION"`.

---

## UI Changes (klaire_app.py)

### PA Request tab — Pre-Auth sub-flow

When `enc_type == "INPATIENT"` is selected, show a new step **before** the procedure/diagnosis form:

```
Is the enrollee being admitted?
  [✅ Yes — Admission]    [❌ No — Not Admitted]
```

**If Admitted:**
- Room type selector (ADM01/ADM02/ADM03)
- Number of days (quantity, min 1)
- Admitting diagnoses multiselect (reuse `load_diagnoses()`)
- "Submit Admission Request" button → `POST /api/v1/klaire/admission`
- Result shown as pending card — user must wait for agent approval in Agent Review tab
- After approval: unlock procedures/diagnoses form (store `admission_approved: True` in session state)

**If Not Admitted:**
- Proceed directly to procedures/diagnoses form (no gate)
- Session state: `admission_status: "NOT_ADMITTED"`

### Agent Review tab

New review card type `PA_ADMISSION` displayed alongside existing `PA_OUTPATIENT` and `SPECIALIST` cards:
- Badge: `🏥 PA ADMISSION`
- Shows: room type, days, admitting diagnoses, AI advisory, Agree/Override buttons
- Buttons: `✅ Approve Admission` / `❌ Deny Admission`

Existing `PA_OUTPATIENT` review cards in the agent queue get renamed badge to `💊 PA NO-AUTH`.
Pre-Auth procedure review cards use badge `💊 PA PRE-AUTH`.

---

## API Changes (main.py)

### New endpoint
```
POST /api/v1/klaire/admission
```
- Validates admission code, days, diagnoses
- Calls AI for admission appropriateness advisory (Claude: "Is this diagnosis appropriate for inpatient admission given the room type and expected stay?")
- Inserts `klaire_reviews` doc with `review_type: "PA_ADMISSION"`
- Returns `review_id` to frontend

### Existing PA endpoint
```
POST /api/v1/klaire/pa
```
- Accepts `encounter_type: "INPATIENT"` and new field `admission_status: "ADMITTED" | "NOT_ADMITTED"`
- `admission_approved_id` (optional) — links to the approved admission review doc
- For INPATIENT: after all rule results computed, force all non-`DENY` results to `PENDING_REVIEW`
- Rule 12 injected into `ComprehensiveVettingEngine` for INPATIENT + NOT_ADMITTED cases

### New endpoint for admission codes
```
GET /api/v1/klaire/admission-codes
```
Returns the three ADM codes — allows frontend to load them dynamically same as specialist codes.

---

## Data Flow

```
Agent selects Pre-Auth
  → Is admitted?
      YES → submit admission → klaire_reviews (PA_ADMISSION) → agent approves
            → unlock PA form → submit procedures → klaire_reviews (PA_PREAUTH) per procedure
      NO  → submit procedures → Rule 12 fires for IV/IM → klaire_reviews (PA_PREAUTH) per procedure
```

All `PA_PREAUTH` review items: AI reasoning shown as advisory. Agent has `✅ Agree with AI` / `❌ Override AI` same as existing No-Auth flow.

---

## What Does NOT Change

- Rules 1–11 all run the same way — pre-auth just forces PENDING_REVIEW on exit
- Learning tables still record after agent decisions (learning still happens)
- Disease combo (Rule 10) and procedure combo (Rule 11) banners still appear
- No-Auth flow is untouched

---

## Out of Scope (this phase)

- Restricting No-Auth to a specific procedure subset table (user noted for later)
- Pre-Auth procedure code restrictions (IV/IM restricted to pre-auth only — enforcement deferred; current phase only flags via Rule 12)
