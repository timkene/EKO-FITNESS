"""
KLAIRE — Clearline Contact Centre Agent
Consultation pre-authorisation workflow — step-by-step wizard.
"""

import os
import streamlit as st
import requests
import pandas as pd
from datetime import date

API  = "http://localhost:8000"
BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

st.set_page_config(page_title="KLAIRE", page_icon="🤖", layout="wide")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background:#0f172a; }
[data-testid="stSidebar"]           { background:#1e293b; }
section[data-testid="stSidebar"] *  { color:#cbd5e1 !important; }

h1,h2,h3,h4 { color:#f1f5f9 !important; }
p, label, .stCaption, div[data-testid="stText"] { color:#cbd5e1 !important; }

div[data-baseweb="select"] > div   { background:#1e293b !important; border-color:#334155 !important; color:#f1f5f9 !important; }
div[data-baseweb="select"] span    { color:#f1f5f9 !important; }
div[data-baseweb="popover"] *      { background:#1e293b !important; color:#f1f5f9 !important; }
div[data-testid="stMultiSelect"] * { color:#f1f5f9 !important; }
input, textarea                    { background:#1e293b !important; color:#f1f5f9 !important; border-color:#334155 !important; }

.type-btn {
    display:block; width:100%; padding:22px 16px; border-radius:12px;
    text-align:center; font-size:1.15em; font-weight:700; cursor:pointer;
    border:2px solid; margin-bottom:8px; transition:all 0.15s;
}
.gp-btn    { background:#0c2340; border-color:#3b82f6; color:#93c5fd; }
.spec-btn  { background:#1c0f00; border-color:#d97706; color:#fcd34d; }

.section-card {
    background:#1e293b; border:1px solid #334155;
    border-radius:12px; padding:20px 24px; margin-bottom:16px;
}
.step-card  { background:#0f172a; border:1px solid #1e293b;
              border-radius:8px; padding:12px 16px; margin-bottom:8px; }
.step-pass  { border-left:5px solid #22c55e; }
.step-fail  { border-left:5px solid #ef4444; }
.step-change{ border-left:5px solid #f59e0b; }
.step-info  { border-left:5px solid #3b82f6; }

.verdict    { border-radius:12px; padding:22px 26px; margin-top:16px; }
.v-approve  { background:#052e16; border:1px solid #16a34a; }
.v-deny     { background:#2d0a0a; border:1px solid #dc2626; }
.v-change   { background:#1c0f00; border:1px solid #d97706; }

.badge { display:inline-block; padding:4px 16px; border-radius:20px; font-weight:700; font-size:0.82em; }
.b-approve { background:#166534; color:#bbf7d0; }
.b-deny    { background:#7f1d1d; color:#fecaca; }
.b-change  { background:#78350f; color:#fde68a; }
.b-qa      { background:#1e3a5f; color:#bae6fd; }

.divider-line { border-top:1px solid #1e293b; margin:20px 0; }
</style>
""", unsafe_allow_html=True)


# ── Cached data loaders ───────────────────────────────────────────────────────
@st.cache_data(ttl=86400)
def load_symptom_codes():
    path = os.path.join(BASE, "ICD10_Symptom_Codes_R00_R99.xlsx")
    df   = pd.read_excel(path, header=1)
    df.columns = ["code", "description", "section", "body_system"]
    df = df[df["code"].astype(str).str.match(r"^R\d", na=False)].copy()
    df["display"] = df["code"] + " — " + df["description"]
    # Group by body system
    groups = {}
    for sys, grp in df.groupby("body_system"):
        groups[sys] = grp["display"].tolist()
    return groups, df["display"].tolist()


@st.cache_data(ttl=3600)
def load_procedures():
    """Returns {display_label: {code, name, branch}} for PA procedure dropdown.

    branch is 'NO-AUTH', 'PRE-AUTH', or None (not in master → treated as PRE-AUTH).
    """
    try:
        r = requests.get(f"{API}/api/v1/klaire/procedures", timeout=10)
        r.raise_for_status()
        procs = r.json().get("procedures", [])
        result = {}
        for p in procs:
            code   = p.get("procedure_code", "")
            name   = p.get("procedure_name", code)
            branch = p.get("branch")  # NO-AUTH | PRE-AUTH | None
            cls    = p.get("procedure_class", "")
            label  = f"{code} — {name}" + (f"  [{cls}]" if cls else "")
            result[label] = {"code": code, "name": name, "branch": branch}
        return result
    except Exception:
        return {}


def _filter_procedures(enc_type: str, adm_status: str | None) -> dict:
    """Return filtered procedure map based on encounter type and admission status.

    Rules:
    - OUTPATIENT (No-Auth):           only branch == NO-AUTH
    - INPATIENT + NOT_ADMITTED:       branch == PRE-AUTH or branch is None (not in master); excludes NO-AUTH
    - INPATIENT + ADMITTED:           all procedures (no filter)
    """
    all_procs = load_procedures()
    if enc_type == "OUTPATIENT":
        return {k: v for k, v in all_procs.items() if v.get("branch") == "NO-AUTH"}
    if enc_type == "INPATIENT" and adm_status != "ADMITTED":
        return {k: v for k, v in all_procs.items() if v.get("branch") != "NO-AUTH"}
    return all_procs  # ADMITTED: all


@st.cache_data(ttl=3600)
def load_specialist_codes():
    try:
        r = requests.get(f"{API}/api/v1/klaire/consultation-codes", timeout=10)
        r.raise_for_status()
        data = r.json()
        initials = [s for s in data.get("specialists", [])
                    if str(s.get("type", "")).upper() == "INITIAL"]
        return {f"{s['code']} — {s['name']}": s['code'] for s in initials}
    except Exception:
        return {}


@st.cache_data(ttl=3600)
def load_diagnoses():
    try:
        r = requests.get(f"{API}/api/v1/klaire/diagnoses", timeout=10)
        r.raise_for_status()
        rows = r.json().get("diagnoses", [])
        return {f"{d['code']} — {d['name']}": {"code": d["code"], "name": d["name"]} for d in rows}
    except Exception:
        return {}


@st.cache_data(ttl=86400)
def load_admission_codes():
    """Returns list of {code, name} for admission room type selector."""
    try:
        r = requests.get(f"{API}/api/v1/klaire/admission-codes", timeout=10)
        r.raise_for_status()
        return r.json().get("codes", [])
    except Exception:
        return [
            {"code": "ADM01", "name": "Private Room"},
            {"code": "ADM02", "name": "Semi-Private Room"},
            {"code": "ADM03", "name": "General Room"},
        ]


@st.cache_data(ttl=3600)
def _proc_code_to_name() -> dict:
    """code → name lookup for learning review display."""
    return {v["code"]: v["name"] for v in load_procedures().values()}


@st.cache_data(ttl=3600)
def _diag_code_to_name() -> dict:
    """code → name lookup from the full DIAGNOSIS table (broader than DIAGNOSIS_MASTER)."""
    try:
        r = requests.get(f"{API}/api/v1/klaire/all-diagnoses", timeout=15)
        if r.ok:
            return {d["code"]: d["name"] for d in r.json().get("diagnoses", [])}
    except Exception:
        pass
    return {v["code"]: v["name"] for v in load_diagnoses().values()}


# ── Shared result renderer ────────────────────────────────────────────────────
def render_result(result: dict):
    decision = result.get("decision", "")
    steps    = result.get("steps", [])

    st.markdown("---")
    st.markdown("#### Decision Trail")
    for s in steps:
        r    = (s.get("result") or "INFO").upper()
        css  = {"PASS":"step-pass","FAIL":"step-fail","CHANGE":"step-change","INFO":"step-info"}.get(r,"step-info")
        icon = {"PASS":"✅","FAIL":"❌","CHANGE":"🔄","INFO":"ℹ️"}.get(r,"•")
        st.markdown(
            f'<div class="step-card {css}">'
            f'<strong>Step {s["step"]} &nbsp;·&nbsp; {s["name"]}</strong>'
            f'&nbsp;&nbsp;{icon} <strong style="font-size:0.82em;">{r}</strong><br>'
            f'<span style="font-size:0.88em;color:#94a3b8;margin-top:4px;display:block;">{s["details"]}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("#### KLAIRE Decision")

    if decision == "APPROVE":
        code = result.get("approved_code","")
        name = result.get("approved_code_name","")
        st.markdown(
            f'<div class="verdict v-approve">'
            f'<span class="badge b-approve">✅ &nbsp;APPROVED</span>'
            f'<h3 style="color:#4ade80;margin:12px 0 4px 0;">{code}</h3>'
            f'<p style="color:#86efac;margin:0;">{name}</p>'
            f'<p style="color:#6ee7b7;margin:8px 0 0 0;font-size:0.9em;">Proceed with this consultation code.</p>'
            f'</div>', unsafe_allow_html=True)
        if result.get("qa_flag"):
            st.markdown(
                f'<div style="background:#0c2340;border:1px solid #3b82f6;border-radius:8px;padding:12px 16px;margin-top:10px;">'
                f'<span class="badge b-qa">📋 &nbsp;QA FLAG</span>'
                f'<p style="color:#7dd3fc;margin:8px 0 0 0;font-size:0.9em;">{result.get("qa_reason","")}</p>'
                f'</div>', unsafe_allow_html=True)

    elif decision == "DENY":
        last  = steps[-1] if steps else {}
        reason = last.get("details","Request rejected.")
        st.markdown(
            f'<div class="verdict v-deny">'
            f'<span class="badge b-deny">❌ &nbsp;DENIED</span>'
            f'<h3 style="color:#f87171;margin:12px 0 4px 0;">Request Cannot Be Approved</h3>'
            f'<p style="color:#fca5a5;margin:0;font-size:0.9em;">{reason}</p>'
            f'</div>', unsafe_allow_html=True)

    elif decision == "CHANGE":
        orig = result.get("approved_code","")
        new  = result.get("change_to_code","")
        name = result.get("change_to_name", new)
        why  = result.get("change_reason","")
        st.markdown(
            f'<div class="verdict v-change">'
            f'<span class="badge b-change">🔄 &nbsp;CODE CHANGED</span>'
            f'<h3 style="color:#fbbf24;margin:12px 0 4px 0;">{orig} &nbsp;→&nbsp; {new}</h3>'
            f'<p style="color:#fde68a;margin:0;">{name}</p>'
            f'<p style="color:#fcd34d;margin:8px 0 0 0;font-size:0.9em;">{why}</p>'
            f'</div>', unsafe_allow_html=True)

    elif decision == "PENDING_REVIEW":
        ai_rec  = result.get("ai_recommendation","")
        ai_conf = result.get("ai_confidence", 0)
        ai_why  = result.get("ai_reasoning","")
        src     = result.get("learning_source","ai")
        src_label = "Learning table" if src == "learning_table" else "AI (first evaluation)"
        badge_color = "#166534" if ai_rec == "APPROVE" else "#7f1d1d"
        text_color  = "#bbf7d0" if ai_rec == "APPROVE" else "#fecaca"
        st.markdown(
            f'<div style="background:#1a1a2e;border:1px solid #7c3aed;border-radius:12px;padding:22px 26px;margin-top:16px;">'
            f'<span class="badge" style="background:#4c1d95;color:#ddd6fe;">🔍 &nbsp;PENDING AGENT REVIEW</span>'
            f'<h3 style="color:#a78bfa;margin:12px 0 4px 0;">Specialist-Diagnosis Compatibility Check</h3>'
            f'<p style="color:#c4b5fd;margin:0 0 12px 0;">Source: {src_label}</p>'
            f'<div style="background:{badge_color};border-radius:8px;padding:10px 14px;margin-bottom:10px;">'
            f'<strong style="color:{text_color};">AI Recommendation: {ai_rec} ({ai_conf}% confidence)</strong><br>'
            f'<span style="color:{text_color};font-size:0.9em;opacity:0.9;">{ai_why}</span>'
            f'</div>'
            f'<p style="color:#a78bfa;margin:0;font-size:0.88em;">An agent must confirm or override this recommendation below.</p>'
            f'</div>',
            unsafe_allow_html=True,
        )

        review_id = result.get("review_id","")
        if review_id:
            st.markdown("**Agent Action — Specialist-Diagnosis Compatibility:**")
            agent_name = st.text_input("Your name", value="Agent", key=f"agent_name_{review_id}")
            notes      = st.text_area("Notes (optional)", height=70, key=f"notes_{review_id}",
                                       placeholder="Add clinical justification or override reason…")
            ca, cd, _ = st.columns([1, 1, 3])
            with ca:
                if st.button("✅ Approve", type="primary", key=f"approve_{review_id}", use_container_width=True):
                    _submit_review(review_id, "APPROVE", agent_name, notes)
            with cd:
                if st.button("❌ Deny", key=f"deny_{review_id}", use_container_width=True):
                    _submit_review(review_id, "DENY", agent_name, notes)

    with st.expander("View raw API response"):
        st.json(result)


def _submit_review(review_id: str, action: str, reviewed_by: str, notes: str):
    try:
        resp = requests.post(
            f"{API}/api/v1/klaire/review/{review_id}",
            json={"action": action, "reviewed_by": reviewed_by, "notes": notes},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if action in ("APPROVE", "AGREE"):
            st.success(f"✅ {data.get('message', 'Agreed with AI — learning recorded.')}")
        else:
            st.warning(f"⚠️ {data.get('message', 'AI overridden — decision recorded.')}")

        # Mirror the decision back into pa_result so PA Request tab shows updated status
        if "pa_result" in st.session_state:
            pa    = st.session_state["pa_result"]
            items = pa.get("items", [])
            new_d = "APPROVED_BY_AGENT" if action in ("APPROVE", "AGREE") else "DENIED_BY_AGENT"
            for item in items:
                if item.get("review_id") == review_id:
                    item["decision"] = new_d
            # Recalculate overall
            decisions = [i.get("decision", "") for i in items]
            terminal  = {"APPROVE", "APPROVED_BY_AGENT", "DENY", "DENIED_BY_AGENT"}
            approved_set = {"APPROVE", "APPROVED_BY_AGENT"}
            denied_set   = {"DENY", "DENIED_BY_AGENT"}
            if all(d in approved_set for d in decisions):
                pa["overall_decision"] = "APPROVE"
            elif all(d in denied_set for d in decisions):
                pa["overall_decision"] = "DENY"
            elif any(d == "PENDING_REVIEW" for d in decisions):
                pa["overall_decision"] = "PENDING_REVIEW"
            else:
                pa["overall_decision"] = "PARTIAL"
            st.session_state["pa_result"] = pa
    except Exception as e:
        st.error(f"Review submission failed: {e}")


def _render_pa_item(proc_res: dict, encounter_type: str = "OUTPATIENT"):
    """Render one procedure's PA result card."""
    decision    = proc_res.get("decision", "")
    proc_code   = proc_res.get("procedure_code", "")
    proc_name   = proc_res.get("procedure_name", proc_code)
    approved    = proc_res.get("approved_diagnoses", [])
    denied      = proc_res.get("denied_diagnoses", [])
    diag_detail = proc_res.get("diag_detail", {})
    diag_names  = proc_res.get("diag_names", {})
    proc_rules  = proc_res.get("proc_rules", [])
    first_line  = proc_res.get("first_line", {})
    review_reasons = proc_res.get("review_reasons", [])
    review_id   = proc_res.get("review_id")
    quantity     = proc_res.get("quantity", 1)
    adjusted_qty = proc_res.get("adjusted_qty", quantity)
    max_qty      = proc_res.get("max_qty")
    qty_adjusted = proc_res.get("qty_adjusted", False)
    qty_reason   = proc_res.get("qty_reason", "")

    color_map = {
        "APPROVE":             ("#052e16", "#22c55e", "#4ade80", "✅ APPROVED"),
        "DENY":                ("#2d0a0a", "#dc2626", "#f87171", "❌ DENIED"),
        "PENDING_REVIEW":      ("#1a1a2e", "#7c3aed", "#a78bfa", "🔍 PENDING REVIEW"),
        "APPROVED_BY_AGENT":   ("#052e16", "#16a34a", "#86efac", "✅ APPROVED BY AGENT"),
        "DENIED_BY_AGENT":     ("#2d0a0a", "#b91c1c", "#fca5a5", "❌ DENIED BY AGENT"),
    }
    bg, border, text, label = color_map.get(decision, ("#0f172a", "#334155", "#f1f5f9", decision))

    with st.container(border=True):
        st.markdown(
            f'<div style="border-left:4px solid {border};padding:4px 12px;margin-bottom:8px;">'
            f'<strong style="color:{text};">{label}</strong>'
            f'<span style="color:#94a3b8;margin-left:10px;font-size:0.9em;">{proc_code}</span>'
            f'<span style="color:#cbd5e1;margin-left:6px;">{proc_name}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Approved diagnoses (green pills)
        if approved:
            pills = " ".join(
                f'<span style="background:#166534;color:#bbf7d0;border-radius:16px;padding:3px 10px;'
                f'font-size:0.82em;margin-right:4px;">'
                f'✅ {c} — {diag_detail.get(c,{}).get("name", diag_names.get(c,c))}</span>'
                for c in approved
            )
            st.markdown(f"**Approved diagnoses:** {pills}", unsafe_allow_html=True)

        # Denied diagnoses (red pills)
        if denied:
            pills = " ".join(
                f'<span style="background:#7f1d1d;color:#fecaca;border-radius:16px;padding:3px 10px;'
                f'font-size:0.82em;margin-right:4px;">'
                f'❌ {c} — {diag_detail.get(c,{}).get("name", diag_names.get(c,c))}</span>'
                for c in denied
            )
            st.markdown(f"**Delisted diagnoses:** {pills}", unsafe_allow_html=True)

        # First-line check
        fl_dec   = first_line.get("decision", "")
        fl_color = "#22c55e" if fl_dec == "APPROVE" else "#ef4444"
        fl_src   = "Auto" if first_line.get("auto") else first_line.get("source", "ai").replace("_", " ").title()
        st.markdown(
            f'<span style="color:#94a3b8;font-size:0.88em;">First-line check: '
            f'<strong style="color:{fl_color};">{fl_dec}</strong>'
            f' ({first_line.get("confidence", 0)}% · {fl_src}) — '
            f'{first_line.get("reasoning","")}</span>',
            unsafe_allow_html=True,
        )

        # Quantity check result
        if qty_adjusted:
            st.markdown(
                f'<div style="background:#1c0f00;border:1px solid #d97706;border-radius:6px;'
                f'padding:6px 12px;margin-top:6px;">'
                f'<span style="color:#fcd34d;font-size:0.85em;">📦 Quantity adjusted: '
                f'{quantity} → <strong>{adjusted_qty}</strong> (max allowed: {max_qty})'
                + (f' — {qty_reason}' if qty_reason else '') +
                f'</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.caption(f"📦 Quantity: {quantity}")

        # Rule 12 — injection-without-admission advisory
        injection_check = proc_res.get("injection_check", {})
        if injection_check.get("triggered"):
            if injection_check.get("justified"):
                st.markdown(
                    '<div style="background:#052e16;border-left:4px solid #16a34a;'
                    'border-radius:6px;padding:8px 14px;margin-top:6px;">'
                    '<span style="color:#bbf7d0;font-size:0.88em;">'
                    '✅ <strong>Injection-Without-Admission:</strong> diagnosis justifies direct parenteral route'
                    f'<br><span style="color:#86efac;font-size:0.82em;">{injection_check.get("reasoning","")}</span>'
                    '</span></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div style="background:#431407;border-left:4px solid #ea580c;'
                    'border-radius:6px;padding:8px 14px;margin-top:6px;">'
                    '<span style="color:#fed7aa;font-size:0.88em;">'
                    '🟠 <strong>Injection-Without-Admission:</strong> oral alternative not documented — agent to verify'
                    f'<br><span style="color:#fde8d8;font-size:0.82em;">{injection_check.get("reasoning","")}</span>'
                    '</span></div>',
                    unsafe_allow_html=True,
                )

        # Review reasons
        if review_reasons:
            for rr in review_reasons:
                st.caption(f"⚠️ {rr}")

        # Rule breakdown (collapsible)
        all_rules = proc_rules[:]
        for d_info in diag_detail.values():
            all_rules += d_info.get("rules", [])

        if all_rules:
            with st.expander("Rule breakdown"):
                for r in all_rules:
                    icon   = "✅" if r["passed"] else "❌"
                    source = r.get("source", "")
                    st.markdown(
                        f"{icon} **{r['rule_name']}** "
                        f"<span style='color:#64748b;font-size:0.85em;'>({source}, {r.get('confidence',0)}%)</span>"
                        f"<br><span style='color:#94a3b8;font-size:0.88em;'>{r.get('reasoning','')}</span>",
                        unsafe_allow_html=True,
                    )

        # Agent action for PENDING_REVIEW
        if decision == "PENDING_REVIEW" and review_id:
            st.markdown("---")
            escalated_by    = proc_res.get("escalated_by", "")
            ai_rec          = proc_res.get("ai_recommendation", "APPROVE")
            combo_flag_reason = proc_res.get("combo_flag_reason", "")
            is_combo_flag   = escalated_by in ("combo_check", "disease_combo_check")

            if is_combo_flag and ai_rec == "DENY":
                # AI recommends DENY based on combination check — show this prominently
                st.markdown(
                    f'<div style="background:#2d1b1b;border-left:3px solid #ef4444;border-radius:6px;padding:10px 14px;margin-bottom:8px;">'
                    f'<span style="color:#fca5a5;font-size:0.85em;font-weight:700;">🤖 AI Recommendation: DENY</span><br>'
                    f'<span style="color:#fecaca;font-size:0.82em;">{combo_flag_reason}</span><br><br>'
                    f'<span style="color:#fca5a5;font-size:0.82em;">'
                    f'✅ <strong>Agree (Deny)</strong> — Remove this procedure from the approved list.<br>'
                    f'❌ <strong>Override (Approve)</strong> — Approve it anyway. Provide a clinical justification below.'
                    f'</span></div>',
                    unsafe_allow_html=True,
                )
                agree_label    = "✅ Agree (Deny)"
                override_label = "❌ Override (Approve)"
            else:
                st.markdown(
                    '<div style="background:#0f2a3a;border-left:3px solid #38bdf8;border-radius:6px;padding:10px 14px;margin-bottom:8px;">'
                    '<span style="color:#7dd3fc;font-size:0.85em;font-weight:700;">How to decide:</span><br>'
                    '<span style="color:#bae6fd;font-size:0.82em;">'
                    '✅ <strong>Agree with AI</strong> — AI\'s individual rule results are correct. '
                    'Each compatibility/age/gender match is learnt exactly as AI assessed. '
                    'The request outcome follows the AI recommendation.<br>'
                    '❌ <strong>Override AI</strong> — You disagree with the AI. '
                    'Your decision overrides and is recorded instead.'
                    '</span></div>',
                    unsafe_allow_html=True,
                )
                agree_label    = "✅ Agree with AI"
                override_label = "❌ Override AI"

            a_name  = st.text_input("Agent name", value="Agent", key=f"pa_agent_{review_id}", label_visibility="collapsed")
            a_notes = st.text_input("Override reason (required if overriding)", placeholder="e.g. Patient has recurrent URTI — investigation warranted", key=f"pa_notes_{review_id}")
            ac1, ac2, _ = st.columns([1, 1, 3])
            with ac1:
                if st.button(agree_label, key=f"pa_app_{review_id}", use_container_width=True, type="primary"):
                    _submit_review(review_id, "AGREE", a_name, a_notes)
            with ac2:
                if st.button(override_label, key=f"pa_den_{review_id}", use_container_width=True):
                    _submit_review(review_id, "OVERRIDE", a_name, a_notes)


def call_api(payload: dict):
    try:
        resp = requests.post(f"{API}/api/v1/klaire/consult", json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json(), None
    except Exception as e:
        return None, str(e)


# ── Header ────────────────────────────────────────────────────────────────────
hc1, hc2 = st.columns([5, 1])
with hc1:
    st.markdown("# 🤖 KLAIRE")
    st.caption("Clearline International · Contact Centre · Consultation Pre-Authorisation")
with hc2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 Start Over", use_container_width=True):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

st.divider()


# ── Sidebar — enrollee + provider ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 👤 Enrollee & Provider")
    enrollee_id    = st.text_input("Enrollee ID *",       placeholder="CL/OCTA/723449/2023-A", key="s_enr")
    provider_id    = st.text_input("Provider ID *",       placeholder="118",                   key="s_prov")
    hospital_name  = st.text_input("Hospital / Facility", placeholder="General Hospital",      key="s_hosp")
    encounter_date = st.date_input("Encounter Date",      value=date.today(),                  key="s_date")
    st.divider()
    st.caption("Complete all fields before submitting.")

# ── Top-level mode selector ───────────────────────────────────────────────────
pending_count   = 0
learning_count  = 0
try:
    rc = requests.get(f"{API}/api/v1/klaire/reviews?limit=1", timeout=5)
    if rc.ok:
        pending_count = rc.json().get("total_pending", 0)
except Exception:
    pass
try:
    lc = requests.get(f"{API}/api/v1/klaire/all-learnings?limit=1", timeout=5)
    if lc.ok:
        learning_count = lc.json().get("total", 0)
except Exception:
    pass

review_label   = "🛡️  Agent Review"  + (f" ({pending_count})"  if pending_count  else "")
learning_label = "📚 Learning Review" + (f" ({learning_count})" if learning_count else "")

_mode_options = ["📋 New Request", "💊 PA Request", review_label, learning_label]

# Use a stable index so dynamic count badges don't break selection on rerun
if "klaire_mode_idx" not in st.session_state:
    st.session_state["klaire_mode_idx"] = 0

_selected = st.radio(
    "Mode",
    _mode_options,
    horizontal=True, label_visibility="collapsed",
    index=st.session_state["klaire_mode_idx"],
    key="klaire_mode",
)
st.session_state["klaire_mode_idx"] = _mode_options.index(_selected)
mode = _selected

if "Learning Review" in mode:
    # ══════════════════════════════════════════════════════════════════════════
    # ADMIN LEARNING REVIEW
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("### 📚 Learning Review")
    st.caption(
        "All new data the system has learnt across every learning table. "
        "An entry stays here until you **approve** it (trusted immediately), "
        "**delete** it, or the agent confirms it **3 or more times** (auto-trusted)."
    )

    if st.button("🔄 Refresh", key="refresh_learnings"):
        st.rerun()

    api_ok  = False
    entries = []
    total   = 0
    try:
        resp = requests.get(f"{API}/api/v1/klaire/all-learnings?limit=500", timeout=15)
        resp.raise_for_status()
        data    = resp.json()
        entries = data.get("entries", [])
        total   = data.get("total", 0)
        api_ok  = True
    except Exception as e:
        st.error(f"Could not load learning entries: {e}")
        st.info("Restart the API server if this is the first time after a code change.")

    if api_ok and not entries:
        st.info("No learning entries yet. Approve or agree with AI on a PA request to start learning.")
    else:
        total_all = data.get("total", len(entries))
        n_trusted = data.get("trusted", sum(1 for e in entries if e.get("_trusted")))
        n_pending = data.get("pending", total_all - n_trusted)

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Total Learned", total_all)
        mc2.metric("✅ Trusted", n_trusted)
        mc3.metric("⏳ Pending", n_pending)

        # Filter
        status_filter = st.radio(
            "Show", ["All", "Trusted", "Pending"],
            horizontal=True, key="lrn_filter"
        )
        if status_filter == "Trusted":
            entries = [e for e in entries if e.get("_trusted")]
        elif status_filter == "Pending":
            entries = [e for e in entries if not e.get("_trusted")]

        # Group by collection label
        from collections import defaultdict
        grouped: dict = defaultdict(list)
        for e in entries:
            grouped[e.get("_label", e.get("_collection", "Unknown"))].append(e)

        proc_names = _proc_code_to_name()
        diag_names = _diag_code_to_name()

        def _lrn_row(entry: dict, coll: str) -> dict:
            """Normalise a learning entry into display fields based on collection type."""
            pn  = proc_names
            dn  = diag_names
            pc  = entry.get("procedure_code", "") or entry.get("procedure_code_1", "")
            dc  = entry.get("diagnosis_code", "")
            sc  = entry.get("specialist_code", "")
            pc2 = entry.get("procedure_code_2", "")

            subj_code = pc or sc or dc   # primary code
            subj_name = (
                pn.get(pc, pc) if pc else
                dn.get(sc, sc) if sc else
                dn.get(dc, dc)
            )

            # qualifier column — varies by collection
            if coll in ("ai_human_procedure_age", "ai_human_diagnosis_age"):
                mn, mx = entry.get("min_age", ""), entry.get("max_age", "")
                qual_code = f"Age {mn}–{mx}" if (mn != "" or mx != "") else "—"
                qual_name = ""
            elif coll in ("ai_human_procedure_gender", "ai_human_diagnosis_gender"):
                qual_code = entry.get("gender", "—")
                qual_name = ""
            elif coll == "ai_human_procedure_class":
                qual_code = pc2
                qual_name = pn.get(pc2, pc2)
            elif coll == "ai_human_diagnosis_stacking":
                codes = entry.get("codes", [])
                qual_code = " + ".join(str(c) for c in codes[:3])
                qual_name = ""
                subj_code = ""
                subj_name = entry.get("combo_key", "")[:60]
            elif coll == "ai_human_specialist_diagnosis":
                qual_code = dc
                qual_name = dn.get(dc, dc)
            else:
                # procedure_diagnosis and anything else
                qual_code = dc
                qual_name = dn.get(dc, dc)

            # AI decision — field name varies by collection
            # Use explicit None checks so False values aren't swallowed by `or`
            for _fld in ("is_valid_match", "is_valid_for_age", "is_valid_for_gender", "is_valid"):
                if entry.get(_fld) is not None:
                    is_valid = entry[_fld]
                    break
            else:
                is_valid = None
            verdict  = entry.get("verdict", "")          # diagnosis_stacking uses verdict
            same_cls = entry.get("same_class")            # procedure_class
            conf     = entry.get("confidence") or entry.get("ai_confidence") or "—"

            if verdict:
                ai_ok    = verdict.upper() == "PLAUSIBLE"
                ai_label = ("✅ PLAUSIBLE" if ai_ok else "❌ IMPLAUSIBLE")
            elif same_cls is not None:
                ai_ok    = bool(same_cls)
                ai_label = ("✅ SAME CLASS" if ai_ok else "❌ DIFFERENT CLASS")
            elif is_valid is True:
                ai_ok, ai_label = True, "✅ VALID"
            elif is_valid is False:
                ai_ok, ai_label = False, "❌ NOT VALID"
            else:
                ai_ok, ai_label = None, "—"

            ai_color = "#22c55e" if ai_ok else ("#ef4444" if ai_ok is False else "#94a3b8")

            # doc_filter for approve/remove
            filt: dict = {}
            for fk in ("procedure_code", "procedure_code_1", "procedure_code_2",
                       "diagnosis_code", "specialist_code",
                       "gender", "min_age", "max_age", "combo_key"):
                if entry.get(fk) not in (None, ""):
                    filt[fk] = entry[fk]

            reasoning = (entry.get("reasoning") or entry.get("match_reason")
                         or entry.get("ai_reasoning") or "")

            return dict(
                subj_code=subj_code, subj_name=subj_name,
                qual_code=qual_code, qual_name=qual_name,
                ai_label=ai_label, ai_color=ai_color,
                conf=conf, usage=entry.get("usage_count", 0),
                reasoning=reasoning, doc_filter=filt,
            )

        COL_W = [1.0, 2.2, 1.3, 2.0, 1.3, 0.6, 0.5, 0.9, 0.5, 0.5]
        HDR   = ["Code", "Name", "Qualifier", "Qualifier Name",
                 "AI Decision", "Conf%", "Used", "Status", "✅", "🗑️"]

        for label, group_entries in grouped.items():
            n_t = sum(1 for e in group_entries if e.get("_trusted"))
            n_p = len(group_entries) - n_t
            badge = f"{len(group_entries)} entr{'y' if len(group_entries)==1 else 'ies'} · ✅ {n_t} trusted · ⏳ {n_p} pending"
            with st.expander(f"**{label}** — {badge}", expanded=True):

                hdr = st.columns(COL_W)
                for col, txt in zip(hdr, HDR):
                    col.markdown(
                        f'<span style="color:#64748b;font-size:0.78em;font-weight:700;">{txt}</span>',
                        unsafe_allow_html=True,
                    )
                st.markdown('<hr style="border-color:#1e293b;margin:4px 0 8px 0;">', unsafe_allow_html=True)

                for idx, entry in enumerate(group_entries):
                    col_name = entry.get("_collection", "")
                    trusted  = entry.get("_trusted", False)
                    r        = _lrn_row(entry, col_name)

                    row = st.columns(COL_W)
                    row[0].markdown(
                        f'<span style="color:#e2e8f0;font-size:0.85em;word-break:break-all;">{r["subj_code"]}</span>',
                        unsafe_allow_html=True)
                    row[1].markdown(
                        f'<span style="color:#cbd5e1;font-size:0.82em;">{r["subj_name"]}</span>',
                        unsafe_allow_html=True)
                    row[2].markdown(
                        f'<span style="color:#e2e8f0;font-size:0.85em;">{r["qual_code"]}</span>',
                        unsafe_allow_html=True)
                    row[3].markdown(
                        f'<span style="color:#cbd5e1;font-size:0.82em;">{r["qual_name"]}</span>',
                        unsafe_allow_html=True)
                    row[4].markdown(
                        f'<span style="color:{r["ai_color"]};font-weight:700;font-size:0.82em;">{r["ai_label"]}</span>',
                        unsafe_allow_html=True)
                    row[5].markdown(
                        f'<span style="color:#94a3b8;font-size:0.82em;">{r["conf"]}</span>',
                        unsafe_allow_html=True)
                    row[6].markdown(
                        f'<span style="color:#64748b;font-size:0.82em;">{r["usage"]}x</span>',
                        unsafe_allow_html=True)
                    status_color = "#22c55e" if trusted else "#f59e0b"
                    status_label = "✅ Trusted" if trusted else "⏳ Pending"
                    row[7].markdown(
                        f'<span style="color:{status_color};font-size:0.78em;font-weight:700;">{status_label}</span>',
                        unsafe_allow_html=True)

                    if row[8].button("✅", key=f"lrn_app_{col_name}_{idx}", use_container_width=True, help="Mark trusted"):
                        try:
                            res = requests.post(
                                f"{API}/api/v1/klaire/learning/approve",
                                json={"collection": col_name, "doc_filter": r["doc_filter"]},
                                timeout=10,
                            )
                            res.raise_for_status()
                            st.success("Approved — now trusted.")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Approve failed: {ex}")

                    if row[9].button("🗑️", key=f"lrn_del_{col_name}_{idx}", use_container_width=True, help="Remove"):
                        try:
                            res = requests.delete(
                                f"{API}/api/v1/klaire/learning/remove",
                                json={"collection": col_name, "doc_filter": r["doc_filter"]},
                                timeout=10,
                            )
                            res.raise_for_status()
                            st.warning("Removed.")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Remove failed: {ex}")

                    if r["reasoning"]:
                        st.markdown(
                            f'<span style="color:#64748b;font-size:0.76em;padding-left:4px;">↳ {r["reasoning"][:200]}</span>',
                            unsafe_allow_html=True,
                        )
                    st.markdown('<hr style="border-color:#1e293b;margin:2px 0;">', unsafe_allow_html=True)

    st.stop()

elif "PA Request" in mode:
    # ══════════════════════════════════════════════════════════════════════════
    # PA REQUEST — multi-procedure, multi-diagnosis
    # ══════════════════════════════════════════════════════════════════════════

    # ── GP gate: consultation must be completed first ─────────────────────────
    if not st.session_state.get("result"):
        st.warning(
            "⚠️  **Consultation must be completed before submitting a PA request.**  \n"
            "Go to **📋 New Request**, complete the GP or Specialist consultation, "
            "then return here."
        )
        if st.button("Go to New Request →", type="primary"):
            st.session_state["_force_mode"] = "📋 New Request"
            st.rerun()
        st.stop()

    consult_result = st.session_state.get("result", {})
    st.markdown("## 💊 PA Request")
    st.caption(
        "Select every procedure the doctor is requesting and its corresponding diagnosis "
        "(or diagnoses). One procedure can have multiple diagnoses; each diagnosis belongs "
        "to only one procedure."
    )

    # Show consultation summary
    consult_dec = consult_result.get("decision", "")
    consult_code = consult_result.get("approved_code") or consult_result.get("change_to_code", "")
    if consult_dec in ("APPROVE", "CHANGE"):
        st.markdown(
            f'<div style="background:#052e16;border:1px solid #16a34a;border-radius:8px;'
            f'padding:10px 16px;margin-bottom:12px;">'
            f'<span style="color:#4ade80;font-weight:700;">✅ Consultation approved: {consult_code}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    _enc_display = st.radio(
        "Encounter Type",
        options=["No-Auth (Outpatient)", "Pre-Auth (Inpatient)"],
        horizontal=True,
        key="pa_enc_type",
    )
    enc_type = "OUTPATIENT" if _enc_display.startswith("No-Auth") else "INPATIENT"

    st.markdown("---")

    # ── Pre-Auth sub-flow gate ─────────────────────────────────────────────────
    if enc_type == "INPATIENT":
        if "pa_admission_status" not in st.session_state:
            st.session_state["pa_admission_status"] = None  # None | ADMITTED | NOT_ADMITTED
        if "admission_approved" not in st.session_state:
            st.session_state["admission_approved"] = False
        if "admission_review_id" not in st.session_state:
            st.session_state["admission_review_id"] = None

        adm_status = st.session_state["pa_admission_status"]

        if adm_status is None:
            st.markdown("#### Is the enrollee being admitted?")
            gc1, gc2, _ = st.columns([1, 1, 3])
            with gc1:
                if st.button("✅ Yes — Admission", key="pa_adm_yes", use_container_width=True, type="primary"):
                    st.session_state["pa_admission_status"] = "ADMITTED"
                    st.session_state["admission_approved"] = False
                    st.rerun()
            with gc2:
                if st.button("❌ No — Not Admitted", key="pa_adm_no", use_container_width=True):
                    st.session_state["pa_admission_status"] = "NOT_ADMITTED"
                    st.rerun()
            st.stop()

        if adm_status == "ADMITTED" and not st.session_state["admission_approved"]:
            st.markdown("#### 🏥 Admission Request")
            adm_codes = load_admission_codes()
            adm_code_options = {f"{c['code']} — {c['name']}": c["code"] for c in adm_codes}
            adm_diag_options = load_diagnoses()

            adm_c1, adm_c2 = st.columns([1, 1])
            with adm_c1:
                adm_room_display = st.selectbox(
                    "Room Type",
                    options=list(adm_code_options.keys()),
                    key="pa_adm_room",
                )
                adm_code = adm_code_options[adm_room_display]
            with adm_c2:
                adm_days = st.number_input("Number of Days", min_value=1, value=1, step=1, key="pa_adm_days")

            adm_diag_selected = st.multiselect(
                "Admitting Diagnoses",
                options=list(adm_diag_options.keys()),
                placeholder="Select one or more admitting diagnoses…",
                key="pa_adm_diags",
            )
            adm_diag_codes = [adm_diag_options[d]["code"] for d in adm_diag_selected]
            adm_diag_names = {adm_diag_options[d]["code"]: adm_diag_options[d]["name"] for d in adm_diag_selected}

            bc1, bc2 = st.columns([2, 3])
            with bc1:
                if st.button("📤 Submit Admission Request", key="pa_adm_submit", type="primary", use_container_width=True):
                    if not adm_diag_codes:
                        st.error("At least one admitting diagnosis is required.")
                    else:
                        adm_payload = {
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
                                adm_resp = requests.post(
                                    f"{API}/api/v1/klaire/admission",
                                    json=adm_payload,
                                    timeout=60,
                                )
                                adm_resp.raise_for_status()
                                adm_data = adm_resp.json()
                                st.session_state["admission_review_id"] = adm_data.get("review_id")
                                st.success(
                                    f"Admission request submitted (review ID: {adm_data.get('review_id', '?')}). "
                                    "Go to Agent Review to approve, then return here to submit procedures."
                                )
                            except Exception as ex:
                                st.error(f"Admission request failed: {ex}")
            with bc2:
                if st.button("↩ Change admission status", key="pa_adm_reset", use_container_width=True):
                    st.session_state["pa_admission_status"] = None
                    st.session_state["admission_approved"] = False
                    st.session_state["admission_review_id"] = None
                    st.rerun()

            # Allow agent to mark admission as approved in this session
            if st.session_state.get("admission_review_id"):
                st.markdown("---")
                st.info(
                    "⏳ Waiting for admission approval in Agent Review tab. "
                    "Once approved, click the button below to unlock the procedure form."
                )
                if st.button("✅ Admission approved — proceed to procedures", key="pa_adm_unlock", type="primary"):
                    st.session_state["admission_approved"] = True
                    st.rerun()
            st.stop()

        if adm_status == "ADMITTED" and st.session_state["admission_approved"]:
            adm_codes = load_admission_codes()
            adm_code_map = {c["code"]: c["name"] for c in adm_codes}
            st.markdown(
                '<div style="background:#052e16;border:1px solid #16a34a;border-radius:8px;'
                'padding:10px 16px;margin-bottom:12px;">'
                '<span style="color:#4ade80;font-weight:700;">🏥 Admission approved — procedures unlocked</span>'
                '</div>',
                unsafe_allow_html=True,
            )

    _adm_status  = st.session_state.get("pa_admission_status")
    proc_options = _filter_procedures(enc_type, _adm_status)   # branch-filtered
    diag_options = load_diagnoses()    # {display: {code, name}}

    # ── Item management via session state ─────────────────────────────────────
    if "pa_items" not in st.session_state:
        st.session_state["pa_items"] = [{"proc_display": "", "diags": []}]

    def _add_row():
        st.session_state["pa_items"].append({"proc_display": "", "diags": []})

    def _remove_row(i):
        if len(st.session_state["pa_items"]) > 1:
            st.session_state["pa_items"].pop(i)

    # ── Tariff lookup helper (cached per provider + code) ─────────────────────
    @st.cache_data(ttl=3600, show_spinner=False)
    def _fetch_tariff(prov_id: str, proc_code: str):
        if not prov_id or not proc_code:
            return None
        try:
            r = requests.get(
                f"{API}/api/v1/klaire/tariff",
                params={"provider_id": prov_id, "procedure_code": proc_code},
                timeout=8,
            )
            if r.ok:
                return r.json().get("tariff_price")
        except Exception:
            pass
        return None

    # ── Procedure rows ────────────────────────────────────────────────────────
    st.markdown("### Procedures & Diagnoses")

    proc_opts_list = ["— Select procedure —"] + list(proc_options.keys())

    for idx, item in enumerate(st.session_state["pa_items"]):
        with st.container(border=True):
            # Row 1: Procedure | Diagnosis | Delete
            pc1, pc2, pc3 = st.columns([3, 4, 1])
            with pc1:
                if proc_options:
                    proc_display = st.selectbox(
                        f"Procedure #{idx + 1}",
                        options=proc_opts_list,
                        index=proc_opts_list.index(item.get("proc_display", "— Select procedure —"))
                              if item.get("proc_display") in proc_opts_list else 0,
                        key=f"pa_proc_{idx}",
                    )
                    st.session_state["pa_items"][idx]["proc_display"] = proc_display
                    if proc_display != "— Select procedure —":
                        p = proc_options[proc_display]
                        st.session_state["pa_items"][idx]["proc"] = p["code"]
                        st.session_state["pa_items"][idx]["proc_name"] = p["name"]
                    else:
                        st.session_state["pa_items"][idx]["proc"] = ""
                        st.session_state["pa_items"][idx]["proc_name"] = ""
                else:
                    proc_val = st.text_input(
                        f"Procedure Code #{idx + 1}",
                        value=item.get("proc", ""),
                        placeholder="e.g. DRG1106",
                        key=f"pa_proc_txt_{idx}",
                    )
                    st.session_state["pa_items"][idx]["proc"] = proc_val.strip().upper()
                    st.session_state["pa_items"][idx]["proc_name"] = proc_val.strip()

            with pc2:
                if diag_options:
                    selected_diags = st.multiselect(
                        f"Diagnosis / Diagnoses #{idx + 1}",
                        options=list(diag_options.keys()),
                        default=[
                            k for k in diag_options
                            if diag_options[k]["code"] in item.get("diags", [])
                        ],
                        placeholder="Select one or more diagnoses…",
                        key=f"pa_diags_{idx}",
                    )
                    st.session_state["pa_items"][idx]["diags"] = [
                        diag_options[d]["code"] for d in selected_diags
                    ]
                    st.session_state["pa_items"][idx]["diag_names"] = {
                        diag_options[d]["code"]: diag_options[d]["name"] for d in selected_diags
                    }
                else:
                    raw = st.text_input(
                        f"Diagnosis Code(s) #{idx + 1}",
                        key=f"pa_diags_raw_{idx}",
                        placeholder="e.g. J069, B969 (comma-separated)",
                    )
                    codes = [c.strip().upper() for c in raw.split(",") if c.strip()]
                    st.session_state["pa_items"][idx]["diags"] = codes
                    st.session_state["pa_items"][idx]["diag_names"] = {c: c for c in codes}

            with pc3:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("✕", key=f"pa_del_{idx}", help="Remove this row"):
                    _remove_row(idx)
                    st.rerun()

            # Row 2: Tariff display | Quantity | Provider price | Comment
            current_proc = st.session_state["pa_items"][idx].get("proc", "")
            tariff_price = None
            if current_proc and (provider_id or "").strip():
                tariff_price = _fetch_tariff(provider_id.strip(), current_proc)
                st.session_state["pa_items"][idx]["tariff_price"] = tariff_price

            pr1, pr1b, pr2, pr3 = st.columns([2, 1, 2, 3])
            with pr1:
                if current_proc:
                    if tariff_price is not None:
                        st.markdown(
                            f'<div style="background:#0f2a1a;border:1px solid #16a34a;border-radius:6px;'
                            f'padding:8px 12px;margin-top:4px;">'
                            f'<span style="color:#86efac;font-size:0.82em;">Tariff Price</span><br>'
                            f'<strong style="color:#4ade80;font-size:1.1em;">₦{tariff_price:,.2f}</strong>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            f'<div style="background:#1c0f00;border:1px solid #92400e;border-radius:6px;'
                            f'padding:8px 12px;margin-top:4px;">'
                            f'<span style="color:#fcd34d;font-size:0.82em;">Tariff Price</span><br>'
                            f'<strong style="color:#fbbf24;">No contracted tariff</strong>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

            with pr1b:
                qty_val = st.number_input(
                    "Qty",
                    min_value=1,
                    value=int(item.get("quantity") or 1),
                    step=1,
                    key=f"pa_qty_{idx}",
                )
                st.session_state["pa_items"][idx]["quantity"] = int(qty_val)

            with pr2:
                prov_price_val = st.number_input(
                    "Provider Price (₦) — optional",
                    min_value=0.0,
                    value=item.get("provider_price") or 0.0,
                    step=100.0,
                    format="%.2f",
                    key=f"pa_pprice_{idx}",
                    label_visibility="visible",
                )
                # Store None if zero (means not set)
                prov_price = prov_price_val if prov_price_val > 0 else None
                st.session_state["pa_items"][idx]["provider_price"] = prov_price

                # Warn immediately if price overrides tariff
                if prov_price and tariff_price is not None and round(prov_price, 2) != round(tariff_price, 2):
                    diff = prov_price - tariff_price
                    col  = "#ef4444" if diff > 0 else "#22c55e"
                    sign = "+" if diff > 0 else ""
                    st.markdown(
                        f'<span style="color:{col};font-size:0.82em;">⚠️ {sign}₦{diff:,.2f} vs tariff — '
                        f'will go for review</span>',
                        unsafe_allow_html=True,
                    )
                elif prov_price and tariff_price is None:
                    st.markdown(
                        '<span style="color:#f59e0b;font-size:0.82em;">⚠️ No tariff on file — '
                        'will go for review</span>',
                        unsafe_allow_html=True,
                    )

            with pr3:
                comment_val = st.text_input(
                    "Comment",
                    value=item.get("comment", ""),
                    placeholder="Clinical note, override reason, or any remark…",
                    key=f"pa_comment_{idx}",
                )
                st.session_state["pa_items"][idx]["comment"] = comment_val

    st.button("＋ Add Another Procedure", on_click=_add_row, key="pa_add")
    st.markdown("---")

    # ── Submit ────────────────────────────────────────────────────────────────
    if st.button("▶ Submit PA Request", type="primary", key="pa_submit"):
        errors = []
        if not (enrollee_id or "").strip():
            errors.append("Enrollee ID is required (fill in the sidebar).")
        if not (provider_id or "").strip():
            errors.append("Provider ID is required (fill in the sidebar).")
        items_payload = []
        for idx, item in enumerate(st.session_state["pa_items"]):
            if not item["proc"]:
                errors.append(f"Row {idx + 1}: Procedure code is required.")
                continue
            if not item.get("diags"):
                errors.append(f"Row {idx + 1}: At least one diagnosis is required.")
                continue
            items_payload.append({
                "procedure_code":  item["proc"],
                "procedure_name":  item.get("proc_name", ""),
                "diagnosis_codes": item["diags"],
                "diagnosis_names": item.get("diag_names", {}),
                "quantity":        int(item.get("quantity") or 1),
                "tariff_price":    item.get("tariff_price"),
                "provider_price":  item.get("provider_price"),
                "comment":         item.get("comment") or None,
            })
        if errors:
            for e in errors:
                st.error(e)
        else:
            payload = {
                "enrollee_id":         enrollee_id.strip(),
                "provider_id":         provider_id.strip(),
                "hospital_name":       (hospital_name or "").strip() or None,
                "encounter_date":      str(encounter_date),
                "encounter_type":      enc_type,
                "admission_status":    st.session_state.get("pa_admission_status") or "NOT_ADMITTED",
                "admission_approved_id": st.session_state.get("admission_review_id"),
                "items":               items_payload,
            }
            with st.spinner("KLAIRE is evaluating the PA request…"):
                try:
                    resp = requests.post(f"{API}/api/v1/klaire/pa", json=payload, timeout=180)
                    resp.raise_for_status()
                    st.session_state["pa_result"] = resp.json()
                except Exception as ex:
                    st.error(f"API error: {ex}")

    # ── Results ───────────────────────────────────────────────────────────────
    pa_result = st.session_state.get("pa_result")
    if pa_result:
        overall = pa_result.get("overall_decision", "")
        badge_map = {
            "APPROVE":        ('<span class="badge b-approve">✅ &nbsp;ALL APPROVED</span>', "#052e16"),
            "DENY":           ('<span class="badge b-deny">❌ &nbsp;ALL DENIED</span>',   "#2d0a0a"),
            "PARTIAL":        ('<span class="badge b-change">⚡ &nbsp;PARTIAL APPROVAL</span>', "#1c0f00"),
            "PENDING_REVIEW": ('<span class="badge" style="background:#4c1d95;color:#ddd6fe;">🔍 &nbsp;PENDING REVIEW</span>', "#1a1a2e"),
        }
        badge, bg = badge_map.get(overall, (overall, "#0f172a"))
        st.markdown(
            f'<div style="background:{bg};border-radius:12px;padding:16px 22px;margin-bottom:16px;">'
            f'{badge}<h3 style="color:#f1f5f9;margin:10px 0 0 0;">Overall: {overall}</h3>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # ── Disease Combination banner ─────────────────────────────────────────
        combo = pa_result.get("disease_combination")
        if combo and len(pa_result.get("items", [])) >= 1:
            flagged = combo.get("flagged_pairs", [])
            plausible = combo.get("plausible", True)
            combo_conf = combo.get("confidence", 100)
            combo_reason = combo.get("reasoning", "")
            if not plausible or flagged:
                st.markdown(
                    f'<div style="background:#431407;border-left:4px solid #ea580c;'
                    f'border-radius:8px;padding:12px 16px;margin-bottom:12px;">'
                    f'<strong style="color:#fed7aa;">⚠️ Disease Combination Flag</strong> '
                    f'<span style="color:#fdba74;font-size:0.85em;">({combo_conf}% confidence)</span><br>'
                    f'<span style="color:#fde8d8;font-size:0.9em;">{combo_reason}</span>'
                    + (
                        f'<br><span style="color:#fb923c;font-size:0.8em;margin-top:4px;display:block;">'
                        f'Flagged pairs: {", ".join(" + ".join(p) for p in flagged)}</span>'
                        if flagged else ""
                    )
                    + '</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div style="background:#052e16;border-left:4px solid #16a34a;'
                    f'border-radius:8px;padding:10px 16px;margin-bottom:12px;">'
                    f'<strong style="color:#bbf7d0;">✅ Disease Combination: Plausible</strong> '
                    f'<span style="color:#86efac;font-size:0.85em;">({combo_conf}% confidence)</span><br>'
                    f'<span style="color:#d1fae5;font-size:0.85em;">{combo_reason}</span></div>',
                    unsafe_allow_html=True,
                )

        # ── Procedure Combination Necessity banner ─────────────────────────────
        proc_combo = pa_result.get("procedure_combination")
        if proc_combo and len(pa_result.get("items", [])) >= 2:
            necessary     = proc_combo.get("necessary", True)
            pc_conf       = proc_combo.get("confidence", 100)
            pc_reason     = proc_combo.get("reasoning", "")
            pc_flagged    = proc_combo.get("flagged_items", [])
            if not necessary or pc_flagged:
                flag_lines = "".join(
                    f'<br><span style="color:#fb923c;font-size:0.8em;">• {f.get("name_a","?")} + {f.get("name_b","?")}: {f.get("reason","")}</span>'
                    for f in pc_flagged
                )
                st.markdown(
                    f'<div style="background:#1e1b4b;border-left:4px solid #818cf8;'
                    f'border-radius:8px;padding:12px 16px;margin-bottom:12px;">'
                    f'<strong style="color:#c7d2fe;">⚠️ Procedure Necessity Flag</strong> '
                    f'<span style="color:#a5b4fc;font-size:0.85em;">({pc_conf}% confidence)</span><br>'
                    f'<span style="color:#e0e7ff;font-size:0.9em;">{pc_reason}</span>'
                    + flag_lines + '</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div style="background:#052e16;border-left:4px solid #16a34a;'
                    f'border-radius:8px;padding:10px 16px;margin-bottom:12px;">'
                    f'<strong style="color:#bbf7d0;">✅ Procedure Necessity: All Justified</strong> '
                    f'<span style="color:#86efac;font-size:0.85em;">({pc_conf}% confidence)</span><br>'
                    f'<span style="color:#d1fae5;font-size:0.85em;">{pc_reason}</span></div>',
                    unsafe_allow_html=True,
                )

        for proc_res in pa_result.get("items", []):
            _render_pa_item(proc_res, pa_result.get("encounter_type", "OUTPATIENT"))

        with st.expander("Raw API response"):
            st.json(pa_result)

    st.stop()

elif "Agent Review" in mode:
    # ══════════════════════════════════════════════════════════════════════════
    # AGENT REVIEW QUEUE — ALL review types
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("### 🛡️  Agent Review Queue")
    st.caption(
        "All pending AI recommendations that need agent confirmation — "
        "specialist-diagnosis checks, first-line treatment checks, and PA rule evaluations."
    )

    btn_col1, btn_col2, _ = st.columns([1, 1, 5])
    with btn_col1:
        if st.button("🔄 Refresh", key="refresh_reviews", use_container_width=True):
            st.rerun()
    with btn_col2:
        if st.button("🗑️ Clear All", key="clear_all_reviews", use_container_width=True, help="Delete every entry in the review queue"):
            try:
                r = requests.delete(f"{API}/api/v1/klaire/reviews/clear-all", timeout=10)
                r.raise_for_status()
                st.success(r.json().get("message", "Queue cleared."))
                st.rerun()
            except Exception as ex:
                st.error(f"Clear failed: {ex}")

    try:
        resp = requests.get(f"{API}/api/v1/klaire/reviews?limit=100", timeout=10)
        resp.raise_for_status()
        data    = resp.json()
        reviews = data.get("reviews", [])
        total   = data.get("total_pending", 0)
    except Exception as e:
        st.error(f"Could not load review queue: {e}")
        reviews = []
        total   = 0

    if not reviews:
        st.info("No pending reviews. All evaluations are resolved.")
    else:
        st.caption(f"{total} pending review(s)")
        for rv in reviews:
            rid          = rv.get("review_id", "")
            review_type  = rv.get("review_type", "SPECIALIST")

            with st.container(border=True):
                rc1, rc2 = st.columns([3, 1])

                with rc1:
                    if review_type in ("PA_OUTPATIENT", "PA_PREAUTH"):
                        # ── PA No-Auth / Pre-Auth procedure review card ────
                        proc_code  = rv.get("procedure_code", "")
                        proc_name  = rv.get("procedure_name", proc_code)
                        approved   = rv.get("approved_diagnoses", [])
                        denied     = rv.get("denied_diagnoses", [])
                        diag_names = rv.get("diag_names", {})
                        reasons    = rv.get("review_reasons", [])
                        fl         = rv.get("first_line", {})
                        inj_check  = rv.get("injection_check", {})

                        if review_type == "PA_PREAUTH":
                            st.markdown(
                                '<span style="background:#7c3aed;color:#ede9fe;border-radius:4px;'
                                'padding:2px 8px;font-size:0.8em;">💊 PA PRE-AUTH</span>',
                                unsafe_allow_html=True,
                            )
                        else:
                            st.markdown(
                                '<span style="background:#4c1d95;color:#ddd6fe;border-radius:4px;'
                                'padding:2px 8px;font-size:0.8em;">💊 PA NO-AUTH</span>',
                                unsafe_allow_html=True,
                            )
                        st.markdown(
                            f"**{proc_code}** — {proc_name}  \n"
                            f"Enrollee: `{rv.get('enrollee_id','')}`"
                        )
                        if approved:
                            pills = "  ".join(f'`{c}` {diag_names.get(c, c)}' for c in approved)
                            st.markdown(f"✅ Approved diagnoses: {pills}")
                        if denied:
                            pills = "  ".join(f'`{c}` {diag_names.get(c, c)}' for c in denied)
                            st.markdown(f"❌ Delisted diagnoses: {pills}")
                        if fl:
                            fl_col = "#22c55e" if fl.get("decision") == "APPROVE" else "#ef4444"
                            st.markdown(
                                f'<span style="color:#94a3b8;font-size:0.88em;">First-line: '
                                f'<strong style="color:{fl_col};">{fl.get("decision","")}</strong>'
                                f' ({fl.get("confidence",0)}%) — {fl.get("reasoning","")}</span>',
                                unsafe_allow_html=True,
                            )

                        # Rule 12 injection advisory
                        if inj_check.get("triggered"):
                            if inj_check.get("justified"):
                                st.markdown(
                                    '<div style="background:#052e16;border-left:3px solid #16a34a;'
                                    'border-radius:5px;padding:6px 12px;margin-top:6px;">'
                                    '<span style="color:#bbf7d0;font-size:0.85em;">'
                                    f'✅ Injection-Without-Admission: diagnosis justifies parenteral route — {inj_check.get("reasoning","")}'
                                    '</span></div>',
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.markdown(
                                    '<div style="background:#431407;border-left:3px solid #ea580c;'
                                    'border-radius:5px;padding:6px 12px;margin-top:6px;">'
                                    '<span style="color:#fed7aa;font-size:0.85em;">'
                                    f'🟠 Injection-Without-Admission: oral alternative not documented — {inj_check.get("reasoning","")}'
                                    '</span></div>',
                                    unsafe_allow_html=True,
                                )

                        # Price info
                        tariff_price   = rv.get("tariff_price")
                        provider_price = rv.get("provider_price")
                        price_override = rv.get("price_override", False)
                        comment        = rv.get("comment", "")

                        if tariff_price is not None or provider_price is not None:
                            if price_override:
                                diff_pct = ""
                                if tariff_price and provider_price:
                                    diff = abs(provider_price - tariff_price) / tariff_price * 100
                                    direction = "above" if provider_price > tariff_price else "below"
                                    diff_pct = f" — {diff:.1f}% {direction} tariff"
                                tariff_str   = f"₦{tariff_price:,.2f}" if tariff_price else "No contracted tariff"
                                provider_str = f"₦{provider_price:,.2f}" if provider_price else "—"
                                st.markdown(
                                    f'<div style="background:#1c0f00;border:1px solid #d97706;'
                                    f'border-radius:6px;padding:8px 12px;margin-top:6px;">'
                                    f'<span style="color:#fcd34d;font-size:0.88em;">💰 Price Override{diff_pct}</span><br>'
                                    f'<span style="color:#fde68a;font-size:0.85em;">Tariff: {tariff_str} &nbsp;|&nbsp; Provider: {provider_str}</span>'
                                    f'</div>',
                                    unsafe_allow_html=True,
                                )
                            elif tariff_price is not None:
                                st.caption(f"💰 Tariff price: ₦{tariff_price:,.2f}")

                        if comment:
                            st.caption(f"💬 Comment: {comment}")
                        for r in reasons:
                            st.caption(f"⚠️ {r}")

                    elif review_type == "PA_ADMISSION":
                        # ── Admission review card ──────────────────────────
                        adm_code_map = {"ADM01": "Private Room", "ADM02": "Semi-Private Room", "ADM03": "General Room"}
                        adm_code  = rv.get("admission_code", "")
                        adm_name  = adm_code_map.get(adm_code, adm_code)
                        adm_days  = rv.get("days", "?")
                        adm_diags = rv.get("admitting_diagnosis_names", {})
                        ai_adv    = rv.get("ai_advisory", "")

                        st.markdown(
                            '<span style="background:#1e3a5f;color:#bae6fd;border-radius:4px;'
                            'padding:2px 8px;font-size:0.8em;">🏥 PA ADMISSION</span>',
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            f"**{adm_code} — {adm_name}** · {adm_days} day(s)  \n"
                            f"Enrollee: `{rv.get('enrollee_id','')}`  \n"
                            f"Provider: {rv.get('hospital_name','') or rv.get('provider_id','')}"
                        )
                        if adm_diags:
                            diag_pills = "  ".join(f'`{c}` {n}' for c, n in adm_diags.items())
                            st.markdown(f"**Admitting diagnoses:** {diag_pills}")
                        if ai_adv:
                            st.markdown(
                                f'<div style="background:#0f2a3a;border-left:3px solid #38bdf8;'
                                f'border-radius:5px;padding:8px 14px;margin-top:6px;">'
                                f'<span style="color:#7dd3fc;font-size:0.85em;font-weight:700;">AI Advisory</span><br>'
                                f'<span style="color:#bae6fd;font-size:0.85em;">{ai_adv}</span>'
                                f'</div>',
                                unsafe_allow_html=True,
                            )

                    else:
                        # ── Specialist-diagnosis review card ───────────────
                        ai_rec  = rv.get("ai_decision", "")
                        ai_conf = rv.get("ai_confidence", 0)
                        ai_why  = rv.get("ai_reasoning", "")
                        src     = rv.get("learning_source", "ai")
                        uses    = rv.get("usage_count", 1)

                        badge_col = "#166534" if ai_rec == "APPROVE" else "#7f1d1d"
                        badge_txt = "#bbf7d0" if ai_rec == "APPROVE" else "#fecaca"

                        st.markdown(
                            f'<span style="background:#1e3a5f;color:#bae6fd;border-radius:4px;'
                            f'padding:2px 8px;font-size:0.8em;">🏥 SPECIALIST</span>',
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            f"**{rv.get('specialist_code','')} — {rv.get('specialist_name','')}**  \n"
                            f"Diagnosis: `{rv.get('diagnosis_code','')}` — {rv.get('diagnosis_name','')}  \n"
                            f"Enrollee: `{rv.get('enrollee_id','')}` · {rv.get('encounter_date','')} · {rv.get('hospital_name','')}"
                        )
                        src_label = f"Learning table (used {uses}x)" if src == "learning_table" else "AI — first evaluation"
                        st.markdown(
                            f'<div style="background:{badge_col};border-radius:6px;padding:8px 12px;margin-top:6px;">'
                            f'<strong style="color:{badge_txt};">AI: {ai_rec} ({ai_conf}%)</strong>'
                            f'<span style="color:{badge_txt};font-size:0.88em;"> · {src_label}</span><br>'
                            f'<span style="color:{badge_txt};font-size:0.88em;">{ai_why}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                        if rv.get("qa_flag"):
                            st.caption(f"⚠️ QA: {rv.get('qa_reason','')}")

                with rc2:
                    agent = st.text_input("Agent", value="Agent", key=f"rv_agent_{rid}", label_visibility="collapsed")
                    notes = st.text_input("Notes", placeholder="Justification…", key=f"rv_notes_{rid}", label_visibility="collapsed")
                    if review_type in ("PA_OUTPATIENT", "PA_PREAUTH"):
                        if st.button("✅ Agree with AI", key=f"rv_app_{rid}", use_container_width=True, type="primary", help="Confirm AI's individual rule results are correct"):
                            _submit_review(rid, "AGREE", agent, notes)
                            st.rerun()
                        if st.button("❌ Override AI", key=f"rv_den_{rid}", use_container_width=True, help="Disagree with AI — your decision overrides"):
                            _submit_review(rid, "OVERRIDE", agent, notes)
                            st.rerun()
                    elif review_type == "PA_ADMISSION":
                        if st.button("✅ Approve Admission", key=f"rv_app_{rid}", use_container_width=True, type="primary"):
                            _submit_review(rid, "APPROVE", agent, notes)
                            st.rerun()
                        if st.button("❌ Deny Admission", key=f"rv_den_{rid}", use_container_width=True):
                            _submit_review(rid, "DENY", agent, notes)
                            st.rerun()
                    else:
                        if st.button("✅ Approve", key=f"rv_app_{rid}", use_container_width=True, type="primary"):
                            _submit_review(rid, "APPROVE", agent, notes)
                            st.rerun()
                        if st.button("❌ Deny", key=f"rv_den_{rid}", use_container_width=True):
                            _submit_review(rid, "DENY", agent, notes)
                            st.rerun()

    st.stop()

# ── STEP 1 — Select to see a doctor ──────────────────────────────────────────
st.markdown("## Select to see a doctor")
st.caption("Choose the type of doctor the enrollee needs to see.")

col_gp, col_spec = st.columns(2)
with col_gp:
    if st.button("👨‍⚕️  GP (General Practitioner)", use_container_width=True,
                 type="primary" if st.session_state.get("flow") == "GP" else "secondary"):
        st.session_state["flow"]    = "GP"
        st.session_state["gp_type"] = None
        st.session_state["result"]  = None
        st.rerun()
with col_spec:
    if st.button("🏥  Specialist", use_container_width=True,
                 type="primary" if st.session_state.get("flow") == "SPECIALIST" else "secondary"):
        st.session_state["flow"]   = "SPECIALIST"
        st.session_state["result"] = None
        st.rerun()

flow = st.session_state.get("flow")

if not flow:
    st.stop()

st.markdown('<div class="divider-line"></div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════════
# GP FLOW
# ════════════════════════════════════════════════════════════════════════════════
if flow == "GP":
    st.markdown("### 👨‍⚕️  GP Consultation")

    # ── STEP 2 — Initial or Review ────────────────────────────────────────────
    st.markdown("**Is this an initial visit or a follow-up review?**")
    col_init, col_rev = st.columns(2)
    with col_init:
        if st.button("📋  GP Initial  (CONS021)", use_container_width=True,
                     type="primary" if st.session_state.get("gp_type") == "INITIAL" else "secondary"):
            st.session_state["gp_type"] = "INITIAL"
            st.session_state["result"]  = None
            st.rerun()
    with col_rev:
        if st.button("🔁  GP Review  (CONS022)", use_container_width=True,
                     type="primary" if st.session_state.get("gp_type") == "REVIEW" else "secondary"):
            st.session_state["gp_type"] = "REVIEW"
            st.session_state["result"]  = None
            st.rerun()

    gp_type = st.session_state.get("gp_type")
    if not gp_type:
        st.stop()

    st.markdown('<div class="divider-line"></div>', unsafe_allow_html=True)

    if gp_type == "INITIAL":
        st.markdown(
            "**CONS021 — GP Initial**  \n"
            "The enrollee is presenting with symptoms and has not seen a GP yet. "
            "Select all symptoms the enrollee presents with."
        )
    else:
        st.markdown(
            "**CONS022 — GP Review / Follow-up**  \n"
            "The enrollee is returning to continue treatment. "
            "Select any symptoms still present."
        )

    # ── STEP 3 — Symptom selection ────────────────────────────────────────────
    st.markdown("#### Select Symptoms")
    symptom_groups, all_symptoms = load_symptom_codes()

    body_systems = sorted(symptom_groups.keys())
    filter_sys   = st.selectbox(
        "Filter by body system",
        options=["All Systems"] + body_systems,
        key="sym_filter",
    )

    if filter_sys == "All Systems":
        sym_options = all_symptoms
    else:
        sym_options = symptom_groups.get(filter_sys, [])

    selected_symptoms = st.multiselect(
        "Symptoms presented",
        options=sym_options,
        placeholder="Type to search or scroll…",
        key="sym_select",
    )

    st.caption(
        f"{len(selected_symptoms)} symptom(s) selected"
        + (f": {', '.join(s.split(' — ')[0] for s in selected_symptoms)}" if selected_symptoms else "")
    )

    st.markdown("---")

    gp_submit = st.button("▶ Submit GP Request", type="primary", key="gp_btn")

    if gp_submit:
        errors = []
        if not (enrollee_id or "").strip():
            errors.append("Enrollee ID is required (fill in the sidebar).")
        if not (provider_id or "").strip():
            errors.append("Provider ID is required (fill in the sidebar).")
        if errors:
            for e in errors:
                st.error(e)
        else:
            # Extract just the ICD-10 codes from "R00.1 — Description"
            sym_codes = [s.split(" — ")[0].strip() for s in selected_symptoms]
            payload = {
                "enrollee_id":       enrollee_id.strip(),
                "provider_id":       provider_id.strip(),
                "hospital_name":     (hospital_name or "").strip() or None,
                "encounter_date":    str(encounter_date),
                "consultation_type": "GP",
                "gp_type":           gp_type,
                "symptoms":          sym_codes,
            }
            with st.spinner("KLAIRE is evaluating the request..."):
                result, err = call_api(payload)

            if err:
                st.error(f"API error: {err}")
            else:
                st.session_state["result"] = result

    if st.session_state.get("result"):
        render_result(st.session_state["result"])


# ════════════════════════════════════════════════════════════════════════════════
# SPECIALIST FLOW
# ════════════════════════════════════════════════════════════════════════════════
elif flow == "SPECIALIST":
    st.markdown("### 🏥  Specialist Consultation")
    st.caption(
        "A specialist visit requires a GP referral within the last **7 days**. "
        "Select the specialist and the enrollee's diagnosis below."
    )

    spec_options = load_specialist_codes()
    diag_options = load_diagnoses()

    with st.container(border=True):
        # ── STEP 2 — Specialist selection ─────────────────────────────────────
        st.markdown("**Step 2 — Select Specialist**")
        if spec_options:
            spec_display = st.selectbox(
                "Specialist",
                options=["— Select specialist —"] + list(spec_options.keys()),
                key="spec_sel",
                label_visibility="collapsed",
            )
            spec_code = spec_options.get(spec_display, "") if spec_display != "— Select specialist —" else ""
        else:
            st.warning("Specialist codes unavailable. Check API connection.")
            spec_code_raw = st.text_input("Enter specialist code manually", placeholder="CONS035", key="spec_manual")
            spec_code     = spec_code_raw.strip().upper()

        st.markdown('<div class="divider-line"></div>', unsafe_allow_html=True)

        # ── STEP 3 — Diagnosis selection ──────────────────────────────────────
        st.markdown("**Step 3 — Select Diagnosis**")
        if diag_options:
            diag_display = st.selectbox(
                "Diagnosis (one only)",
                options=["— Select diagnosis —"] + list(diag_options.keys()),
                key="diag_sel",
                label_visibility="collapsed",
            )
            diag_info = diag_options.get(diag_display) if diag_display != "— Select diagnosis —" else None
        else:
            st.warning("Diagnosis list unavailable. Enter manually.")
            diag_raw  = st.text_input("Diagnosis code", placeholder="e.g. B509", key="diag_manual")
            diag_info = {"code": diag_raw.strip().upper(), "name": diag_raw.strip()} if diag_raw.strip() else None

    st.markdown("---")

    spec_submit = st.button("▶ Submit Specialist Request", type="primary", key="spec_btn")

    if spec_submit:
        errors = []
        if not (enrollee_id or "").strip():
            errors.append("Enrollee ID is required (fill in the sidebar).")
        if not (provider_id or "").strip():
            errors.append("Provider ID is required (fill in the sidebar).")
        if not spec_code:
            errors.append("Please select a specialist.")
        if not diag_info:
            errors.append("Please select a diagnosis.")
        if errors:
            for e in errors:
                st.error(e)
        else:
            payload = {
                "enrollee_id":       enrollee_id.strip(),
                "provider_id":       provider_id.strip(),
                "hospital_name":     (hospital_name or "").strip() or None,
                "encounter_date":    str(encounter_date),
                "consultation_type": "SPECIALIST",
                "specialist_code":   spec_code,
                "diagnosis_code":    diag_info["code"],
                "diagnosis_name":    diag_info["name"],
            }
            with st.spinner("KLAIRE is evaluating the request..."):
                result, err = call_api(payload)

            if err:
                st.error(f"API error: {err}")
            else:
                st.session_state["result"] = result

    if st.session_state.get("result"):
        render_result(st.session_state["result"])
