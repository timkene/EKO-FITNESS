"""
CLEARLINE — Hospital PA Request Portal
=======================================
Submit pre-authorization requests and track their status.
Talks to the vetting API running on localhost:8000
"""

import streamlit as st
import requests
from datetime import date, datetime

API = "http://localhost:8000"

st.set_page_config(page_title="Hospital PA Portal", page_icon="🏥", layout="wide")

st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background: #f6f7f8; }
.approve  { background:#dcfce7; border-left:4px solid #16a34a; padding:10px 14px; border-radius:6px; }
.deny     { background:#fee2e2; border-left:4px solid #dc2626; padding:10px 14px; border-radius:6px; }
.pending  { background:#fef9c3; border-left:4px solid #ca8a04; padding:10px 14px; border-radius:6px; }
.rule-pass { color:#16a34a; font-weight:600; }
.rule-fail { color:#dc2626; font-weight:600; }
.line-card {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 8px;
}
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Data Source")
    st.caption("☁️ **MotherDuck** (always active)")
    st.divider()
    st.caption("MongoDB (PROCEDURE_DIAGNOSIS) always active")

# ── Header ────────────────────────────────────────────────────────────────────
col1, col2 = st.columns([3, 1])
with col1:
    st.markdown("## 🏥 Clearline — PA Request Portal")
    st.caption("Submit pre-authorization requests for review")
with col2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_submit, tab_track, tab_history = st.tabs(["📋 Submit Request", "🔍 Track Request", "📜 History"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — SUBMIT (BULK)
# ══════════════════════════════════════════════════════════════════════════════
with tab_submit:
    st.markdown("### New PA Request")
    st.caption(
        "Enter the enrollee details, then list **all procedures and their diagnoses** "
        "for this visit. One PA number covers the entire submission."
    )

    # ── Enrollee details ──────────────────────────────────────────────────────
    h1, h2, h3, h4 = st.columns([3, 2, 2, 2])
    with h1:
        enrollee_id   = st.text_input("Enrollee ID *", placeholder="e.g. CL/OCTA/723449/2023-A",
                                       key="enr_id")
    with h2:
        encounter_date = st.date_input("Encounter Date", value=date.today(), key="enc_date")
    with h3:
        hospital_name  = st.text_input("Hospital Name", placeholder="General Hospital Lagos",
                                        key="hosp_name")
    with h4:
        provider_id    = st.text_input("Provider ID *", placeholder="e.g. 118",
                                        key="prov_id")

    encounter_type = st.radio(
        "Encounter Type",
        options=["OUTPATIENT", "INPATIENT"],
        index=0,
        horizontal=True,
        key="enc_type",
        help="OUTPATIENT = walk-in visit. INPATIENT = patient is being admitted.",
    )

    st.divider()

    # ── Dynamic procedure rows ────────────────────────────────────────────────
    if "procedures" not in st.session_state:
        st.session_state.procedures = [
            {"procedure_code": "", "diagnosis_code": "", "price": "", "quantity": 1, "notes": ""}
        ]

    st.markdown("**Procedures & Diagnoses**")
    st.caption("Price = stated unit price (₦). Qty = number of units. Total = Price × Qty.")

    to_remove = None
    for i, row in enumerate(st.session_state.procedures):
        c1, c2, c3, c4, c5, c6 = st.columns([2, 2, 1.5, 0.8, 2, 0.5])
        with c1:
            val = st.text_input(
                "Procedure Code" if i == 0 else "\u200E",
                value=row["procedure_code"],
                placeholder="e.g. DRG2813",
                key=f"proc_{i}",
            )
            st.session_state.procedures[i]["procedure_code"] = val
        with c2:
            val = st.text_input(
                "Diagnosis Code" if i == 0 else "\u200E",
                value=row["diagnosis_code"],
                placeholder="e.g. MX1429",
                key=f"diag_{i}",
            )
            st.session_state.procedures[i]["diagnosis_code"] = val
        with c3:
            val = st.text_input(
                "Price ₦" if i == 0 else "\u200E",
                value=str(row.get("price", "")),
                placeholder="e.g. 5000",
                key=f"price_{i}",
            )
            st.session_state.procedures[i]["price"] = val
        with c4:
            val = st.number_input(
                "Qty" if i == 0 else "\u200E",
                min_value=1, value=int(row.get("quantity", 1)),
                key=f"qty_{i}",
            )
            st.session_state.procedures[i]["quantity"] = val
        with c5:
            val = st.text_input(
                "Clinical Notes (optional)" if i == 0 else "\u200E",
                value=row.get("notes", ""),
                placeholder="Optional...",
                key=f"notes_{i}",
            )
            st.session_state.procedures[i]["notes"] = val
        with c6:
            if i == 0:
                st.markdown("\u200E")
            if len(st.session_state.procedures) > 1:
                if st.button("🗑️", key=f"del_{i}", help="Remove this row"):
                    to_remove = i

    if to_remove is not None:
        st.session_state.procedures.pop(to_remove)
        st.rerun()

    ba, bb = st.columns([1, 5])
    with ba:
        if st.button("➕ Add Procedure", use_container_width=True):
            st.session_state.procedures.append(
                {"procedure_code": "", "diagnosis_code": "", "price": "", "quantity": 1, "notes": ""}
            )
            st.rerun()

    st.divider()

    submitted = st.button("🚀 Submit for Validation", type="primary", use_container_width=False)

    if submitted:
        if not enrollee_id.strip():
            st.error("Enrollee ID is required.")
        elif not provider_id.strip():
            st.error("Provider ID is required.")
        else:
            procs = [
                p for p in st.session_state.procedures
                if p["procedure_code"].strip() and p["diagnosis_code"].strip()
            ]
            if not procs:
                st.error("At least one procedure code and diagnosis code are required.")
            else:
                payload = {
                    "enrollee_id":    enrollee_id.strip(),
                    "encounter_date": str(encounter_date),
                    "hospital_name":  hospital_name.strip() or None,
                    "provider_id":    provider_id.strip() or None,
                    "encounter_type": encounter_type,
                    "procedures": [
                        {
                            "procedure_code": p["procedure_code"].strip().upper(),
                            "diagnosis_code": p["diagnosis_code"].strip().upper(),
                            "price":          float(p["price"]) if str(p.get("price", "")).strip() else None,
                            "quantity":       int(p.get("quantity", 1)),
                            "notes":          p["notes"].strip() or None,
                        }
                        for p in procs
                    ],
                }

                with st.spinner(f"Validating {len(procs)} procedure(s)..."):
                    try:
                        resp = requests.post(
                            f"{API}/api/v1/validate/bulk",
                            json=payload,
                            timeout=300,
                        )
                        resp.raise_for_status()
                        r = resp.json()
                    except Exception as e:
                        st.error(f"API error: {e}")
                        st.stop()

                # ── Overall banner ─────────────────────────────────────────
                line_items   = r.get("line_items", [])
                total_amt    = r.get("total_approved_amount", 0)
                n_approved   = sum(1 for l in line_items if l.get("status") == "AUTO_APPROVED")
                n_denied     = sum(1 for l in line_items if l.get("status") == "AUTO_DENIED")
                n_total      = len(line_items)
                denied_amt   = sum(
                    (l.get("total_amount") or 0)
                    or ((l.get("stated_price") or 0) * (l.get("stated_quantity") or 1))
                    for l in line_items if l.get("status") == "AUTO_DENIED"
                )

                if n_approved == n_total:
                    st.markdown(
                        f'<div class="approve">✅ <b>BATCH AUTO APPROVED</b> — '
                        f'All {n_total} procedure(s) approved — '
                        f'Total: ₦{total_amt:,.2f}</div>',
                        unsafe_allow_html=True,
                    )
                elif n_approved > 0:
                    denied_str = f" &nbsp;·&nbsp; ❌ Denied: ₦{denied_amt:,.2f}" if denied_amt else f" &nbsp;·&nbsp; {n_denied} denied"
                    st.markdown(
                        f'<div class="approve">✅ <b>BATCH PARTIALLY APPROVED</b> — '
                        f'{n_approved} of {n_total} procedure(s) approved &nbsp;·&nbsp; '
                        f'✅ Approved: ₦{total_amt:,.2f}'
                        f'{denied_str}</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    denied_str = f" — Denied: ₦{denied_amt:,.2f}" if denied_amt else ""
                    st.markdown(
                        f'<div class="deny">❌ <b>BATCH AUTO DENIED</b> — '
                        f'All {n_total} procedure(s) denied{denied_str}</div>',
                        unsafe_allow_html=True,
                    )

                st.markdown(
                    f"**Batch ID:** `{r.get('batch_id')}` — "
                    f"save this to track all requests in this batch"
                )

                # Enrollee + totals summary
                ea, eg = r.get("enrollee_age", "—"), r.get("enrollee_gender", "—")
                col_a, col_b, col_c = st.columns(3)
                col_a.metric("Enrollee Age", ea)
                col_b.metric("Gender", eg)
                col_c.metric("Total Approved Amount", f"₦{total_amt:,.2f}" if total_amt else "—")

                # ── Group lines by pipeline stage ──────────────────────────
                all_lines = r.get("line_items", [])
                dropped15 = [l for l in all_lines if l.get("pipeline_stage") == "DROPPED_STEP15"]
                dropped2  = [l for l in all_lines if l.get("pipeline_stage") == "DROPPED_STEP2"]
                dropped3  = [l for l in all_lines if l.get("pipeline_stage") == "DROPPED_STEP3"]
                passed    = [l for l in all_lines if l.get("pipeline_stage") == "PASSED"]

                def render_line(line, idx):
                    line_status = line.get("status", "")
                    icon = ("✅" if "APPROVED" in line_status
                            else "❌" if "DENIED" in line_status else "⏳")
                    drop = f"  — *{line.get('drop_reason','')}*" if line.get("drop_reason") else ""
                    header = (
                        f"{icon} **{idx}.** `{line['procedure_code']}` {line.get('procedure_name','')}  "
                        f"| `{line['diagnosis_code']}` {line.get('diagnosis_name','')}  "
                        f"→ **{line_status}** ({line.get('confidence',0)}%){drop}"
                    )
                    with st.expander(header, expanded=(line_status != "AUTO_APPROVED")):
                        st.markdown(f"**Request ID:** `{line['request_id']}`")
                        st.markdown(f"**Reasoning:** {line.get('reasoning','—')}")

                        # Tariff / quantity info (for passed lines)
                        sp  = line.get("stated_price")
                        ap  = line.get("adjusted_price")
                        tp  = line.get("tariff_price")
                        sq  = line.get("stated_quantity", 1)
                        aq  = line.get("adjusted_quantity", sq)
                        mq  = line.get("max_allowed_quantity")
                        tot = line.get("total_amount")
                        if sp is not None or tp is not None:
                            t1, t2, t3, t4 = st.columns(4)
                            t1.metric("Stated Price", f"₦{sp:,.2f}" if sp else "—")
                            t2.metric("Tariff Price", f"₦{tp:,.2f}" if tp else "No contract")
                            t3.metric("Approved Price", f"₦{ap:,.2f}" if ap else "—",
                                      delta=f"₦{ap-sp:,.2f}" if (ap and sp and ap != sp) else None)
                            t4.metric("Total Amount", f"₦{tot:,.2f}" if tot else "—")
                        if sq is not None:
                            q1, q2 = st.columns(2)
                            q1.metric("Stated Qty", sq)
                            aq_display = aq if aq is not None else sq
                            q2.metric("Approved Qty", aq_display,
                                      delta=str(aq_display - sq) if aq_display != sq else None,
                                      delta_color="inverse" if aq_display < sq else "normal")
                            if mq:
                                st.caption(f"Max allowed quantity: {mq}")

                        rules = line.get("rules", [])
                        if rules:
                            st.markdown("**Rules:**")
                            for rule in rules:
                                r_icon   = "✅" if rule["passed"] else "❌"
                                r_source = f"[{rule.get('source','?').upper()}]"
                                r_conf   = rule.get("confidence", 0)
                                st.markdown(
                                    f"{r_icon} **{rule['rule_name']}** {r_source} — {r_conf}%  \n"
                                    f"_{rule.get('reasoning','')}_"
                                )

                st.markdown("#### Line-by-Line Results")

                if passed:
                    st.markdown("**Approved / Pending Review**")
                    for idx, line in enumerate(passed, 1):
                        render_line(line, idx)

                if dropped3:
                    st.markdown("**Denied by Validation Rules (Step 3 — Age / Gender / Compatibility)**")
                    for idx, line in enumerate(dropped3, 1):
                        render_line(line, idx)

                if dropped15:
                    st.markdown("**Denied by Clinical Rules (Step 1.5 — Polypharmacy / Level-of-Care / Padding)**")
                    for idx, line in enumerate(dropped15, 1):
                        render_line(line, idx)

                if dropped2:
                    st.markdown("**Denied at Screening (Step 2 — Duplicate / Capitation)**")
                    for idx, line in enumerate(dropped2, 1):
                        render_line(line, idx)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — TRACK
# ══════════════════════════════════════════════════════════════════════════════
with tab_track:
    st.markdown("### Track a Request")
    rid = st.text_input("Enter Request ID", placeholder="e.g. a1b2c3d4e5f6")
    if st.button("🔍 Fetch Status", type="primary") and rid:
        try:
            resp = requests.get(f"{API}/api/v1/requests/{rid.strip()}", timeout=15)
            if resp.status_code == 404:
                st.warning("Request not found.")
            else:
                resp.raise_for_status()
                r = resp.json()
                status = r.get("status","")
                if "APPROVED" in status:
                    st.markdown(f'<div class="approve">✅ <b>{status}</b></div>', unsafe_allow_html=True)
                elif "DENIED" in status:
                    st.markdown(f'<div class="deny">❌ <b>{status}</b></div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="pending">⏳ <b>{status}</b> — pending agent review</div>', unsafe_allow_html=True)

                c1, c2, c3 = st.columns(3)
                c1.metric("Decision",   r.get("decision","—"))
                c2.metric("Confidence", f"{r.get('confidence',0)}%")
                c3.metric("Reviewed By", r.get("reviewed_by") or "—")

                st.markdown(f"**Reasoning:** {r.get('reasoning','—')}")
                if r.get("reviewed_at"):
                    st.caption(f"Reviewed at: {r['reviewed_at']}")
        except Exception as e:
            st.error(f"Error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — HISTORY
# ══════════════════════════════════════════════════════════════════════════════
with tab_history:
    st.markdown("### My Submitted Requests")

    f1, f2 = st.columns([2,1])
    with f1:
        flt_enrollee = st.text_input("Filter by Enrollee ID", placeholder="Leave blank for all")
    with f2:
        flt_status = st.selectbox("Filter by Status", ["All","AUTO_APPROVED","AUTO_DENIED","PENDING_REVIEW","HUMAN_APPROVED","HUMAN_DENIED"])

    if st.button("📋 Load History"):
        params: dict = {"limit": 100}
        if flt_enrollee.strip(): params["enrollee_id"] = flt_enrollee.strip()
        if flt_status != "All": params["status"] = flt_status
        try:
            resp = requests.get(f"{API}/api/v1/history", params=params, timeout=15)
            resp.raise_for_status()
            rows = resp.json().get("requests", [])
            if not rows:
                st.info("No requests found.")
            else:
                import pandas as pd
                df = pd.DataFrame(rows)
                cols = ["request_id","enrollee_id","procedure_code","procedure_name",
                        "diagnosis_code","status","decision","confidence","created_at"]
                if "batch_id" in df.columns:
                    cols = ["batch_id"] + cols
                df = df[[c for c in cols if c in df.columns]]
                rename = {
                    "batch_id":"Batch","request_id":"ID","enrollee_id":"Enrollee",
                    "procedure_code":"Proc Code","procedure_name":"Procedure",
                    "diagnosis_code":"Diag Code","status":"Status",
                    "decision":"Decision","confidence":"Conf%","created_at":"Submitted",
                }
                df = df.rename(columns=rename)
                status_color = {
                    "AUTO_APPROVED":"🟢","AUTO_DENIED":"🔴",
                    "PENDING_REVIEW":"🟡","HUMAN_APPROVED":"🟢","HUMAN_DENIED":"🔴",
                }
                df["Status"] = df["Status"].apply(lambda s: f"{status_color.get(s,'⚪')} {s}")
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"{len(df)} request(s)")
        except Exception as e:
            st.error(f"Error: {e}")
