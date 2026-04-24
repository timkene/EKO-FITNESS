"""
CLEARLINE — Agent Review Portal
=================================
View PA submissions by date → enrollee bucket.
Each bucket shows: all sent, denied with reasons, approved, totals.
Pending Review items can be acted on inline.
"""

import streamlit as st
import requests
from datetime import datetime
from collections import defaultdict

API = "http://localhost:8000"

st.set_page_config(page_title="Agent Review Portal", page_icon="🛡️", layout="wide")

st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background: #f0f4f8; }

/* Status pills */
.pill-approved  { display:inline-block; background:#dcfce7; color:#166534;
                  font-weight:700; font-size:11px; padding:2px 8px;
                  border-radius:99px; border:1px solid #bbf7d0; }
.pill-denied    { display:inline-block; background:#fee2e2; color:#991b1b;
                  font-weight:700; font-size:11px; padding:2px 8px;
                  border-radius:99px; border:1px solid #fecaca; }
.pill-pending   { display:inline-block; background:#fef9c3; color:#854d0e;
                  font-weight:700; font-size:11px; padding:2px 8px;
                  border-radius:99px; border:1px solid #fde68a; }

/* Enrollee card header */
.enr-header {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 14px 18px;
    margin-bottom: 4px;
}
.enr-header .enr-id  { font-weight:700; font-size:15px; color:#1e293b; }
.enr-header .enr-sub { font-size:12px; color:#64748b; margin-top:2px; }

/* Line item row */
.line-row { padding: 6px 0; border-bottom: 1px solid #f1f5f9; font-size:13px; }
.line-row:last-child { border-bottom: none; }

/* Section label */
.sec-label { font-size:11px; font-weight:700; letter-spacing:.05em;
             color:#64748b; text-transform:uppercase; margin: 10px 0 4px; }

.approve  { background:#dcfce7; border-left:5px solid #16a34a; padding:12px 16px; border-radius:8px; }
.deny     { background:#fee2e2; border-left:5px solid #dc2626; padding:12px 16px; border-radius:8px; }
.pending  { background:#fef9c3; border-left:5px solid #ca8a04; padding:12px 16px; border-radius:8px; }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
c1, c2 = st.columns([4, 1])
with c1:
    st.markdown("## 🛡️ Clearline — Agent Review Portal")
    st.caption("PA submissions grouped by date and enrollee")
with c2:
    reviewer_name = st.text_input("Your name", value="Casey", key="reviewer")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_browse, tab_pending, tab_stats = st.tabs(["📅 Browse by Date", "⏳ Pending Review", "📊 Stats"])


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

STATUS_ICON = {
    "AUTO_APPROVED":  "🟢",
    "HUMAN_APPROVED": "🟢",
    "AUTO_DENIED":    "🔴",
    "HUMAN_DENIED":   "🔴",
    "PENDING_REVIEW": "🟡",
}

def fmt_date(iso: str) -> str:
    try:
        return datetime.fromisoformat(str(iso)).strftime("%d %b %Y")
    except:
        return str(iso)[:10]

def fmt_dt(iso: str) -> str:
    try:
        return datetime.fromisoformat(str(iso)).strftime("%d %b %Y %H:%M")
    except:
        return str(iso)

def is_approved(status: str) -> bool:
    return status in ("AUTO_APPROVED", "HUMAN_APPROVED")

def is_denied(status: str) -> bool:
    return status in ("AUTO_DENIED", "HUMAN_DENIED")

def pill(status: str) -> str:
    cls = ("approved" if is_approved(status)
           else "denied" if is_denied(status)
           else "pending")
    return f'<span class="pill-{cls}">{STATUS_ICON.get(status,"")} {status.replace("_"," ")}</span>'

def fetch_history(limit=500, status=None, enrollee_id=None) -> list:
    params = {"limit": limit}
    if status:       params["status"]      = status
    if enrollee_id:  params["enrollee_id"] = enrollee_id
    resp = requests.get(f"{API}/api/v1/history", params=params, timeout=20)
    resp.raise_for_status()
    return resp.json().get("requests", [])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — BROWSE BY DATE → ENROLLEE
# ══════════════════════════════════════════════════════════════════════════════
with tab_browse:

    # ── Filters ──────────────────────────────────────────────────────────────
    f1, f2, f3 = st.columns([2, 2, 1])
    with f1:
        flt_enrollee = st.text_input("Filter by Enrollee ID", placeholder="Leave blank for all",
                                     key="browse_enr")
    with f2:
        flt_status = st.selectbox("Filter by Status",
                                  ["All", "AUTO_APPROVED", "AUTO_DENIED",
                                   "PENDING_REVIEW", "HUMAN_APPROVED", "HUMAN_DENIED"],
                                  key="browse_status")
    with f3:
        st.markdown("<br>", unsafe_allow_html=True)
        load_btn = st.button("📋 Load", use_container_width=True, key="browse_load")

    if "browse_rows" not in st.session_state:
        st.session_state.browse_rows = None

    if load_btn:
        try:
            rows = fetch_history(
                limit=500,
                status=flt_status if flt_status != "All" else None,
                enrollee_id=flt_enrollee.strip() or None,
            )
            st.session_state.browse_rows = rows
        except Exception as e:
            st.error(f"Failed to load: {e}")

    rows = st.session_state.browse_rows
    if rows is None:
        st.info("Use the filters above and click Load.")
    elif not rows:
        st.info("No records found.")
    else:
        # ── Group: encounter_date → enrollee_id → [items] ────────────────────
        by_date = defaultdict(lambda: defaultdict(list))
        for row in rows:
            date_key    = str(row.get("encounter_date", ""))[:10]
            enrollee_key = row.get("enrollee_id", "—")
            by_date[date_key][enrollee_key].append(row)

        sorted_dates = sorted(by_date.keys(), reverse=True)

        for date_key in sorted_dates:
            enrollees = by_date[date_key]

            # Date-level summary
            total_sent     = sum(len(v) for v in enrollees.values())
            total_approved = sum(sum(1 for r in v if is_approved(r["status"]))
                                 for v in enrollees.values())
            total_denied   = sum(sum(1 for r in v if is_denied(r["status"]))
                                 for v in enrollees.values())
            total_pending  = sum(sum(1 for r in v if r["status"] == "PENDING_REVIEW")
                                 for v in enrollees.values())

            st.markdown(
                f"### 📅 {fmt_date(date_key)}"
                f"&nbsp;&nbsp;<span style='font-size:13px;color:#64748b;'>"
                f"{len(enrollees)} enrollee(s) &nbsp;·&nbsp; "
                f"🟢 {total_approved} approved &nbsp;·&nbsp; "
                f"🔴 {total_denied} denied &nbsp;·&nbsp; "
                f"🟡 {total_pending} pending</span>",
                unsafe_allow_html=True,
            )

            for enrollee_id, items in sorted(enrollees.items()):
                approved_items = [r for r in items if is_approved(r["status"])]
                denied_items   = [r for r in items if is_denied(r["status"])]
                pending_items  = [r for r in items if r["status"] == "PENDING_REVIEW"]

                # Compute totals
                total_amount = sum(
                    r.get("total_amount", 0) or 0
                    for r in approved_items
                )
                age    = items[0].get("enrollee_age", "—")
                gender = items[0].get("enrollee_gender", "—")
                hosp   = items[0].get("hospital_name") or "—"

                label = (
                    f"👤 **{enrollee_id}** — Age {age}, {gender} &nbsp;|&nbsp; "
                    f"🏥 {hosp} &nbsp;|&nbsp; "
                    f"Sent: **{len(items)}** &nbsp; "
                    f"🟢 {len(approved_items)} &nbsp; "
                    f"🔴 {len(denied_items)} &nbsp; "
                    f"🟡 {len(pending_items)}"
                    + (f" &nbsp;|&nbsp; ✅ ₦{total_amount:,.2f}" if total_amount else "")
                )

                with st.expander(label, expanded=(len(pending_items) > 0)):

                    # ── ALL SENT ─────────────────────────────────────────────
                    st.markdown(
                        f'<div class="sec-label">All Sent ({len(items)})</div>',
                        unsafe_allow_html=True,
                    )
                    for r in items:
                        pcode = r.get("procedure_code", "")
                        pname = r.get("procedure_name", "")
                        dcode = r.get("diagnosis_code", "")
                        dname = r.get("diagnosis_name", "")
                        st.markdown(
                            f'<div class="line-row">'
                            f'{pill(r["status"])} &nbsp; '
                            f'<b>{pcode}</b> {pname} &nbsp;·&nbsp; '
                            f'{dcode} {dname}'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                    # ── APPROVED ─────────────────────────────────────────────
                    if approved_items:
                        st.markdown(
                            f'<div class="sec-label">Approved ({len(approved_items)})'
                            + (f" — Total ₦{total_amount:,.2f}" if total_amount else "")
                            + '</div>',
                            unsafe_allow_html=True,
                        )
                        for r in approved_items:
                            ap  = r.get("adjusted_price")
                            aq  = r.get("adjusted_quantity", 1)
                            tot = r.get("total_amount")
                            price_info = ""
                            if ap:
                                price_info = f" &nbsp;·&nbsp; ₦{ap:,.2f} × {aq}"
                                if tot:
                                    price_info += f" = ₦{tot:,.2f}"
                            st.markdown(
                                f'<div class="line-row" style="color:#166534;">'
                                f'✅ <b>{r.get("procedure_code")}</b> {r.get("procedure_name","")}'
                                f'{price_info}'
                                f'</div>',
                                unsafe_allow_html=True,
                            )

                    # ── DENIED ───────────────────────────────────────────────
                    if denied_items:
                        st.markdown(
                            f'<div class="sec-label">Denied ({len(denied_items)})</div>',
                            unsafe_allow_html=True,
                        )
                        for r in denied_items:
                            reasoning = r.get("reasoning", "—")
                            # Trim long reasoning to first sentence for inline view
                            short_reason = reasoning.split("|")[0].strip()[:120]
                            if len(reasoning) > len(short_reason):
                                short_reason += "…"
                            st.markdown(
                                f'<div class="line-row" style="color:#991b1b;">'
                                f'❌ <b>{r.get("procedure_code")}</b> {r.get("procedure_name","")}'
                                f'<br><span style="font-size:11px;color:#64748b;">{short_reason}</span>'
                                f'</div>',
                                unsafe_allow_html=True,
                            )

                    # ── PENDING REVIEW (with action form) ────────────────────
                    if pending_items:
                        st.markdown(
                            f'<div class="sec-label">Pending Review ({len(pending_items)})</div>',
                            unsafe_allow_html=True,
                        )
                        for r in pending_items:
                            rid   = r.get("request_id", "")
                            conf  = r.get("confidence", 0)
                            ai_rec = r.get("decision", "—")
                            rec_color = "#16a34a" if ai_rec == "APPROVE" else "#dc2626"

                            st.markdown(
                                f'<div style="background:#fef9c3; border-radius:8px; '
                                f'padding:10px 14px; margin:6px 0;">'
                                f'<b>{r.get("procedure_code")}</b> {r.get("procedure_name","")} '
                                f'&nbsp;·&nbsp; {r.get("diagnosis_code")} {r.get("diagnosis_name","")}'
                                f'<br><span style="font-size:12px;color:{rec_color};font-weight:700;">'
                                f'AI: {ai_rec} ({conf}%)</span>'
                                f'<br><span style="font-size:12px;color:#475569;">'
                                f'{r.get("reasoning","")[:200]}</span>'
                                f'</div>',
                                unsafe_allow_html=True,
                            )

                            # Rule breakdown
                            with st.expander(f"📋 Rules — {rid}", expanded=False):
                                try:
                                    det   = requests.get(f"{API}/api/v1/requests/{rid}", timeout=10).json()
                                    rules = det.get("rules", [])
                                    for rule in rules:
                                        icon = "✅" if rule["passed"] else "❌"
                                        src  = rule.get("source","?").upper()
                                        cr   = rule.get("confidence", 0)
                                        st.markdown(
                                            f"{icon} **{rule['rule_name']}** `[{src}]` {cr}%  \n"
                                            f"_{rule.get('reasoning','')}_"
                                        )
                                except:
                                    st.warning("Could not load rules.")

                            # Review form
                            with st.form(key=f"browse_review_{rid}"):
                                fa, fb, fc = st.columns([2, 2, 3])
                                with fa:
                                    action = st.radio(
                                        "Action", ["CONFIRM (agree with AI)", "OVERRIDE (disagree)"],
                                        key=f"ba_{rid}", horizontal=False,
                                    )
                                with fb:
                                    override_dec = st.radio(
                                        "Override to:", ["APPROVE", "DENY"],
                                        key=f"bo_{rid}",
                                        disabled=("CONFIRM" in action),
                                    )
                                with fc:
                                    review_notes = st.text_area("Notes", key=f"bn_{rid}", height=80)
                                sub = st.form_submit_button("✅ Submit Review", type="primary")

                            if sub:
                                is_confirm = "CONFIRM" in action
                                payload = {
                                    "action":            "CONFIRM" if is_confirm else "OVERRIDE",
                                    "override_decision": None if is_confirm else override_dec,
                                    "reviewed_by":       reviewer_name or "Agent",
                                    "notes":             review_notes or None,
                                }
                                try:
                                    res = requests.post(
                                        f"{API}/api/v1/review/{rid}",
                                        json=payload, timeout=30,
                                    ).json()
                                    final = res.get("final_decision", "")
                                    msg   = res.get("message", "")
                                    if final == "APPROVE":
                                        st.markdown(f'<div class="approve">✅ <b>APPROVED</b> — {msg}</div>',
                                                    unsafe_allow_html=True)
                                    else:
                                        st.markdown(f'<div class="deny">❌ <b>DENIED</b> — {msg}</div>',
                                                    unsafe_allow_html=True)
                                    # Invalidate cache so next Load shows fresh data
                                    st.session_state.browse_rows = None
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Review failed: {e}")

            st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PENDING REVIEW QUEUE (original focused view)
# ══════════════════════════════════════════════════════════════════════════════
with tab_pending:
    col_refresh, col_count = st.columns([1, 3])
    with col_refresh:
        if st.button("🔄 Refresh Queue", use_container_width=True):
            st.rerun()

    try:
        resp    = requests.get(f"{API}/api/v1/pending", params={"limit": 50}, timeout=15)
        resp.raise_for_status()
        data    = resp.json()
        pending = data.get("requests", [])
        total   = data.get("total_pending", 0)
    except Exception as e:
        st.error(f"Cannot reach API: {e}")
        st.stop()

    with col_count:
        st.markdown(f"### {total} request(s) awaiting review")

    if not pending:
        st.success("✅ No pending requests — queue is clear!")
    else:
        for req in pending:
            rid       = req.get("request_id", "")
            ai_rec    = req.get("decision", req.get("ai_recommendation", "—"))
            conf      = req.get("confidence", 0)
            enrollee  = req.get("enrollee_id", "")
            age       = req.get("enrollee_age", "—")
            gender    = req.get("enrollee_gender", "—")
            hospital  = req.get("hospital_name") or "—"
            reasoning = req.get("reasoning", "—")
            enc_date  = req.get("encounter_date", "")
            rec_color = "#16a34a" if ai_rec == "APPROVE" else "#dc2626"

            st.markdown(f"""
<div style="background:white;border:1px solid #e2e8f0;border-radius:10px;padding:14px 18px;margin-bottom:10px;">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
    <span style="font-weight:700;font-size:15px;">{enrollee} &nbsp;
      <span style="font-weight:400;font-size:12px;color:#64748b;">Age {age} · {gender} · {hospital} · {enc_date}</span>
    </span>
    <span style="color:{rec_color};font-weight:700;">AI: {ai_rec} ({conf}%)</span>
  </div>
  <div style="background:#f8fafc;padding:8px 12px;border-radius:6px;margin-bottom:8px;">
    <b>{req.get("procedure_code")}</b> {req.get("procedure_name","—")} &nbsp;·&nbsp;
    {req.get("diagnosis_code")} {req.get("diagnosis_name","—")}
  </div>
  <div style="font-size:12px;color:#475569;font-style:italic;">💬 {reasoning[:300]}</div>
  <div style="font-size:11px;color:#94a3b8;margin-top:4px;">ID: {rid}</div>
</div>
""", unsafe_allow_html=True)

            with st.expander(f"📋 Rule Breakdown — {rid}"):
                try:
                    det   = requests.get(f"{API}/api/v1/requests/{rid}", timeout=15).json()
                    rules = det.get("rules", [])
                    if rules:
                        for rule in rules:
                            icon = "✅" if rule["passed"] else "❌"
                            src  = rule.get("source","?").upper()
                            cr   = rule.get("confidence", 0)
                            st.markdown(
                                f"{icon} **{rule['rule_name']}** `[{src}]` {cr}%  \n"
                                f"_{rule.get('reasoning','')}_"
                            )
                    else:
                        st.info("No rule details.")
                except:
                    st.warning("Could not load rules.")

            with st.form(key=f"pq_review_{rid}"):
                fc1, fc2, fc3 = st.columns([2, 2, 3])
                with fc1:
                    action = st.radio("Action",
                                      ["CONFIRM (agree with AI)", "OVERRIDE (disagree)"],
                                      key=f"pq_action_{rid}", horizontal=False)
                with fc2:
                    override_dec = st.radio("Override to:", ["APPROVE", "DENY"],
                                            key=f"pq_over_{rid}",
                                            disabled=("CONFIRM" in action))
                with fc3:
                    review_notes = st.text_area("Notes (optional)",
                                                key=f"pq_notes_{rid}", height=90)
                submitted = st.form_submit_button("✅ Submit Review", type="primary",
                                                   use_container_width=True)

            if submitted:
                is_confirm = "CONFIRM" in action
                payload = {
                    "action":            "CONFIRM" if is_confirm else "OVERRIDE",
                    "override_decision": None if is_confirm else override_dec,
                    "reviewed_by":       reviewer_name or "Agent",
                    "notes":             review_notes or None,
                }
                try:
                    r2  = requests.post(f"{API}/api/v1/review/{rid}", json=payload, timeout=30)
                    r2.raise_for_status()
                    res = r2.json()
                    final = res.get("final_decision", "")
                    msg   = res.get("message", "")
                    if final == "APPROVE":
                        st.markdown(f'<div class="approve">✅ <b>APPROVED</b> — {msg}</div>',
                                    unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="deny">❌ <b>DENIED</b> — {msg}</div>',
                                    unsafe_allow_html=True)
                    st.rerun()
                except Exception as e:
                    st.error(f"Review failed: {e}")

            st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — STATS
# ══════════════════════════════════════════════════════════════════════════════
with tab_stats:
    if st.button("📊 Load Stats"):
        try:
            r = requests.get(f"{API}/api/v1/stats", timeout=15).json()
        except Exception as e:
            st.error(f"Error: {e}")
            st.stop()

        queue      = r.get("queue", {})
        today      = r.get("today", {})
        ls         = r.get("learning_summary", {})
        automation = r.get("automation_rate", 0)

        st.markdown("### Overall Queue")
        labels = ["AUTO_APPROVED","AUTO_DENIED","PENDING_REVIEW","HUMAN_APPROVED","HUMAN_DENIED"]
        icons  = ["🟢","🔴","🟡","🟢","🔴"]
        cols   = st.columns(5)
        for col, label, icon in zip(cols, labels, icons):
            col.metric(f"{icon} {label.replace('_',' ')}", queue.get(label, 0))

        st.markdown("### Today")
        t_cols = st.columns(5)
        for col, label, icon in zip(t_cols, labels, icons):
            col.metric(f"{icon} {label.replace('_',' ')}", today.get(label, 0))

        st.markdown("### Learning Engine")
        l1, l2, l3 = st.columns(3)
        l1.metric("Automation Rate", f"{automation}%")
        l2.metric("Learning Entries", ls.get("total_entries", 0))
        l3.metric("AI Calls Saved", ls.get("total_ai_calls_saved", 0))

        with st.expander("Learning Table Detail"):
            import pandas as pd
            learning = r.get("learning", {})
            df = pd.DataFrame([
                {"Table": k, "Entries": v["entries"], "Total Usage": v["total_usage"]}
                for k, v in learning.items()
            ])
            st.dataframe(df, use_container_width=True, hide_index=True)
