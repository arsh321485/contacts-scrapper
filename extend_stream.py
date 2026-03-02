"""
╔══════════════════════════════════════════════════════════╗
║   SecureITLab Pipeline Dashboard — Streamlit             ║
║   Reads from MongoDB → secureitlab_job_pipeline          ║
╠══════════════════════════════════════════════════════════╣
║  Install: pip install streamlit pymongo python-dotenv    ║
║  Run:     streamlit run streamlit_dashboard.py           ║
╚══════════════════════════════════════════════════════════╝
"""

import streamlit as st
from pymongo import MongoClient
import json
import threading
import time
from datetime import datetime, timezone

# ── Thread-safe shared state ──────────────────────────────────────────────────
# st.session_state CANNOT be accessed from background threads — Streamlit
# raises "missing ScriptRunContext". We use plain Python module-level objects
# instead, and sync them into session_state from the main thread on each rerun.
_thread_logs   = []          # list[str]  — appended by background thread
_thread_result = [None]      # list[1]    — set by background thread when done
_thread_done   = [False]     # list[1]    — flag set when thread finishes
_thread_lock   = threading.Lock()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SecureITLab Pipeline",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS (light theme) ──────────────────────────────────────────────────
st.markdown("""
<style>

@import url('https://fonts.googleapis.com/css2?family=Syne:wght@500;600;700;800&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap');

:root {
    --bg:        #f4f7fb;
    --surface:   #ffffff;
    --surface2:  #eef2f7;
    --border:    #d9e2ec;
    --accent:    #2563eb;
    --accent2:   #7c3aed;
    --green:     #16a34a;
    --yellow:    #f59e0b;
    --red:       #dc2626;
    --text:      #0f172a;
    --muted:     #64748b;
}

html, body, [class*="css"] {
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif !important;
}

[data-testid="stSidebar"] {
    background: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color: var(--text) !important; }

.main .block-container { padding: 2rem 2rem 3rem !important; }

h1, h2, h3, h4 {
    font-family: 'Syne', sans-serif !important;
    color: var(--text) !important;
}

.sil-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 1.25rem 1.5rem;
    margin-bottom: 1rem;
    transition: all 0.25s ease;
}
.sil-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 22px rgba(0,0,0,0.05);
}
.sil-card-accent { border-left: 4px solid var(--accent); }

.metric-row { display:flex; gap:1rem; flex-wrap:wrap; margin-bottom:1.5rem; }
.metric-tile {
    flex:1; min-width:140px;
    background:var(--surface2);
    border:1px solid var(--border);
    border-radius:12px;
    padding:1rem; text-align:center;
    transition:all .25s ease;
}
.metric-tile:hover { transform:translateY(-3px); box-shadow:0 10px 24px rgba(0,0,0,0.06); }
.metric-tile .val { font-family:'Syne',sans-serif; font-size:2rem; font-weight:800; color:var(--accent); }
.metric-tile .lbl { font-size:.72rem; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; }

.badge { padding:.25rem .7rem; border-radius:20px; font-size:.72rem; font-weight:600; font-family:'DM Mono',monospace; }
.badge-green  { background:#ecfdf5; color:#15803d; }
.badge-yellow { background:#fffbeb; color:#b45309; }
.badge-red    { background:#fef2f2; color:#b91c1c; }
.badge-blue   { background:#eff6ff; color:#1d4ed8; }
.badge-purple { background:#f5f3ff; color:#6d28d9; }

.contact-card {
    background:var(--surface2); border:1px solid var(--border);
    border-radius:10px; padding:1rem; margin-bottom:.8rem;
}
.contact-name  { font-family:'Syne',sans-serif; font-weight:700; color:var(--text); }
.contact-title { color:var(--muted); font-size:.85rem; }
.contact-email { font-family:'DM Mono',monospace; color:var(--accent); font-size:.8rem; }

/* ✅ EMAIL BOX — light background so text is readable */
.email-box {
    background: #f8fafc;
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 1rem 1.25rem;
    font-size: .9rem;
    line-height: 1.65;
    white-space: pre-wrap;
    color: var(--text);
}
.email-subject { font-family:'Syne',sans-serif; font-weight:700; color:var(--accent); margin-bottom:.5rem; }

.section-label {
    font-family:'DM Mono',monospace; font-size:.72rem;
    text-transform:uppercase; letter-spacing:.12em;
    color:var(--accent); margin-bottom:.6rem;
}
.sil-divider { border-top:1px solid var(--border); margin:1rem 0; }

[data-testid="stExpander"] {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
}
[data-testid="stSelectbox"] > div,
[data-testid="stMultiSelect"] > div {
    background: var(--surface2) !important;
    border-color: var(--border) !important;
}
[data-testid="stTabs"] button {
    font-family:'Syne',sans-serif !important;
    font-weight:600 !important;
}
::-webkit-scrollbar { width:6px; }
::-webkit-scrollbar-thumb { background:var(--border); border-radius:3px; }

/* Pipeline live log box */
.pipeline-log {
    background: #0f172a;
    color: #e2e8f0;
    border-radius: 10px;
    padding: 1rem 1.25rem;
    font-family: 'DM Mono', monospace;
    font-size: .8rem;
    line-height: 1.6;
    max-height: 380px;
    overflow-y: auto;
    white-space: pre-wrap;
}

/* Service fit boxes in light theme */
.fit-box {
    border-radius: 8px;
    padding: .75rem;
    margin-bottom: .5rem;
    font-size: .85rem;
}

</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  Session state
# ══════════════════════════════════════════════════════════════════════════════
for _k, _v in [
    ("pipeline_running", False),
    ("pipeline_logs",    []),
    ("pipeline_result",  None),
    ("pipeline_done",    False),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ══════════════════════════════════════════════════════════════════════════════
#  MongoDB helpers
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_db():
    URI = st.secrets.get("MONGO_URI", "mongodb://localhost:27017")
    DB  = st.secrets.get("MONGO_DB",  "secureitlab_job_pipeline")
    client = MongoClient(URI, serverSelectionTimeoutMS=6000)
    return client[DB]

@st.cache_data(ttl=60)
def load_all_jobs():
    db = get_db()
    return list(db.jobs.find({}, {
        "_id": 1, "company": 1, "role": 1, "job_number": 1,
        "opp_score": 1, "contacts_found": 1, "pipeline_ok": 1,
        "coverage_score": 1, "run_at": 1, "contact_domain": 1,
    }))

@st.cache_data(ttl=60)
def load_job(job_id):
    from bson import ObjectId
    return get_db().jobs.find_one({"_id": ObjectId(job_id)})


# ══════════════════════════════════════════════════════════════════════════════
#  Render helpers
# ══════════════════════════════════════════════════════════════════════════════

def badge(text, color="blue"):
    return f'<span class="badge badge-{color}">{text}</span>'

def safe_str(val, limit=300):
    if val is None: return "—"
    s = str(val)
    return s[:limit] + "…" if len(s) > limit else s

def as_dict(raw):
    if isinstance(raw, dict): return raw
    if isinstance(raw, list): return next((x for x in raw if isinstance(x, dict)), {})
    return {}

def render_json_pretty(data, title=""):
    if not data: return
    with st.expander(f"📄 Raw JSON — {title}", expanded=False):
        st.code(json.dumps(data, indent=2, default=str), language="json")

def render_qa_block(data, label):
    if not data:
        st.markdown(f'<div class="sil-card"><b>{label}</b> — <i>No data</i></div>', unsafe_allow_html=True)
        return
    data = as_dict(data)
    if not data: return

    passed    = data.get("passed") or data.get("Passed") or False
    rec       = data.get("recommendation") or data.get("Recommendation", "")
    issues    = data.get("issues") or data.get("Issues") or []
    checklist = data.get("checklist") or data.get("Checklist") or []
    color     = "green" if passed else "yellow"
    status    = "✅ APPROVED" if passed else "⚠️ NEEDS REWORK"

    html = f"""
    <div class="sil-card sil-card-accent">
      <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:.75rem">
        <span style="font-family:'Syne',sans-serif;font-weight:700;font-size:1rem">{label}</span>
        {badge(status, color)}
      </div>"""
    if rec:
        html += f'<div style="font-size:.85rem;color:var(--muted);margin-bottom:.6rem">📝 {rec}</div>'
    if checklist:
        html += '<div style="font-size:.82rem;margin-bottom:.5rem">'
        for item in (checklist if isinstance(checklist, list) else [checklist]):
            if isinstance(item, dict):
                ip   = item.get("pass") or item.get("passed") or item.get("status","") == "pass"
                nm   = item.get("item") or item.get("name") or item.get("check","")
                nt   = item.get("reason") or item.get("note") or item.get("issue","")
                html += f'<div style="margin:.25rem 0">{"✅" if ip else "❌"} <b>{nm}</b>'
                if nt: html += f' — <span style="color:var(--muted)">{str(nt)[:120]}</span>'
                html += '</div>'
        html += '</div>'
    if issues:
        html += '<div style="margin-top:.5rem">'
        for iss in (issues if isinstance(issues, list) else [issues])[:4]:
            txt = iss if isinstance(iss, str) else json.dumps(iss)
            html += f'<div style="font-size:.8rem;color:#f59e0b;margin:.2rem 0">• {txt[:200]}</div>'
        html += '</div>'
    st.markdown(html + '</div>', unsafe_allow_html=True)

def render_contacts(contacts, title="Contacts"):
    if not contacts: st.info("No contacts found for this job."); return
    pri_color = {"Primary":"red","Secondary":"yellow","Tertiary":"blue","General":"purple"}
    st.markdown(f'<div class="section-label">👥 {title} ({len(contacts)})</div>', unsafe_allow_html=True)
    cols = st.columns(2)
    for i, c in enumerate(contacts):
        col = cols[i % 2]
        prio = c.get("priority", "General")
        email = c.get("email", ""); li = c.get("linkedin_url", "")
        patterns = c.get("email_patterns", []); src = c.get("source", "")
        with col:
            html = f"""<div class="contact-card">
              <div style="display:flex;justify-content:space-between;align-items:flex-start">
                <div>
                  <div class="contact-name">{c.get('name','—')}</div>
                  <div class="contact-title">{c.get('title','—')}</div>
                </div>
                {badge(prio, pri_color.get(prio,'blue'))}
              </div>"""
            if email:    html += f'<div class="contact-email" style="margin-top:.5rem">✉️ {email}</div>'
            elif patterns: html += f'<div style="font-size:.75rem;color:var(--muted);margin-top:.4rem">📧 {patterns[0]}</div>'
            if li:       html += f'<div style="font-size:.75rem;margin-top:.3rem"><a href="{li}" target="_blank" style="color:var(--accent);text-decoration:none">🔗 LinkedIn</a></div>'
            if src:      html += f'<div style="font-size:.68rem;color:var(--muted);margin-top:.4rem">via {src}</div>'
            st.markdown(html + '</div>', unsafe_allow_html=True)

def render_emails(emails_data):
    if not emails_data: st.info("No email data available."); return
    emails_data = as_dict(emails_data)
    if not emails_data: return
    variants = {}
    for k, v in emails_data.items():
        kl = k.lower().replace("_","").replace(" ","")
        if any(x in kl for x in ["varianta","variant_a","emaila"]) or kl == "a":
            variants["Variant A — Hiring Manager"] = v
        elif any(x in kl for x in ["variantb","variant_b","emailb"]) or kl == "b":
            variants["Variant B — CISO / VP Level"] = v
        else:
            variants[k] = v
    for label, v in variants.items():
        st.markdown(f'<div class="section-label">✉️ {label}</div>', unsafe_allow_html=True)
        if isinstance(v, dict):
            subj = v.get("subject") or v.get("Subject","")
            body = v.get("body") or v.get("Body") or v.get("content","")
            if subj: st.markdown(f'<div class="email-subject">Subject: {subj}</div>', unsafe_allow_html=True)
            if body: st.markdown(f'<div class="email-box">{body}</div>', unsafe_allow_html=True)
            else:    st.code(json.dumps(v, indent=2), language="json")
        elif isinstance(v, str):
            st.markdown(f'<div class="email-box">{v}</div>', unsafe_allow_html=True)
        st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)

def render_service_mapping(data):
    if not data: st.info("No service mapping data."); return
    items = data if isinstance(data, list) else []
    if not items and isinstance(data, dict):
        for key in ("services","mappings","service_mapping","ServiceMapping","items"):
            if isinstance(data.get(key), list): items = data[key]; break
        if not items: items = [data]
    fit_colors = {"STRONG FIT":"green","PARTIAL FIT":"yellow","GAP":"red"}
    for item in items:
        if not isinstance(item, dict): continue
        svc  = item.get("service") or item.get("Service") or item.get("name","")
        fit  = (item.get("fit") or item.get("classification") or item.get("Fit") or item.get("status","")).upper()
        why  = item.get("justification") or item.get("rationale") or item.get("why","")
        reqs = item.get("requirements_addressed") or item.get("requirements") or ""
        eng  = item.get("engagement_type") or item.get("engagement","")
        color = fit_colors.get(fit, "blue")
        html = f"""<div class="sil-card" style="margin-bottom:.75rem">
          <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:.5rem">
            <span style="font-family:'Syne',sans-serif;font-weight:700;color:var(--text)">{svc}</span>
            {badge(fit or "MAPPED", color) if fit else ""}
          </div>"""
        if why:  html += f'<div style="font-size:.85rem;color:var(--muted);margin-bottom:.4rem">💡 {str(why)[:250]}</div>'
        if reqs:
            rs = ", ".join(reqs) if isinstance(reqs, list) else str(reqs)
            html += f'<div style="font-size:.8rem;color:var(--muted)">📌 {rs[:200]}</div>'
        if eng:  html += f'<div style="font-size:.8rem;color:var(--accent2);margin-top:.3rem">🔧 {eng}</div>'
        st.markdown(html + '</div>', unsafe_allow_html=True)
    render_json_pretty(data, "Service Mapping")

def render_microplans(data):
    if not data: st.info("No micro-plan data."); return
    plans = data if isinstance(data, list) else []
    if not plans and isinstance(data, dict):
        for k in ("plans","micro_plans","microplans","top_3","improvements"):
            if isinstance(data.get(k), list): plans = data[k]; break
        if not plans: plans = [data]
    for i, plan in enumerate(plans[:3], 1):
        if not isinstance(plan, dict): continue
        title = plan.get("title") or plan.get("objective") or plan.get("name") or f"Plan {i}"
        weeks = plan.get("duration") or plan.get("timeline","")
        obj   = plan.get("objective") or plan.get("goal","")
        kpis  = plan.get("kpis") or plan.get("KPIs") or []
        tasks = plan.get("tasks") or plan.get("workstreams") or []
        with st.expander(f"📋 Plan {i}: {title} {f'({weeks})' if weeks else ''}", expanded=(i==1)):
            if obj and obj != title: st.markdown(f"**Objective:** {obj}")
            if kpis:
                st.markdown("**KPIs:**")
                for kpi in (kpis if isinstance(kpis, list) else [kpis]): st.markdown(f"• {kpi}")
            if tasks:
                st.markdown("**Tasks / Workstreams:**")
                for t in (tasks if isinstance(tasks, list) else [tasks]):
                    if isinstance(t, dict):
                        tn = t.get("task") or t.get("name","")
                        te = t.get("effort") or t.get("duration","")
                        st.markdown(f"• **{tn}** {f'— {te}' if te else ''}")
                    else: st.markdown(f"• {t}")
            render_json_pretty(plan, f"Plan {i}")

def render_deal_assurance(data):
    if not data: st.info("No deal assurance data."); return
    if not isinstance(data, dict): render_json_pretty(data, "Deal Assurance Pack"); return
    evp = (data.get("executive_value_proposition") or
           data.get("value_proposition") or data.get("ExecutiveValueProposition",""))
    if evp:
        st.markdown('<div class="section-label">💼 Executive Value Proposition</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="sil-card sil-card-accent" style="font-size:.9rem;line-height:1.7;color:var(--text)">{evp}</div>', unsafe_allow_html=True)
    caps = data.get("mandatory_capabilities") or data.get("capabilities_checklist") or []
    if caps:
        st.markdown('<div class="section-label" style="margin-top:1rem">✅ Mandatory Capabilities</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        for i, cap in enumerate(caps if isinstance(caps, list) else [caps]):
            (c1 if i%2==0 else c2).markdown(f"✅ {cap}")
    risk = data.get("risk_mitigation") or data.get("RiskMitigation","")
    if risk:
        st.markdown('<div class="section-label" style="margin-top:1rem">🛡️ Risk Mitigation</div>', unsafe_allow_html=True)
        if isinstance(risk, dict):
            for k, v in risk.items(): st.markdown(f"**{k}:** {v}")
        else: st.markdown(str(risk))
    render_json_pretty(data, "Deal Assurance Pack")


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("""
    <div style="padding:.75rem 0 1.25rem">
      <div style="font-family:'Syne',sans-serif;font-size:1.35rem;font-weight:800;
                  color:#2563eb">🛡️ SecureITLab</div>
      <div style="font-size:.72rem;color:#64748b;letter-spacing:.1em;
                  text-transform:uppercase;margin-top:.2rem">Pipeline Intelligence</div>
    </div>
    """, unsafe_allow_html=True)

    # ── ✅ FIND JOBS BUTTON ────────────────────────────────────────────────────
    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)
    st.markdown("**🔍 Find New Jobs**")
    st.caption(
        "Runs scraper → checks MongoDB for duplicates → "
        "runs AI pipeline only on NEW jobs → stores in MongoDB"
    )

    find_jobs_clicked = st.button(
        "🔍  Find Jobs",
        disabled=st.session_state.pipeline_running,
        use_container_width=True,
        type="primary",
    )

    # Running indicator
    if st.session_state.pipeline_running:
        st.markdown("""
        <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;
                    padding:.6rem .9rem;margin-top:.5rem;font-family:'DM Mono',monospace;
                    font-size:.8rem;color:#1d4ed8">
          ⏳ Pipeline is running… check log below
        </div>""", unsafe_allow_html=True)

    # Last run result box
    if st.session_state.pipeline_done and st.session_state.pipeline_result:
        res = st.session_state.pipeline_result
        ok  = res.get("success", False)
        bg  = "#ecfdf5" if ok else "#fef2f2"
        bc  = "#bbf7d0" if ok else "#fecaca"
        tc  = "#15803d" if ok else "#b91c1c"
        ic  = "✅" if ok else "❌"
        st.markdown(f"""
        <div style="background:{bg};border:1px solid {bc};border-radius:8px;
                    padding:.75rem;margin-top:.5rem;font-size:.82rem">
          <div style="font-weight:700;color:{tc};margin-bottom:.4rem">{ic} Last Run</div>
          <div>📦 Scraped: <b>{res.get('scraped',0)}</b></div>
          <div>🆕 New jobs: <b>{res.get('new_jobs',0)}</b></div>
          <div>⏭️ Already in DB (skipped): <b>{res.get('skipped_db',0)}</b></div>
          <div>🤖 Processed by AI: <b>{res.get('processed',0)}</b></div>
          {f'<div style="color:{tc};margin-top:.3rem">⚠️ {res.get("error","")}</div>' if res.get("error") else ""}
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)

    # ── Job list ──────────────────────────────────────────────────────────────
    try:
        all_jobs = load_all_jobs()
    except Exception as e:
        st.error(f"MongoDB connection failed: {e}")
        st.markdown("**Add to `.streamlit/secrets.toml`:**")
        st.code('[secrets]\nMONGO_URI = "mongodb+srv://..."\nMONGO_DB  = "secureitlab_job_pipeline"')
        st.stop()

    if not all_jobs:
        st.warning("No jobs in MongoDB yet. Click **Find Jobs** to scrape and process.")
        st.stop()

    st.markdown(
        f'<div style="font-size:.75rem;color:#64748b;margin-bottom:.75rem">'
        f'{len(all_jobs)} jobs in database</div>',
        unsafe_allow_html=True,
    )

    search   = st.text_input("🔍 Filter by company / role", placeholder="e.g. Bounteous")
    filtered = [j for j in all_jobs
                if search.lower() in (j.get("company","")+" "+j.get("role","")).lower()]

    def job_label(j):
        score = j.get("opp_score")
        s_str = f" [{score}/10]" if score else ""
        ok    = "✅" if j.get("pipeline_ok") else "❌"
        return f"{ok} {j.get('company','?')} — {j.get('role','?')[:32]}{s_str}"

    if not filtered:
        st.warning("No matching jobs.")
        st.stop()

    sel_label  = st.selectbox("Select a Job", [job_label(j) for j in filtered], index=0)
    sel_idx    = [job_label(j) for j in filtered].index(sel_label)
    selected_id = str(filtered[sel_idx]["_id"])

    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)
    ok_count = sum(1 for j in all_jobs if j.get("pipeline_ok"))
    total_c  = sum(j.get("contacts_found",0) for j in all_jobs)
    st.markdown(f"""
    <div style="font-size:.75rem;color:#64748b">
      <div>✅ Pipeline OK: <b style="color:#16a34a">{ok_count}/{len(all_jobs)}</b></div>
      <div>👥 Total Contacts: <b style="color:#2563eb">{total_c}</b></div>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
#  FIND JOBS — trigger main.py in a background thread
# ══════════════════════════════════════════════════════════════════════════════

if find_jobs_clicked and not st.session_state.pipeline_running:
    # Reset module-level shared state before starting
    with _thread_lock:
        _thread_logs.clear()
        _thread_result[0] = None
        _thread_done[0]   = False

    st.session_state.pipeline_running = True
    st.session_state.pipeline_done    = False
    st.session_state.pipeline_logs    = []
    st.session_state.pipeline_result  = None
    st.cache_data.clear()

    def _run_pipeline_bg():
        """
        Runs in a background thread.
        ✅ NEVER touches st.session_state — uses module-level lists instead.
        """
        try:
            import main as _main

            def _cb(msg: str):
                # ✅ Safe — plain list append, no Streamlit context needed
                with _thread_lock:
                    _thread_logs.append(
                        f"{datetime.now().strftime('%H:%M:%S')} | {msg}"
                    )

            result = _main.run_pipeline(progress_callback=_cb)

            with _thread_lock:
                _thread_result[0] = result

        except Exception as e:
            with _thread_lock:
                _thread_logs.append(f"❌ Unexpected error: {e}")
                _thread_result[0] = {
                    "success": False, "error": str(e),
                    "scraped": 0, "new_jobs": 0, "skipped_db": 0, "processed": 0,
                }
        finally:
            with _thread_lock:
                _thread_done[0] = True

    threading.Thread(target=_run_pipeline_bg, daemon=True).start()
    st.rerun()


# ── Sync module-level thread state → session_state on every rerun ────────────
# (This runs on the main thread so it IS safe to write session_state)
with _thread_lock:
    if _thread_logs:
        st.session_state.pipeline_logs = list(_thread_logs)
    if _thread_done[0] and _thread_result[0] is not None:
        st.session_state.pipeline_result  = _thread_result[0]
        st.session_state.pipeline_running = False
        st.session_state.pipeline_done    = True


# ── Live log panel (shown while running OR after finish) ─────────────────────
if st.session_state.pipeline_running or (
        st.session_state.pipeline_done and st.session_state.pipeline_logs):
    heading = "⏳ Pipeline running — live log…" if st.session_state.pipeline_running \
              else "📋 Last pipeline run log"
    with st.expander(heading, expanded=st.session_state.pipeline_running):
        log_text = "\n".join(st.session_state.pipeline_logs[-100:]) \
                   if st.session_state.pipeline_logs else "Starting…"
        st.markdown(f'<div class="pipeline-log">{log_text}</div>', unsafe_allow_html=True)
        if st.session_state.pipeline_running:
            time.sleep(2)
            st.rerun()   # auto-refresh every 2s to pull new log lines


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN CONTENT — selected job detail
# ══════════════════════════════════════════════════════════════════════════════

with st.spinner("Loading job from MongoDB…"):
    job = load_job(selected_id)

if not job:
    st.error("Could not load job document.")
    st.stop()

company   = job.get("company",  "Unknown")
role      = job.get("role",     "Unknown")
opp_score = job.get("opp_score")
p_ok      = job.get("pipeline_ok", False)
p_min     = job.get("pipeline_min", "?")
c_found   = job.get("contacts_found", 0)
c_cov     = job.get("coverage_score")
c_domain  = job.get("contact_domain","")
run_at    = job.get("run_at","")

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-bottom:1.75rem">
  <div style="font-family:'DM Mono',monospace;font-size:.72rem;color:#2563eb;
              letter-spacing:.12em;text-transform:uppercase;margin-bottom:.35rem">
    Job #{job.get('job_number','?')} · {run_at[:10] if run_at else ''}
  </div>
  <h1 style="font-family:'Syne',sans-serif;font-size:2rem;font-weight:800;
             color:#0f172a;margin:0;line-height:1.15">{role}</h1>
  <div style="font-size:1.05rem;color:#64748b;margin-top:.3rem">
    @ <span style="color:#334155;font-weight:600">{company}</span>
    {f'<span style="color:#cbd5e1;margin:0 .5rem">·</span><span style="font-family:DM Mono,monospace;font-size:.82rem;color:#94a3b8">{c_domain}</span>' if c_domain else ""}
  </div>
</div>
""", unsafe_allow_html=True)

# ── Metric tiles ──────────────────────────────────────────────────────────────
try:
    sn = float(str(opp_score).split("/")[0].split(".")[0]) if opp_score else 0
    sc = "#16a34a" if sn >= 7 else "#f59e0b" if sn >= 5 else "#dc2626"
except Exception:
    sc = "#2563eb"

st.markdown(f"""
<div class="metric-row">
  <div class="metric-tile">
    <div class="val" style="color:{sc}">{f"{opp_score}/10" if opp_score else "—"}</div>
    <div class="lbl">Opportunity Score</div>
  </div>
  <div class="metric-tile">
    <div class="val">{c_found}</div>
    <div class="lbl">Contacts Found</div>
  </div>
  <div class="metric-tile">
    <div class="val">{f"{c_cov}%" if c_cov else "—"}</div>
    <div class="lbl">Contact Coverage</div>
  </div>
  <div class="metric-tile">
    <div class="val" style="color:{'#16a34a' if p_ok else '#dc2626'}">
      {'✅ OK' if p_ok else '❌ Failed'}
    </div>
    <div class="lbl">Pipeline ({p_min} min)</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  TABS
# ══════════════════════════════════════════════════════════════════════════════

tabs = st.tabs([
    "📋 Job & Enrichment",
    "🎯 Service Mapping",
    "🔍 Fit / Gap",
    "🛠️ Capability & Plans",
    "📦 Deal Assurance",
    "✉️ Outreach Emails",
    "👥 Contacts",
    "✅ QA Gates",
    "🗄️ Raw Data",
])


# ── Tab 1: Job Research + Enrichment ─────────────────────────────────────────
with tabs[0]:
    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown('<div class="section-label">📄 Job Research</div>', unsafe_allow_html=True)
        jr = as_dict(job.get("agent_job_research") or {})
        if jr:
            for label, keys in [
                ("Job Role",        ["job_role","Job Role","role","title"]),
                ("Company",         ["company_name","Company Name","company"]),
                ("Location",        ["location","Location"]),
                ("Organization URL",["organization_url","Organization URL","url"]),
            ]:
                val = next((jr.get(k) for k in keys if jr.get(k)), None)
                if val: st.markdown(f"**{label}:** {val}")
            desc = jr.get("job_description") or jr.get("Job Description","")
            if desc:
                st.markdown("**Job Description:**")
                st.markdown(
                    f'<div class="sil-card" style="font-size:.85rem;line-height:1.7;'
                    f'max-height:300px;overflow-y:auto;color:var(--text)">{desc[:2000]}</div>',
                    unsafe_allow_html=True,
                )
            render_json_pretty(jr, "Job Research")
        else:
            st.info("No job research data.")

    with col2:
        st.markdown('<div class="section-label">🏢 Company Enrichment</div>', unsafe_allow_html=True)
        enr = as_dict(job.get("agent_enrichment") or {})
        if enr:
            for label, keys in [
                ("Industry",          ["industry","Industry"]),
                ("Company Size",      ["company_size","size","Company Size"]),
                ("Regulatory Env",    ["regulatory_environment","regulatory"]),
                ("Certifications",    ["certifications","Certifications"]),
                ("Tech Stack",        ["tech_stack","technology_stack"]),
                ("Security Maturity", ["security_maturity","maturity"]),
            ]:
                val = next((enr.get(k) for k in keys if enr.get(k)), None)
                if val:
                    if isinstance(val, list): val = ", ".join(str(v) for v in val)
                    st.markdown(f"**{label}:** {safe_str(val, 200)}")
            render_json_pretty(enr, "Enrichment")
        else:
            st.info("No enrichment data.")


# ── Tab 2: Service Mapping ────────────────────────────────────────────────────
with tabs[1]:
    st.markdown('<div class="section-label">🗺️ Service Mapping Matrix</div>', unsafe_allow_html=True)
    render_service_mapping(job.get("agent_service_mapping"))


# ── Tab 3: Fit / Gap Analysis ─────────────────────────────────────────────────
with tabs[2]:
    fg = as_dict(job.get("agent_fit_gap") or {})

    if opp_score:
        try:
            sn = float(str(opp_score).split("/")[0])
            bc = "#16a34a" if sn >= 7 else "#f59e0b" if sn >= 5 else "#dc2626"
            bp = int(sn / 10 * 100)
            st.markdown(f"""
            <div style="margin-bottom:1.5rem">
              <div style="display:flex;align-items:center;gap:1rem;margin-bottom:.5rem">
                <span style="font-family:'Syne',sans-serif;font-weight:700;color:var(--text)">
                  Opportunity Score
                </span>
                <span style="font-family:'Syne',sans-serif;font-size:1.8rem;font-weight:800;color:{bc}">
                  {opp_score}/10
                </span>
              </div>
              <div style="background:#e2e8f0;border-radius:4px;height:8px;width:100%">
                <div style="background:{bc};width:{bp}%;height:100%;border-radius:4px"></div>
              </div>
            </div>""", unsafe_allow_html=True)
        except Exception:
            pass

    st.markdown('<div class="section-label">📊 Service Classifications</div>', unsafe_allow_html=True)

    services = []
    if isinstance(fg, dict):
        for k in ("services","classifications","service_classifications","items","fit_gap"):
            v = fg.get(k)
            if isinstance(v, list): services = v; break
        if not services and (fg.get("service") or fg.get("Service")):
            services = [fg]
    elif isinstance(fg, list):
        services = fg

    if services:
        buckets = {"STRONG FIT": [], "PARTIAL FIT": [], "GAP": []}
        other   = []
        for s in services:
            if not isinstance(s, dict): continue
            fit = (s.get("fit") or s.get("classification") or s.get("Fit","")).upper()
            if "STRONG"  in fit: buckets["STRONG FIT"].append(s)
            elif "PARTIAL" in fit: buckets["PARTIAL FIT"].append(s)
            elif "GAP"   in fit: buckets["GAP"].append(s)
            else: other.append(s)

        c1, c2, c3 = st.columns(3)
        cm  = {"STRONG FIT":"#16a34a","PARTIAL FIT":"#f59e0b","GAP":"#dc2626"}
        bgm = {"STRONG FIT":"#f0fdf4","PARTIAL FIT":"#fffbeb","GAP":"#fef2f2"}
        bdm = {"STRONG FIT":"#bbf7d0","PARTIAL FIT":"#fde68a","GAP":"#fecaca"}
        for col, (fl, items) in zip([c1, c2, c3], buckets.items()):
            col.markdown(
                f'<div style="font-family:Syne,sans-serif;font-weight:700;'
                f'color:{cm[fl]};margin-bottom:.5rem">{fl} ({len(items)})</div>',
                unsafe_allow_html=True,
            )
            for s in items:
                svc  = s.get("service") or s.get("Service") or s.get("name","")
                just = s.get("justification") or s.get("reason","")
                col.markdown(
                    f'<div style="background:{bgm[fl]};border:1px solid {bdm[fl]};'
                    f'border-radius:8px;padding:.75rem;margin-bottom:.5rem;font-size:.85rem">'
                    f'<div style="font-weight:600;color:#0f172a">{svc}</div>'
                    f'<div style="color:#64748b;font-size:.78rem;margin-top:.2rem">'
                    f'{safe_str(just,150)}</div></div>',
                    unsafe_allow_html=True,
                )
        if other:
            st.markdown("**Other:**")
            for s in other:
                st.markdown(f"• {s.get('service','')}: {s.get('justification','')}")
    elif fg:
        st.json(fg)
    else:
        st.info("No fit/gap data.")

    render_json_pretty(job.get("agent_fit_gap"), "Fit/Gap Analysis")


# ── Tab 4: Capability + Micro-plans ──────────────────────────────────────────
with tabs[3]:
    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown('<div class="section-label">🔧 Capability Improvements</div>', unsafe_allow_html=True)
        cap = job.get("agent_capability") or {}
        items_cap = cap if isinstance(cap, list) else []
        if not items_cap and isinstance(cap, dict):
            for k in ("improvements","recommendations","capabilities","items"):
                v = cap.get(k)
                if isinstance(v, list): items_cap = v; break
            if not items_cap: items_cap = [cap]
        for item in items_cap:
            if not isinstance(item, dict): continue
            title  = item.get("title") or item.get("gap") or item.get("service","")
            rec    = item.get("recommendation") or item.get("steps","")
            effort = item.get("build_effort") or item.get("effort","")
            demand = item.get("market_demand") or item.get("priority","")
            st.markdown(f"""
            <div class="sil-card" style="margin-bottom:.6rem">
              <div style="font-family:'Syne',sans-serif;font-weight:700;margin-bottom:.3rem;color:var(--text)">{title}</div>
              <div style="font-size:.82rem;color:var(--muted)">{safe_str(rec, 250)}</div>
              {f'<div style="font-size:.75rem;color:var(--accent2);margin-top:.3rem">Priority: {demand} · Effort: {effort}</div>' if demand or effort else ""}
            </div>""", unsafe_allow_html=True)
        if not items_cap:
            render_json_pretty(cap, "Capability Improvement")

    with col2:
        st.markdown('<div class="section-label">📅 Maturity Micro-Plans</div>', unsafe_allow_html=True)
        render_microplans(job.get("agent_microplans"))


# ── Tab 5: Deal Assurance Pack ────────────────────────────────────────────────
with tabs[4]:
    render_deal_assurance(job.get("agent_deal_assurance"))


# ── Tab 6: Outreach Emails ────────────────────────────────────────────────────
with tabs[5]:
    st.markdown('<div class="section-label">✉️ Outreach Email Variants</div>', unsafe_allow_html=True)
    emails_src = job.get("agent_outreach_emails") or job.get("outreach_emails") or {}
    oq = as_dict(job.get("agent_outreach_qa") or {})
    improved = (oq.get("improved_emails") or oq.get("ImprovedEmails")) if oq else None
    if improved:
        st.info("⚡ Showing QA-improved versions where available")
        render_emails(improved)
        with st.expander("📬 Original (pre-QA) versions"):
            render_emails(emails_src)
    else:
        render_emails(emails_src)


# ── Tab 7: Contacts ───────────────────────────────────────────────────────────
with tabs[6]:
    contacts        = job.get("contacts") or []
    contact_sources = job.get("contact_sources") or []

    pri = [c for c in contacts if c.get("priority") == "Primary"]
    sec = [c for c in contacts if c.get("priority") == "Secondary"]
    ter = [c for c in contacts if c.get("priority") == "Tertiary"]
    gen = [c for c in contacts if c.get("priority") == "General"]

    st.markdown(f"""
    <div class="metric-row" style="margin-bottom:1.5rem">
      <div class="metric-tile"><div class="val" style="color:#dc2626">{len(pri)}</div><div class="lbl">Primary</div></div>
      <div class="metric-tile"><div class="val" style="color:#f59e0b">{len(sec)}</div><div class="lbl">Secondary</div></div>
      <div class="metric-tile"><div class="val" style="color:#2563eb">{len(ter)}</div><div class="lbl">Tertiary</div></div>
      <div class="metric-tile"><div class="val" style="color:#94a3b8">{len(gen)}</div><div class="lbl">General</div></div>
    </div>""", unsafe_allow_html=True)

    if contact_sources:
        st.markdown(
            'Sources: ' + " ".join(badge(s,"blue") for s in contact_sources),
            unsafe_allow_html=True,
        )
        st.markdown("")

    missing = job.get("missing_roles") or []
    if missing:
        st.markdown(
            '⚠️ Missing roles: ' + " ".join(badge(r,"red") for r in missing),
            unsafe_allow_html=True,
        )
        st.markdown("")

    if contacts:
        pri_filter = st.multiselect(
            "Filter by priority",
            ["Primary","Secondary","Tertiary","General"],
            default=["Primary","Secondary","Tertiary","General"],
        )
        shown = [c for c in contacts if c.get("priority","General") in pri_filter]
        render_contacts(shown, f"Contacts ({len(shown)} shown)")

        # Also show CrewAI agent's contact search
        agent_contacts = job.get("agent_prospect_contacts") or {}
        if agent_contacts:
            with st.expander("🤖 CrewAI Agent's Contact Search"):
                if isinstance(agent_contacts, dict):
                    ac_list = agent_contacts.get("contacts") or []
                    if ac_list: render_contacts(ac_list, "Agent Contacts")
                    else:       st.json(agent_contacts)
                else:           st.json(agent_contacts)
    else:
        st.info("No contacts found for this job.")


# ── Tab 8: QA Gates ───────────────────────────────────────────────────────────
with tabs[7]:
    st.markdown(
        '<div class="section-label" style="margin-bottom:1rem">🔍 All 4 QA Gate Results</div>',
        unsafe_allow_html=True,
    )
    col1, col2 = st.columns(2)
    for i, (label, key) in enumerate([
        ("Research QA",       "agent_research_qa"),
        ("Service Mapping QA","agent_mapping_qa"),
        ("Deal Assurance QA", "agent_assurance_qa"),
        ("Outreach Email QA", "agent_outreach_qa"),
    ]):
        with (col1 if i % 2 == 0 else col2):
            render_qa_block(job.get(key), label)

    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-label">🎯 Prospect Enforcer Result</div>', unsafe_allow_html=True)
    enf = as_dict(job.get("agent_prospect_enforcer") or {})
    if enf:
        cov  = enf.get("coverage_score","?")
        miss = enf.get("missing_roles",[])
        note = enf.get("note","")
        ec   = enf.get("contacts",[])
        x1, x2, x3 = st.columns(3)
        x1.metric("Coverage Score",    f"{cov}%")
        x2.metric("Missing Roles",     len(miss))
        x3.metric("Contacts Verified", len(ec))
        if miss: st.markdown(f"**Missing:** {', '.join(str(m) for m in miss)}")
        if note: st.caption(note)
    else:
        st.info("No enforcer data.")


# ── Tab 9: Raw Data ───────────────────────────────────────────────────────────
with tabs[8]:
    st.markdown('<div class="section-label">🗄️ Raw MongoDB Document</div>', unsafe_allow_html=True)
    st.caption("All fields stored in the `jobs` collection for this document.")

    rows = []
    for k, v in job.items():
        if k == "_id": continue
        rows.append({
            "Field": k,
            "Type":  type(v).__name__,
            "Len":   len(v) if isinstance(v,(list,dict)) else len(str(v)) if v else 0,
        })

    hc1, hc2, hc3 = st.columns([3,1,1])
    hc1.markdown("**Field**"); hc2.markdown("**Type**"); hc3.markdown("**Len**")
    for r in rows:
        rc1, rc2, rc3 = st.columns([3,1,1])
        rc1.code(r["Field"], language=None)
        rc2.markdown(f'<span style="color:#64748b;font-size:.8rem">{r["Type"]}</span>', unsafe_allow_html=True)
        rc3.markdown(f'<span style="color:#64748b;font-size:.8rem">{r["Len"]}</span>',  unsafe_allow_html=True)

    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)

    for label, key in [
        ("Job Research",       "agent_job_research"),
        ("Enrichment",         "agent_enrichment"),
        ("Service Mapping",    "agent_service_mapping"),
        ("Fit/Gap Analysis",   "agent_fit_gap"),
        ("Capability",         "agent_capability"),
        ("Micro-Plans",        "agent_microplans"),
        ("Deal Assurance",     "agent_deal_assurance"),
        ("Outreach Emails",    "agent_outreach_emails"),
        ("Prospect Contacts",  "agent_prospect_contacts"),
        ("Prospect Enforcer",  "agent_prospect_enforcer"),
        ("Research QA",        "agent_research_qa"),
        ("Mapping QA",         "agent_mapping_qa"),
        ("Assurance QA",       "agent_assurance_qa"),
        ("Outreach QA",        "agent_outreach_qa"),
        ("Contacts (5-source)","contacts"),
    ]:
        data = job.get(key)
        if data:
            with st.expander(f"📄 {label}"):
                st.code(json.dumps(data, indent=2, default=str), language="json")