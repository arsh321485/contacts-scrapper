"""
╔══════════════════════════════════════════════════════════╗
║   SecureITLab Pipeline Dashboard — Streamlit             ║
║   Reads from MongoDB → secureitlab_job_pipeline          ║
╠══════════════════════════════════════════════════════════╣
║  Install: pip install streamlit pymongo                  ║
║  Run:     streamlit run streamlit_dashboard.py           ║
╚══════════════════════════════════════════════════════════╝
"""

import streamlit as st
from pymongo import MongoClient
import json

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SecureITLab Pipeline",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>

/* ===============================
   SecureITLab — Professional Theme
   =============================== */

@import url('https://fonts.googleapis.com/css2?family=Syne:wght@500;600;700;800&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap');

/* ---------- COLOR SYSTEM ---------- */

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

/* ---------- GLOBAL ---------- */

html, body, [class*="css"] {
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', sans-serif !important;
}

/* ---------- SIDEBAR ---------- */

[data-testid="stSidebar"] {
    background: var(--surface);
    border-right: 1px solid var(--border);
}

[data-testid="stSidebar"] * {
    color: var(--text) !important;
}

/* ---------- MAIN AREA ---------- */

.main .block-container {
    padding: 2rem 2rem 3rem;
}

/* ---------- HEADINGS ---------- */

h1, h2, h3, h4 {
    font-family: 'Syne', sans-serif !important;
    color: var(--text);
}

/* ---------- CARDS ---------- */

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

.sil-card-accent {
    border-left: 4px solid var(--accent);
}

/* ---------- METRICS ---------- */

.metric-row {
    display:flex;
    gap:1rem;
    flex-wrap:wrap;
    margin-bottom:1.5rem;
}

.metric-tile {
    flex:1;
    min-width:140px;
    background:var(--surface2);
    border:1px solid var(--border);
    border-radius:12px;
    padding:1rem;
    text-align:center;
    transition:all .25s ease;
}

.metric-tile:hover {
    transform:translateY(-3px);
    box-shadow:0 10px 24px rgba(0,0,0,0.06);
}

.metric-tile .val {
    font-family:'Syne',sans-serif;
    font-size:2rem;
    font-weight:800;
    color:var(--accent);
}

.metric-tile .lbl {
    font-size:.72rem;
    color:var(--muted);
    text-transform:uppercase;
    letter-spacing:.08em;
}

/* ---------- BADGES ---------- */

.badge {
    padding:.25rem .7rem;
    border-radius:20px;
    font-size:.72rem;
    font-weight:600;
    font-family:'DM Mono', monospace;
}

.badge-green {
    background:#ecfdf5;
    color:#15803d;
}

.badge-yellow {
    background:#fffbeb;
    color:#b45309;
}

.badge-red {
    background:#fef2f2;
    color:#b91c1c;
}

.badge-blue {
    background:#eff6ff;
    color:#1d4ed8;
}

.badge-purple {
    background:#f5f3ff;
    color:#6d28d9;
}

/* ---------- CONTACT CARDS ---------- */

.contact-card {
    background:var(--surface2);
    border:1px solid var(--border);
    border-radius:10px;
    padding:1rem;
    margin-bottom:.8rem;
}

.contact-name {
    font-family:'Syne',sans-serif;
    font-weight:700;
}

.contact-title {
    color:var(--muted);
    font-size:.85rem;
}

.contact-email {
    font-family:'DM Mono', monospace;
    color:var(--accent);
    font-size:.8rem;
}

/* ---------- EMAIL BOX ---------- */

.email-box {
    background:#f8fafc;
    border:1px solid var(--border);
    border-radius:10px;
    padding:1rem;
    font-size:.9rem;
    line-height:1.6;
    white-space:pre-wrap;
}

.email-subject {
    font-family:'Syne';
    font-weight:700;
    color:var(--accent);
}

/* ---------- SECTION LABEL ---------- */

.section-label {
    font-family:'DM Mono';
    font-size:.72rem;
    text-transform:uppercase;
    letter-spacing:.12em;
    color:var(--accent);
    margin-bottom:.6rem;
}

/* ---------- EXPANDERS ---------- */

[data-testid="stExpander"] {
    background:var(--surface);
    border:1px solid var(--border);
    border-radius:10px;
}

/* ---------- SELECT BOX ---------- */

[data-testid="stSelectbox"] > div,
[data-testid="stMultiSelect"] > div {
    background:var(--surface2);
    border-color:var(--border);
}

/* ---------- TABS ---------- */

[data-testid="stTabs"] button {
    font-family:'Syne';
    font-weight:600;
}

/* ---------- SCROLLBAR ---------- */

::-webkit-scrollbar {
    width:6px;
}

::-webkit-scrollbar-thumb {
    background:var(--border);
    border-radius:3px;
}

</style>
""", unsafe_allow_html=True)
# ══════════════════════════════════════════════════════════════════════════════
#  MongoDB connection (cached)
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_db():
    MONGO_URI = st.secrets.get("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DB  = st.secrets.get("MONGO_DB",  "secureitlab_job_pipeline")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
    return client[MONGO_DB]

@st.cache_data(ttl=60)
def load_all_jobs():
    db   = get_db()
    docs = list(db.jobs.find({}, {
        "_id": 1, "company": 1, "role": 1, "job_number": 1,
        "opp_score": 1, "contacts_found": 1, "pipeline_ok": 1,
        "coverage_score": 1, "run_at": 1, "contact_domain": 1,
    }))
    return docs

@st.cache_data(ttl=60)
def load_job(job_id):
    from bson import ObjectId
    db  = get_db()
    doc = db.jobs.find_one({"_id": ObjectId(job_id)})
    return doc


# ══════════════════════════════════════════════════════════════════════════════
#  Helper renderers
# ══════════════════════════════════════════════════════════════════════════════

def badge(text, color="blue"):
    return f'<span class="badge badge-{color}">{text}</span>'

def render_json_pretty(data, title=""):
    """Render any dict/list in an expander as formatted JSON."""
    if not data: return
    with st.expander(f"📄 Raw JSON — {title}", expanded=False):
        st.code(json.dumps(data, indent=2, default=str), language="json")

def render_qa_block(data, label):
    """Render a QA gate result as a styled card."""
    if not data:
        st.markdown(f'<div class="sil-card"><b>{label}</b> — <i>No data</i></div>',
                    unsafe_allow_html=True)
        return

    if isinstance(data, list):
        data = next((x for x in data if isinstance(x, dict)), {})
    if not isinstance(data, dict): return

    passed = data.get("passed") or data.get("Passed") or False
    rec    = data.get("recommendation") or data.get("Recommendation", "")
    issues = data.get("issues") or data.get("Issues") or []
    checklist = data.get("checklist") or data.get("Checklist") or []

    color  = "green" if passed else "yellow"
    status = "✅ APPROVED" if passed else "⚠️ NEEDS REWORK"

    html = f"""
    <div class="sil-card sil-card-accent">
      <div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:0.75rem">
        <span style="font-family:'Syne',sans-serif;font-weight:700;font-size:1rem">{label}</span>
        {badge(status, color)}
      </div>
    """
    if rec:
        html += f'<div style="font-size:0.85rem;color:#94a3b8;margin-bottom:0.6rem">📝 {rec}</div>'

    if checklist:
        html += '<div style="font-size:0.82rem;margin-bottom:0.5rem">'
        items = checklist if isinstance(checklist, list) else [checklist]
        for item in items:
            if isinstance(item, dict):
                item_pass = item.get("pass") or item.get("passed") or item.get("status","") == "pass"
                item_name = item.get("item") or item.get("name") or item.get("check","")
                item_note = item.get("reason") or item.get("note") or item.get("issue","")
                icon = "✅" if item_pass else "❌"
                html += f'<div style="margin:0.25rem 0">{icon} <b>{item_name}</b>'
                if item_note:
                    html += f' — <span style="color:#94a3b8">{str(item_note)[:120]}</span>'
                html += '</div>'
        html += '</div>'

    if issues:
        html += '<div style="margin-top:0.5rem">'
        issue_list = issues if isinstance(issues, list) else [issues]
        for iss in issue_list[:4]:
            txt = iss if isinstance(iss, str) else json.dumps(iss)
            html += f'<div style="font-size:0.8rem;color:#fbbf24;margin:0.2rem 0">• {txt[:200]}</div>'
        html += '</div>'

    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)

def render_contacts(contacts, title="Contacts"):
    """Render contact cards with priority badges."""
    if not contacts:
        st.info("No contacts found for this job.")
        return

    pri_color = {"Primary": "red", "Secondary": "yellow", "Tertiary": "blue", "General": "purple"}

    st.markdown(f'<div class="section-label">👥 {title} ({len(contacts)})</div>',
                unsafe_allow_html=True)

    cols = st.columns(2)
    for i, c in enumerate(contacts):
        col = cols[i % 2]
        prio  = c.get("priority", "General")
        email = c.get("email", "")
        li    = c.get("linkedin_url", "")
        patterns = c.get("email_patterns", [])
        src   = c.get("source", "")

        with col:
            html = f"""
            <div class="contact-card">
              <div style="display:flex;justify-content:space-between;align-items:flex-start">
                <div>
                  <div class="contact-name">{c.get('name','—')}</div>
                  <div class="contact-title">{c.get('title','—')}</div>
                </div>
                {badge(prio, pri_color.get(prio,'blue'))}
              </div>
            """
            if email:
                html += f'<div class="contact-email" style="margin-top:0.5rem">✉️ {email}</div>'
            elif patterns:
                html += f'<div style="font-size:0.75rem;color:#475569;margin-top:0.4rem">📧 {patterns[0]}</div>'
            if li:
                html += f'<div style="font-size:0.75rem;margin-top:0.3rem"><a href="{li}" target="_blank" style="color:#60a5fa;text-decoration:none">🔗 LinkedIn</a></div>'
            if src:
                html += f'<div style="font-size:0.68rem;color:#475569;margin-top:0.4rem">via {src}</div>'
            html += '</div>'
            st.markdown(html, unsafe_allow_html=True)

def render_emails(emails_data):
    """Render outreach email variants."""
    if not emails_data:
        st.info("No email data available.")
        return

    if isinstance(emails_data, list):
        emails_data = next((x for x in emails_data if isinstance(x, dict)), {})
    if not isinstance(emails_data, dict): return

    # Detect keys — could be "variant_a","variant_b" or "VariantA","VariantB" etc.
    variants = {}
    for k, v in emails_data.items():
        kl = k.lower().replace("_","").replace(" ","")
        if any(x in kl for x in ["varianta","variant_a","emaila","a"]):
            variants["Variant A — Hiring Manager"] = v
        elif any(x in kl for x in ["variantb","variant_b","emailb","b"]):
            variants["Variant B — CISO / VP Level"] = v
        else:
            variants[k] = v

    for label, v in variants.items():
        st.markdown(f'<div class="section-label">✉️ {label}</div>', unsafe_allow_html=True)
        if isinstance(v, dict):
            subj = v.get("subject") or v.get("Subject","")
            body = v.get("body") or v.get("Body") or v.get("content","")
            if subj:
                st.markdown(f'<div class="email-subject">Subject: {subj}</div>',
                            unsafe_allow_html=True)
            if body:
                st.markdown(f'<div class="email-box">{body}</div>', unsafe_allow_html=True)
            else:
                st.code(json.dumps(v, indent=2), language="json")
        elif isinstance(v, str):
            st.markdown(f'<div class="email-box">{v}</div>', unsafe_allow_html=True)
        st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)

def render_service_mapping(data):
    if not data: st.info("No service mapping data."); return
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # Try common keys
        for key in ("services","mappings","service_mapping","ServiceMapping","items"):
            if isinstance(data.get(key), list):
                items = data[key]; break
        else:
            items = [data]

    fit_colors = {"STRONG FIT": "green", "PARTIAL FIT": "yellow", "GAP": "red"}

    for item in items:
        if not isinstance(item, dict): continue
        svc  = item.get("service") or item.get("Service") or item.get("name","")
        fit  = (item.get("fit") or item.get("classification") or
                item.get("Fit") or item.get("status","")).upper()
        why  = item.get("justification") or item.get("rationale") or item.get("why","")
        reqs = item.get("requirements_addressed") or item.get("requirements") or ""
        eng  = item.get("engagement_type") or item.get("engagement","")

        color = fit_colors.get(fit, "blue")
        html = f"""
        <div class="sil-card" style="margin-bottom:0.75rem">
          <div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:0.5rem">
            <span style="font-family:'Syne',sans-serif;font-weight:700">{svc}</span>
            {badge(fit or "MAPPED", color) if fit else ""}
          </div>
        """
        if why:
            html += f'<div style="font-size:0.85rem;color:#94a3b8;margin-bottom:0.4rem">💡 {why}</div>'
        if reqs:
            reqs_str = ", ".join(reqs) if isinstance(reqs, list) else str(reqs)
            html += f'<div style="font-size:0.8rem;color:#64748b">📌 {reqs_str[:200]}</div>'
        if eng:
            html += f'<div style="font-size:0.8rem;color:#7c3aed;margin-top:0.3rem">🔧 {eng}</div>'
        html += '</div>'
        st.markdown(html, unsafe_allow_html=True)
    render_json_pretty(data, "Service Mapping")

def render_microplans(data):
    if not data: st.info("No micro-plan data."); return
    plans = []
    if isinstance(data, list): plans = data
    elif isinstance(data, dict):
        for k in ("plans","micro_plans","microplans","top_3","improvements"):
            if isinstance(data.get(k), list):
                plans = data[k]; break
        if not plans: plans = [data]

    for i, plan in enumerate(plans[:3], 1):
        if not isinstance(plan, dict): continue
        title  = plan.get("title") or plan.get("objective") or plan.get("name") or f"Plan {i}"
        weeks  = plan.get("duration") or plan.get("timeline","")
        obj    = plan.get("objective") or plan.get("goal","")
        kpis   = plan.get("kpis") or plan.get("KPIs") or []
        tasks  = plan.get("tasks") or plan.get("workstreams") or []

        with st.expander(f"📋 Plan {i}: {title} {f'({weeks})' if weeks else ''}", expanded=(i==1)):
            if obj and obj != title:
                st.markdown(f"**Objective:** {obj}")
            if kpis:
                st.markdown("**KPIs:**")
                kpi_list = kpis if isinstance(kpis, list) else [kpis]
                for kpi in kpi_list:
                    st.markdown(f"• {kpi}")
            if tasks:
                st.markdown("**Tasks / Workstreams:**")
                task_list = tasks if isinstance(tasks, list) else [tasks]
                for t in task_list:
                    if isinstance(t, dict):
                        tname  = t.get("task") or t.get("name","")
                        teffort = t.get("effort") or t.get("duration","")
                        st.markdown(f"• **{tname}** {f'— {teffort}' if teffort else ''}")
                    else:
                        st.markdown(f"• {t}")
            render_json_pretty(plan, f"Plan {i}")

def render_deal_assurance(data):
    if not data: st.info("No deal assurance data."); return
    if not isinstance(data, dict): render_json_pretty(data, "Deal Assurance Pack"); return

    # Executive value prop
    evp = (data.get("executive_value_proposition") or
           data.get("value_proposition") or
           data.get("ExecutiveValueProposition",""))
    if evp:
        st.markdown('<div class="section-label">💼 Executive Value Proposition</div>',
                    unsafe_allow_html=True)
        st.markdown(f'<div class="sil-card sil-card-accent" style="font-size:0.9rem;line-height:1.7">{evp}</div>',
                    unsafe_allow_html=True)

    # Capabilities checklist
    caps = data.get("mandatory_capabilities") or data.get("capabilities_checklist") or []
    if caps:
        st.markdown('<div class="section-label" style="margin-top:1rem">✅ Mandatory Capabilities</div>',
                    unsafe_allow_html=True)
        cap_list = caps if isinstance(caps, list) else [caps]
        cols = st.columns(2)
        for i, cap in enumerate(cap_list):
            cols[i%2].markdown(f"✅ {cap}")

    # Risk mitigation
    risk = data.get("risk_mitigation") or data.get("RiskMitigation","")
    if risk:
        st.markdown('<div class="section-label" style="margin-top:1rem">🛡️ Risk Mitigation</div>',
                    unsafe_allow_html=True)
        if isinstance(risk, dict):
            for k, v in risk.items():
                st.markdown(f"**{k}:** {v}")
        else:
            st.markdown(str(risk))

    render_json_pretty(data, "Deal Assurance Pack")

def safe_str(val, limit=300):
    if val is None: return "—"
    s = str(val)
    return s[:limit] + "…" if len(s) > limit else s


# ══════════════════════════════════════════════════════════════════════════════
#  Sidebar — job list
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("""
    <div style="padding:1rem 0 1.5rem">
      <div style="font-family:'Syne',sans-serif;font-size:1.4rem;font-weight:800;
                  color:#00d4ff;letter-spacing:-0.02em">🛡️ SecureITLab</div>
      <div style="font-size:0.75rem;color:#475569;letter-spacing:0.1em;
                  text-transform:uppercase;margin-top:0.2rem">Pipeline Intelligence</div>
    </div>
    """, unsafe_allow_html=True)

    try:
        all_jobs = load_all_jobs()
    except Exception as e:
        st.error(f"MongoDB connection failed: {e}")
        st.markdown("**Add your MongoDB URI to `.streamlit/secrets.toml`:**")
        st.code("""
[secrets]
MONGO_URI = "mongodb+srv://user:pass@cluster/..."
MONGO_DB  = "secureitlab_job_pipeline"
        """)
        st.stop()

    if not all_jobs:
        st.warning("No jobs found in MongoDB.")
        st.stop()

    st.markdown(f'<div style="font-size:0.75rem;color:#475569;margin-bottom:1rem">'
                f'{len(all_jobs)} jobs in database</div>', unsafe_allow_html=True)

    # Search filter
    search = st.text_input("🔍 Filter by company / role", placeholder="e.g. Bounteous")
    filtered = [j for j in all_jobs
                if search.lower() in (j.get("company","") + " " + j.get("role","")).lower()]

    # Build labels for selectbox
    def job_label(j):
        score = j.get("opp_score")
        score_str = f" [{score}/10]" if score else ""
        ok    = "✅" if j.get("pipeline_ok") else "❌"
        return f"{ok} {j.get('company','?')} — {j.get('role','?')[:35]}{score_str}"

    if not filtered:
        st.warning("No matching jobs.")
        st.stop()

    selected_label = st.selectbox(
        "Select a Job",
        options=[job_label(j) for j in filtered],
        index=0,
    )
    selected_idx   = [job_label(j) for j in filtered].index(selected_label)
    selected_meta  = filtered[selected_idx]
    selected_id    = str(selected_meta["_id"])

    # Mini stats in sidebar
    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)
    ok_count = sum(1 for j in all_jobs if j.get("pipeline_ok"))
    total_contacts = sum(j.get("contacts_found", 0) for j in all_jobs)
    st.markdown(f"""
    <div style="font-size:0.75rem;color:#64748b">
      <div>✅ Pipeline OK: <b style="color:#10b981">{ok_count}/{len(all_jobs)}</b></div>
      <div>👥 Total Contacts: <b style="color:#00d4ff">{total_contacts}</b></div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
#  Main — load selected job
# ══════════════════════════════════════════════════════════════════════════════

with st.spinner("Loading job from MongoDB…"):
    job = load_job(selected_id)

if not job:
    st.error("Could not load job document.")
    st.stop()

company    = job.get("company", "Unknown")
role       = job.get("role", "Unknown")
opp_score  = job.get("opp_score")
p_ok       = job.get("pipeline_ok", False)
p_min      = job.get("pipeline_min", "?")
c_found    = job.get("contacts_found", 0)
c_cov      = job.get("coverage_score")
c_domain   = job.get("contact_domain","")
run_at     = job.get("run_at","")


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-bottom:2rem">
  <div style="font-family:'DM Mono',monospace;font-size:0.72rem;color:#00d4ff;
              letter-spacing:0.12em;text-transform:uppercase;margin-bottom:0.4rem">
    Job #{job.get('job_number','?')} · {run_at[:10] if run_at else ''}
  </div>
  <h1 style="font-family:'Syne',sans-serif;font-size:2rem;font-weight:800;
             color:#0f172a;margin:0;line-height:1.15">{role}</h1>
  <div style="font-size:1.1rem;color:#64748b;margin-top:0.3rem">
    @ <span style="color:#94a3b8;font-weight:500">{company}</span>
    {f'<span style="color:#334155;margin:0 0.5rem">·</span><span style="font-family:DM Mono,monospace;font-size:0.82rem;color:#475569">{c_domain}</span>' if c_domain else ""}
  </div>
</div>
""", unsafe_allow_html=True)

# ── Top metric tiles ──────────────────────────────────────────────────────────
score_display = f"{opp_score}/10" if opp_score else "—"
cov_display   = f"{c_cov}%" if c_cov else "—"
pipe_status   = "✅ OK" if p_ok else "❌ Failed"
pipe_color    = "color:#10b981" if p_ok else "color:#ef4444"

st.markdown(f"""
<div class="metric-row">
  <div class="metric-tile">
    <div class="val" style="color:{'#10b981' if opp_score and int(str(opp_score).split('/')[0].split('.')[0] or 0) >= 7 else '#f59e0b'}">{score_display}</div>
    <div class="lbl">Opportunity Score</div>
  </div>
  <div class="metric-tile">
    <div class="val">{c_found}</div>
    <div class="lbl">Contacts Found</div>
  </div>
  <div class="metric-tile">
    <div class="val">{cov_display}</div>
    <div class="lbl">Contact Coverage</div>
  </div>
  <div class="metric-tile">
    <div class="val" style="{pipe_color}">{pipe_status}</div>
    <div class="lbl">Pipeline ({p_min} min)</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  Tabs
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
        jr = job.get("agent_job_research") or {}
        if isinstance(jr, list): jr = next((x for x in jr if isinstance(x, dict)), {})
        if jr:
            fields = {
                "Job Role":        jr.get("job_role") or jr.get("Job Role") or jr.get("role",""),
                "Company":         jr.get("company_name") or jr.get("Company Name") or jr.get("company",""),
                "Location":        jr.get("location") or jr.get("Location",""),
                "Organization URL":jr.get("organization_url") or jr.get("Organization URL") or jr.get("url",""),
            }
            for label, val in fields.items():
                if val:
                    st.markdown(f"**{label}:** {val}")
            desc = jr.get("job_description") or jr.get("Job Description","")
            if desc:
                st.markdown("**Job Description:**")
                st.markdown(f'<div class="sil-card" style="font-size:0.85rem;line-height:1.7;max-height:300px;overflow-y:auto">{desc[:2000]}</div>',
                            unsafe_allow_html=True)
            render_json_pretty(jr, "Job Research")
        else:
            st.info("No job research data.")

    with col2:
        st.markdown('<div class="section-label">🏢 Company Enrichment</div>', unsafe_allow_html=True)
        enr = job.get("agent_enrichment") or {}
        if isinstance(enr, list): enr = next((x for x in enr if isinstance(x, dict)), {})
        if enr:
            enr_fields = {
                "Industry":         enr.get("industry") or enr.get("Industry",""),
                "Company Size":     enr.get("company_size") or enr.get("size") or enr.get("Company Size",""),
                "Regulatory Env":   enr.get("regulatory_environment") or enr.get("regulatory",""),
                "Certifications":   enr.get("certifications",""),
                "Tech Stack":       enr.get("tech_stack") or enr.get("technology_stack",""),
                "Security Maturity":enr.get("security_maturity") or enr.get("maturity",""),
            }
            for label, val in enr_fields.items():
                if val:
                    if isinstance(val, list): val = ", ".join(str(v) for v in val)
                    st.markdown(f"**{label}:** {safe_str(val, 200)}")
            render_json_pretty(enr, "Enrichment")
        else:
            st.info("No enrichment data.")

# ── Tab 2: Service Mapping ────────────────────────────────────────────────────
with tabs[1]:
    st.markdown('<div class="section-label">🗺️ Service Mapping Matrix</div>', unsafe_allow_html=True)
    sm = job.get("agent_service_mapping")
    render_service_mapping(sm)

# ── Tab 3: Fit / Gap Analysis ─────────────────────────────────────────────────
with tabs[2]:
    fg = job.get("agent_fit_gap") or {}
    if isinstance(fg, list): fg = next((x for x in fg if isinstance(x, dict)), {})

    # Opportunity score highlight
    if opp_score:
        score_num = str(opp_score).split("/")[0]
        try:
            sn = float(score_num)
            bar_color = "#10b981" if sn >= 7 else "#f59e0b" if sn >= 5 else "#ef4444"
            bar_pct   = int(sn / 10 * 100)
            st.markdown(f"""
            <div style="margin-bottom:1.5rem">
              <div style="display:flex;align-items:center;gap:1rem;margin-bottom:0.5rem">
                <span style="font-family:'Syne',sans-serif;font-weight:700">Opportunity Score</span>
                <span style="font-family:'Syne',sans-serif;font-size:1.8rem;font-weight:800;color:{bar_color}">{opp_score}/10</span>
              </div>
              <div style="background:#1a2235;border-radius:4px;height:8px;width:100%">
                <div style="background:{bar_color};width:{bar_pct}%;height:100%;border-radius:4px;transition:width 0.5s"></div>
              </div>
            </div>
            """, unsafe_allow_html=True)
        except: pass

    st.markdown('<div class="section-label">📊 Service Classifications</div>', unsafe_allow_html=True)

    # Try to find the services list in fit/gap
    services = []
    if isinstance(fg, dict):
        for k in ("services","classifications","service_classifications","items","fit_gap"):
            v = fg.get(k)
            if isinstance(v, list): services = v; break
        if not services:
            # Maybe the dict itself has fit/classification keys — treat as single item
            if fg.get("service") or fg.get("Service"):
                services = [fg]
    elif isinstance(fg, list):
        services = fg

    if services:
        cols_fits = {"STRONG FIT": [], "PARTIAL FIT": [], "GAP": []}
        other = []
        for s in services:
            if not isinstance(s, dict): continue
            fit = (s.get("fit") or s.get("classification") or s.get("Fit","")).upper()
            if "STRONG" in fit: cols_fits["STRONG FIT"].append(s)
            elif "PARTIAL" in fit: cols_fits["PARTIAL FIT"].append(s)
            elif "GAP" in fit: cols_fits["GAP"].append(s)
            else: other.append(s)

        c1, c2, c3 = st.columns(3)
        for col, (fit_label, items) in zip([c1, c2, c3], cols_fits.items()):
            color_map = {"STRONG FIT":"#10b981","PARTIAL FIT":"#f59e0b","GAP":"#ef4444"}
            col.markdown(f'<div style="font-family:Syne,sans-serif;font-weight:700;'
                         f'color:{color_map[fit_label]};margin-bottom:0.5rem">'
                         f'{fit_label} ({len(items)})</div>', unsafe_allow_html=True)
            for s in items:
                svc = s.get("service") or s.get("Service") or s.get("name","")
                just = s.get("justification") or s.get("reason","")
                col.markdown(f"""
                <div style="background:#111827;border:1px solid #1f2d45;border-radius:8px;
                             padding:0.75rem;margin-bottom:0.5rem;font-size:0.85rem">
                  <div style="font-weight:600">{svc}</div>
                  <div style="color:#64748b;font-size:0.78rem;margin-top:0.25rem">{safe_str(just,150)}</div>
                </div>""", unsafe_allow_html=True)
        if other:
            st.markdown("**Other services:**")
            for s in other:
                st.markdown(f"• {s.get('service','')}: {s.get('justification','')}")
    elif fg:
        st.json(fg)
    else:
        st.info("No fit/gap data.")

    render_json_pretty(job.get("agent_fit_gap"), "Fit/Gap Analysis")

# ── Tab 4: Capability Improvement + Micro-plans ───────────────────────────────
with tabs[3]:
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown('<div class="section-label">🔧 Capability Improvements</div>',
                    unsafe_allow_html=True)
        cap = job.get("agent_capability") or {}
        if isinstance(cap, list): items_cap = cap
        elif isinstance(cap, dict):
            items_cap = []
            for k in ("improvements","recommendations","capabilities","items"):
                v = cap.get(k)
                if isinstance(v, list): items_cap = v; break
            if not items_cap: items_cap = [cap]
        else: items_cap = []

        for item in items_cap:
            if not isinstance(item, dict): continue
            title  = item.get("title") or item.get("gap") or item.get("service","")
            rec    = item.get("recommendation") or item.get("steps") or ""
            effort = item.get("build_effort") or item.get("effort","")
            demand = item.get("market_demand") or item.get("priority","")
            st.markdown(f"""
            <div class="sil-card" style="margin-bottom:0.6rem">
              <div style="font-family:'Syne',sans-serif;font-weight:700;margin-bottom:0.3rem">{title}</div>
              <div style="font-size:0.82rem;color:#94a3b8">{safe_str(rec, 250)}</div>
              {f'<div style="font-size:0.75rem;color:#7c3aed;margin-top:0.3rem">Priority: {demand} · Effort: {effort}</div>' if demand or effort else ""}
            </div>""", unsafe_allow_html=True)
        if not items_cap:
            render_json_pretty(cap, "Capability Improvement")

    with col2:
        st.markdown('<div class="section-label">📅 Maturity Micro-Plans</div>',
                    unsafe_allow_html=True)
        render_microplans(job.get("agent_microplans"))

# ── Tab 5: Deal Assurance Pack ────────────────────────────────────────────────
with tabs[4]:
    render_deal_assurance(job.get("agent_deal_assurance"))

# ── Tab 6: Outreach Emails ────────────────────────────────────────────────────
with tabs[5]:
    st.markdown('<div class="section-label">✉️ Outreach Email Variants</div>',
                unsafe_allow_html=True)
    # Try emails from agent output first, then top-level outreach_emails
    emails_src = job.get("agent_outreach_emails") or job.get("outreach_emails") or {}

    # Check if outreach QA has improved versions
    oq = job.get("agent_outreach_qa") or {}
    if isinstance(oq, list): oq = next((x for x in oq if isinstance(x, dict)), {})
    improved = (oq.get("improved_emails") or oq.get("ImprovedEmails")) if isinstance(oq, dict) else None

    if improved:
        st.info("⚡ Showing QA-improved versions where available")
        render_emails(improved)
        with st.expander("📬 Original (pre-QA) versions"):
            render_emails(emails_src)
    else:
        render_emails(emails_src)

# ── Tab 7: Contacts ───────────────────────────────────────────────────────────
with tabs[6]:
    contacts = job.get("contacts") or []
    contact_sources = job.get("contact_sources") or []

    # Summary row
    pri   = [c for c in contacts if c.get("priority") == "Primary"]
    sec   = [c for c in contacts if c.get("priority") == "Secondary"]
    ter   = [c for c in contacts if c.get("priority") == "Tertiary"]
    gen   = [c for c in contacts if c.get("priority") == "General"]

    st.markdown(f"""
    <div class="metric-row" style="margin-bottom:1.5rem">
      <div class="metric-tile"><div class="val" style="color:#ef4444">{len(pri)}</div><div class="lbl">Primary</div></div>
      <div class="metric-tile"><div class="val" style="color:#f59e0b">{len(sec)}</div><div class="lbl">Secondary</div></div>
      <div class="metric-tile"><div class="val" style="color:#60a5fa">{len(ter)}</div><div class="lbl">Tertiary</div></div>
      <div class="metric-tile"><div class="val" style="color:#94a3b8">{len(gen)}</div><div class="lbl">General</div></div>
    </div>
    """, unsafe_allow_html=True)

    if contact_sources:
        src_html = " ".join(badge(s, "blue") for s in contact_sources)
        st.markdown(f'<div style="margin-bottom:1rem">Sources: {src_html}</div>',
                    unsafe_allow_html=True)

    missing = job.get("missing_roles") or []
    if missing:
        miss_html = " ".join(badge(r, "red") for r in missing)
        st.markdown(f'<div style="margin-bottom:1rem">⚠️ Missing roles: {miss_html}</div>',
                    unsafe_allow_html=True)

    if contacts:
        # Filter
        pri_filter = st.multiselect("Filter by priority",
                                    ["Primary","Secondary","Tertiary","General"],
                                    default=["Primary","Secondary","Tertiary","General"])
        shown = [c for c in contacts if c.get("priority","General") in pri_filter]
        render_contacts(shown, f"Contacts ({len(shown)} shown)")

        # Also show CrewAI agent's prospect contacts
        agent_contacts = job.get("agent_prospect_contacts") or {}
        if agent_contacts:
            with st.expander("🤖 CrewAI Agent's Contact Search"):
                if isinstance(agent_contacts, dict):
                    ac_list = agent_contacts.get("contacts") or []
                    if ac_list:
                        render_contacts(ac_list, "Agent Contacts")
                    else:
                        st.json(agent_contacts)
                else:
                    st.json(agent_contacts)
    else:
        st.info("No contacts found for this job.")

# ── Tab 8: QA Gates ───────────────────────────────────────────────────────────
with tabs[7]:
    st.markdown('<div class="section-label" style="margin-bottom:1rem">🔍 All 4 QA Gate Results</div>',
                unsafe_allow_html=True)

    qa_items = [
        ("Research QA",   "agent_research_qa"),
        ("Service Mapping QA", "agent_mapping_qa"),
        ("Deal Assurance QA",  "agent_assurance_qa"),
        ("Outreach Email QA",  "agent_outreach_qa"),
    ]

    col1, col2 = st.columns(2)
    for i, (label, key) in enumerate(qa_items):
        col = col1 if i % 2 == 0 else col2
        with col:
            render_qa_block(job.get(key), label)

    # Prospect enforcer
    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-label">🎯 Prospect Enforcer Result</div>',
                unsafe_allow_html=True)
    enf = job.get("agent_prospect_enforcer") or {}
    if isinstance(enf, list): enf = next((x for x in enf if isinstance(x, dict)), {})
    if enf and isinstance(enf, dict):
        cov   = enf.get("coverage_score","?")
        miss  = enf.get("missing_roles",[])
        note  = enf.get("note","")
        enf_contacts = enf.get("contacts",[])
        col1, col2, col3 = st.columns(3)
        col1.metric("Coverage Score", f"{cov}%")
        col2.metric("Missing Roles",  len(miss))
        col3.metric("Contacts Verified", len(enf_contacts))
        if miss:
            st.markdown(f"**Missing:** {', '.join(str(m) for m in miss)}")
        if note:
            st.caption(note)
    else:
        st.info("No enforcer data.")

# ── Tab 9: Raw Data ───────────────────────────────────────────────────────────
with tabs[8]:
    st.markdown('<div class="section-label">🗄️ Raw MongoDB Document</div>', unsafe_allow_html=True)
    st.caption("All fields stored in the `jobs` collection for this document.")

    # Show all top-level keys + their types/sizes
    st.markdown("**Document fields:**")
    rows = []
    for k, v in job.items():
        if k == "_id": continue
        vtype = type(v).__name__
        vsize = len(v) if isinstance(v, (list, dict)) else len(str(v)) if v else 0
        rows.append({"Field": k, "Type": vtype, "Size/Len": vsize})

    col_f, col_t, col_s = st.columns([3,1,1])
    col_f.markdown("**Field**"); col_t.markdown("**Type**"); col_s.markdown("**Items/Len**")
    for row in rows:
        c1, c2, c3 = st.columns([3,1,1])
        c1.code(row["Field"], language=None)
        c2.markdown(f'<span style="color:#64748b;font-size:0.8rem">{row["Type"]}</span>',
                    unsafe_allow_html=True)
        c3.markdown(f'<span style="color:#64748b;font-size:0.8rem">{row["Size/Len"]}</span>',
                    unsafe_allow_html=True)

    st.markdown('<div class="sil-divider"></div>', unsafe_allow_html=True)

    # Expandable raw JSON per section
    sections = [
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
    ]
    for label, key in sections:
        data = job.get(key)
        if data:
            with st.expander(f"📄 {label}"):
                st.code(json.dumps(data, indent=2, default=str), language="json")