
"""
╔══════════════════════════════════════════════════════════════════╗
║   UNIFIED PIPELINE — 14 AGENTS + 5-SOURCE CONTACTS × 15 JOBS   ║
║   + MONGODB STORAGE                                              ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  WHAT IT DOES PER JOB:                                           ║
║  1.  14-agent CrewAI pipeline (research → QA → emails → pack)   ║
║  2.  5-source contact finder (Hunter/Minelead/PDL/theorg/Bing)  ║
║  3.  Saves everything to MongoDB (4 collections)                 ║
║  4.  Saves local JSON files as backup                            ║
║                                                                  ║
║  MONGODB COLLECTIONS:                                            ║
║  • jobs          — full pipeline output per job                  ║
║  • contacts      — all contacts found per job                    ║
║  • emails        — outreach email variants per job               ║
║  • run_summary   — one doc per run (stats across all 15 jobs)    ║
║                                                                  ║
║  CONTACT SOURCES (in order):                                     ║
║  1. Hunter.io        25 free/month  → hunter.io                  ║
║  2. Minelead.io      25 free/month  → minelead.io/signup         ║
║  3. People Data Labs 100 free/month → peopledatalabs.com/signup  ║
║  4. theorg.com       free scraping  → no signup                  ║
║  5. Bing             free scraping  → no signup                  ║
║  + Email guesser always runs                                     ║
║                                                                  ║
║  SETUP:                                                          ║
║  pip install crewai crewai-tools langchain-openai                ║
║             pymongo python-dotenv requests beautifulsoup4 lxml   ║
║                                                                  ║
║  Run: py -3.12 crewai_15jobs_full.py                             ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os, json, re, time, requests
import datetime as _dt
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from crewai import Agent, Task, Crew, Process
from crewai_tools import ScrapeWebsiteTool
from langchain_openai import ChatOpenAI
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

load_dotenv()


# ╔══════════════════════════════════════════════════════════════════╗
#  ║                    CONFIGURATION                               ║
# ╚══════════════════════════════════════════════════════════════════╝
# ── OpenAI ───────────────────────────────────────────────────────────────────
OPENAI_API_KEY=os.getenv("OPENAI_API_KEY")


# ── Contact API Keys (all free — no credit card) ─────────────────────────────
HUNTER_API_KEY   = os.getenv("HUNTER_API_KEY")  # hunter.io
MINELEAD_API_KEY = os.getenv("MINELEAD_API_KEY")  # minelead.io
PDL_API_KEY      = os.getenv("PDL_API_KEY")   # peopledatalabs.com
CLEARBIT_API_KEY = os.getenv("CLEARBIT_API_KEY")

# ── MongoDB ──────────────────────────────────────────────────────────────────
MONGO_URI= os.getenv("MONGO_URI")
MONGO_DB  =os.getenv("MONGO_DB")


# ── Pipeline settings ────────────────────────────────────────────────────────
JOB_FILE   = "new_jobs_temp.json"
MAX_JOBS   = 2
OUTPUT_DIR = Path("pipeline_output_15_jobs")

# ╔══════════════════════════════════════════════════════════════════╗
#  ║                    MONGODB SETUP                               ║
# ╚══════════════════════════════════════════════════════════════════╝

def get_mongo_db():
    """Connect to MongoDB and return the database. Returns None if unavailable."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        print(f"  [MongoDB] ✅ Connected → {MONGO_URI} / {MONGO_DB}")
        return db
    except ConnectionFailure as e:
        print(f"  [MongoDB] ⚠️  Cannot connect: {e}")
        print(f"  [MongoDB] ⚠️  Continuing with local JSON only")
        return None

def upsert_job(db, doc: dict):
    """Insert or update a job document by company+role."""
    if db is None: return
    try:
        db.jobs.update_one(
            {"company": doc.get("company"), "role": doc.get("role")},
            {"$set": doc},
            upsert=True
        )
    except Exception as e:
        print(f"  [MongoDB] ⚠️  jobs write failed: {e}")

def upsert_contacts(db, company: str, role: str, contacts: list):
    """Store individual contacts — one doc per contact."""
    if db is None or not contacts: return
    try:
        for c in contacts:
            doc = {**c, "company": company, "role": role,
                   "stored_at": datetime.now(timezone.utc).isoformat()}
            db.contacts.update_one(
                {"company": company, "name": c.get("name"), "email": c.get("email","")},
                {"$set": doc},
                upsert=True
            )
    except Exception as e:
        print(f"  [MongoDB] ⚠️  contacts write failed: {e}")

def upsert_emails(db, company: str, role: str, emails_doc: dict):
    """Store outreach email variants."""
    if db is None or not emails_doc: return
    try:
        doc = {"company": company, "role": role,
               "emails": emails_doc, "stored_at": datetime.now(timezone.utc).isoformat()}
        db.emails.update_one(
            {"company": company, "role": role},
            {"$set": doc},
            upsert=True
        )
    except Exception as e:
        print(f"  [MongoDB] ⚠️  emails write failed: {e}")

def insert_run_summary(db, summary: dict):
    """Store run-level summary."""
    if db is None: return
    try:
        db.run_summary.insert_one({**summary, "stored_at": datetime.now(timezone.utc).isoformat()})
    except Exception as e:
        print(f"  [MongoDB] ⚠️  run_summary write failed: {e}")


# ╔══════════════════════════════════════════════════════════════════╗
#  ║               CONTACT FINDER — 5 SOURCE CHAIN                 ║
# ╚══════════════════════════════════════════════════════════════════╝

TARGET_TITLES = [
    "ciso","chief information security","vp security","vp of security",
    "head of security","head of information security","director of security",
    "security director","compliance manager","head of compliance",
    "data protection officer","dpo","privacy officer",
    "it director","director of it","vp it","head of it",
    "security manager","information security manager","grc","risk manager",
    "cto","chief technology officer","coo","ceo","president","founder",
]
PRIORITY_MAP = {
    "ciso":"Primary","chief information security":"Primary",
    "vp security":"Primary","head of security":"Primary",
    "head of information security":"Primary","director of security":"Primary",
    "security director":"Primary","compliance manager":"Primary",
    "data protection officer":"Primary","privacy officer":"Primary",
    "it director":"Secondary","vp it":"Secondary","head of it":"Secondary",
    "security manager":"Secondary","grc":"Secondary","risk manager":"Secondary",
    "cto":"Secondary","chief technology officer":"Secondary",
    "ceo":"Tertiary","president":"Tertiary","founder":"Tertiary","coo":"Tertiary",
}

def is_target(t):
    if not t: return False
    return any(k in t.lower() for k in TARGET_TITLES)

def get_pri(t):
    if not t: return "General"
    tl = t.lower()
    for k, v in PRIORITY_MAP.items():
        if k in tl: return v
    if any(k in tl for k in ["vp","vice president","director","head of","chief","partner"]):
        return "Tertiary"
    return "General"

def make_c(name, title, li="", email="", co="", src=""):
    return {"name": name.strip(), "title": (title or "").strip(), "company": co,
            "linkedin_url": (li or "").strip(), "email": (email or "").strip(),
            "priority": get_pri(title), "source": src}

def real_name(name):
    w = name.strip().split()
    if not (2 <= len(w) <= 4): return False
    if not all(x[0].isupper() for x in w if x): return False
    bad = {"security","engineer","manager","director","about","team","contact",
           "home","services","company","blog","learn","read","view","our","the",
           "and","all","new","get","see","use","more","join","sign","log"}
    if any(x.lower() in bad for x in w): return False
    return any(len(x) >= 3 for x in w)

def dedupe(cs):
    seen, out = set(), []
    for c in cs:
        k = c["name"].lower().strip()
        if k and len(k) > 4 and k not in seen:
            seen.add(k); out.append(c)
    return out

def sort_p(cs):
    o = {"Primary": 0, "Secondary": 1, "Tertiary": 2, "General": 3}
    return sorted(cs, key=lambda c: o.get(c["priority"], 4))

def safe_get(url, timeout=12):
    try:
        h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
             "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
             "Accept-Language": "en-US,en;q=0.9"}
        r = requests.get(url, headers=h, timeout=timeout)
        print(f"      HTTP {r.status_code} | {url[:70]}")
        return r if r.status_code == 200 else None
    except Exception as e:
        print(f"      Err: {str(e)[:70]}"); return None

NAME_TITLE_RE = re.compile(
    r'([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})\s*(?:[-–|,\n])\s*'
    r'((?:CISO|CTO|VP|Head|Director|Manager|Chief|Officer|Lead|President|Founder|CEO|COO)'
    r'[A-Za-z\s&/,]{2,60})', re.MULTILINE)

EMAIL_PATS = ["{f}.{l}@{d}", "{f}@{d}", "{fi}{l}@{d}", "{f}{l}@{d}", "{l}@{d}"]

def guess_emails(name, domain):
    if not name or not domain: return []
    p = name.lower().split()
    if len(p) < 2: return []
    f, l, fi = p[0], p[-1], p[0][0]
    return [pt.format(f=f, l=l, fi=fi, d=domain) for pt in EMAIL_PATS]


# ── Domain Finder ─────────────────────────────────────────────────────────────

def find_domain(company_name):
    if CLEARBIT_API_KEY not in ("...", "", None):
        try:
            url  = f"https://company.clearbit.com/v1/domains/find?name={quote(company_name)}"
            resp = requests.get(url, auth=(CLEARBIT_API_KEY, ""), timeout=10)
            if resp.status_code == 200:
                domain = resp.json().get("domain", "")
                if domain:
                    print(f"  [Domain] ✅ Clearbit: {domain}"); return domain
        except Exception: pass

    if HUNTER_API_KEY not in ("...", "", None):
        try:
            url  = f"https://api.hunter.io/v2/domain-search?company={quote(company_name)}&api_key={HUNTER_API_KEY}&limit=1"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                domain = resp.json().get("data", {}).get("domain", "")
                if domain:
                    print(f"  [Domain] ✅ Hunter: {domain}"); return domain
        except Exception: pass

    slug   = re.sub(r"[^a-z0-9]", "", company_name.lower())
    domain = f"{slug}.com"
    print(f"  [Domain] ⚠️  Guessing: {domain}")
    return domain


# ── Source 1: Hunter.io ───────────────────────────────────────────────────────

def hunter_search(company_name, domain):
    if HUNTER_API_KEY in ("...", "", None):
        print("  [Hunter] ⚠️  No key — skipping"); return []

    print(f"\n  [Hunter.io] 🔍 {company_name}")
    url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}&limit=10"
    try:
        resp = requests.get(url, timeout=15)
        print(f"      HTTP {resp.status_code}")
        if resp.status_code == 401: print("      ❌ Invalid key"); return []
        if resp.status_code == 429: print("      ❌ Rate limit"); return []
        if resp.status_code != 200: print(f"      ❌ {resp.text[:80]}"); return []

        emails   = resp.json().get("data", {}).get("emails", [])
        contacts = []
        print(f"      Found {len(emails)} emails")
        for e in emails:
            try:
                first = e.get("first_name") or ""
                last  = e.get("last_name") or ""
                name  = f"{first} {last}".strip()
                title = e.get("position") or e.get("seniority") or ""
                email = e.get("value") or ""
                li    = e.get("linkedin") or ""
                conf  = e.get("confidence") or 0
                if not name: continue
                contacts.append(make_c(name, title or "Unknown", li, email,
                                       company_name, f"Hunter.io (conf:{conf}%)"))
                print(f"      {'🎯' if is_target(title) else '👤'} {name} | {title or '?'} | {email}")
            except Exception as ex:
                print(f"      ⚠️  Skipped: {ex}"); continue
        print(f"  [Hunter.io] ✅ {len(contacts)} contacts")
        return contacts
    except Exception as e:
        print(f"      ❌ {e}"); return []


# ── Source 2: Minelead.io ─────────────────────────────────────────────────────

def minelead_search(company_name, domain):
    if MINELEAD_API_KEY in ("...", "", None):
        print("  [Minelead] ⚠️  No key — skipping"); return []

    print(f"\n  [Minelead.io] 🔍 {company_name} ({domain})")
    url = f"https://api.minelead.io/v1/search?key={MINELEAD_API_KEY}&domain={domain}"
    try:
        resp = requests.get(url, timeout=15)
        print(f"      HTTP {resp.status_code}")
        if resp.status_code == 403: print("      ❌ Invalid key"); return []
        if resp.status_code == 429: print("      ❌ Rate limit"); return []
        if resp.status_code != 200: print(f"      ❌ {resp.text[:80]}"); return []

        data     = resp.json()
        emails   = data.get("emails", []) or data.get("data", []) or []
        contacts = []
        print(f"      Found {len(emails)} emails")
        for e in emails:
            try:
                if isinstance(e, str):
                    contacts.append(make_c("Unknown", "Unknown", "", e, company_name, "Minelead.io"))
                    continue
                name  = f"{e.get('first_name','') or ''} {e.get('last_name','') or ''}".strip()
                title = e.get("position") or e.get("title") or ""
                email = e.get("email") or e.get("value") or ""
                li    = e.get("linkedin") or ""
                if not email: continue
                contacts.append(make_c(name or "Unknown", title or "Unknown",
                                       li, email, company_name, "Minelead.io"))
                print(f"      {'🎯' if is_target(title) else '👤'} {name or '?'} | {title or '?'} | {email}")
            except Exception as ex:
                print(f"      ⚠️  Skipped: {ex}"); continue
        print(f"  [Minelead.io] ✅ {len(contacts)} contacts")
        return contacts
    except Exception as e:
        print(f"      ❌ {e}"); return []


# ── Source 3: People Data Labs ────────────────────────────────────────────────

def pdl_search(company_name, domain):
    if PDL_API_KEY in ("...", "", None):
        print("  [PDL] ⚠️  No key — skipping"); return []

    print(f"\n  [People Data Labs] 🔍 {company_name}")
    url     = "https://api.peopledatalabs.com/v5/person/search"
    headers = {"X-Api-Key": PDL_API_KEY, "Content-Type": "application/json"}
    sql = (
        f"SELECT * FROM person WHERE "
        f"job_company_name LIKE '%{company_name}%' AND "
        f"(job_title LIKE '%CISO%' OR job_title LIKE '%Security%' OR "
        f"job_title LIKE '%Compliance%' OR job_title LIKE '%IT Director%' OR "
        f"job_title LIKE '%CTO%' OR job_title LIKE '%Chief%')"
    )
    try:
        resp = requests.post(url, headers=headers, json={"sql": sql, "size": 10}, timeout=15)
        print(f"      HTTP {resp.status_code}")
        if resp.status_code == 401: print("      ❌ Invalid key"); return []
        if resp.status_code == 402: print("      ❌ Free credits used up"); return []
        if resp.status_code == 429: print("      ❌ Rate limit"); return []
        if resp.status_code != 200: print(f"      ❌ {resp.text[:80]}"); return []

        people   = resp.json().get("data", [])
        contacts = []
        print(f"      Found {len(people)} people")
        for p in people:
            try:
                # PDL can return booleans for some fields — coerce to str safely
                def _s(v): return str(v).strip() if v and not isinstance(v, bool) else ""
                first = _s(p.get("first_name"))
                last  = _s(p.get("last_name"))
                name  = _s(p.get("full_name")) or f"{first} {last}".strip()
                title = _s(p.get("job_title"))
                # work_email can be True/False (boolean) in PDL free tier
                raw_email = p.get("work_email")
                email = _s(raw_email) if raw_email and not isinstance(raw_email, bool) else ""
                # personal_emails can be a bool too
                if not email:
                    pe = p.get("personal_emails")
                    if isinstance(pe, list) and pe and not isinstance(pe[0], bool):
                        email = _s(pe[0])
                li = _s(p.get("linkedin_url"))
                if not name: continue
                contacts.append(make_c(name, title or "Unknown", li, email,
                                       company_name, "People Data Labs"))
                print(f"      {'🎯' if is_target(title) else '👤'} {name} | {title or '?'} | {email or '—'}")
            except Exception as ex:
                print(f"      ⚠️  Skipped: {ex}"); continue
        print(f"  [PDL] ✅ {len(contacts)} contacts")
        return contacts
    except Exception as e:
        print(f"      ❌ {e}"); return []


# ── Source 4: theorg.com ──────────────────────────────────────────────────────

def _walk_json(data, company_name, depth=0):
    contacts = []
    if depth > 8: return contacts
    if isinstance(data, dict):
        name  = data.get("name", "") or f"{data.get('firstName','') or ''} {data.get('lastName','') or ''}".strip()
        title = data.get("title", "") or data.get("jobTitle", "") or data.get("role", "")
        li    = data.get("linkedinUrl", "") or data.get("linkedin", "")
        if name and real_name(name) and is_target(title):
            contacts.append(make_c(name, title, li, "", company_name, "theorg.com"))
        for v in data.values():
            contacts.extend(_walk_json(v, company_name, depth + 1))
    elif isinstance(data, list):
        for item in data:
            contacts.extend(_walk_json(item, company_name, depth + 1))
    return contacts

def scrape_theorg(company_name):
    print(f"\n  [TheOrg] 🔍 {company_name}")
    slug = company_name.lower().replace(" ", "-").replace(".", "").replace(",", "")
    resp = safe_get(f"https://theorg.com/org/{slug}")
    if not resp:
        resp = safe_get(f"https://theorg.com/search?q={quote(company_name)}")
    if not resp:
        print(f"  [TheOrg] ❌ Not found"); return []

    soup     = BeautifulSoup(resp.text, "lxml")
    text     = soup.get_text(" ", strip=True)
    contacts = []

    for sc in soup.find_all("script", id="__NEXT_DATA__"):
        try:
            people = _walk_json(json.loads(sc.string or ""), company_name)
            contacts.extend(people)
            print(f"      {len(people)} from __NEXT_DATA__")
        except Exception: pass

    for card in soup.find_all(["div", "article", "li"],
                               class_=re.compile(r"(person|member|leader|profile|card|team)", re.I)):
        nt = card.find(["h2", "h3", "h4", "strong", "b", "a"])
        tt = card.find(["p", "span", "small"])
        nm = re.sub(r"\s+", " ", nt.get_text(strip=True)).strip() if nt else ""
        ti = re.sub(r"\s+", " ", tt.get_text(strip=True)).strip() if tt else ""
        if nm and real_name(nm) and nm not in [c["name"] for c in contacts]:
            contacts.append(make_c(nm, ti or "Unknown", "", "", company_name, "theorg.com"))
            print(f"      ✅ {nm} | {ti}")

    for m in NAME_TITLE_RE.finditer(text):
        nm, ti = m.group(1).strip(), m.group(2).strip()
        if real_name(nm) and nm not in [c["name"] for c in contacts]:
            contacts.append(make_c(nm, ti, "", "", company_name, "theorg.com text"))

    print(f"  [TheOrg] ✅ {len(contacts)} contacts")
    return contacts


# ── Source 5: Bing Search ─────────────────────────────────────────────────────

BING_Q = [
    '"{c}" CISO linkedin', '"{c}" "Head of Security" linkedin',
    '"{c}" "Compliance Manager" linkedin', '"{c}" "IT Director" linkedin',
]

def search_bing(company_name):
    print(f"\n  [Bing] 🔍 {company_name}")
    contacts, sess = [], requests.Session()
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"}

    for tmpl in BING_Q:
        q = tmpl.format(c=company_name)
        try:
            r = sess.get(f"https://www.bing.com/search?q={quote(q)}&count=10", headers=h, timeout=12)
            print(f"      HTTP {r.status_code} | {q[:55]}")
            if r.status_code != 200: time.sleep(2); continue
        except Exception as e:
            print(f"      Err: {e}"); continue

        soup  = BeautifulSoup(r.text, "lxml")
        slugs = list(dict.fromkeys(re.findall(r'linkedin\.com/in/([A-Za-z0-9_%-]{3,40})', r.text)))
        pairs = []

        for res in soup.find_all("li", class_="b_algo"):
            h2   = res.find("h2")
            snip = res.find("p") or res.find("div", class_=re.compile(r"b_caption"))
            txt  = (h2.get_text(" ") if h2 else "") + " " + (snip.get_text(" ") if snip else "")
            for m in re.finditer(
                r'([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,3})\s*[-–]\s*'
                r'((?:CISO|CTO|VP|Head|Director|Manager|Chief|Officer|Lead|President|CEO)'
                r'[A-Za-z\s&/,]{2,55}?)(?:\s+(?:at|@|for)\s+|\s*[|,])', txt):
                nm, ti = m.group(1).strip(), m.group(2).strip()
                if is_target(ti) and real_name(nm):
                    pairs.append((nm, ti)); print(f"      ✅ {nm} | {ti}")

        used = set()
        for slug in slugs[:6]:
            li_full = f"https://www.linkedin.com/in/{slug}"
            nm, ti  = "", ""
            for n2, t2 in pairs:
                if n2.lower() not in used:
                    nm, ti = n2, t2; used.add(nm.lower()); break
            if not nm:
                clean = re.sub(r'\d+$', '', slug).replace("-", " ").strip().split()
                if 2 <= len(clean) <= 3:
                    nm = " ".join(p.capitalize() for p in clean[:2])
            if nm and real_name(nm):
                contacts.append(make_c(nm, ti or "Security/IT Professional",
                                       li_full, "", company_name, "Bing"))
        for nm, ti in pairs:
            if nm.lower() not in used:
                contacts.append(make_c(nm, ti, "", "", company_name, "Bing Snippet"))
                used.add(nm.lower())

        time.sleep(2)
        if len(contacts) >= 5: break

    print(f"  [Bing] ✅ {len(contacts)} contacts")
    return contacts


# ── Hunter email finder for contacts missing email ────────────────────────────

def hunter_email_finder(first, last, domain):
    if HUNTER_API_KEY in ("...", "", None) or not domain: return ""
    url = (f"https://api.hunter.io/v2/email-finder?"
           f"domain={domain}&first_name={first}&last_name={last}&api_key={HUNTER_API_KEY}")
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            d = r.json().get("data", {})
            e = d.get("email", "")
            if e and d.get("score", 0) > 50: return e
    except Exception: pass
    return ""


# ── Main contact finder — orchestrates all 5 sources ─────────────────────────

def find_contacts(company_name, jd_url=""):
    print(f"\n  {'─'*56}")
    print(f"  CONTACT SEARCH: {company_name}")
    print(f"  Chain: Hunter → Minelead → PDL → theorg → Bing")
    print(f"  {'─'*56}")

    domain       = find_domain(company_name)
    all_contacts = []
    sources      = []

    hc = hunter_search(company_name, domain)
    all_contacts.extend(hc)
    if hc: sources.append("Hunter.io")
    time.sleep(1)

    mc = minelead_search(company_name, domain)
    all_contacts.extend(mc)
    if mc: sources.append("Minelead.io")
    time.sleep(1)

    pc = pdl_search(company_name, domain)
    all_contacts.extend(pc)
    if pc: sources.append("People Data Labs")
    time.sleep(1)

    tc = scrape_theorg(company_name)
    all_contacts.extend(tc)
    if tc: sources.append("theorg.com")
    time.sleep(1)

    if len(dedupe(all_contacts)) < 3:
        bc = search_bing(company_name)
        all_contacts.extend(bc)
        if bc: sources.append("Bing Search")

    final = sort_p(dedupe(all_contacts))

    if HUNTER_API_KEY not in ("...", "", None):
        used = 0
        for c in final:
            if not c["email"] and c["priority"] == "Primary" and used < 3:
                parts = c["name"].split()
                if len(parts) >= 2:
                    print(f"\n  [Hunter] 📧 Finding email: {c['name']}")
                    email = hunter_email_finder(parts[0], parts[-1], domain)
                    if email:
                        c["email"] = email; used += 1
                        print(f"      ✅ {email}")
                    time.sleep(1)

    for c in final:
        if not c.get("email") and c["name"] and domain:
            c["email_patterns"] = guess_emails(c["name"], domain)

    slug = company_name.lower().replace(" ", "-")
    if final:
        return {"status": "success", "company": company_name, "domain": domain,
                "sources_tried": sources, "total_found": len(final),
                "contacts": final, "note": "email_patterns are guesses — not verified."}
    return {"status": "not_found", "company": company_name, "domain": domain,
            "sources_tried": sources, "total_found": 0, "contacts": [],
            "note": f"No contacts found. Manual: linkedin.com/company/{slug}/people"}


# ╔══════════════════════════════════════════════════════════════════╗
#  ║              14-AGENT CREWAI PIPELINE                         ║
# ╚══════════════════════════════════════════════════════════════════╝

def build_agents(llm, scrape_tool):
    a1 = Agent(
        role="Job Posting Researcher & Scraper",
        goal="Normalize the given job posting into a clean JSON payload with "
             "Job Role, Job Description, Company Name, Organization URL, and Location.",
        backstory="You are a reverse-prospecting analyst specialising in mining hiring "
                  "signals to infer buying intent for cybersecurity and compliance services.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a2 = Agent(
        role="Job Context Enrichment Researcher",
        goal="Enrich the job posting with company intel: industry, size, regulatory "
             "environment, certifications, tech stack, and security maturity signals.",
        backstory="You are a senior GRC researcher who quickly interprets a company's "
                  "public footprint to reveal security and compliance needs.",
        tools=[scrape_tool], llm=llm, verbose=False, allow_delegation=False
    )
    a3 = Agent(
        role="SecureITLab Service Mapping Specialist",
        goal="Map the enriched job to SecureITLab's service portfolio and explain "
             "which services address which job requirements.",
        backstory="You are a senior solutions consultant at SecureITLab. Services:\n"
                  "• Cybersecurity Consulting & Strategy\n"
                  "• Compliance & Audit (ISO 27001, SOC 2, GDPR, HIPAA)\n"
                  "• Proactive Security Assurance\n"
                  "• Risk Assessment & GRC\n"
                  "• Security Training & Awareness\n"
                  "• Staff Augmentation (vCISO, SOC, pen testers)\n"
                  "• Incident Response & Forensics",
        llm=llm, verbose=False, allow_delegation=False
    )
    a4 = Agent(
        role="Service Fit & Gap Analyst",
        goal="Classify each mapped service as STRONG FIT, PARTIAL FIT, or GAP. "
             "Give justification and an overall opportunity score out of 10.",
        backstory="You are a pragmatic portfolio manager who never over-promises.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a5 = Agent(
        role="Capability Uplift Strategist",
        goal="For every PARTIAL FIT and GAP recommend specific steps to close the gap: "
             "hiring, partnerships, training, certifications, tooling.",
        backstory="You are a GRC operating-model architect who has grown boutique consultancies.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a6 = Agent(
        role="Service Maturity Planner",
        goal="Convert the top 3 capability improvements into 2-12 week micro-plans "
             "with objectives, tasks, owners, dependencies, and KPIs.",
        backstory="You are a delivery-focused program manager who breaks strategic goals "
                  "into practical, auditable roadmaps.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a7 = Agent(
        role="Deal Assurance & Bid Readiness Architect",
        goal="Produce a Deal Assurance Pack: mandatory capabilities checklist, "
             "proof points, compliance evidence, risk mitigation, and a 1-page "
             "executive value proposition.",
        backstory="You are a seasoned pre-sales lead expert at SecureITLab.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a8 = Agent(
        role="First-Touch Outreach Copywriter",
        goal="Write two personalised outreach emails: "
             "Variant A for Hiring Manager/Security Lead (150-200 words), "
             "Variant B for CISO/VP Level (executive-focused, business impact).",
        backstory="You are a cybersecurity-savvy sales copywriter trained on "
                  "SecureITLab's positioning as a proactive, lean, senior consulting team.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a9 = Agent(
        role="Prospect Contact Finder",
        goal="Find real decision-maker contacts (CISO, Compliance Manager, IT Director). "
             "If not found output not_found. Do NOT invent contacts.",
        backstory="You are an SDR research agent who never fabricates details.",
        tools=[scrape_tool], llm=llm, verbose=False, allow_delegation=False
    )
    a10 = Agent(
        role="Job Research QA Validator",
        goal=(
            "Validate the job research output against 6 items:\n"
            "1.Job Role  2.Job Description  3.Company Name  "
            "4.Organization URL  5.Location  6.No hallucinations\n"
            "Output JSON: passed, checklist, issues, recommendation (APPROVE/REWORK)"
        ),
        backstory="Former Big 4 audit reviewer turned AI pipeline inspector.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a11 = Agent(
        role="Service Mapping & Analysis QA Validator",
        goal=(
            "Validate service mapping and fit/gap: services tied to requirements, "
            "proof points present, opportunity score exists, ≥2 service lines mapped.\n"
            "Output JSON: passed, checklist, issues, recommendation"
        ),
        backstory="Senior solutions consultant quality reviewer at SecureITLab.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a12 = Agent(
        role="Deal Assurance QA Validator",
        goal=(
            "Validate Deal Assurance Pack: capabilities checklist, proof points, "
            "compliance frameworks, risk mitigation, exec value prop <200 words.\n"
            "Output JSON: passed, checklist, issues, recommendation"
        ),
        backstory="Former Big 4 bid assurance reviewer for cybersecurity engagements.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a13 = Agent(
        role="Outreach Email QA Validator",
        goal=(
            "Validate both email variants: word count, personalisation, CTA present, "
            "no unfilled placeholders, SecureITLab positioned correctly.\n"
            "Output JSON: passed, checklist, issues, recommendation, improved_emails if needed"
        ),
        backstory="B2B cybersecurity sales email specialist.",
        llm=llm, verbose=False, allow_delegation=False
    )
    a14 = Agent(
        role="Prospect Contact Completeness Enforcer",
        goal=(
            "Check coverage of CISO, Compliance Manager, IT Director. "
            "If missing attempt one more search. Assign email variants.\n"
            "Output JSON: coverage_score (0-100), contacts, missing_roles, note"
        ),
        backstory="Relentless SDR playbook enforcer. Never fabricates contacts.",
        tools=[scrape_tool], llm=llm, verbose=False, allow_delegation=False
    )
    return (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14)


def build_tasks(job_data: dict, out_dir: Path, agents: tuple):
    a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14 = agents
    job_json = json.dumps(job_data, indent=2)

    t1 = Task(
        description=f"Normalize this job posting into clean JSON.\n\nRAW DATA:\n{job_json}\n\n"
                    "Extract: Job Role, Job Description, Company Name, Organization URL, Location.",
        expected_output="Clean JSON with normalized job posting fields",
        agent=a1, output_file=str(out_dir / "01_job_research.json")
    )
    t2 = Task(
        description="Review the job research from Task 1. Validate all 6 items. Output JSON QA report.",
        expected_output="JSON QA report with passed, checklist, issues, recommendation",
        agent=a10, context=[t1], output_file=str(out_dir / "02_research_qa.json")
    )
    t3 = Task(
        description="Using the QA-approved job from Task 1, visit the company website. "
                    "Collect: industry, company size, regulatory environment, certs, tech stack, "
                    "security maturity. Output enriched JSON.",
        expected_output="JSON with job data + company context",
        agent=a2, context=[t1, t2], output_file=str(out_dir / "03_enrichment.json")
    )
    t4 = Task(
        description="Map the enriched job to SecureITLab's 7 service lines. "
                    "For each: why relevant, which requirements it addresses, engagement type. JSON.",
        expected_output="JSON service mapping matrix",
        agent=a3, context=[t3], output_file=str(out_dir / "04_service_mapping.json")
    )
    t5 = Task(
        description="Classify each service as STRONG FIT / PARTIAL FIT / GAP. "
                    "Justify each, add proof points, delivery risk, opportunity score 1-10. JSON.",
        expected_output="JSON with service classifications and opportunity score",
        agent=a4, context=[t4], output_file=str(out_dir / "05_fit_gap_analysis.json")
    )
    t6 = Task(
        description="Review service mapping (Task 4) and fit/gap (Task 5). "
                    "Validate 6 items. Output JSON QA report.",
        expected_output="JSON QA report",
        agent=a11, context=[t4, t5], output_file=str(out_dir / "06_mapping_qa.json")
    )
    t7 = Task(
        description="For every PARTIAL FIT and GAP from Task 5, recommend: "
                    "hiring, partnerships, training, certifications, tooling. "
                    "Prioritise by demand and effort. JSON.",
        expected_output="JSON capability improvement recommendations",
        agent=a5, context=[t5, t6], output_file=str(out_dir / "07_capability_improvement.json")
    )
    t8 = Task(
        description="Top 3 capability improvements from Task 7 → 2-12 week micro-plans. "
                    "Each: objective, tasks, owners, dependencies, KPIs, milestones. JSON.",
        expected_output="JSON with 3 micro-plans",
        agent=a6, context=[t7], output_file=str(out_dir / "08_maturity_microplans.json")
    )
    t9 = Task(
        description="Create Deal Assurance Pack:\n"
                    "1. Mandatory capabilities checklist\n"
                    "2. Proof points (case studies, credentials, methodology)\n"
                    "3. Compliance evidence (frameworks, audit support)\n"
                    "4. Risk mitigation (SLAs, governance)\n"
                    "5. Executive value proposition <200 words\nOutput JSON.",
        expected_output="JSON deal assurance pack",
        agent=a7, context=[t5, t8], output_file=str(out_dir / "09_deal_assurance_pack.json")
    )
    t10 = Task(
        description="Review Deal Assurance Pack (Task 9). Validate 6 items. "
                    "Flag vague claims with specific replacements. JSON QA report.",
        expected_output="JSON QA report",
        agent=a12, context=[t9], output_file=str(out_dir / "10_assurance_qa.json")
    )
    t11 = Task(
        description="Write TWO outreach email variants as JSON:\n"
                    "VARIANT A — Hiring Manager 150-200 words, references job, 15-min CTA, subject line.\n"
                    "VARIANT B — CISO/VP executive tone, business impact, subject line.\n"
                    "Use Deal Assurance Pack for proof points.",
        expected_output="JSON with subject + body for each variant",
        agent=a8, context=[t9, t10], output_file=str(out_dir / "11_outreach_emails.json")
    )
    t12 = Task(
        description="Review both email variants (Task 11). Validate 5 items. "
                    "Provide improved_emails if issues found. JSON QA report.",
        expected_output="JSON QA report with optional improved_emails",
        agent=a13, context=[t11], output_file=str(out_dir / "12_outreach_qa.json")
    )
    t13 = Task(
        description="Search for real decision-maker contacts at the company from Task 1.\n"
                    "Targets: CISO, VP Security, Head of InfoSec, Compliance Manager, IT Director.\n"
                    "1. Visit company website team/leadership page\n"
                    "2. Check linkedin.com/company/[company]/people\n"
                    "Output JSON with real contacts. Do NOT invent anyone.",
        expected_output="JSON with real contacts or not_found",
        agent=a9, context=[t1, t11], output_file=str(out_dir / "13_prospect_contacts.json")
    )
    t14 = Task(
        description="Review contacts (Task 13). Check coverage: CISO, Compliance, IT Director. "
                    "If missing try one more search. Assign email variants (CISO/VP → B, others → A). "
                    "Output JSON: coverage_score, contacts, missing_roles, note. No fabrication.",
        expected_output="JSON with coverage_score (0-100), contacts, missing_roles, note",
        agent=a14, context=[t13], output_file=str(out_dir / "14_prospect_enforcer.json")
    )
    return [t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14]


# ╔══════════════════════════════════════════════════════════════════╗
#  ║                 JSON PARSER HELPER                             ║
# ╚══════════════════════════════════════════════════════════════════╝

def read_json_file(filepath: Path):
    if not filepath.exists(): return None
    raw = filepath.read_text(encoding="utf-8").strip()
    if not raw: return None
    try: return json.loads(raw)
    except Exception: pass
    fence = re.search(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", raw, re.DOTALL)
    if fence:
        try: return json.loads(fence.group(1))
        except Exception: pass
    for s, e in [('{', '}'), ('[', ']')]:
        start = raw.find(s)
        if start == -1: continue
        depth = end = 0
        for i, ch in enumerate(raw[start:], start=start):
            if ch == s: depth += 1
            elif ch == e:
                depth -= 1
                if depth == 0: end = i + 1; break
        if end > start:
            try: return json.loads(raw[start:end])
            except Exception: pass
    return {"raw_text": raw}

def print_qa(label: str, filepath: Path):
    data = read_json_file(filepath)
    if not data:
        print(f"  {label}: ❓ missing"); return
    if isinstance(data, dict):
        passed = data.get("passed") or data.get("Passed")
        rec    = data.get("recommendation") or data.get("Recommendation", "")
        issues = data.get("issues") or data.get("Issues", [])
        icon   = "✅" if passed else "⚠️ "
        print(f"  {label}: {icon} {'APPROVED' if passed else 'REWORK'} — {str(rec)[:80]}")
        for issue in (issues[:2] if isinstance(issues, list) else []):
            print(f"     • {str(issue)[:90]}")


# ╔══════════════════════════════════════════════════════════════════╗
#  ║                        MAIN LOOP                               ║
# ╚══════════════════════════════════════════════════════════════════╝

def main():
    print("\n" + "═"*65)
    print("  UNIFIED PIPELINE — 14 AGENTS + 5-SOURCE CONTACTS × 15 JOBS")
    print("  + MONGODB STORAGE")
    print("═"*65)

    # ── Check keys ───────────────────────────────────────────────────────────
    print("\n  API Keys:")
    print(f"  {'✅' if OPENAI_API_KEY not in ('...','',None) else '❌'} OpenAI")
    for label, key in [("Hunter.io", HUNTER_API_KEY), ("Minelead.io", MINELEAD_API_KEY),
                        ("People Data Labs", PDL_API_KEY), ("Clearbit", CLEARBIT_API_KEY)]:
        print(f"  {'✅' if key not in ('...','',None) else '⚠️ '} {label}: "
              f"{'Set' if key not in ('...','',None) else 'Not set — will skip'}")
    print(f"  ✅ theorg.com + Bing: Free scraping (always runs)")

    if OPENAI_API_KEY in ("...", "", None):
        print("\n  ❌ OPENAI_API_KEY not set. Add it to .env or top of file."); return

    # ── MongoDB ──────────────────────────────────────────────────────────────
    print()
    db = get_mongo_db()

    # ── Load jobs ─────────────────────────────────────────────────────────────
    if not Path(JOB_FILE).exists():
        print(f"\n  ❌ {JOB_FILE} not found"); return

    raw  = Path(JOB_FILE).read_text(encoding="utf-8")
    data = json.loads(raw)
    if isinstance(data, list):
        all_jobs = data
    else:
        all_jobs = next((data[k] for k in ("jobs","postings","results","data","listings")
                         if k in data and isinstance(data[k], list)), [])

    jobs_to_run = all_jobs[:MAX_JOBS]
    print(f"\n  📂 {len(all_jobs)} jobs in file → running first {len(jobs_to_run)}")
    print(f"  📁 Output → {OUTPUT_DIR}/")
    print(f"  🗄️  MongoDB → {MONGO_DB} ({4} collections)\n")

    # ── Init LLM + tool ───────────────────────────────────────────────────────
    llm         = ChatOpenAI(model="gpt-4o", temperature=0.7, api_key=OPENAI_API_KEY)
    scrape_tool = ScrapeWebsiteTool()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_start   = time.time()
    run_results = []

    # ═════════════════════════════════════════════════════════════════════════
    #  JOB LOOP
    # ═════════════════════════════════════════════════════════════════════════
    for idx, job_data in enumerate(jobs_to_run, start=1):
        company = (job_data.get("company") or job_data.get("organization") or
                   job_data.get("company_name") or "Unknown")
        role    = (job_data.get("title") or job_data.get("role") or
                   job_data.get("job_title") or "Unknown")
        jd_url  = (job_data.get("full_jd_url") or job_data.get("job_url") or "")

        print(f"\n{'═'*65}")
        print(f"  JOB {idx}/{len(jobs_to_run)}: {role}")
        print(f"  COMPANY: {company}")
        print(f"{'═'*65}")

        # ── Per-job output folder ────────────────────────────────────────────
        safe_co  = re.sub(r"[^a-z0-9]", "_", company.lower())[:30]
        job_dir  = OUTPUT_DIR / f"job_{idx:02d}_{safe_co}"
        job_dir.mkdir(exist_ok=True)
        (job_dir / "00_raw_input.json").write_text(
            json.dumps(job_data, indent=2), encoding="utf-8")

        job_record = {
            "job_number": idx,
            "company":    company,
            "role":       role,
            "jd_url":     jd_url,
            "run_at":     datetime.now(timezone.utc).isoformat(),
        }

        # ── ① Run 14-agent CrewAI pipeline ───────────────────────────────────
        print(f"\n  🤖 Running 14-agent pipeline...")
        pipeline_ok = False
        pipeline_start = time.time()

        try:
            agents = build_agents(llm, scrape_tool)
            tasks  = build_tasks(job_data, job_dir, agents)
            crew   = Crew(agents=list(agents), tasks=tasks,
                          process=Process.sequential, verbose=False)
            crew.kickoff()
            pipeline_elapsed = time.time() - pipeline_start
            print(f"  ✅ Pipeline done in {pipeline_elapsed/60:.1f} min")
            pipeline_ok = True
        except Exception as e:
            print(f"  ❌ Pipeline error: {e}")
            pipeline_elapsed = time.time() - pipeline_start

        # ── Read pipeline outputs ────────────────────────────────────────────
        print(f"\n  🔍 QA Results:")
        print_qa("Research QA ", job_dir / "02_research_qa.json")
        print_qa("Mapping QA  ", job_dir / "06_mapping_qa.json")
        print_qa("Assurance QA", job_dir / "10_assurance_qa.json")
        print_qa("Outreach QA ", job_dir / "12_outreach_qa.json")

        # Agent may return list or dict — coerce safely
        def _as_dict(raw):
            if isinstance(raw, dict): return raw
            if isinstance(raw, list):
                return next((x for x in raw if isinstance(x, dict)), {})
            return {}

        fit_data  = _as_dict(read_json_file(job_dir / "05_fit_gap_analysis.json"))
        opp_score = None
        for key in ("opportunity_score","overall_score","score","OverallOpportunityScore"):
            val = fit_data.get(key) if hasattr(fit_data, "get") else None
            if val is not None and val is not False and val != "":
                opp_score = val; break
        if opp_score:
            print(f"  📊 Opportunity Score: {opp_score}/10")

        emails_data   = _as_dict(read_json_file(job_dir / "11_outreach_emails.json"))
        enforcer_data = _as_dict(read_json_file(job_dir / "14_prospect_enforcer.json"))

        job_record["pipeline_ok"]    = pipeline_ok
        job_record["pipeline_min"]   = round(pipeline_elapsed / 60, 1)
        job_record["opp_score"]      = opp_score
        job_record["outreach_emails"] = emails_data
        job_record["coverage_score"] = enforcer_data.get("coverage_score")
        job_record["missing_roles"]  = enforcer_data.get("missing_roles", [])

        # Attach all 14 file outputs into the job record
        file_map = {
            "job_research": "01_job_research.json",
            "research_qa": "02_research_qa.json",
            "enrichment": "03_enrichment.json",
            "service_mapping": "04_service_mapping.json",
            "fit_gap": "05_fit_gap_analysis.json",
            "mapping_qa": "06_mapping_qa.json",
            "capability": "07_capability_improvement.json",
            "microplans": "08_maturity_microplans.json",
            "deal_assurance": "09_deal_assurance_pack.json",
            "assurance_qa": "10_assurance_qa.json",
            "outreach_emails": "11_outreach_emails.json",
            "outreach_qa": "12_outreach_qa.json",
            "prospect_contacts": "13_prospect_contacts.json",
            "prospect_enforcer": "14_prospect_enforcer.json",
        }
        for field, filename in file_map.items():
            parsed = read_json_file(job_dir / filename)
            if parsed:
                job_record[f"agent_{field}"] = parsed

        # ── ② Run 5-source contact finder ────────────────────────────────────
        print(f"\n  📇 Running 5-source contact finder...")
        contact_result = find_contacts(company, jd_url)

        # Save contact JSON locally
        contact_file = job_dir / "15_contacts_5source.json"
        contact_file.write_text(json.dumps(contact_result, indent=2), encoding="utf-8")

        job_record["contacts_found"]   = contact_result["total_found"]
        job_record["contact_sources"]  = contact_result["sources_tried"]
        job_record["contact_domain"]   = contact_result.get("domain", "")
        job_record["contacts"]         = contact_result["contacts"]
        job_record["contact_status"]   = contact_result["status"]

        print(f"\n  📇 {contact_result['status']} | "
              f"{contact_result['total_found']} contacts | "
              f"Sources: {', '.join(contact_result['sources_tried']) or 'none'}")
        for c in contact_result["contacts"][:3]:
            print(f"    [{c['priority']}] {c['name']} | {c['title']} | {c.get('email','—')}")
        if contact_result["total_found"] > 3:
            print(f"    ... +{contact_result['total_found']-3} more")

        # ── ③ Store everything to MongoDB ────────────────────────────────────
        if db is not None:
            print(f"\n  🗄️  Saving to MongoDB...")
            upsert_job(db, job_record)
            upsert_contacts(db, company, role, contact_result["contacts"])
            if emails_data:
                upsert_emails(db, company, role, emails_data)
            print(f"  🗄️  ✅ Saved → jobs / contacts / emails")

        run_results.append({
            "job_number":      idx,
            "company":         company,
            "role":            role,
            "pipeline_ok":     pipeline_ok,
            "pipeline_min":    round(pipeline_elapsed / 60, 1),
            "opp_score":       opp_score,
            "contacts_found":  contact_result["total_found"],
            "contact_sources": contact_result["sources_tried"],
            "coverage_score":  enforcer_data.get("coverage_score"),
        })

        print(f"\n  💾 Saved to: {job_dir.name}/")

        if idx < len(jobs_to_run):
            print(f"\n  ⏳ 5s pause before next job...")
            time.sleep(5)

    # ═════════════════════════════════════════════════════════════════════════
    #  RUN SUMMARY
    # ═════════════════════════════════════════════════════════════════════════
    total_elapsed = time.time() - run_start
    summary = {
        "run_at":          datetime.now(timezone.utc).isoformat(),
        "total_jobs":      len(jobs_to_run),
        "pipeline_ok":     sum(1 for r in run_results if r["pipeline_ok"]),
        "pipeline_failed": sum(1 for r in run_results if not r["pipeline_ok"]),
        "contacts_found":  sum(r["contacts_found"] for r in run_results),
        "total_min":       round(total_elapsed / 60, 1),
        "jobs":            run_results,
    }
    (OUTPUT_DIR / "run_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8")
    if db is not None:
        insert_run_summary(db, summary)

    # ── Final print ───────────────────────────────────────────────────────────
    print("\n" + "═"*65)
    print("  ✅ ALL 15 JOBS COMPLETE")
    print("═"*65)
    print(f"\n  Total time : {summary['total_min']} min")
    print(f"  Pipeline   : {summary['pipeline_ok']} OK  |  {summary['pipeline_failed']} failed")
    print(f"  Contacts   : {summary['contacts_found']} total found across all jobs")
    print(f"\n  {'Job':<4} {'Company':<28} {'Score':>5} {'Contacts':>8} {'Pipeline'}")
    print(f"  {'─'*4} {'─'*28} {'─'*5} {'─'*8} {'─'*8}")
    for r in run_results:
        icon = "✅" if r["pipeline_ok"] else "❌"
        score = f"{r['opp_score']}/10" if r["opp_score"] else "  —  "
        print(f"  {r['job_number']:>2}.  {r['company'][:27]:<28} {score:>5}"
              f"  {r['contacts_found']:>6}   {icon}")

    print(f"\n  📁 Local files : {OUTPUT_DIR}/")
    if db is not None:
        print(f"  🗄️  MongoDB     : {MONGO_DB}")
        print(f"     • jobs         — {len(jobs_to_run)} docs (full pipeline per job)")
        print(f"     • contacts     — {summary['contacts_found']} docs (one per contact)")
        print(f"     • emails       — {len(jobs_to_run)} docs (outreach variants per job)")
        print(f"     • run_summary  — 1 doc (this run)")
    print("═"*65 + "\n")


if __name__ == "__main__":
    main()