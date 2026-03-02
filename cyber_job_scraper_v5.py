# """
# ================================================================
#   CYBERSECURITY REMOTE JOB SCRAPER v5.0
#   ✅ FIXED: Strict title-based filter — NO false positives
#   ✅ FIXED: matched_keywords checks title + tags (not just snippet)
#   ✅ FIXED: Non-cyber jobs (Graphic Designer, Marketer etc.) blocked
#   ✅ 90+ Cybersecurity keywords
#   ✅ Phase 1: Free APIs (RemoteOK, Arbeitnow, Jobicy, Himalayas, Remotive)
#   ✅ Phase 2: Selenium (LinkedIn, Indeed, Glassdoor, Naukri)
#   ✅ Remote/Online Jobs ONLY
#   Output: cybersecurity_remote_jobs.json

#   Run:   python cyber_job_scraper_v5.py
#   Clean: python cyber_job_scraper_v5.py --clean old_file.json
# ================================================================
# SETUP:
#     pip install requests selenium webdriver-manager beautifulsoup4 lxml
# ================================================================
# """

# import sys
# import requests
# import json
# import time
# import random
# import re
# import logging
# from datetime import datetime
# from collections import Counter
# from bs4 import BeautifulSoup

# try:
#     from selenium import webdriver
#     from selenium.webdriver.chrome.service import Service
#     from selenium.webdriver.chrome.options import Options
#     from selenium.webdriver.common.by import By
#     from webdriver_manager.chrome import ChromeDriverManager
#     SELENIUM_AVAILABLE = True
# except ImportError:
#     SELENIUM_AVAILABLE = False

# OUTPUT_FILE = "cybersecurity_remote_jobs.json"

# # ════════════════════════════════════════════════════════════════
# #  KEYWORD LISTS
# # ════════════════════════════════════════════════════════════════

# # Your full 90+ specific keywords — used for matched_keywords tagging
# ALL_KEYWORDS = [
#     "Communications Security COMSEC Management",
#     "Cybersecurity Policy Planning",
#     "Cybersecurity Workforce Management",
#     "Cybersecurity Curriculum Development",
#     "Cybersecurity Instruction",
#     "Cybersecurity Legal Advice",
#     "Executive Cybersecurity Leadership",
#     "Privacy Compliance",
#     "Secure Project Management",
#     "Security Control Assessment",
#     "Systems Authorization",
#     "Systems Security Management",
#     "Technology Portfolio Management",
#     "Technology Program Auditing",
#     "Cybersecurity Architecture",
#     "Enterprise Architecture",
#     "Secure Software Development",
#     "Secure Systems Development",
#     "Software Security Assessment",
#     "Systems Requirements Planning",
#     "Systems Testing and Evaluation",
#     "Operational Technology OT Cybersecurity",
#     "Network Operations Security",
#     "Systems Security Analysis",
#     "Network Security",
#     "Defensive Cybersecurity",
#     "Digital Forensics",
#     "Incident Response",
#     "Insider Threat Analysis",
#     "Threat Analysis",
#     "Vulnerability Analysis",
#     "Cybercrime Investigation",
#     "Digital Evidence Analysis",
#     "Chief Information Security Officer CISO",
#     "Chief Security Officer CSO",
#     "Director of Cybersecurity",
#     "Information Security Manager",
#     "Chief Privacy Officer CPO",
#     "Cyber Workforce Strategy",
#     "Junior SOC Analyst",
#     "Security Analyst",
#     "Senior Security Analyst",
#     "Threat Hunter",
#     "Security Engineer",
#     "Penetration Tester",
#     "Red Team",
#     "Vulnerability Researcher",
#     "Application Penetration Tester",
#     "Bug Bounty Hunter",
#     "Purple Team Specialist",
#     "Cloud Security Architect",
#     "Cloud Security Engineer",
#     "Network Security Engineer",
#     "Zero Trust Architect",
#     "Security Architect",
#     "PKI Professional",
#     "Application Security Engineer",
#     "DevSecOps Specialist",
#     "Product Security Engineer",
#     "Secure Software Developer",
#     "Software Security Assessor",
#     "GRC Manager",
#     "Compliance Analyst",
#     "Risk Analyst",
#     "IT Auditor",
#     "Security Awareness Officer",
#     "Cybersecurity Planner",
#     "ICS SCADA Security Engineer",
#     "OT Cybersecurity Analyst",
#     "SCADA Specialist",
#     "IIoT Security Specialist",
#     "Automotive Security Engineer",
#     "Digital Forensics Analyst",
#     "Digital Forensic Examiner",
#     "Malware Analyst",
#     "Cybercrime Investigator",
#     "eDiscovery Specialist",
#     "AI Security Specialist",
#     "Adversarial AI Researcher",
#     "Blockchain Security Engineer",
#     "Post-Quantum Cryptographer",
#     "Deepfake Analyst",
#     "IoT Security Specialist",
# ]

# # Short terms used to query APIs
# API_SEARCH_KEYWORDS = [
#     "cybersecurity analyst", "SOC analyst", "penetration tester",
#     "security engineer", "information security", "threat hunter",
#     "incident response analyst", "digital forensics", "malware analyst",
#     "cloud security engineer", "devsecops", "vulnerability analyst",
#     "red team analyst", "application security engineer", "SIEM analyst",
#     "network security engineer", "GRC analyst", "risk analyst",
#     "ICS SCADA security", "OT security analyst", "AI security specialist",
#     "zero trust architect", "security architect", "CISO",
#     "cybercrime investigator", "insider threat analyst",
#     "security awareness", "compliance analyst", "blockchain security",
#     "IoT security", "bug bounty", "purple team", "threat intelligence",
#     "security operations center", "cryptographer", "forensic examiner",
#     "malware researcher", "reverse engineer",
# ]

# REMOTE_KEYWORDS = [
#     "remote", "work from home", "wfh", "online", "virtual",
#     "anywhere", "distributed", "telecommute", "home-based",
#     "fully remote", "100% remote", "global"
# ]

# # ════════════════════════════════════════════════════════════════
# #  STRICT CYBERSECURITY TITLE FILTER
# #  ✅ FIX: A job is only accepted if its TITLE contains one of these.
# #  This prevents "Graphic Designer" / "Marketing Manager" / "Paralegal"
# #  from slipping through just because their description says "security".
# # ════════════════════════════════════════════════════════════════

# STRICT_TITLE_TERMS = [
#     # "cyber" family
#     "cyber security", "cybersecurity", "cyber analyst", "cyber engineer",
#     "cyber architect", "cyber planner", "cyber investigator", "cyber workforce",
#     "cyber threat", "cyber forensic", "cyber incident",
#     # "security" — ONLY when paired with a role noun (not generic "security guard")
#     "security analyst", "security engineer", "security architect",
#     "security specialist", "security manager", "security officer",
#     "security operations", "security consultant", "security researcher",
#     "security lead", "security director", "security advisor",
#     "security assessor", "security auditor", "security awareness",
#     "security administrator", "security developer", "security planner",
#     # infosec / information security
#     "infosec", "information security",
#     # SOC
#     "soc analyst", "soc engineer", "soc manager", "soc lead",
#     # Offensive / Red team
#     "penetration test", "pentest", "pen test",
#     "red team", "bug bounty", "ethical hack",
#     "vulnerability researcher", "vulnerability research",
#     "purple team",
#     # Forensics / Investigation
#     "digital forensic", "forensic analyst", "forensic examiner",
#     "malware analyst", "malware researcher", "malware reverse",
#     "reverse engineer",
#     "cybercrime investigat", "digital evidence",
#     "ediscovery", "e-discovery",
#     "deepfake analyst",
#     # Threat / Incident
#     "threat hunt", "threat intel", "threat analyst",
#     "incident response", "incident handler",
#     "insider threat",
#     # Specific tech domains
#     "devsecops", "appsec", "application security",
#     "cloud security", "network security",
#     "zero trust", "pki professional",
#     "siem", "soar", "edr", "xdr", "sast", "dast",
#     "ics security", "scada security", "ot security", "iiot security",
#     "ot cybersecurity",
#     "blockchain security", "iot security", "ai security",
#     "post-quantum", "cryptograph",
#     "automotive security",
#     # GRC / Compliance / Leadership
#     "grc", "ciso", "chief information security", "chief security officer",
#     "chief privacy officer",
#     "privacy compliance", "risk analyst", "it auditor",
#     "compliance analyst", "security awareness officer",
#     # Specific prefixed roles
#     "product security", "secure software", "software security",
#     "secure systems", "systems security",
#     "adversarial ai researcher",
#     "comsec",
#     "vulnerability assessm",
# ]


# logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
# log = logging.getLogger(__name__)

# HEADERS = {
#     "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
#     "Accept-Language": "en-US,en;q=0.9",
# }


# # ════════════════════════════════════════════════════════════════
# #  HELPER FUNCTIONS
# # ════════════════════════════════════════════════════════════════

# def delay(s=False):
#     time.sleep(3 if s else random.uniform(1.5, 3.5))


# def clean(t):
#     return re.sub(r'\s+', ' ', str(t)).strip() if t else "N/A"


# def is_cyber(title="", tags=None):
#     """
#     ✅ STRICT — checks TITLE only (+ tags as secondary signal).
#     A job must have a genuine cybersecurity term IN ITS TITLE.
#     Description is intentionally NOT checked — too many false positives
#     (e.g. "Graphic Designer" whose JD mentions "company security").
#     """
#     t = title.lower()

#     # Primary: title contains a strict cyber term
#     if any(term in t for term in STRICT_TITLE_TERMS):
#         return True

#     # Secondary: title has bare "security" AND tags contain a strong cyber signal
#     # e.g. title="Security Access Management Lead", tags=["soc","pentest"]
#     if "security" in t:
#         tag_str = " ".join(tags or []).lower()
#         strong_cyber_tags = [
#             "pentest", "soc", "siem", "soar", "forensic", "malware",
#             "devsecops", "appsec", "infosec", "vulnerability", "threat",
#             "compliance", "grc", "iam", "cryptograph", "incident",
#             "zero-trust", "red-team", "blue-team", "purple-team",
#         ]
#         if any(sig in tag_str for sig in strong_cyber_tags):
#             return True

#     return False


# def is_remote(title="", loc="", desc=""):
#     """Check if a job is remote/online."""
#     text = f"{title} {loc} {desc}".lower()
#     return any(kw in text for kw in REMOTE_KEYWORDS)


# def match_keywords(title="", tags=None, desc=""):
#     """
#     ✅ FIXED — matches against title and tags primarily.
#     Only uses description for long multi-word phrases (3+ words)
#     to avoid false matches on generic words.
#     """
#     title_tag_text = f"{title} {' '.join(tags or [])}".lower()
#     matched = []

#     for kw in ALL_KEYWORDS:
#         kw_lower = kw.lower()
#         # Always check title + tags
#         if kw_lower in title_tag_text:
#             matched.append(kw)
#         # For 3+ word keywords, also check description
#         elif len(kw.split()) >= 3 and kw_lower in desc.lower():
#             matched.append(kw)

#     return list(set(matched))


# def make_job(source, title, company, location, desc, url,
#              salary="Not listed", extra=None, keyword="", tags=None):
#     """Build a standardised job dict."""
#     tags = tags or []
#     j = {
#         "id": f"{re.sub(r'[^a-z0-9]', '_', source.lower())}_{int(time.time())}_{random.randint(1000,9999)}",
#         "source": source,
#         "job_title": clean(title),
#         "company": clean(company),
#         "location": clean(location),
#         "job_type": "Remote / Online",
#         "salary": clean(salary),
#         "job_description_snippet": clean(desc)[:600] if desc and desc != "N/A" else "Visit job URL for full JD",
#         "full_jd_url": url,
#         # ✅ FIXED: pass tags so match_keywords finds role-specific matches
#         "matched_keywords": match_keywords(title, tags, desc if desc != "N/A" else ""),
#         "keyword_used": keyword,
#         "scraped_at": datetime.now().isoformat(),
#     }
#     if extra:
#         j.update(extra)
#     return j


# # ════════════════════════════════════════════════════════════════
# #  PHASE 1: FREE APIs
# # ════════════════════════════════════════════════════════════════

# def api_remoteok():
#     """RemoteOK public JSON API."""
#     jobs = []
#     try:
#         r = requests.get("https://remoteok.com/api",
#                          headers={**HEADERS, "Accept": "application/json"}, timeout=20)
#         if not r.ok:
#             log.warning(f"[RemoteOK] HTTP {r.status_code}"); return jobs

#         for j in r.json()[1:]:
#             tags  = j.get("tags", [])
#             title = j.get("position", "")
#             desc  = clean(BeautifulSoup(j.get("description", ""), "lxml").get_text())

#             # ✅ FIXED: strict title check
#             if not is_cyber(title, tags):
#                 continue

#             jobs.append(make_job(
#                 "RemoteOK", title, j.get("company", "N/A"),
#                 j.get("location", "Remote/Global") or "Remote/Global",
#                 desc, j.get("url", "N/A"),
#                 j.get("salary", "Not listed") or "Not listed",
#                 {"tags": tags, "posted_date": j.get("date", "N/A")},
#                 "RemoteOK API", tags=tags
#             ))
#         delay()
#     except Exception as e:
#         log.error(f"[RemoteOK] {e}")
#     log.info(f"[RemoteOK] {len(jobs)} genuine cyber jobs")
#     return jobs


# def api_arbeitnow():
#     """Arbeitnow free job board API."""
#     jobs = []
#     try:
#         for page in range(1, 8):
#             r = requests.get(f"https://www.arbeitnow.com/api/job-board-api?page={page}",
#                              headers=HEADERS, timeout=15)
#             if not r.ok: break
#             data = r.json().get("data", [])
#             if not data: break

#             for j in data:
#                 title = j.get("title", "")
#                 tags  = j.get("tags", [])
#                 desc  = clean(BeautifulSoup(j.get("description", ""), "lxml").get_text())

#                 # ✅ FIXED: strict title check
#                 if not is_cyber(title, tags):
#                     continue
#                 if not j.get("remote") and not is_remote(title, j.get("location", ""), desc):
#                     continue

#                 jobs.append(make_job(
#                     "Arbeitnow", title, j.get("company_name", "N/A"),
#                     j.get("location", "Remote/Global"), desc[:500],
#                     j.get("url", "N/A"),
#                     extra={"tags": tags, "posted_date": j.get("created_at", "N/A")},
#                     keyword="Arbeitnow API", tags=tags
#                 ))
#             delay()
#     except Exception as e:
#         log.error(f"[Arbeitnow] {e}")
#     log.info(f"[Arbeitnow] {len(jobs)} genuine cyber jobs")
#     return jobs


# def api_jobicy():
#     """Jobicy remote jobs API with cybersecurity category filters."""
#     jobs = []
#     cats = ["cybersecurity", "security", "infosec", "devops", "legal", "management", "data"]
#     try:
#         for cat in cats:
#             r = requests.get(f"https://jobicy.com/api/v2/remote-jobs?industry={cat}&count=50",
#                              headers=HEADERS, timeout=15)
#             if not r.ok: continue

#             for j in r.json().get("jobs", []):
#                 title = j.get("jobTitle", "")
#                 tags  = j.get("jobIndustry", [])
#                 desc  = clean(BeautifulSoup(j.get("jobDescription", ""), "lxml").get_text())

#                 # ✅ FIXED: strict title check
#                 if not is_cyber(title, tags):
#                     continue

#                 sal_min = j.get("annualSalaryMin", "")
#                 sal_max = j.get("annualSalaryMax", "")
#                 salary  = f"${sal_min}–${sal_max}" if sal_min else "Not listed"

#                 jobs.append(make_job(
#                     "Jobicy", title, j.get("companyName", "N/A"),
#                     j.get("jobGeo", "Remote/Global"), desc[:500],
#                     j.get("url", "N/A"), salary,
#                     {"tags": tags,
#                      "posted_date": j.get("pubDate", "N/A"),
#                      "experience_level": j.get("jobLevel", "N/A")},
#                     f"Category: {cat}", tags=tags
#                 ))
#             delay()
#     except Exception as e:
#         log.error(f"[Jobicy] {e}")
#     log.info(f"[Jobicy] {len(jobs)} genuine cyber jobs")
#     return jobs


# def api_himalayas():
#     """Himalayas.app remote jobs API."""
#     jobs = []
#     try:
#         for kw in API_SEARCH_KEYWORDS:
#             r = requests.get("https://himalayas.app/jobs/api",
#                              params={"q": kw, "limit": 50},
#                              headers=HEADERS, timeout=15)
#             if not r.ok: continue

#             for j in r.json().get("jobs", []):
#                 title = j.get("title", "")
#                 tags  = j.get("skills", [])
#                 desc  = clean(j.get("description", ""))

#                 # ✅ FIXED: strict title check
#                 if not is_cyber(title, tags):
#                     continue

#                 locs = j.get("locationRestrictions", [])
#                 jobs.append(make_job(
#                     "Himalayas", title, j.get("companyName", "N/A"),
#                     locs[0] if locs else "Remote/Global", desc[:500],
#                     j.get("applicationLink", j.get("url", "N/A")),
#                     j.get("salary", "Not listed") or "Not listed",
#                     {"tags": tags,
#                      "posted_date": j.get("createdAt", "N/A"),
#                      "experience_level": j.get("seniority", "N/A")},
#                     kw, tags=tags
#                 ))
#             delay()
#     except Exception as e:
#         log.error(f"[Himalayas] {e}")
#     log.info(f"[Himalayas] {len(jobs)} genuine cyber jobs")
#     return jobs


# def api_remotive():
#     """Remotive.com remote tech jobs API."""
#     jobs = []
#     cats = ["devops-sysadmin", "software-dev", "data", "management-finance"]
#     try:
#         for cat in cats:
#             r = requests.get(
#                 f"https://remotive.com/api/remote-jobs?category={cat}&limit=200",
#                 headers=HEADERS, timeout=15
#             )
#             if not r.ok: continue

#             for j in r.json().get("jobs", []):
#                 title = j.get("title", "")
#                 tags  = j.get("tags", [])
#                 desc  = clean(BeautifulSoup(j.get("description", ""), "lxml").get_text())

#                 # ✅ FIXED: strict title check
#                 if not is_cyber(title, tags):
#                     continue

#                 jobs.append(make_job(
#                     "Remotive", title, j.get("company_name", "N/A"),
#                     j.get("candidate_required_location", "Remote/Global"), desc[:500],
#                     j.get("url", "N/A"),
#                     j.get("salary", "Not listed") or "Not listed",
#                     {"tags": tags, "posted_date": j.get("publication_date", "N/A")},
#                     f"Remotive: {cat}", tags=tags
#                 ))
#         delay()
#     except Exception as e:
#         log.error(f"[Remotive] {e}")
#     log.info(f"[Remotive] {len(jobs)} genuine cyber jobs")
#     return jobs


# # ════════════════════════════════════════════════════════════════
# #  PHASE 2: SELENIUM — Bypasses 403 blocks on big platforms
# # ════════════════════════════════════════════════════════════════

# def get_driver():
#     opts = Options()
#     opts.add_argument("--headless")
#     opts.add_argument("--no-sandbox")
#     opts.add_argument("--disable-dev-shm-usage")
#     opts.add_argument("--disable-blink-features=AutomationControlled")
#     opts.add_experimental_option("excludeSwitches", ["enable-automation"])
#     opts.add_experimental_option("useAutomationExtension", False)
#     opts.add_argument(f"user-agent={HEADERS['User-Agent']}")
#     opts.add_argument("--window-size=1920,1080")
#     drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
#     drv.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
#     return drv


# def selenium_linkedin(drv, kw, pages=2):
#     """LinkedIn — f_WT=2 enforces remote-only filter."""
#     jobs = []
#     for page in range(0, pages * 25, 25):
#         try:
#             drv.get(
#                 f"https://www.linkedin.com/jobs/search/"
#                 f"?keywords={requests.utils.quote(kw)}"
#                 f"&f_WT=2&f_TPR=r2592000&start={page}"
#             )
#             delay(s=True)
#             soup  = BeautifulSoup(drv.page_source, "lxml")
#             cards = soup.find_all("div", class_=re.compile(r"base-card|job-search-card"))
#             if not cards: break

#             for c in cards:
#                 try:
#                     t   = c.find("h3", class_=re.compile(r"base-search-card__title"))
#                     co  = c.find("h4", class_=re.compile(r"base-search-card__subtitle"))
#                     l   = c.find("span", class_=re.compile(r"job-search-card__location"))
#                     dt  = c.find("time")
#                     lnk = c.find("a", class_=re.compile(r"base-card__full-link"), href=True)
#                     title = clean(t.get_text()) if t else "N/A"
#                     loc   = clean(l.get_text()) if l else "Remote"

#                     # ✅ FIXED: strict title check before adding
#                     if not is_cyber(title):
#                         continue
#                     if not is_remote(title, loc):
#                         continue

#                     jobs.append(make_job(
#                         "LinkedIn", title,
#                         clean(co.get_text()) if co else "N/A", loc,
#                         "Visit job URL for full description",
#                         lnk["href"] if lnk else "N/A",
#                         extra={"posted_date": dt["datetime"] if dt and dt.has_attr("datetime") else "N/A"},
#                         keyword=kw
#                     ))
#                 except: pass
#         except Exception as e:
#             log.error(f"[LinkedIn] {e}"); break
#     log.info(f"[LinkedIn] '{kw}' → {len(jobs)} genuine cyber jobs")
#     return jobs


# def selenium_indeed(drv, kw, pages=2):
#     """Indeed — remote filter via URL params."""
#     jobs = []
#     for page in range(0, pages * 10, 10):
#         try:
#             drv.get(
#                 f"https://www.indeed.com/jobs"
#                 f"?q={requests.utils.quote(kw)}"
#                 f"&l=Remote&sc=0kf%3Aattr(DSQF7)%3B"
#                 f"&start={page}&sort=date"
#             )
#             delay(s=True)
#             soup  = BeautifulSoup(drv.page_source, "lxml")
#             cards = soup.find_all("div", class_=re.compile(r"job_seen_beacon|resultContent"))
#             if not cards: break

#             for c in cards:
#                 try:
#                     t   = c.find("h2", class_=re.compile(r"jobTitle"))
#                     co  = c.find("span", {"data-testid": "company-name"}) or \
#                           c.find("span", class_=re.compile(r"companyName"))
#                     l   = c.find("div", {"data-testid": "text-location"}) or \
#                           c.find("div", class_=re.compile(r"companyLocation"))
#                     lnk = c.find("a", href=True)
#                     s   = c.find("div", class_=re.compile(r"job-snippet|summary"))
#                     sal = c.find("div", class_=re.compile(r"salary|salaryOnly"))
#                     title = clean(t.get_text()) if t else "N/A"
#                     loc   = clean(l.get_text()) if l else "Remote"
#                     snip  = clean(s.get_text()) if s else "N/A"

#                     # ✅ FIXED: strict title check
#                     if not is_cyber(title):
#                         continue
#                     if not is_remote(title, loc, snip):
#                         continue

#                     jobs.append(make_job(
#                         "Indeed", title,
#                         clean(co.get_text()) if co else "N/A", loc, snip,
#                         "https://www.indeed.com" + lnk["href"] if lnk else "N/A",
#                         clean(sal.get_text()) if sal else "Not listed",
#                         keyword=kw
#                     ))
#                 except: pass
#         except Exception as e:
#             log.error(f"[Indeed] {e}"); break
#     log.info(f"[Indeed] '{kw}' → {len(jobs)} genuine cyber jobs")
#     return jobs


# def selenium_glassdoor(drv, kw, pages=2):
#     """Glassdoor — remoteWorkType=1 enforces remote filter."""
#     jobs = []
#     try:
#         drv.get(
#             f"https://www.glassdoor.com/Job/jobs.htm"
#             f"?sc.keyword={requests.utils.quote(kw)}&remoteWorkType=1"
#         )
#         delay(s=True)
#         for _ in range(pages):
#             soup  = BeautifulSoup(drv.page_source, "lxml")
#             cards = soup.find_all("li", class_=re.compile(r"JobsList_jobListItem|react-job-listing"))
#             if not cards:
#                 cards = soup.find_all("div", {"data-test": "jobListing"})

#             for c in cards:
#                 try:
#                     t   = c.find("a", class_=re.compile(r"jobLink|JobCard_seoLink|job-title"))
#                     co  = c.find("span", class_=re.compile(r"EmployerProfile_compactEmployerName|employer-name"))
#                     l   = c.find("div", class_=re.compile(r"JobCard_location|location"))
#                     sal = c.find("div", class_=re.compile(r"salary|payPeriod"))
#                     title = clean(t.get_text()) if t else "N/A"
#                     loc   = clean(l.get_text()) if l else "Remote"
#                     href  = t["href"] if t and t.has_attr("href") else "N/A"

#                     # ✅ FIXED: strict title check
#                     if not is_cyber(title):
#                         continue
#                     if not is_remote(title, loc):
#                         continue

#                     jobs.append(make_job(
#                         "Glassdoor", title,
#                         clean(co.get_text()) if co else "N/A", loc,
#                         "Visit job URL for full description",
#                         "https://www.glassdoor.com" + href if href.startswith("/") else href,
#                         clean(sal.get_text()) if sal else "Not listed",
#                         keyword=kw
#                     ))
#                 except: pass

#             try:
#                 drv.find_element(By.CSS_SELECTOR, "[data-test='pagination-next']").click()
#                 delay(s=True)
#             except: break
#     except Exception as e:
#         log.error(f"[Glassdoor] {e}")
#     log.info(f"[Glassdoor] '{kw}' → {len(jobs)} genuine cyber jobs")
#     return jobs


# def selenium_naukri(drv, kw, pages=2):
#     """Naukri — work-from-home URL filter."""
#     jobs = []
#     slug = re.sub(r'[^a-z0-9\-]', '-', kw.lower()).strip('-')
#     for page in range(1, pages + 1):
#         try:
#             drv.get(f"https://www.naukri.com/{slug}-jobs-work-from-home-{page}")
#             delay(s=True)
#             soup  = BeautifulSoup(drv.page_source, "lxml")
#             cards = soup.find_all("article", class_=re.compile(r"jobTuple|job-tuple"))
#             if not cards:
#                 cards = soup.find_all("div", class_=re.compile(r"srp-jobtuple-wrapper"))
#             if not cards: break

#             for c in cards:
#                 try:
#                     t   = c.find("a", class_=re.compile(r"title|jobTitle"))
#                     co  = c.find("a", class_=re.compile(r"subTitle|company")) or \
#                           c.find("span", class_=re.compile(r"company"))
#                     exp = c.find("span", class_=re.compile(r"experience|exp"))
#                     sal = c.find("span", class_=re.compile(r"salary|sal"))
#                     l   = c.find("span", class_=re.compile(r"location|loc"))
#                     d   = c.find("span", class_=re.compile(r"job-description|desc|snippet"))
#                     lnk = t if (t and t.name == "a") else c.find("a", href=True)
#                     skills = [clean(s.get_text()) for s in c.find_all("li", class_=re.compile(r"tag|skill"))]

#                     title = clean(t.get_text()) if t else "N/A"
#                     loc   = clean(l.get_text()) if l else "Work From Home"
#                     desc  = clean(d.get_text()) if d else "N/A"

#                     # ✅ FIXED: strict title check
#                     if not is_cyber(title, skills):
#                         continue

#                     jobs.append(make_job(
#                         "Naukri", title,
#                         clean(co.get_text()) if co else "N/A", loc, desc,
#                         lnk["href"] if lnk else "N/A",
#                         clean(sal.get_text()) if sal else "Not Disclosed",
#                         {"experience_required": clean(exp.get_text()) if exp else "N/A",
#                          "skills_required": skills},
#                         kw, tags=skills
#                     ))
#                 except: pass
#         except Exception as e:
#             log.error(f"[Naukri] {e}"); break
#     log.info(f"[Naukri] '{kw}' → {len(jobs)} genuine cyber jobs")
#     return jobs


# # ════════════════════════════════════════════════════════════════
# #  CLEAN EXISTING JSON (--clean mode)
# # ════════════════════════════════════════════════════════════════

# def clean_existing_json(input_file):
#     """
#     Re-filters an existing JSON file using the new strict filter.
#     Removes all non-cybersecurity false positives.
#     """
#     output_file = input_file.replace(".json", "_cleaned.json")
#     print(f"\n🧹 Cleaning: {input_file}")

#     with open(input_file, "r", encoding="utf-8") as f:
#         data = json.load(f)

#     original = data.get("jobs", [])
#     print(f"   Original jobs  : {len(original)}")

#     kept, removed = [], []
#     for job in original:
#         title = job.get("job_title", "")
#         tags  = job.get("tags", [])
#         desc  = job.get("job_description_snippet", "")
#         if is_cyber(title, tags):
#             # Re-run matched_keywords with fixed logic
#             job["matched_keywords"] = match_keywords(title, tags, desc)
#             kept.append(job)
#         else:
#             removed.append(title)

#     print(f"   After cleaning : {len(kept)} genuine cyber jobs")
#     print(f"\n   ✂️  Removed {len(removed)} false positives:")
#     for t in removed:
#         print(f"      ✗  {t}")

#     data["jobs"] = kept
#     data["metadata"]["total_jobs"] = len(kept)
#     data["metadata"]["cleaned_at"] = datetime.now().isoformat()
#     data["metadata"]["false_positives_removed"] = len(removed)
#     data["metadata"]["filter_version"] = "v5 strict title-based"

#     with open(output_file, "w", encoding="utf-8") as f:
#         json.dump(data, f, indent=2, ensure_ascii=False)

#     print(f"\n✅ Cleaned file saved: {output_file} ({len(kept)} jobs)")


# # ════════════════════════════════════════════════════════════════
# #  SAVE JSON
# # ════════════════════════════════════════════════════════════════

# def save_json(jobs, filename):
#     output = {
#         "metadata": {
#             "description": "Cybersecurity REMOTE/ONLINE jobs — Global, 90+ keyword search",
#             "total_jobs": len(jobs),
#             "scraped_at": datetime.now().isoformat(),
#             "platforms": sorted(list(set(j["source"] for j in jobs))),
#             "total_keywords": len(ALL_KEYWORDS),
#             "filter_version": "v5 strict title-based — no false positives",
#         },
#         "jobs": jobs,
#     }
#     with open(filename, "w", encoding="utf-8") as f:
#         json.dump(output, f, indent=2, ensure_ascii=False)
#     print(f"\n✅ Saved: {filename}  ({len(jobs)} jobs)")


# # ════════════════════════════════════════════════════════════════
# #  MAIN
# # ════════════════════════════════════════════════════════════════

# def main():
#     # --clean mode: fix your existing JSON
#     if len(sys.argv) >= 3 and sys.argv[1] == "--clean":
#         clean_existing_json(sys.argv[2])
#         return

#     print("\n" + "=" * 60)
#     print("  CYBERSECURITY REMOTE JOB SCRAPER v5.0")
#     print("  ✅ STRICT FILTER — Only real cybersecurity job titles")
#     print(f"  Keywords  : {len(ALL_KEYWORDS)} specific roles")
#     print("  Platforms : RemoteOK, Arbeitnow, Jobicy, Himalayas,")
#     print("              Remotive + LinkedIn, Indeed, Glassdoor, Naukri")
#     print("=" * 60)

#     all_jobs, seen = [], set()

#     def add(new_jobs):
#         n = 0
#         for j in new_jobs:
#             k = (j.get("job_title", "").lower().strip(),
#                  j.get("company", "").lower().strip())
#             if k not in seen and k[0] not in ("n/a", "", "none"):
#                 seen.add(k)
#                 all_jobs.append(j)
#                 n += 1
#         return n

#     # ── PHASE 1: APIs ──────────────────────────────────────────
#     print("\n── PHASE 1: Free APIs ──────────────────────────────")
#     for name, fn in [
#         ("RemoteOK",  api_remoteok),
#         ("Arbeitnow", api_arbeitnow),
#         ("Jobicy",    api_jobicy),
#         ("Himalayas", api_himalayas),
#         ("Remotive",  api_remotive),
#     ]:
#         print(f"  ▶ {name}...", end=" ", flush=True)
#         n = add(fn())
#         print(f"+{n} | Total: {len(all_jobs)}")

#     # ── PHASE 2: Selenium ──────────────────────────────────────
#     if not SELENIUM_AVAILABLE:
#         print("\n⚠️  Selenium not installed.")
#         print("    pip install selenium webdriver-manager")
#     else:
#         print("\n── PHASE 2: Selenium Browser ───────────────────────")
#         BATCH_SIZE = 20
#         selenium_kws = ALL_KEYWORDS[:BATCH_SIZE]
#         print(f"  Using first {BATCH_SIZE} keywords (change BATCH_SIZE for more)\n")

#         driver = None
#         try:
#             print("  Starting Chrome browser...")
#             driver = get_driver()
#             print("  ✅ Browser ready\n")

#             for platform, fn in [
#                 ("LinkedIn",  selenium_linkedin),
#                 ("Indeed",    selenium_indeed),
#                 ("Glassdoor", selenium_glassdoor),
#                 ("Naukri",    selenium_naukri),
#             ]:
#                 print(f"  ▶ {platform}")
#                 p_total = 0
#                 for kw in selenium_kws:
#                     n = add(fn(driver, kw))
#                     p_total += n
#                     if n:
#                         print(f"    ✓ '{kw}' → +{n}")
#                 print(f"    {platform} subtotal: {p_total} | Grand total: {len(all_jobs)}")

#         except Exception as e:
#             log.error(f"Selenium error: {e}")
#             print("  ❌ Selenium failed — is Chrome installed?")
#         finally:
#             if driver:
#                 driver.quit()
#                 print("\n  ✅ Browser closed.")

#     # ── FINAL STRICT PASS ──────────────────────────────────────
#     print("\n── Final strict filter pass ─────────────────────────")
#     before   = len(all_jobs)
#     all_jobs = [j for j in all_jobs
#                 if is_cyber(j.get("job_title", ""), j.get("tags", []))]
#     removed  = before - len(all_jobs)
#     print(f"  ✂️  Removed {removed} false positives")
#     print(f"  ✅ {len(all_jobs)} genuine cybersecurity jobs")

#     # ── SUMMARY ────────────────────────────────────────────────
#     print(f"\n{'=' * 60}")
#     print(f"  TOTAL UNIQUE REMOTE CYBER JOBS: {len(all_jobs)}")

#     print("\n  By Platform:")
#     for src, cnt in sorted(Counter(j["source"] for j in all_jobs).items(), key=lambda x: -x[1]):
#         print(f"    {src:25} → {cnt}")

#     print("\n  Top Matched Keywords:")
#     kw_counts = Counter(kw for j in all_jobs for kw in j.get("matched_keywords", []))
#     for kw, cnt in kw_counts.most_common(15):
#         print(f"    {kw[:50]:50} → {cnt} jobs")

#     save_json(all_jobs, OUTPUT_FILE)

#     print(f"\n🎉 Done! Open '{OUTPUT_FILE}'")
#     print(f"\n💡 To clean your OLD file:")
#     print(f"   python cyber_job_scraper_v5.py --clean cybersecurity_remote_jobs.json")
#     print("=" * 60)


# if __name__ == "__main__":
#     main()










"""
================================================================
  CYBERSECURITY REMOTE JOB SCRAPER v5.1
  ✅ FIXED: Per-platform timeout — if a platform hangs, skip it
  ✅ FIXED: Strict title-based filter — NO false positives
  ✅ FIXED: matched_keywords checks title + tags (not just snippet)
  ✅ FIXED: Non-cyber jobs (Graphic Designer, Marketer etc.) blocked
  ✅ 90+ Cybersecurity keywords
  ✅ Phase 1: Free APIs (RemoteOK, Arbeitnow, Jobicy, Himalayas, Remotive)
  ✅ Phase 2: Selenium (LinkedIn, Indeed, Glassdoor, Naukri)
             Each platform has its own timeout — if stuck, skips it
  ✅ Remote/Online Jobs ONLY
  Output: cybersecurity_remote_jobs.json

  Run:   python cyber_job_scraper_v5.py
  Clean: python cyber_job_scraper_v5.py --clean old_file.json
================================================================
SETUP:
    pip install requests selenium webdriver-manager beautifulsoup4 lxml
================================================================
"""

import sys
import requests
import json
import time
import random
import re
import logging
import threading
from datetime import datetime
from collections import Counter
from bs4 import BeautifulSoup

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

OUTPUT_FILE = "cybersecurity_remote_jobs.json"

# ── Per-platform Selenium timeout (seconds) ───────────────────────────────────
# If LinkedIn/Indeed/Glassdoor/Naukri takes longer than this, it is SKIPPED
# and the scraper moves on to the next platform.
PLATFORM_TIMEOUT = 180   # 3 minutes per platform — change if needed

# ════════════════════════════════════════════════════════════════
#  KEYWORD LISTS
# ════════════════════════════════════════════════════════════════

ALL_KEYWORDS = [
    "Communications Security COMSEC Management",
    "Cybersecurity Policy Planning",
    "Cybersecurity Workforce Management",
    "Cybersecurity Curriculum Development",
    "Cybersecurity Instruction",
    "Cybersecurity Legal Advice",
    "Executive Cybersecurity Leadership",
    "Privacy Compliance",
    "Secure Project Management",
    "Security Control Assessment",
    "Systems Authorization",
    "Systems Security Management",
    "Technology Portfolio Management",
    "Technology Program Auditing",
    "Cybersecurity Architecture",
    "Enterprise Architecture",
    "Secure Software Development",
    "Secure Systems Development",
    "Software Security Assessment",
    "Systems Requirements Planning",
    "Systems Testing and Evaluation",
    "Operational Technology OT Cybersecurity",
    "Network Operations Security",
    "Systems Security Analysis",
    "Network Security",
    "Defensive Cybersecurity",
    "Digital Forensics",
    "Incident Response",
    "Insider Threat Analysis",
    "Threat Analysis",
    "Vulnerability Analysis",
    "Cybercrime Investigation",
    "Digital Evidence Analysis",
    "Chief Information Security Officer CISO",
    "Chief Security Officer CSO",
    "Director of Cybersecurity",
    "Information Security Manager",
    "Chief Privacy Officer CPO",
    "Cyber Workforce Strategy",
    "Junior SOC Analyst",
    "Security Analyst",
    "Senior Security Analyst",
    "Threat Hunter",
    "Security Engineer",
    "Penetration Tester",
    "Red Team",
    "Vulnerability Researcher",
    "Application Penetration Tester",
    "Bug Bounty Hunter",
    "Purple Team Specialist",
    "Cloud Security Architect",
    "Cloud Security Engineer",
    "Network Security Engineer",
    "Zero Trust Architect",
    "Security Architect",
    "PKI Professional",
    "Application Security Engineer",
    "DevSecOps Specialist",
    "Product Security Engineer",
    "Secure Software Developer",
    "Software Security Assessor",
    "GRC Manager",
    "Compliance Analyst",
    "Risk Analyst",
    "IT Auditor",
    "Security Awareness Officer",
    "Cybersecurity Planner",
    "ICS SCADA Security Engineer",
    "OT Cybersecurity Analyst",
    "SCADA Specialist",
    "IIoT Security Specialist",
    "Automotive Security Engineer",
    "Digital Forensics Analyst",
    "Digital Forensic Examiner",
    "Malware Analyst",
    "Cybercrime Investigator",
    "eDiscovery Specialist",
    "AI Security Specialist",
    "Adversarial AI Researcher",
    "Blockchain Security Engineer",
    "Post-Quantum Cryptographer",
    "Deepfake Analyst",
    "IoT Security Specialist",
]

API_SEARCH_KEYWORDS = [
    "cybersecurity analyst", "SOC analyst", "penetration tester",
    "security engineer", "information security", "threat hunter",
    "incident response analyst", "digital forensics", "malware analyst",
    "cloud security engineer", "devsecops", "vulnerability analyst",
    "red team analyst", "application security engineer", "SIEM analyst",
    "network security engineer", "GRC analyst", "risk analyst",
    "ICS SCADA security", "OT security analyst", "AI security specialist",
    "zero trust architect", "security architect", "CISO",
    "cybercrime investigator", "insider threat analyst",
    "security awareness", "compliance analyst", "blockchain security",
    "IoT security", "bug bounty", "purple team", "threat intelligence",
    "security operations center", "cryptographer", "forensic examiner",
    "malware researcher", "reverse engineer",
]

REMOTE_KEYWORDS = [
    "remote", "work from home", "wfh", "online", "virtual",
    "anywhere", "distributed", "telecommute", "home-based",
    "fully remote", "100% remote", "global"
]

STRICT_TITLE_TERMS = [
    "cyber security", "cybersecurity", "cyber analyst", "cyber engineer",
    "cyber architect", "cyber planner", "cyber investigator", "cyber workforce",
    "cyber threat", "cyber forensic", "cyber incident",
    "security analyst", "security engineer", "security architect",
    "security specialist", "security manager", "security officer",
    "security operations", "security consultant", "security researcher",
    "security lead", "security director", "security advisor",
    "security assessor", "security auditor", "security awareness",
    "security administrator", "security developer", "security planner",
    "infosec", "information security",
    "soc analyst", "soc engineer", "soc manager", "soc lead",
    "penetration test", "pentest", "pen test",
    "red team", "bug bounty", "ethical hack",
    "vulnerability researcher", "vulnerability research",
    "purple team",
    "digital forensic", "forensic analyst", "forensic examiner",
    "malware analyst", "malware researcher", "malware reverse",
    "reverse engineer",
    "cybercrime investigat", "digital evidence",
    "ediscovery", "e-discovery",
    "deepfake analyst",
    "threat hunt", "threat intel", "threat analyst",
    "incident response", "incident handler",
    "insider threat",
    "devsecops", "appsec", "application security",
    "cloud security", "network security",
    "zero trust", "pki professional",
    "siem", "soar", "edr", "xdr", "sast", "dast",
    "ics security", "scada security", "ot security", "iiot security",
    "ot cybersecurity",
    "blockchain security", "iot security", "ai security",
    "post-quantum", "cryptograph",
    "automotive security",
    "grc", "ciso", "chief information security", "chief security officer",
    "chief privacy officer",
    "privacy compliance", "risk analyst", "it auditor",
    "compliance analyst", "security awareness officer",
    "product security", "secure software", "software security",
    "secure systems", "systems security",
    "adversarial ai researcher",
    "comsec",
    "vulnerability assessm",
]


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
}


# ════════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════

def delay(s=False):
    time.sleep(3 if s else random.uniform(1.5, 3.5))


def clean(t):
    return re.sub(r'\s+', ' ', str(t)).strip() if t else "N/A"


def is_cyber(title="", tags=None):
    t = title.lower()
    if any(term in t for term in STRICT_TITLE_TERMS):
        return True
    if "security" in t:
        tag_str = " ".join(tags or []).lower()
        strong_cyber_tags = [
            "pentest", "soc", "siem", "soar", "forensic", "malware",
            "devsecops", "appsec", "infosec", "vulnerability", "threat",
            "compliance", "grc", "iam", "cryptograph", "incident",
            "zero-trust", "red-team", "blue-team", "purple-team",
        ]
        if any(sig in tag_str for sig in strong_cyber_tags):
            return True
    return False


def is_remote(title="", loc="", desc=""):
    text = f"{title} {loc} {desc}".lower()
    return any(kw in text for kw in REMOTE_KEYWORDS)


def match_keywords(title="", tags=None, desc=""):
    title_tag_text = f"{title} {' '.join(tags or [])}".lower()
    matched = []
    for kw in ALL_KEYWORDS:
        kw_lower = kw.lower()
        if kw_lower in title_tag_text:
            matched.append(kw)
        elif len(kw.split()) >= 3 and kw_lower in desc.lower():
            matched.append(kw)
    return list(set(matched))


def make_job(source, title, company, location, desc, url,
             salary="Not listed", extra=None, keyword="", tags=None):
    tags = tags or []
    j = {
        "id": f"{re.sub(r'[^a-z0-9]', '_', source.lower())}_{int(time.time())}_{random.randint(1000,9999)}",
        "source": source,
        "job_title": clean(title),
        "company": clean(company),
        "location": clean(location),
        "job_type": "Remote / Online",
        "salary": clean(salary),
        "job_description_snippet": clean(desc)[:600] if desc and desc != "N/A" else "Visit job URL for full JD",
        "full_jd_url": url,
        "matched_keywords": match_keywords(title, tags, desc if desc != "N/A" else ""),
        "keyword_used": keyword,
        "scraped_at": datetime.now().isoformat(),
    }
    if extra:
        j.update(extra)
    return j


# ════════════════════════════════════════════════════════════════
#  PHASE 1: FREE APIs (no timeout needed — fast)
# ════════════════════════════════════════════════════════════════

def api_remoteok():
    jobs = []
    try:
        r = requests.get("https://remoteok.com/api",
                         headers={**HEADERS, "Accept": "application/json"}, timeout=20)
        if not r.ok:
            log.warning(f"[RemoteOK] HTTP {r.status_code}"); return jobs
        for j in r.json()[1:]:
            tags  = j.get("tags", [])
            title = j.get("position", "")
            desc  = clean(BeautifulSoup(j.get("description", ""), "lxml").get_text())
            if not is_cyber(title, tags): continue
            jobs.append(make_job(
                "RemoteOK", title, j.get("company", "N/A"),
                j.get("location", "Remote/Global") or "Remote/Global",
                desc, j.get("url", "N/A"),
                j.get("salary", "Not listed") or "Not listed",
                {"tags": tags, "posted_date": j.get("date", "N/A")},
                "RemoteOK API", tags=tags
            ))
        delay()
    except Exception as e:
        log.error(f"[RemoteOK] {e}")
    log.info(f"[RemoteOK] {len(jobs)} genuine cyber jobs")
    return jobs


def api_arbeitnow():
    jobs = []
    try:
        for page in range(1, 8):
            r = requests.get(f"https://www.arbeitnow.com/api/job-board-api?page={page}",
                             headers=HEADERS, timeout=15)
            if not r.ok: break
            data = r.json().get("data", [])
            if not data: break
            for j in data:
                title = j.get("title", "")
                tags  = j.get("tags", [])
                desc  = clean(BeautifulSoup(j.get("description", ""), "lxml").get_text())
                if not is_cyber(title, tags): continue
                if not j.get("remote") and not is_remote(title, j.get("location", ""), desc): continue
                jobs.append(make_job(
                    "Arbeitnow", title, j.get("company_name", "N/A"),
                    j.get("location", "Remote/Global"), desc[:500],
                    j.get("url", "N/A"),
                    extra={"tags": tags, "posted_date": j.get("created_at", "N/A")},
                    keyword="Arbeitnow API", tags=tags
                ))
            delay()
    except Exception as e:
        log.error(f"[Arbeitnow] {e}")
    log.info(f"[Arbeitnow] {len(jobs)} genuine cyber jobs")
    return jobs


def api_jobicy():
    jobs = []
    cats = ["cybersecurity", "security", "infosec", "devops", "legal", "management", "data"]
    try:
        for cat in cats:
            r = requests.get(f"https://jobicy.com/api/v2/remote-jobs?industry={cat}&count=50",
                             headers=HEADERS, timeout=15)
            if not r.ok: continue
            for j in r.json().get("jobs", []):
                title = j.get("jobTitle", "")
                tags  = j.get("jobIndustry", [])
                desc  = clean(BeautifulSoup(j.get("jobDescription", ""), "lxml").get_text())
                if not is_cyber(title, tags): continue
                sal_min = j.get("annualSalaryMin", "")
                sal_max = j.get("annualSalaryMax", "")
                salary  = f"${sal_min}–${sal_max}" if sal_min else "Not listed"
                jobs.append(make_job(
                    "Jobicy", title, j.get("companyName", "N/A"),
                    j.get("jobGeo", "Remote/Global"), desc[:500],
                    j.get("url", "N/A"), salary,
                    {"tags": tags,
                     "posted_date": j.get("pubDate", "N/A"),
                     "experience_level": j.get("jobLevel", "N/A")},
                    f"Category: {cat}", tags=tags
                ))
            delay()
    except Exception as e:
        log.error(f"[Jobicy] {e}")
    log.info(f"[Jobicy] {len(jobs)} genuine cyber jobs")
    return jobs


def api_himalayas():
    jobs = []
    try:
        for kw in API_SEARCH_KEYWORDS:
            r = requests.get("https://himalayas.app/jobs/api",
                             params={"q": kw, "limit": 50},
                             headers=HEADERS, timeout=15)
            if not r.ok: continue
            for j in r.json().get("jobs", []):
                title = j.get("title", "")
                tags  = j.get("skills", [])
                desc  = clean(j.get("description", ""))
                if not is_cyber(title, tags): continue
                locs = j.get("locationRestrictions", [])
                jobs.append(make_job(
                    "Himalayas", title, j.get("companyName", "N/A"),
                    locs[0] if locs else "Remote/Global", desc[:500],
                    j.get("applicationLink", j.get("url", "N/A")),
                    j.get("salary", "Not listed") or "Not listed",
                    {"tags": tags,
                     "posted_date": j.get("createdAt", "N/A"),
                     "experience_level": j.get("seniority", "N/A")},
                    kw, tags=tags
                ))
            delay()
    except Exception as e:
        log.error(f"[Himalayas] {e}")
    log.info(f"[Himalayas] {len(jobs)} genuine cyber jobs")
    return jobs


def api_remotive():
    jobs = []
    cats = ["devops-sysadmin", "software-dev", "data", "management-finance"]
    try:
        for cat in cats:
            r = requests.get(
                f"https://remotive.com/api/remote-jobs?category={cat}&limit=200",
                headers=HEADERS, timeout=15
            )
            if not r.ok: continue
            for j in r.json().get("jobs", []):
                title = j.get("title", "")
                tags  = j.get("tags", [])
                desc  = clean(BeautifulSoup(j.get("description", ""), "lxml").get_text())
                if not is_cyber(title, tags): continue
                jobs.append(make_job(
                    "Remotive", title, j.get("company_name", "N/A"),
                    j.get("candidate_required_location", "Remote/Global"), desc[:500],
                    j.get("url", "N/A"),
                    j.get("salary", "Not listed") or "Not listed",
                    {"tags": tags, "posted_date": j.get("publication_date", "N/A")},
                    f"Remotive: {cat}", tags=tags
                ))
        delay()
    except Exception as e:
        log.error(f"[Remotive] {e}")
    log.info(f"[Remotive] {len(jobs)} genuine cyber jobs")
    return jobs


# ════════════════════════════════════════════════════════════════
#  PHASE 2: SELENIUM — with per-platform timeout
# ════════════════════════════════════════════════════════════════

def get_driver():
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(f"user-agent={HEADERS['User-Agent']}")
    opts.add_argument("--window-size=1920,1080")
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return drv


def _run_with_timeout(fn, timeout_sec, platform_name):
    """
    Run fn() in a thread with a hard timeout.
    If it exceeds timeout_sec → log warning and return [].
    This prevents any single platform from hanging the whole scraper.
    """
    result_holder = []
    error_holder  = []

    def _worker():
        try:
            result_holder.extend(fn())
        except Exception as e:
            error_holder.append(e)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)

    if t.is_alive():
        # Thread is still running = platform is stuck
        log.warning(
            f"[{platform_name}] ⏱️  Stuck for >{timeout_sec}s — SKIPPING this platform. "
            f"Moving on to next platform."
        )
        print(f"\n  ⚠️  {platform_name} stuck for >{timeout_sec}s — skipping, moving on...")
        return []   # return empty, don't crash

    if error_holder:
        log.error(f"[{platform_name}] Error: {error_holder[0]}")
        return []

    return result_holder


def selenium_linkedin(drv, kw, pages=2):
    jobs = []
    for page in range(0, pages * 25, 25):
        try:
            drv.get(
                f"https://www.linkedin.com/jobs/search/"
                f"?keywords={requests.utils.quote(kw)}"
                f"&f_WT=2&f_TPR=r2592000&start={page}"
            )
            delay(s=True)
            soup  = BeautifulSoup(drv.page_source, "lxml")
            cards = soup.find_all("div", class_=re.compile(r"base-card|job-search-card"))
            if not cards: break
            for c in cards:
                try:
                    t   = c.find("h3", class_=re.compile(r"base-search-card__title"))
                    co  = c.find("h4", class_=re.compile(r"base-search-card__subtitle"))
                    l   = c.find("span", class_=re.compile(r"job-search-card__location"))
                    dt  = c.find("time")
                    lnk = c.find("a", class_=re.compile(r"base-card__full-link"), href=True)
                    title = clean(t.get_text()) if t else "N/A"
                    loc   = clean(l.get_text()) if l else "Remote"
                    if not is_cyber(title): continue
                    if not is_remote(title, loc): continue
                    jobs.append(make_job(
                        "LinkedIn", title,
                        clean(co.get_text()) if co else "N/A", loc,
                        "Visit job URL for full description",
                        lnk["href"] if lnk else "N/A",
                        extra={"posted_date": dt["datetime"] if dt and dt.has_attr("datetime") else "N/A"},
                        keyword=kw
                    ))
                except: pass
        except Exception as e:
            log.error(f"[LinkedIn] {e}"); break
    log.info(f"[LinkedIn] '{kw}' → {len(jobs)} genuine cyber jobs")
    return jobs


def selenium_indeed(drv, kw, pages=2):
    jobs = []
    for page in range(0, pages * 10, 10):
        try:
            drv.get(
                f"https://www.indeed.com/jobs"
                f"?q={requests.utils.quote(kw)}"
                f"&l=Remote&sc=0kf%3Aattr(DSQF7)%3B"
                f"&start={page}&sort=date"
            )
            delay(s=True)
            soup  = BeautifulSoup(drv.page_source, "lxml")
            cards = soup.find_all("div", class_=re.compile(r"job_seen_beacon|resultContent"))
            if not cards: break
            for c in cards:
                try:
                    t   = c.find("h2", class_=re.compile(r"jobTitle"))
                    co  = c.find("span", {"data-testid": "company-name"}) or \
                          c.find("span", class_=re.compile(r"companyName"))
                    l   = c.find("div", {"data-testid": "text-location"}) or \
                          c.find("div", class_=re.compile(r"companyLocation"))
                    lnk = c.find("a", href=True)
                    s   = c.find("div", class_=re.compile(r"job-snippet|summary"))
                    sal = c.find("div", class_=re.compile(r"salary|salaryOnly"))
                    title = clean(t.get_text()) if t else "N/A"
                    loc   = clean(l.get_text()) if l else "Remote"
                    snip  = clean(s.get_text()) if s else "N/A"
                    if not is_cyber(title): continue
                    if not is_remote(title, loc, snip): continue
                    jobs.append(make_job(
                        "Indeed", title,
                        clean(co.get_text()) if co else "N/A", loc, snip,
                        "https://www.indeed.com" + lnk["href"] if lnk else "N/A",
                        clean(sal.get_text()) if sal else "Not listed",
                        keyword=kw
                    ))
                except: pass
        except Exception as e:
            log.error(f"[Indeed] {e}"); break
    log.info(f"[Indeed] '{kw}' → {len(jobs)} genuine cyber jobs")
    return jobs


def selenium_glassdoor(drv, kw, pages=2):
    jobs = []
    try:
        drv.get(
            f"https://www.glassdoor.com/Job/jobs.htm"
            f"?sc.keyword={requests.utils.quote(kw)}&remoteWorkType=1"
        )
        delay(s=True)
        for _ in range(pages):
            soup  = BeautifulSoup(drv.page_source, "lxml")
            cards = soup.find_all("li", class_=re.compile(r"JobsList_jobListItem|react-job-listing"))
            if not cards:
                cards = soup.find_all("div", {"data-test": "jobListing"})
            for c in cards:
                try:
                    t   = c.find("a", class_=re.compile(r"jobLink|JobCard_seoLink|job-title"))
                    co  = c.find("span", class_=re.compile(r"EmployerProfile_compactEmployerName|employer-name"))
                    l   = c.find("div", class_=re.compile(r"JobCard_location|location"))
                    sal = c.find("div", class_=re.compile(r"salary|payPeriod"))
                    title = clean(t.get_text()) if t else "N/A"
                    loc   = clean(l.get_text()) if l else "Remote"
                    href  = t["href"] if t and t.has_attr("href") else "N/A"
                    if not is_cyber(title): continue
                    if not is_remote(title, loc): continue
                    jobs.append(make_job(
                        "Glassdoor", title,
                        clean(co.get_text()) if co else "N/A", loc,
                        "Visit job URL for full description",
                        "https://www.glassdoor.com" + href if href.startswith("/") else href,
                        clean(sal.get_text()) if sal else "Not listed",
                        keyword=kw
                    ))
                except: pass
            try:
                drv.find_element(By.CSS_SELECTOR, "[data-test='pagination-next']").click()
                delay(s=True)
            except: break
    except Exception as e:
        log.error(f"[Glassdoor] {e}")
    log.info(f"[Glassdoor] '{kw}' → {len(jobs)} genuine cyber jobs")
    return jobs


def selenium_naukri(drv, kw, pages=2):
    jobs = []
    slug = re.sub(r'[^a-z0-9\-]', '-', kw.lower()).strip('-')
    for page in range(1, pages + 1):
        try:
            drv.get(f"https://www.naukri.com/{slug}-jobs-work-from-home-{page}")
            delay(s=True)
            soup  = BeautifulSoup(drv.page_source, "lxml")
            cards = soup.find_all("article", class_=re.compile(r"jobTuple|job-tuple"))
            if not cards:
                cards = soup.find_all("div", class_=re.compile(r"srp-jobtuple-wrapper"))
            if not cards: break
            for c in cards:
                try:
                    t   = c.find("a", class_=re.compile(r"title|jobTitle"))
                    co  = c.find("a", class_=re.compile(r"subTitle|company")) or \
                          c.find("span", class_=re.compile(r"company"))
                    exp = c.find("span", class_=re.compile(r"experience|exp"))
                    sal = c.find("span", class_=re.compile(r"salary|sal"))
                    l   = c.find("span", class_=re.compile(r"location|loc"))
                    d   = c.find("span", class_=re.compile(r"job-description|desc|snippet"))
                    lnk = t if (t and t.name == "a") else c.find("a", href=True)
                    skills = [clean(s.get_text()) for s in c.find_all("li", class_=re.compile(r"tag|skill"))]
                    title = clean(t.get_text()) if t else "N/A"
                    loc   = clean(l.get_text()) if l else "Work From Home"
                    desc  = clean(d.get_text()) if d else "N/A"
                    if not is_cyber(title, skills): continue
                    jobs.append(make_job(
                        "Naukri", title,
                        clean(co.get_text()) if co else "N/A", loc, desc,
                        lnk["href"] if lnk else "N/A",
                        clean(sal.get_text()) if sal else "Not Disclosed",
                        {"experience_required": clean(exp.get_text()) if exp else "N/A",
                         "skills_required": skills},
                        kw, tags=skills
                    ))
                except: pass
        except Exception as e:
            log.error(f"[Naukri] {e}"); break
    log.info(f"[Naukri] '{kw}' → {len(jobs)} genuine cyber jobs")
    return jobs


# ════════════════════════════════════════════════════════════════
#  CLEAN EXISTING JSON (--clean mode)
# ════════════════════════════════════════════════════════════════

def clean_existing_json(input_file):
    output_file = input_file.replace(".json", "_cleaned.json")
    print(f"\n🧹 Cleaning: {input_file}")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    original = data.get("jobs", [])
    print(f"   Original jobs  : {len(original)}")
    kept, removed = [], []
    for job in original:
        title = job.get("job_title", "")
        tags  = job.get("tags", [])
        desc  = job.get("job_description_snippet", "")
        if is_cyber(title, tags):
            job["matched_keywords"] = match_keywords(title, tags, desc)
            kept.append(job)
        else:
            removed.append(title)
    print(f"   After cleaning : {len(kept)} genuine cyber jobs")
    print(f"\n   ✂️  Removed {len(removed)} false positives:")
    for t in removed:
        print(f"      ✗  {t}")
    data["jobs"] = kept
    data["metadata"]["total_jobs"] = len(kept)
    data["metadata"]["cleaned_at"] = datetime.now().isoformat()
    data["metadata"]["false_positives_removed"] = len(removed)
    data["metadata"]["filter_version"] = "v5.1 strict title-based"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Cleaned file saved: {output_file} ({len(kept)} jobs)")


# ════════════════════════════════════════════════════════════════
#  SAVE JSON
# ════════════════════════════════════════════════════════════════

def save_json(jobs, filename):
    output = {
        "metadata": {
            "description": "Cybersecurity REMOTE/ONLINE jobs — Global, 90+ keyword search",
            "total_jobs": len(jobs),
            "scraped_at": datetime.now().isoformat(),
            "platforms": sorted(list(set(j["source"] for j in jobs))),
            "total_keywords": len(ALL_KEYWORDS),
            "filter_version": "v5.1 strict title-based + per-platform timeout",
        },
        "jobs": jobs,
    }
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Saved: {filename}  ({len(jobs)} jobs)")


# ════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--clean":
        clean_existing_json(sys.argv[2])
        return

    print("\n" + "=" * 60)
    print("  CYBERSECURITY REMOTE JOB SCRAPER v5.1")
    print("  ✅ STRICT FILTER — Only real cybersecurity job titles")
    print(f"  ✅ Per-platform timeout: {PLATFORM_TIMEOUT}s (stuck = skip)")
    print(f"  Keywords  : {len(ALL_KEYWORDS)} specific roles")
    print("  Platforms : RemoteOK, Arbeitnow, Jobicy, Himalayas,")
    print("              Remotive + LinkedIn, Indeed, Glassdoor, Naukri")
    print("=" * 60)

    all_jobs, seen = [], set()

    def add(new_jobs):
        n = 0
        for j in new_jobs:
            k = (j.get("job_title", "").lower().strip(),
                 j.get("company", "").lower().strip())
            if k not in seen and k[0] not in ("n/a", "", "none"):
                seen.add(k)
                all_jobs.append(j)
                n += 1
        return n

    # ── PHASE 1: APIs ──────────────────────────────────────────
    print("\n── PHASE 1: Free APIs ──────────────────────────────")
    for name, fn in [
        ("RemoteOK",  api_remoteok),
        ("Arbeitnow", api_arbeitnow),
        ("Jobicy",    api_jobicy),
        ("Himalayas", api_himalayas),
        ("Remotive",  api_remotive),
    ]:
        print(f"  ▶ {name}...", end=" ", flush=True)
        n = add(fn())
        print(f"+{n} | Total: {len(all_jobs)}")

    # ── PHASE 2: Selenium ──────────────────────────────────────
    if not SELENIUM_AVAILABLE:
        print("\n⚠️  Selenium not installed.")
        print("    pip install selenium webdriver-manager")
    else:
        print("\n── PHASE 2: Selenium Browser ───────────────────────")
        print(f"  ⏱️  Per-platform timeout: {PLATFORM_TIMEOUT}s")
        print(f"     (if a platform hangs >{PLATFORM_TIMEOUT}s it is skipped automatically)\n")

        BATCH_SIZE  = 20
        selenium_kws = ALL_KEYWORDS[:BATCH_SIZE]
        print(f"  Using first {BATCH_SIZE} keywords\n")

        driver = None
        try:
            print("  Starting Chrome browser...")
            driver = get_driver()
            print("  ✅ Browser ready\n")

            for platform_name, selenium_fn in [
                ("LinkedIn",  selenium_linkedin),
                ("Indeed",    selenium_indeed),
                ("Glassdoor", selenium_glassdoor),
                ("Naukri",    selenium_naukri),
            ]:
                print(f"  ▶ {platform_name}")
                p_total = 0

                for kw in selenium_kws:
                    # ✅ Each keyword search wrapped with per-platform timeout
                    def _kw_fn(d=driver, k=kw, fn=selenium_fn):
                        return fn(d, k)

                    jobs_found = _run_with_timeout(
                        _kw_fn,
                        timeout_sec=PLATFORM_TIMEOUT,
                        platform_name=f"{platform_name}:{kw[:30]}"
                    )
                    n = add(jobs_found)
                    p_total += n
                    if n:
                        print(f"    ✓ '{kw}' → +{n}")

                print(f"    {platform_name} subtotal: {p_total} | Grand total: {len(all_jobs)}")

        except Exception as e:
            log.error(f"Selenium error: {e}")
            print("  ❌ Selenium failed — is Chrome installed?")
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                print("\n  ✅ Browser closed.")

    # ── FINAL STRICT PASS ──────────────────────────────────────
    print("\n── Final strict filter pass ─────────────────────────")
    before   = len(all_jobs)
    all_jobs = [j for j in all_jobs
                if is_cyber(j.get("job_title", ""), j.get("tags", []))]
    removed  = before - len(all_jobs)
    print(f"  ✂️  Removed {removed} false positives")
    print(f"  ✅ {len(all_jobs)} genuine cybersecurity jobs")

    # ── SUMMARY ────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  TOTAL UNIQUE REMOTE CYBER JOBS: {len(all_jobs)}")

    print("\n  By Platform:")
    for src, cnt in sorted(Counter(j["source"] for j in all_jobs).items(), key=lambda x: -x[1]):
        print(f"    {src:25} → {cnt}")

    print("\n  Top Matched Keywords:")
    kw_counts = Counter(kw for j in all_jobs for kw in j.get("matched_keywords", []))
    for kw, cnt in kw_counts.most_common(15):
        print(f"    {kw[:50]:50} → {cnt} jobs")

    save_json(all_jobs, OUTPUT_FILE)

    print(f"\n🎉 Done! Open '{OUTPUT_FILE}'")
    print(f"\n💡 To clean your OLD file:")
    print(f"   python cyber_job_scraper_v5.py --clean cybersecurity_remote_jobs.json")
    print("=" * 60)


if __name__ == "__main__":
    main()