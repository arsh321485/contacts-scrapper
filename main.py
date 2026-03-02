# """
# ╔══════════════════════════════════════════════════════════════════╗
# ║   main.py — SecureITLab Unified Runner                          ║
# ║                                                                  ║
# ║   FLOW:                                                          ║
# ║   STEP 1 → Run cyber_job_scraper_v5.py                          ║
# ║             → produces cybersecurity_remote_jobs.json           ║
# ║   STEP 2 → Compare scraped jobs vs MongoDB (dedup check)        ║
# ║             → skip jobs already stored                          ║
# ║             → write only NEW jobs to new_jobs_temp.json         ║
# ║   STEP 3 → Run crewai_15jobs_full.py on new jobs only           ║
# ║             → 14 agents per job + contacts + MongoDB store      ║
# ║                                                                  ║
# ║   Called by: streamlit_dashboard.py  "🔍 Find Jobs" button      ║
# ║   OR direct: py -3.12 main.py                                   ║
# ╚══════════════════════════════════════════════════════════════════╝
# """

# import os
# import sys
# import json
# import re
# import time
# import logging
# import subprocess
# from datetime import datetime, timezone
# from pathlib import Path

# from dotenv import load_dotenv
# from pymongo import MongoClient
# from pymongo.errors import ConnectionFailure

# load_dotenv()

# # ── Logging ───────────────────────────────────────────────────────────────────
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s",
#     handlers=[
#         logging.StreamHandler(sys.stdout),
#         logging.FileHandler("main_run.log", encoding="utf-8"),
#     ],
# )
# log = logging.getLogger("main")

# # ── Config ────────────────────────────────────────────────────────────────────
# MONGO_URI       = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# MONGO_DB        = os.getenv("MONGO_DB",  "secureitlab_job_pipeline")

# SCRAPER_SCRIPT  = "cyber_job_scraper_v5.py"     # your scraper file
# AGENT_SCRIPT    = "final.py"        # your agent file
# JOB_FILE        = "cybersecurity_remote_jobs.json"  # scraper output
# TEMP_JOB_FILE   = "new_jobs_temp.json"           # filtered new-only jobs
# MAX_JOBS        = 20                             # max new jobs per run

# # ── Progress callback (injected by Streamlit for live log display) ─────────────
# _progress_cb = None

# def _log(msg: str, level: str = "info"):
#     """Log to file/console AND push to Streamlit callback if set."""
#     getattr(log, level)(msg)
#     if _progress_cb:
#         try:
#             _progress_cb(msg)
#         except Exception:
#             pass


# # ══════════════════════════════════════════════════════════════════════════════
# #  MONGODB HELPERS
# # ══════════════════════════════════════════════════════════════════════════════

# def _get_db():
#     """Connect to MongoDB. Returns db object or None on failure."""
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
#         client.admin.command("ping")
#         db = client[MONGO_DB]
#         _log(f"[MongoDB] ✅ Connected → {MONGO_DB}")
#         return db
#     except ConnectionFailure as e:
#         _log(f"[MongoDB] ❌ Cannot connect: {e}", "error")
#         return None


# def _get_processed_keys(db) -> set:
#     """
#     Return a set of (company_lower, role_lower) tuples already in MongoDB.
#     These will be SKIPPED so we never re-process the same job.
#     """
#     if db is None:
#         return set()
#     keys = set()
#     try:
#         for doc in db.jobs.find({}, {"company": 1, "role": 1, "_id": 0}):
#             co   = (doc.get("company") or "").strip().lower()
#             role = (doc.get("role")    or "").strip().lower()
#             if co and role:
#                 keys.add((co, role))
#         _log(f"[Dedup] {len(keys)} jobs already exist in MongoDB")
#     except Exception as e:
#         _log(f"[Dedup] Warning reading existing jobs: {e}", "warning")
#     return keys


# def _job_key(job: dict) -> tuple:
#     """Canonical dedup key for a scraped job."""
#     company = (
#         job.get("company") or job.get("organization") or
#         job.get("company_name") or "unknown"
#     ).strip().lower()
#     role = (
#         job.get("title") or job.get("role") or
#         job.get("job_title") or "unknown"
#     ).strip().lower()
#     return (company, role)


# # ══════════════════════════════════════════════════════════════════════════════
# #  STEP 1 — RUN THE SCRAPER
# # ══════════════════════════════════════════════════════════════════════════════

# def _run_scraper() -> bool:
#     """
#     Execute cyber_job_scraper_v5.py as a subprocess.
#     Returns True if JOB_FILE was produced.
#     """
#     _log("=" * 62)
#     _log("STEP 1 — Running Job Scraper")
#     _log("=" * 62)

#     if not Path(SCRAPER_SCRIPT).exists():
#         _log(f"❌ Scraper not found: {SCRAPER_SCRIPT}  (must be in same folder)", "error")
#         return False

#     t0 = time.time()
#     try:
#         result = subprocess.run(
#             [sys.executable, SCRAPER_SCRIPT],
#             timeout=3600,   # 10-min hard limit
#         )
#         elapsed = round(time.time() - t0, 1)
#         if result.returncode != 0:
#             _log(f"⚠️  Scraper exited with code {result.returncode} (may still have output)", "warning")
#         else:
#             _log(f"✅ Scraper finished in {elapsed}s")
#     except subprocess.TimeoutExpired:
#         _log("❌ Scraper timed out (> 10 min)", "error")
#         return False
#     except Exception as e:
#         _log(f"❌ Scraper crashed: {e}", "error")
#         return False

#     if not Path(JOB_FILE).exists():
#         _log(f"❌ {JOB_FILE} not found after scraper ran", "error")
#         return False

#     try:
#         raw   = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
#         jobs  = raw.get("jobs", raw) if isinstance(raw, dict) else raw
#         _log(f"✅ {JOB_FILE} → {len(jobs)} scraped jobs")
#     except Exception as e:
#         _log(f"⚠️  Could not count scraped jobs: {e}", "warning")

#     return True


# # ══════════════════════════════════════════════════════════════════════════════
# #  STEP 2 — DEDUP: KEEP ONLY NEW JOBS
# # ══════════════════════════════════════════════════════════════════════════════

# def _filter_new_jobs(db) -> list:
#     """
#     Load all scraped jobs from JOB_FILE.
#     Remove any already in MongoDB (same company + role).
#     Remove duplicates within this batch too.
#     Cap at MAX_JOBS.
#     Returns list of new-only job dicts.
#     """
#     _log("=" * 62)
#     _log("STEP 2 — Deduplication check")
#     _log("=" * 62)

#     if not Path(JOB_FILE).exists():
#         _log(f"❌ {JOB_FILE} missing — run scraper first", "error")
#         return []

#     raw          = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
#     all_scraped  = raw.get("jobs", raw) if isinstance(raw, dict) else raw

#     existing     = _get_processed_keys(db)   # already in MongoDB
#     seen_batch   = set()                     # dupes within this file
#     new_jobs     = []
#     skip_db      = 0
#     skip_dup     = 0

#     for job in all_scraped:
#         key = _job_key(job)

#         # Already in MongoDB?
#         if key in existing:
#             skip_db += 1
#             continue

#         # Duplicate within this scraped batch?
#         if key in seen_batch:
#             skip_dup += 1
#             continue

#         seen_batch.add(key)
#         new_jobs.append(job)

#     _log(f"  Total scraped        : {len(all_scraped)}")
#     _log(f"  Already in MongoDB   : {skip_db}  ← skipped")
#     _log(f"  Duplicates in file   : {skip_dup}  ← skipped")
#     _log(f"  NEW jobs to process  : {len(new_jobs)}")

#     if len(new_jobs) > MAX_JOBS:
#         _log(f"  Capping to first {MAX_JOBS} new jobs")
#         new_jobs = new_jobs[:MAX_JOBS]

#     return new_jobs


# def _write_temp_file(new_jobs: list):
#     """Persist new-only jobs to TEMP_JOB_FILE for the agent script to read."""
#     payload = {
#         "metadata": {
#             "description": "New unique jobs for this pipeline run",
#             "total_jobs":  len(new_jobs),
#             "created_at":  datetime.now(timezone.utc).isoformat(),
#         },
#         "jobs": new_jobs,
#     }
#     Path(TEMP_JOB_FILE).write_text(
#         json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
#     )
#     _log(f"  Written {len(new_jobs)} new jobs → {TEMP_JOB_FILE}")


# # ══════════════════════════════════════════════════════════════════════════════
# #  STEP 3 — RUN THE AGENT PIPELINE
# # ══════════════════════════════════════════════════════════════════════════════

# def _patch_agent_script() -> tuple:
#     """
#     Temporarily rewrite the JOB_FILE constant inside crewai_15jobs_full.py
#     so it reads from TEMP_JOB_FILE (new jobs only).

#     Returns (original_content, was_patched).
#     """
#     content  = Path(AGENT_SCRIPT).read_text(encoding="utf-8")
#     original = content

#     # Match:  JOB_FILE   = "cybersecurity_remote_jobs.json"
#     patched = re.sub(
#         r'(JOB_FILE\s*=\s*)["\']cybersecurity_remote_jobs\.json["\']',
#         f'\\1"{TEMP_JOB_FILE}"',
#         content,
#     )
#     if patched == content:
#         _log("⚠️  Could not patch JOB_FILE in agent script — will process all scraped jobs", "warning")
#         return original, False

#     Path(AGENT_SCRIPT).write_text(patched, encoding="utf-8")
#     _log(f"  Patched {AGENT_SCRIPT} → reads from {TEMP_JOB_FILE}")
#     return original, True


# def _restore_agent_script(original_content: str):
#     Path(AGENT_SCRIPT).write_text(original_content, encoding="utf-8")
#     _log(f"  Restored {AGENT_SCRIPT} to original")


# def _run_agents(new_jobs: list) -> bool:
#     """
#     Write new_jobs to temp file, patch agent script, run it, then restore.
#     Returns True on success.
#     """
#     _log("=" * 62)
#     _log(f"STEP 3 — Running 14-Agent Pipeline ({len(new_jobs)} new jobs)")
#     _log("=" * 62)

#     if not Path(AGENT_SCRIPT).exists():
#         _log(f"❌ Agent script not found: {AGENT_SCRIPT}  (must be in same folder)", "error")
#         return False

#     _write_temp_file(new_jobs)

#     original_content, patched = _patch_agent_script()
#     t0      = time.time()
#     success = False

#     try:
#         result = subprocess.run(
#             [sys.executable, AGENT_SCRIPT],
#             timeout=7200,       # 2-hour hard limit
#             capture_output=True,  # ← ADDED: capture stdout + stderr
#             text=True,            # ← ADDED: return as string not bytes
#         )
#         elapsed = round((time.time() - t0) / 60, 1)

#         if result.returncode != 0:
#             # ← FIXED: show the real error, mark as failed
#             err_output = (result.stderr or result.stdout or "No output captured")[-3000:]
#             _log(f"❌ Agent script exited with code {result.returncode} after {elapsed} min", "error")
#             _log(f"❌ AGENT ERROR OUTPUT:\n{err_output}", "error")
#             success = False
#         else:
#             _log(f"✅ Agent pipeline finished in {elapsed} min")
#             # Also log any stdout for visibility
#             if result.stdout:
#                 _log(f"Agent output (last 1000 chars):\n{result.stdout[-1000:]}")
#             success = True

#     except subprocess.TimeoutExpired:
#         _log("❌ Agent pipeline timed out (> 2 hours)", "error")
#         success = False
#     except Exception as e:
#         _log(f"❌ Agent pipeline crashed: {e}", "error")
#         success = False
#     finally:
#         # ALWAYS restore — even on crash
#         if patched:
#             _restore_agent_script(original_content)
#         # Remove temp file
#         try:
#             Path(TEMP_JOB_FILE).unlink(missing_ok=True)
#         except Exception:
#             pass

#     return success
# # ══════════════════════════════════════════════════════════════════════════════
# #  PUBLIC ENTRY POINT  (called by Streamlit OR direct CLI)
# # ══════════════════════════════════════════════════════════════════════════════

# def run_pipeline(progress_callback=None) -> dict:
#     """
#     Full orchestrated run:
#       1. Scrape jobs
#       2. Dedup against MongoDB
#       3. Run agents on new jobs only

#     Args:
#         progress_callback: optional callable(str) — receives each log line.
#                            Used by Streamlit for live log display.

#     Returns:
#         dict with keys: success, scraped, new_jobs, skipped_db, processed, error
#     """
#     global _progress_cb
#     _progress_cb = progress_callback

#     result = {
#         "success":    False,
#         "scraped":    0,
#         "new_jobs":   0,
#         "skipped_db": 0,
#         "processed":  0,
#         "started_at": datetime.now(timezone.utc).isoformat(),
#         "error":      None,
#     }

#     _log("╔══════════════════════════════════════════════════════════╗")
#     _log("║   SecureITLab — main.py Pipeline Runner                 ║")
#     _log("║   Scraper → Dedup → 14-Agent Pipeline → MongoDB         ║")
#     _log("╚══════════════════════════════════════════════════════════╝")
#     _log(f"Run started: {result['started_at']}")

#     # ── Connect to MongoDB (needed for dedup check) ───────────────────────────
#     db = _get_db()

#     # ── STEP 1: Scraper ───────────────────────────────────────────────────────
#     ok = _run_scraper()
#     if not ok:
#         result["error"] = "Scraper failed — check main_run.log for details"
#         _log(f"❌ Stopping: {result['error']}", "error")
#         return result

#     # Count total scraped
#     try:
#         raw = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
#         all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw
#         result["scraped"] = len(all_scraped)
#     except Exception:
#         pass

#     # ── STEP 2: Dedup ─────────────────────────────────────────────────────────
#     new_jobs          = _filter_new_jobs(db)
#     result["new_jobs"]   = len(new_jobs)
#     result["skipped_db"] = result["scraped"] - len(new_jobs)

#     if not new_jobs:
#         _log("✅ No new jobs to process — all already exist in MongoDB")
#         result["success"]   = True
#         result["processed"] = 0
#         _print_summary(result)
#         return result

#     # ── STEP 3: Agent pipeline ────────────────────────────────────────────────
#     ok                  = _run_agents(new_jobs)
#     result["processed"] = len(new_jobs) if ok else 0
#     result["success"]   = ok

#     _print_summary(result)
#     return result


# def _print_summary(r: dict):
#     _log("=" * 62)
#     _log("  PIPELINE SUMMARY")
#     _log("=" * 62)
#     _log(f"  Jobs scraped         : {r['scraped']}")
#     _log(f"  Already in MongoDB   : {r['skipped_db']}  ← skipped")
#     _log(f"  New jobs found       : {r['new_jobs']}")
#     _log(f"  Processed by AI      : {r['processed']}")
#     _log(f"  Status               : {'✅ SUCCESS' if r['success'] else '❌ FAILED'}")
#     if r.get("error"):
#         _log(f"  Error                : {r['error']}")
#     _log("=" * 62)


# # ══════════════════════════════════════════════════════════════════════════════
# #  CLI ENTRY POINT
# # ══════════════════════════════════════════════════════════════════════════════

# if __name__ == "__main__":
#     result = run_pipeline()
#     sys.exit(0 if result["success"] else 1)
"""
╔══════════════════════════════════════════════════════════════════╗
║   main.py — SecureITLab Unified Runner                          ║
║                                                                  ║
║   FLOW:                                                          ║
║   STEP 1 → Run cyber_job_scraper_v5.py                          ║
║             → produces cybersecurity_remote_jobs.json           ║
║   STEP 2 → Compare scraped jobs vs MongoDB (dedup check)        ║
║             → skip jobs already stored                          ║
║             → write only NEW jobs to new_jobs_temp.json         ║
║   STEP 3 → Run final.py on new jobs only                        ║
║             → 14 agents per job + contacts + MongoDB store      ║
║                                                                  ║
║   Called by: streamlit_dashboard.py  "🔍 Find Jobs" button      ║
║   OR direct: py -3.12 main.py                                   ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import json
import re
import time
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("main_run.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("main")

# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB       = os.getenv("MONGO_DB",  "secureitlab_job_pipeline")

SCRAPER_SCRIPT = "cyber_job_scraper_v5.py"
AGENT_SCRIPT   = "final.py"
PYTHON_EXE = r"C:\Users\DELL\AppData\Local\Programs\Python\Python312\python.exe"
JOB_FILE       = "cybersecurity_remote_jobs.json"
TEMP_JOB_FILE  = "new_jobs_temp.json"
MAX_JOBS       = 20

# ── Progress callback (injected by Streamlit for live log display) ────────────
_progress_cb = None

def _log(msg: str, level: str = "info"):
    """Log to file/console AND push to Streamlit callback if set."""
    getattr(log, level)(msg)
    if _progress_cb:
        try:
            _progress_cb(msg)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  MONGODB HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_db():
    """Connect to MongoDB. Returns db object or None on failure."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        _log(f"[MongoDB] Connected → {MONGO_DB}")
        return db
    except ConnectionFailure as e:
        _log(f"[MongoDB] Cannot connect: {e}", "error")
        return None


def _get_processed_keys(db) -> set:
    """Return set of (company_lower, role_lower) already in MongoDB."""
    if db is None:
        return set()
    keys = set()
    try:
        for doc in db.jobs.find({}, {"company": 1, "role": 1, "_id": 0}):
            co   = (doc.get("company") or "").strip().lower()
            role = (doc.get("role")    or "").strip().lower()
            if co and role:
                keys.add((co, role))
        _log(f"[Dedup] {len(keys)} jobs already exist in MongoDB")
    except Exception as e:
        _log(f"[Dedup] Warning reading existing jobs: {e}", "warning")
    return keys


def _job_key(job: dict) -> tuple:
    """Canonical dedup key for a scraped job."""
    company = (
        job.get("company") or job.get("organization") or
        job.get("company_name") or "unknown"
    ).strip().lower()
    role = (
        job.get("title") or job.get("role") or
        job.get("job_title") or "unknown"
    ).strip().lower()
    return (company, role)


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — RUN THE SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def _run_scraper() -> bool:
    """Execute scraper as subprocess. Returns True if JOB_FILE was produced."""
    _log("=" * 62)
    _log("STEP 1 — Running Job Scraper")
    _log("=" * 62)

    if not Path(SCRAPER_SCRIPT).exists():
        _log(f"Scraper not found: {SCRAPER_SCRIPT}", "error")
        return False

    t0 = time.time()
    try:
        result = subprocess.run(
            [PYTHON_EXE, SCRAPER_SCRIPT],
            timeout=3600,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
        elapsed = round(time.time() - t0, 1)
        if result.returncode != 0:
            _log(f"Scraper exited with code {result.returncode}", "warning")
        else:
            _log(f"Scraper finished in {elapsed}s")
    except subprocess.TimeoutExpired:
        _log("Scraper timed out (> 60 min)", "error")
        return False
    except Exception as e:
        _log(f"Scraper crashed: {e}", "error")
        return False

    if not Path(JOB_FILE).exists():
        _log(f"{JOB_FILE} not found after scraper ran", "error")
        return False

    try:
        raw  = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
        jobs = raw.get("jobs", raw) if isinstance(raw, dict) else raw
        _log(f"{JOB_FILE} → {len(jobs)} scraped jobs")
    except Exception as e:
        _log(f"Could not count scraped jobs: {e}", "warning")

    return True


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — DEDUP: KEEP ONLY NEW JOBS
# ══════════════════════════════════════════════════════════════════════════════

def _filter_new_jobs(db) -> list:
    """Load scraped jobs, remove duplicates and already-processed ones."""
    _log("=" * 62)
    _log("STEP 2 — Deduplication check")
    _log("=" * 62)

    if not Path(JOB_FILE).exists():
        _log(f"{JOB_FILE} missing — run scraper first", "error")
        return []

    raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
    all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw

    existing   = _get_processed_keys(db)
    seen_batch = set()
    new_jobs   = []
    skip_db    = 0
    skip_dup   = 0

    for job in all_scraped:
        key = _job_key(job)
        if key in existing:
            skip_db += 1
            continue
        if key in seen_batch:
            skip_dup += 1
            continue
        seen_batch.add(key)
        new_jobs.append(job)

    _log(f"  Total scraped        : {len(all_scraped)}")
    _log(f"  Already in MongoDB   : {skip_db}  <- skipped")
    _log(f"  Duplicates in file   : {skip_dup}  <- skipped")
    _log(f"  NEW jobs to process  : {len(new_jobs)}")

    if len(new_jobs) > MAX_JOBS:
        _log(f"  Capping to first {MAX_JOBS} new jobs")
        new_jobs = new_jobs[:MAX_JOBS]

    return new_jobs


def _write_temp_file(new_jobs: list):
    """Write new-only jobs to TEMP_JOB_FILE for the agent script."""
    payload = {
        "metadata": {
            "description": "New unique jobs for this pipeline run",
            "total_jobs":  len(new_jobs),
            "created_at":  datetime.now(timezone.utc).isoformat(),
        },
        "jobs": new_jobs,
    }
    Path(TEMP_JOB_FILE).write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    _log(f"  Written {len(new_jobs)} new jobs → {TEMP_JOB_FILE}")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 — RUN THE AGENT PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def _patch_agent_script() -> tuple:
    """Temporarily rewrite JOB_FILE in final.py to read from TEMP_JOB_FILE."""
    content  = Path(AGENT_SCRIPT).read_text(encoding="utf-8")
    original = content

    patched = re.sub(
        r'(JOB_FILE\s*=\s*)["\']cybersecurity_remote_jobs\.json["\']',
        f'\\1"{TEMP_JOB_FILE}"',
        content,
    )
    if patched == content:
        _log("Could not patch JOB_FILE in agent script — will process all scraped jobs", "warning")
        return original, False

    Path(AGENT_SCRIPT).write_text(patched, encoding="utf-8")
    _log(f"  Patched {AGENT_SCRIPT} → reads from {TEMP_JOB_FILE}")
    return original, True


def _restore_agent_script(original_content: str):
    Path(AGENT_SCRIPT).write_text(original_content, encoding="utf-8")
    _log(f"  Restored {AGENT_SCRIPT} to original")


def _run_agents(new_jobs: list) -> bool:
    """Write new_jobs to temp file, patch agent script, run it, then restore."""
    _log("=" * 62)
    _log(f"STEP 3 — Running 14-Agent Pipeline ({len(new_jobs)} new jobs)")
    _log("=" * 62)

    if not Path(AGENT_SCRIPT).exists():
        _log(f"Agent script not found: {AGENT_SCRIPT}", "error")
        return False

    _write_temp_file(new_jobs)

    original_content, patched = _patch_agent_script()
    t0      = time.time()
    success = False

    try:
        result = subprocess.run(
            [PYTHON_EXE, AGENT_SCRIPT],
            timeout=7200,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
        elapsed = round((time.time() - t0) / 60, 1)

        if result.returncode != 0:
            # Show the real error output
            err_output = (result.stderr or result.stdout or "No output captured")[-3000:]
            _log(f"Agent script FAILED with code {result.returncode} after {elapsed} min", "error")
            _log(f"AGENT ERROR:\n{err_output}", "error")
            success = False
        else:
            _log(f"Agent pipeline finished in {elapsed} min")
            if result.stdout:
                _log(f"Agent output:\n{result.stdout[-2000:]}")
            success = True

    except subprocess.TimeoutExpired:
        _log("Agent pipeline timed out (> 2 hours)", "error")
        success = False
    except Exception as e:
        _log(f"Agent pipeline crashed: {e}", "error")
        success = False
    finally:
        # ALWAYS restore — even on crash
        if patched:
            _restore_agent_script(original_content)
        try:
            Path(TEMP_JOB_FILE).unlink(missing_ok=True)
        except Exception:
            pass

    return success


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT  (called by Streamlit OR direct CLI)
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(progress_callback=None) -> dict:
    """
    Full orchestrated run:
      1. Scrape jobs
      2. Dedup against MongoDB
      3. Run agents on new jobs only
    """
    global _progress_cb
    _progress_cb = progress_callback

    result = {
        "success":    False,
        "scraped":    0,
        "new_jobs":   0,
        "skipped_db": 0,
        "processed":  0,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "error":      None,
    }

    _log("SecureITLab — main.py Pipeline Runner")
    _log("Scraper → Dedup → 14-Agent Pipeline → MongoDB")
    _log(f"Run started: {result['started_at']}")

    # Connect to MongoDB
    db = _get_db()

    # STEP 1: Scraper
    ok = _run_scraper()
    if not ok:
        result["error"] = "Scraper failed — check main_run.log for details"
        _log(f"Stopping: {result['error']}", "error")
        return result

    try:
        raw = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
        all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw
        result["scraped"] = len(all_scraped)
    except Exception:
        pass

    # STEP 2: Dedup
    new_jobs             = _filter_new_jobs(db)
    result["new_jobs"]   = len(new_jobs)
    result["skipped_db"] = result["scraped"] - len(new_jobs)

    if not new_jobs:
        _log("No new jobs to process — all already exist in MongoDB")
        result["success"]   = True
        result["processed"] = 0
        _print_summary(result)
        return result

    # STEP 3: Agent pipeline
    ok                   = _run_agents(new_jobs)
    result["processed"]  = len(new_jobs) if ok else 0
    result["success"]    = ok

    _print_summary(result)
    return result


def _print_summary(r: dict):
    _log("=" * 62)
    _log("  PIPELINE SUMMARY")
    _log("=" * 62)
    _log(f"  Jobs scraped         : {r['scraped']}")
    _log(f"  Already in MongoDB   : {r['skipped_db']}  <- skipped")
    _log(f"  New jobs found       : {r['new_jobs']}")
    _log(f"  Processed by AI      : {r['processed']}")
    _log(f"  Status               : {'SUCCESS' if r['success'] else 'FAILED'}")
    if r.get("error"):
        _log(f"  Error                : {r['error']}")
    _log("=" * 62)


# ══════════════════════════════════════════════════════════════════════════════
#  CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    result = run_pipeline()
    sys.exit(0 if result["success"] else 1)