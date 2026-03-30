# # # # # # """
# # # # # # ╔══════════════════════════════════════════════════════════════════╗
# # # # # # ║   main.py — SecureITLab Unified Runner                          ║
# # # # # # ║                                                                  ║
# # # # # # ║   FLOW:                                                          ║
# # # # # # ║   STEP 1 → Run cyber_job_scraper_v5.py                          ║
# # # # # # ║             → produces cybersecurity_remote_jobs.json           ║
# # # # # # ║   STEP 2 → Compare scraped jobs vs MongoDB (dedup check)        ║
# # # # # # ║             → skip jobs already stored                          ║
# # # # # # ║             → write only NEW jobs to new_jobs_temp.json         ║
# # # # # # ║   STEP 3 → Run crewai_15jobs_full.py on new jobs only           ║
# # # # # # ║             → 14 agents per job + contacts + MongoDB store      ║
# # # # # # ║                                                                  ║
# # # # # # ║   Called by: streamlit_dashboard.py  "🔍 Find Jobs" button      ║
# # # # # # ║   OR direct: py -3.12 main.py                                   ║
# # # # # # ╚══════════════════════════════════════════════════════════════════╝
# # # # # # """

# # # # # # import os
# # # # # # import sys
# # # # # # import json
# # # # # # import re
# # # # # # import time
# # # # # # import logging
# # # # # # import subprocess
# # # # # # from datetime import datetime, timezone
# # # # # # from pathlib import Path

# # # # # # from dotenv import load_dotenv
# # # # # # from pymongo import MongoClient
# # # # # # from pymongo.errors import ConnectionFailure

# # # # # # load_dotenv()

# # # # # # # ── Logging ───────────────────────────────────────────────────────────────────
# # # # # # logging.basicConfig(
# # # # # #     level=logging.INFO,
# # # # # #     format="%(asctime)s | %(levelname)s | %(message)s",
# # # # # #     handlers=[
# # # # # #         logging.StreamHandler(sys.stdout),
# # # # # #         logging.FileHandler("main_run.log", encoding="utf-8"),
# # # # # #     ],
# # # # # # )
# # # # # # log = logging.getLogger("main")

# # # # # # # ── Config ────────────────────────────────────────────────────────────────────
# # # # # # MONGO_URI       = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# # # # # # MONGO_DB        = os.getenv("MONGO_DB",  "secureitlab_job_pipeline")

# # # # # # SCRAPER_SCRIPT  = "cyber_job_scraper_v5.py"     # your scraper file
# # # # # # AGENT_SCRIPT    = "final.py"        # your agent file
# # # # # # JOB_FILE        = "cybersecurity_remote_jobs.json"  # scraper output
# # # # # # TEMP_JOB_FILE   = "new_jobs_temp.json"           # filtered new-only jobs
# # # # # # MAX_JOBS        = 20                             # max new jobs per run

# # # # # # # ── Progress callback (injected by Streamlit for live log display) ─────────────
# # # # # # _progress_cb = None

# # # # # # def _log(msg: str, level: str = "info"):
# # # # # #     """Log to file/console AND push to Streamlit callback if set."""
# # # # # #     getattr(log, level)(msg)
# # # # # #     if _progress_cb:
# # # # # #         try:
# # # # # #             _progress_cb(msg)
# # # # # #         except Exception:
# # # # # #             pass


# # # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # # #  MONGODB HELPERS
# # # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # # def _get_db():
# # # # # #     """Connect to MongoDB. Returns db object or None on failure."""
# # # # # #     try:
# # # # # #         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
# # # # # #         client.admin.command("ping")
# # # # # #         db = client[MONGO_DB]
# # # # # #         _log(f"[MongoDB] ✅ Connected → {MONGO_DB}")
# # # # # #         return db
# # # # # #     except ConnectionFailure as e:
# # # # # #         _log(f"[MongoDB] ❌ Cannot connect: {e}", "error")
# # # # # #         return None


# # # # # # def _get_processed_keys(db) -> set:
# # # # # #     """
# # # # # #     Return a set of (company_lower, role_lower) tuples already in MongoDB.
# # # # # #     These will be SKIPPED so we never re-process the same job.
# # # # # #     """
# # # # # #     if db is None:
# # # # # #         return set()
# # # # # #     keys = set()
# # # # # #     try:
# # # # # #         for doc in db.jobs.find({}, {"company": 1, "role": 1, "_id": 0}):
# # # # # #             co   = (doc.get("company") or "").strip().lower()
# # # # # #             role = (doc.get("role")    or "").strip().lower()
# # # # # #             if co and role:
# # # # # #                 keys.add((co, role))
# # # # # #         _log(f"[Dedup] {len(keys)} jobs already exist in MongoDB")
# # # # # #     except Exception as e:
# # # # # #         _log(f"[Dedup] Warning reading existing jobs: {e}", "warning")
# # # # # #     return keys


# # # # # # def _job_key(job: dict) -> tuple:
# # # # # #     """Canonical dedup key for a scraped job."""
# # # # # #     company = (
# # # # # #         job.get("company") or job.get("organization") or
# # # # # #         job.get("company_name") or "unknown"
# # # # # #     ).strip().lower()
# # # # # #     role = (
# # # # # #         job.get("title") or job.get("role") or
# # # # # #         job.get("job_title") or "unknown"
# # # # # #     ).strip().lower()
# # # # # #     return (company, role)


# # # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # # #  STEP 1 — RUN THE SCRAPER
# # # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # # def _run_scraper() -> bool:
# # # # # #     """
# # # # # #     Execute cyber_job_scraper_v5.py as a subprocess.
# # # # # #     Returns True if JOB_FILE was produced.
# # # # # #     """
# # # # # #     _log("=" * 62)
# # # # # #     _log("STEP 1 — Running Job Scraper")
# # # # # #     _log("=" * 62)

# # # # # #     if not Path(SCRAPER_SCRIPT).exists():
# # # # # #         _log(f"❌ Scraper not found: {SCRAPER_SCRIPT}  (must be in same folder)", "error")
# # # # # #         return False

# # # # # #     t0 = time.time()
# # # # # #     try:
# # # # # #         result = subprocess.run(
# # # # # #             [sys.executable, SCRAPER_SCRIPT],
# # # # # #             timeout=3600,   # 10-min hard limit
# # # # # #         )
# # # # # #         elapsed = round(time.time() - t0, 1)
# # # # # #         if result.returncode != 0:
# # # # # #             _log(f"⚠️  Scraper exited with code {result.returncode} (may still have output)", "warning")
# # # # # #         else:
# # # # # #             _log(f"✅ Scraper finished in {elapsed}s")
# # # # # #     except subprocess.TimeoutExpired:
# # # # # #         _log("❌ Scraper timed out (> 10 min)", "error")
# # # # # #         return False
# # # # # #     except Exception as e:
# # # # # #         _log(f"❌ Scraper crashed: {e}", "error")
# # # # # #         return False

# # # # # #     if not Path(JOB_FILE).exists():
# # # # # #         _log(f"❌ {JOB_FILE} not found after scraper ran", "error")
# # # # # #         return False

# # # # # #     try:
# # # # # #         raw   = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # # # # #         jobs  = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# # # # # #         _log(f"✅ {JOB_FILE} → {len(jobs)} scraped jobs")
# # # # # #     except Exception as e:
# # # # # #         _log(f"⚠️  Could not count scraped jobs: {e}", "warning")

# # # # # #     return True


# # # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # # #  STEP 2 — DEDUP: KEEP ONLY NEW JOBS
# # # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # # def _filter_new_jobs(db) -> list:
# # # # # #     """
# # # # # #     Load all scraped jobs from JOB_FILE.
# # # # # #     Remove any already in MongoDB (same company + role).
# # # # # #     Remove duplicates within this batch too.
# # # # # #     Cap at MAX_JOBS.
# # # # # #     Returns list of new-only job dicts.
# # # # # #     """
# # # # # #     _log("=" * 62)
# # # # # #     _log("STEP 2 — Deduplication check")
# # # # # #     _log("=" * 62)

# # # # # #     if not Path(JOB_FILE).exists():
# # # # # #         _log(f"❌ {JOB_FILE} missing — run scraper first", "error")
# # # # # #         return []

# # # # # #     raw          = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # # # # #     all_scraped  = raw.get("jobs", raw) if isinstance(raw, dict) else raw

# # # # # #     existing     = _get_processed_keys(db)   # already in MongoDB
# # # # # #     seen_batch   = set()                     # dupes within this file
# # # # # #     new_jobs     = []
# # # # # #     skip_db      = 0
# # # # # #     skip_dup     = 0

# # # # # #     for job in all_scraped:
# # # # # #         key = _job_key(job)

# # # # # #         # Already in MongoDB?
# # # # # #         if key in existing:
# # # # # #             skip_db += 1
# # # # # #             continue

# # # # # #         # Duplicate within this scraped batch?
# # # # # #         if key in seen_batch:
# # # # # #             skip_dup += 1
# # # # # #             continue

# # # # # #         seen_batch.add(key)
# # # # # #         new_jobs.append(job)

# # # # # #     _log(f"  Total scraped        : {len(all_scraped)}")
# # # # # #     _log(f"  Already in MongoDB   : {skip_db}  ← skipped")
# # # # # #     _log(f"  Duplicates in file   : {skip_dup}  ← skipped")
# # # # # #     _log(f"  NEW jobs to process  : {len(new_jobs)}")

# # # # # #     if len(new_jobs) > MAX_JOBS:
# # # # # #         _log(f"  Capping to first {MAX_JOBS} new jobs")
# # # # # #         new_jobs = new_jobs[:MAX_JOBS]

# # # # # #     return new_jobs


# # # # # # def _write_temp_file(new_jobs: list):
# # # # # #     """Persist new-only jobs to TEMP_JOB_FILE for the agent script to read."""
# # # # # #     payload = {
# # # # # #         "metadata": {
# # # # # #             "description": "New unique jobs for this pipeline run",
# # # # # #             "total_jobs":  len(new_jobs),
# # # # # #             "created_at":  datetime.now(timezone.utc).isoformat(),
# # # # # #         },
# # # # # #         "jobs": new_jobs,
# # # # # #     }
# # # # # #     Path(TEMP_JOB_FILE).write_text(
# # # # # #         json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
# # # # # #     )
# # # # # #     _log(f"  Written {len(new_jobs)} new jobs → {TEMP_JOB_FILE}")


# # # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # # #  STEP 3 — RUN THE AGENT PIPELINE
# # # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # # def _patch_agent_script() -> tuple:
# # # # # #     """
# # # # # #     Temporarily rewrite the JOB_FILE constant inside crewai_15jobs_full.py
# # # # # #     so it reads from TEMP_JOB_FILE (new jobs only).

# # # # # #     Returns (original_content, was_patched).
# # # # # #     """
# # # # # #     content  = Path(AGENT_SCRIPT).read_text(encoding="utf-8")
# # # # # #     original = content

# # # # # #     # Match:  JOB_FILE   = "cybersecurity_remote_jobs.json"
# # # # # #     patched = re.sub(
# # # # # #         r'(JOB_FILE\s*=\s*)["\']cybersecurity_remote_jobs\.json["\']',
# # # # # #         f'\\1"{TEMP_JOB_FILE}"',
# # # # # #         content,
# # # # # #     )
# # # # # #     if patched == content:
# # # # # #         _log("⚠️  Could not patch JOB_FILE in agent script — will process all scraped jobs", "warning")
# # # # # #         return original, False

# # # # # #     Path(AGENT_SCRIPT).write_text(patched, encoding="utf-8")
# # # # # #     _log(f"  Patched {AGENT_SCRIPT} → reads from {TEMP_JOB_FILE}")
# # # # # #     return original, True


# # # # # # def _restore_agent_script(original_content: str):
# # # # # #     Path(AGENT_SCRIPT).write_text(original_content, encoding="utf-8")
# # # # # #     _log(f"  Restored {AGENT_SCRIPT} to original")


# # # # # # def _run_agents(new_jobs: list) -> bool:
# # # # # #     """
# # # # # #     Write new_jobs to temp file, patch agent script, run it, then restore.
# # # # # #     Returns True on success.
# # # # # #     """
# # # # # #     _log("=" * 62)
# # # # # #     _log(f"STEP 3 — Running 14-Agent Pipeline ({len(new_jobs)} new jobs)")
# # # # # #     _log("=" * 62)

# # # # # #     if not Path(AGENT_SCRIPT).exists():
# # # # # #         _log(f"❌ Agent script not found: {AGENT_SCRIPT}  (must be in same folder)", "error")
# # # # # #         return False

# # # # # #     _write_temp_file(new_jobs)

# # # # # #     original_content, patched = _patch_agent_script()
# # # # # #     t0      = time.time()
# # # # # #     success = False

# # # # # #     try:
# # # # # #         result = subprocess.run(
# # # # # #             [sys.executable, AGENT_SCRIPT],
# # # # # #             timeout=7200,       # 2-hour hard limit
# # # # # #             capture_output=True,  # ← ADDED: capture stdout + stderr
# # # # # #             text=True,            # ← ADDED: return as string not bytes
# # # # # #         )
# # # # # #         elapsed = round((time.time() - t0) / 60, 1)

# # # # # #         if result.returncode != 0:
# # # # # #             # ← FIXED: show the real error, mark as failed
# # # # # #             err_output = (result.stderr or result.stdout or "No output captured")[-3000:]
# # # # # #             _log(f"❌ Agent script exited with code {result.returncode} after {elapsed} min", "error")
# # # # # #             _log(f"❌ AGENT ERROR OUTPUT:\n{err_output}", "error")
# # # # # #             success = False
# # # # # #         else:
# # # # # #             _log(f"✅ Agent pipeline finished in {elapsed} min")
# # # # # #             # Also log any stdout for visibility
# # # # # #             if result.stdout:
# # # # # #                 _log(f"Agent output (last 1000 chars):\n{result.stdout[-1000:]}")
# # # # # #             success = True

# # # # # #     except subprocess.TimeoutExpired:
# # # # # #         _log("❌ Agent pipeline timed out (> 2 hours)", "error")
# # # # # #         success = False
# # # # # #     except Exception as e:
# # # # # #         _log(f"❌ Agent pipeline crashed: {e}", "error")
# # # # # #         success = False
# # # # # #     finally:
# # # # # #         # ALWAYS restore — even on crash
# # # # # #         if patched:
# # # # # #             _restore_agent_script(original_content)
# # # # # #         # Remove temp file
# # # # # #         try:
# # # # # #             Path(TEMP_JOB_FILE).unlink(missing_ok=True)
# # # # # #         except Exception:
# # # # # #             pass

# # # # # #     return success
# # # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # # #  PUBLIC ENTRY POINT  (called by Streamlit OR direct CLI)
# # # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # # def run_pipeline(progress_callback=None) -> dict:
# # # # # #     """
# # # # # #     Full orchestrated run:
# # # # # #       1. Scrape jobs
# # # # # #       2. Dedup against MongoDB
# # # # # #       3. Run agents on new jobs only

# # # # # #     Args:
# # # # # #         progress_callback: optional callable(str) — receives each log line.
# # # # # #                            Used by Streamlit for live log display.

# # # # # #     Returns:
# # # # # #         dict with keys: success, scraped, new_jobs, skipped_db, processed, error
# # # # # #     """
# # # # # #     global _progress_cb
# # # # # #     _progress_cb = progress_callback

# # # # # #     result = {
# # # # # #         "success":    False,
# # # # # #         "scraped":    0,
# # # # # #         "new_jobs":   0,
# # # # # #         "skipped_db": 0,
# # # # # #         "processed":  0,
# # # # # #         "started_at": datetime.now(timezone.utc).isoformat(),
# # # # # #         "error":      None,
# # # # # #     }

# # # # # #     _log("╔══════════════════════════════════════════════════════════╗")
# # # # # #     _log("║   SecureITLab — main.py Pipeline Runner                 ║")
# # # # # #     _log("║   Scraper → Dedup → 14-Agent Pipeline → MongoDB         ║")
# # # # # #     _log("╚══════════════════════════════════════════════════════════╝")
# # # # # #     _log(f"Run started: {result['started_at']}")

# # # # # #     # ── Connect to MongoDB (needed for dedup check) ───────────────────────────
# # # # # #     db = _get_db()

# # # # # #     # ── STEP 1: Scraper ───────────────────────────────────────────────────────
# # # # # #     ok = _run_scraper()
# # # # # #     if not ok:
# # # # # #         result["error"] = "Scraper failed — check main_run.log for details"
# # # # # #         _log(f"❌ Stopping: {result['error']}", "error")
# # # # # #         return result

# # # # # #     # Count total scraped
# # # # # #     try:
# # # # # #         raw = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # # # # #         all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# # # # # #         result["scraped"] = len(all_scraped)
# # # # # #     except Exception:
# # # # # #         pass

# # # # # #     # ── STEP 2: Dedup ─────────────────────────────────────────────────────────
# # # # # #     new_jobs          = _filter_new_jobs(db)
# # # # # #     result["new_jobs"]   = len(new_jobs)
# # # # # #     result["skipped_db"] = result["scraped"] - len(new_jobs)

# # # # # #     if not new_jobs:
# # # # # #         _log("✅ No new jobs to process — all already exist in MongoDB")
# # # # # #         result["success"]   = True
# # # # # #         result["processed"] = 0
# # # # # #         _print_summary(result)
# # # # # #         return result

# # # # # #     # ── STEP 3: Agent pipeline ────────────────────────────────────────────────
# # # # # #     ok                  = _run_agents(new_jobs)
# # # # # #     result["processed"] = len(new_jobs) if ok else 0
# # # # # #     result["success"]   = ok

# # # # # #     _print_summary(result)
# # # # # #     return result


# # # # # # def _print_summary(r: dict):
# # # # # #     _log("=" * 62)
# # # # # #     _log("  PIPELINE SUMMARY")
# # # # # #     _log("=" * 62)
# # # # # #     _log(f"  Jobs scraped         : {r['scraped']}")
# # # # # #     _log(f"  Already in MongoDB   : {r['skipped_db']}  ← skipped")
# # # # # #     _log(f"  New jobs found       : {r['new_jobs']}")
# # # # # #     _log(f"  Processed by AI      : {r['processed']}")
# # # # # #     _log(f"  Status               : {'✅ SUCCESS' if r['success'] else '❌ FAILED'}")
# # # # # #     if r.get("error"):
# # # # # #         _log(f"  Error                : {r['error']}")
# # # # # #     _log("=" * 62)


# # # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # # #  CLI ENTRY POINT
# # # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # # if __name__ == "__main__":
# # # # # #     result = run_pipeline()
# # # # # #     sys.exit(0 if result["success"] else 1)
# # # # # """
# # # # # ╔══════════════════════════════════════════════════════════════════╗
# # # # # ║   main.py — SecureITLab Unified Runner                          ║
# # # # # ║                                                                  ║
# # # # # ║   FLOW:                                                          ║
# # # # # ║   STEP 1 → Run cyber_job_scraper_v5.py                          ║
# # # # # ║             → produces cybersecurity_remote_jobs.json           ║
# # # # # ║   STEP 2 → Compare scraped jobs vs MongoDB (dedup check)        ║
# # # # # ║             → skip jobs already stored                          ║
# # # # # ║             → write only NEW jobs to new_jobs_temp.json         ║
# # # # # ║   STEP 3 → Run final.py on new jobs only                        ║
# # # # # ║             → 14 agents per job + contacts + MongoDB store      ║
# # # # # ║                                                                  ║
# # # # # ║   Called by: streamlit_dashboard.py  "🔍 Find Jobs" button      ║
# # # # # ║   OR direct: py -3.12 main.py                                   ║
# # # # # ╚══════════════════════════════════════════════════════════════════╝
# # # # # """

# # # # # import os
# # # # # import sys
# # # # # import json
# # # # # import re
# # # # # import time
# # # # # import logging
# # # # # import subprocess
# # # # # from datetime import datetime, timezone
# # # # # from pathlib import Path

# # # # # from dotenv import load_dotenv
# # # # # from pymongo import MongoClient
# # # # # from pymongo.errors import ConnectionFailure

# # # # # load_dotenv()

# # # # # # ── Logging ───────────────────────────────────────────────────────────────────
# # # # # logging.basicConfig(
# # # # #     level=logging.INFO,
# # # # #     format="%(asctime)s | %(levelname)s | %(message)s",
# # # # #     handlers=[
# # # # #         logging.StreamHandler(sys.stdout),
# # # # #         logging.FileHandler("main_run.log", encoding="utf-8"),
# # # # #     ],
# # # # # )
# # # # # log = logging.getLogger("main")

# # # # # # ── Config ────────────────────────────────────────────────────────────────────
# # # # # MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# # # # # MONGO_DB       = os.getenv("MONGO_DB",  "secureitlab_job_pipeline")

# # # # # SCRAPER_SCRIPT = "cyber_job_scraper_v5.py"
# # # # # AGENT_SCRIPT   = "final.py"
# # # # # PYTHON_EXE = r"C:\Users\DELL\AppData\Local\Programs\Python\Python312\python.exe"
# # # # # JOB_FILE       = "cybersecurity_remote_jobs.json"
# # # # # TEMP_JOB_FILE  = "new_jobs_temp.json"
# # # # # MAX_JOBS       = 20

# # # # # # ── Progress callback (injected by Streamlit for live log display) ────────────
# # # # # _progress_cb = None

# # # # # def _log(msg: str, level: str = "info"):
# # # # #     """Log to file/console AND push to Streamlit callback if set."""
# # # # #     getattr(log, level)(msg)
# # # # #     if _progress_cb:
# # # # #         try:
# # # # #             _progress_cb(msg)
# # # # #         except Exception:
# # # # #             pass


# # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # #  MONGODB HELPERS
# # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # def _get_db():
# # # # #     """Connect to MongoDB. Returns db object or None on failure."""
# # # # #     try:
# # # # #         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
# # # # #         client.admin.command("ping")
# # # # #         db = client[MONGO_DB]
# # # # #         _log(f"[MongoDB] Connected → {MONGO_DB}")
# # # # #         return db
# # # # #     except ConnectionFailure as e:
# # # # #         _log(f"[MongoDB] Cannot connect: {e}", "error")
# # # # #         return None


# # # # # def _get_processed_keys(db) -> set:
# # # # #     """Return set of (company_lower, role_lower) already in MongoDB."""
# # # # #     if db is None:
# # # # #         return set()
# # # # #     keys = set()
# # # # #     try:
# # # # #         for doc in db.jobs.find({}, {"company": 1, "role": 1, "_id": 0}):
# # # # #             co   = (doc.get("company") or "").strip().lower()
# # # # #             role = (doc.get("role")    or "").strip().lower()
# # # # #             if co and role:
# # # # #                 keys.add((co, role))
# # # # #         _log(f"[Dedup] {len(keys)} jobs already exist in MongoDB")
# # # # #     except Exception as e:
# # # # #         _log(f"[Dedup] Warning reading existing jobs: {e}", "warning")
# # # # #     return keys


# # # # # def _job_key(job: dict) -> tuple:
# # # # #     """Canonical dedup key for a scraped job."""
# # # # #     company = (
# # # # #         job.get("company") or job.get("organization") or
# # # # #         job.get("company_name") or "unknown"
# # # # #     ).strip().lower()
# # # # #     role = (
# # # # #         job.get("title") or job.get("role") or
# # # # #         job.get("job_title") or "unknown"
# # # # #     ).strip().lower()
# # # # #     return (company, role)


# # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # #  STEP 1 — RUN THE SCRAPER
# # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # def _run_scraper() -> bool:
# # # # #     """Execute scraper as subprocess. Returns True if JOB_FILE was produced."""
# # # # #     _log("=" * 62)
# # # # #     _log("STEP 1 — Running Job Scraper")
# # # # #     _log("=" * 62)

# # # # #     if not Path(SCRAPER_SCRIPT).exists():
# # # # #         _log(f"Scraper not found: {SCRAPER_SCRIPT}", "error")
# # # # #         return False

# # # # #     t0 = time.time()
# # # # #     try:
# # # # #         result = subprocess.run(
# # # # #             [PYTHON_EXE, SCRAPER_SCRIPT],
# # # # #             timeout=3600,
# # # # #             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
# # # # #         )
# # # # #         elapsed = round(time.time() - t0, 1)
# # # # #         if result.returncode != 0:
# # # # #             _log(f"Scraper exited with code {result.returncode}", "warning")
# # # # #         else:
# # # # #             _log(f"Scraper finished in {elapsed}s")
# # # # #     except subprocess.TimeoutExpired:
# # # # #         _log("Scraper timed out (> 60 min)", "error")
# # # # #         return False
# # # # #     except Exception as e:
# # # # #         _log(f"Scraper crashed: {e}", "error")
# # # # #         return False

# # # # #     if not Path(JOB_FILE).exists():
# # # # #         _log(f"{JOB_FILE} not found after scraper ran", "error")
# # # # #         return False

# # # # #     try:
# # # # #         raw  = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # # # #         jobs = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# # # # #         _log(f"{JOB_FILE} → {len(jobs)} scraped jobs")
# # # # #     except Exception as e:
# # # # #         _log(f"Could not count scraped jobs: {e}", "warning")

# # # # #     return True


# # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # #  STEP 2 — DEDUP: KEEP ONLY NEW JOBS
# # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # def _filter_new_jobs(db) -> list:
# # # # #     """Load scraped jobs, remove duplicates and already-processed ones."""
# # # # #     _log("=" * 62)
# # # # #     _log("STEP 2 — Deduplication check")
# # # # #     _log("=" * 62)

# # # # #     if not Path(JOB_FILE).exists():
# # # # #         _log(f"{JOB_FILE} missing — run scraper first", "error")
# # # # #         return []

# # # # #     raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # # # #     all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw

# # # # #     existing   = _get_processed_keys(db)
# # # # #     seen_batch = set()
# # # # #     new_jobs   = []
# # # # #     skip_db    = 0
# # # # #     skip_dup   = 0

# # # # #     for job in all_scraped:
# # # # #         key = _job_key(job)
# # # # #         if key in existing:
# # # # #             skip_db += 1
# # # # #             continue
# # # # #         if key in seen_batch:
# # # # #             skip_dup += 1
# # # # #             continue
# # # # #         seen_batch.add(key)
# # # # #         new_jobs.append(job)

# # # # #     _log(f"  Total scraped        : {len(all_scraped)}")
# # # # #     _log(f"  Already in MongoDB   : {skip_db}  <- skipped")
# # # # #     _log(f"  Duplicates in file   : {skip_dup}  <- skipped")
# # # # #     _log(f"  NEW jobs to process  : {len(new_jobs)}")

# # # # #     if len(new_jobs) > MAX_JOBS:
# # # # #         _log(f"  Capping to first {MAX_JOBS} new jobs")
# # # # #         new_jobs = new_jobs[:MAX_JOBS]

# # # # #     return new_jobs


# # # # # def _write_temp_file(new_jobs: list):
# # # # #     """Write new-only jobs to TEMP_JOB_FILE for the agent script."""
# # # # #     payload = {
# # # # #         "metadata": {
# # # # #             "description": "New unique jobs for this pipeline run",
# # # # #             "total_jobs":  len(new_jobs),
# # # # #             "created_at":  datetime.now(timezone.utc).isoformat(),
# # # # #         },
# # # # #         "jobs": new_jobs,
# # # # #     }
# # # # #     Path(TEMP_JOB_FILE).write_text(
# # # # #         json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
# # # # #     )
# # # # #     _log(f"  Written {len(new_jobs)} new jobs → {TEMP_JOB_FILE}")


# # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # #  STEP 3 — RUN THE AGENT PIPELINE
# # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # def _patch_agent_script() -> tuple:
# # # # #     """Temporarily rewrite JOB_FILE in final.py to read from TEMP_JOB_FILE."""
# # # # #     content  = Path(AGENT_SCRIPT).read_text(encoding="utf-8")
# # # # #     original = content

# # # # #     patched = re.sub(
# # # # #         r'(JOB_FILE\s*=\s*)["\']cybersecurity_remote_jobs\.json["\']',
# # # # #         f'\\1"{TEMP_JOB_FILE}"',
# # # # #         content,
# # # # #     )
# # # # #     if patched == content:
# # # # #         _log("Could not patch JOB_FILE in agent script — will process all scraped jobs", "warning")
# # # # #         return original, False

# # # # #     Path(AGENT_SCRIPT).write_text(patched, encoding="utf-8")
# # # # #     _log(f"  Patched {AGENT_SCRIPT} → reads from {TEMP_JOB_FILE}")
# # # # #     return original, True


# # # # # def _restore_agent_script(original_content: str):
# # # # #     Path(AGENT_SCRIPT).write_text(original_content, encoding="utf-8")
# # # # #     _log(f"  Restored {AGENT_SCRIPT} to original")


# # # # # def _run_agents(new_jobs: list) -> bool:
# # # # #     """Write new_jobs to temp file, patch agent script, run it, then restore."""
# # # # #     _log("=" * 62)
# # # # #     _log(f"STEP 3 — Running 14-Agent Pipeline ({len(new_jobs)} new jobs)")
# # # # #     _log("=" * 62)

# # # # #     if not Path(AGENT_SCRIPT).exists():
# # # # #         _log(f"Agent script not found: {AGENT_SCRIPT}", "error")
# # # # #         return False

# # # # #     _write_temp_file(new_jobs)

# # # # #     original_content, patched = _patch_agent_script()
# # # # #     t0      = time.time()
# # # # #     success = False

# # # # #     try:
# # # # #         result = subprocess.run(
# # # # #             [PYTHON_EXE, AGENT_SCRIPT],
# # # # #             timeout=7200,
# # # # #             capture_output=True,
# # # # #             text=True,
# # # # #             encoding="utf-8",
# # # # #             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
# # # # #         )
# # # # #         elapsed = round((time.time() - t0) / 60, 1)

# # # # #         if result.returncode != 0:
# # # # #             # Show the real error output
# # # # #             err_output = (result.stderr or result.stdout or "No output captured")[-3000:]
# # # # #             _log(f"Agent script FAILED with code {result.returncode} after {elapsed} min", "error")
# # # # #             _log(f"AGENT ERROR:\n{err_output}", "error")
# # # # #             success = False
# # # # #         else:
# # # # #             _log(f"Agent pipeline finished in {elapsed} min")
# # # # #             if result.stdout:
# # # # #                 _log(f"Agent output:\n{result.stdout[-2000:]}")
# # # # #             success = True

# # # # #     except subprocess.TimeoutExpired:
# # # # #         _log("Agent pipeline timed out (> 2 hours)", "error")
# # # # #         success = False
# # # # #     except Exception as e:
# # # # #         _log(f"Agent pipeline crashed: {e}", "error")
# # # # #         success = False
# # # # #     finally:
# # # # #         # ALWAYS restore — even on crash
# # # # #         if patched:
# # # # #             _restore_agent_script(original_content)
# # # # #         try:
# # # # #             Path(TEMP_JOB_FILE).unlink(missing_ok=True)
# # # # #         except Exception:
# # # # #             pass

# # # # #     return success


# # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # #  PUBLIC ENTRY POINT  (called by Streamlit OR direct CLI)
# # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # def run_pipeline(progress_callback=None) -> dict:
# # # # #     """
# # # # #     Full orchestrated run:
# # # # #       1. Scrape jobs
# # # # #       2. Dedup against MongoDB
# # # # #       3. Run agents on new jobs only
# # # # #     """
# # # # #     global _progress_cb
# # # # #     _progress_cb = progress_callback

# # # # #     result = {
# # # # #         "success":    False,
# # # # #         "scraped":    0,
# # # # #         "new_jobs":   0,
# # # # #         "skipped_db": 0,
# # # # #         "processed":  0,
# # # # #         "started_at": datetime.now(timezone.utc).isoformat(),
# # # # #         "error":      None,
# # # # #     }

# # # # #     _log("SecureITLab — main.py Pipeline Runner")
# # # # #     _log("Scraper → Dedup → 14-Agent Pipeline → MongoDB")
# # # # #     _log(f"Run started: {result['started_at']}")

# # # # #     # Connect to MongoDB
# # # # #     db = _get_db()

# # # # #     # STEP 1: Scraper
# # # # #     ok = _run_scraper()
# # # # #     if not ok:
# # # # #         result["error"] = "Scraper failed — check main_run.log for details"
# # # # #         _log(f"Stopping: {result['error']}", "error")
# # # # #         return result

# # # # #     try:
# # # # #         raw = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # # # #         all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# # # # #         result["scraped"] = len(all_scraped)
# # # # #     except Exception:
# # # # #         pass

# # # # #     # STEP 2: Dedup
# # # # #     new_jobs             = _filter_new_jobs(db)
# # # # #     result["new_jobs"]   = len(new_jobs)
# # # # #     result["skipped_db"] = result["scraped"] - len(new_jobs)

# # # # #     if not new_jobs:
# # # # #         _log("No new jobs to process — all already exist in MongoDB")
# # # # #         result["success"]   = True
# # # # #         result["processed"] = 0
# # # # #         _print_summary(result)
# # # # #         return result

# # # # #     # STEP 3: Agent pipeline
# # # # #     ok                   = _run_agents(new_jobs)
# # # # #     result["processed"]  = len(new_jobs) if ok else 0
# # # # #     result["success"]    = ok

# # # # #     _print_summary(result)
# # # # #     return result


# # # # # def _print_summary(r: dict):
# # # # #     _log("=" * 62)
# # # # #     _log("  PIPELINE SUMMARY")
# # # # #     _log("=" * 62)
# # # # #     _log(f"  Jobs scraped         : {r['scraped']}")
# # # # #     _log(f"  Already in MongoDB   : {r['skipped_db']}  <- skipped")
# # # # #     _log(f"  New jobs found       : {r['new_jobs']}")
# # # # #     _log(f"  Processed by AI      : {r['processed']}")
# # # # #     _log(f"  Status               : {'SUCCESS' if r['success'] else 'FAILED'}")
# # # # #     if r.get("error"):
# # # # #         _log(f"  Error                : {r['error']}")
# # # # #     _log("=" * 62)


# # # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # # #  CLI ENTRY POINT
# # # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # # if __name__ == "__main__":
# # # # #     result = run_pipeline()
# # # # #     sys.exit(0 if result["success"] else 1)




















# # # # """
# # # # ╔══════════════════════════════════════════════════════════════════╗
# # # # ║   main.py — SecureITLab Unified Runner                          ║
# # # # ║                                                                  ║
# # # # ║   FLOW:                                                          ║
# # # # ║   STEP 1 → Run cyber_job_scraper_v5.py                          ║
# # # # ║             → produces cybersecurity_remote_jobs.json           ║
# # # # ║   STEP 2 → Compare scraped jobs vs MongoDB (dedup check)        ║
# # # # ║             → skip jobs already stored                          ║
# # # # ║             → write only NEW jobs to new_jobs_temp.json         ║
# # # # ║   STEP 3 → Run final.py on new jobs only                        ║
# # # # ║             → 14 agents per job + contacts + MongoDB store      ║
# # # # ║                                                                  ║
# # # # ║   Called by: streamlit_dashboard.py  "🔍 Find Jobs" button      ║
# # # # ║   OR direct: py -3.12 main.py                                   ║
# # # # ║                                                                  ║
# # # # ║   SUPER FIXED: Logs sent to UI in real-time!                    ║
# # # # ╚══════════════════════════════════════════════════════════════════╝
# # # # """

# # # # import os
# # # # import sys
# # # # import json
# # # # import re
# # # # import time
# # # # import logging
# # # # import subprocess
# # # # from datetime import datetime, timezone
# # # # from pathlib import Path

# # # # from dotenv import load_dotenv
# # # # from pymongo import MongoClient
# # # # from pymongo.errors import ConnectionFailure

# # # # load_dotenv()

# # # # # ── Global callback for Streamlit ─────────────────────────────────────────────
# # # # _progress_cb = None

# # # # # ── Custom logging handler that sends to callback ─────────────────────────────
# # # # class StreamlitLogHandler(logging.Handler):
# # # #     """Custom handler that sends logs to Streamlit callback"""
# # # #     def emit(self, record):
# # # #         try:
# # # #             msg = self.format(record)
# # # #             if _progress_cb:
# # # #                 _progress_cb(msg)
# # # #         except Exception:
# # # #             pass

# # # # # ── Setup logging ─────────────────────────────────────────────────────────────
# # # # logging.basicConfig(
# # # #     level=logging.INFO,
# # # #     format="%(asctime)s | %(levelname)s | %(message)s",
# # # # )

# # # # # Add our custom handler
# # # # log = logging.getLogger("main")
# # # # log.setLevel(logging.INFO)

# # # # # Console handler (terminal)
# # # # console_handler = logging.StreamHandler(sys.stdout)
# # # # console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# # # # log.addHandler(console_handler)

# # # # # File handler (log file)
# # # # file_handler = logging.FileHandler("main_run.log", encoding="utf-8")
# # # # file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# # # # log.addHandler(file_handler)

# # # # # Streamlit handler (will send to UI)
# # # # streamlit_handler = StreamlitLogHandler()
# # # # streamlit_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# # # # log.addHandler(streamlit_handler)

# # # # # ── Config ────────────────────────────────────────────────────────────────────
# # # # MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# # # # MONGO_DB       = os.getenv("MONGO_DB",  "secureitlab_job_pipeline")

# # # # SCRAPER_SCRIPT = "cyber_job_scraper_v5.py"
# # # # AGENT_SCRIPT   = "final.py"
# # # # PYTHON_EXE = r"C:\Users\DELL\AppData\Local\Programs\Python\Python312\python.exe"
# # # # JOB_FILE       = "cybersecurity_remote_jobs.json"
# # # # TEMP_JOB_FILE  = "new_jobs_temp.json"
# # # # MAX_JOBS       = 20


# # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # #  MONGODB HELPERS
# # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # def _get_db():
# # # #     """Connect to MongoDB. Returns db object or None on failure."""
# # # #     try:
# # # #         log.info("[MongoDB] Connecting…")
# # # #         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
# # # #         client.admin.command("ping")
# # # #         db = client[MONGO_DB]
# # # #         log.info(f"[MongoDB] ✅ Connected to {MONGO_DB}")
# # # #         return db
# # # #     except ConnectionFailure as e:
# # # #         log.error(f"[MongoDB] ❌ Cannot connect: {e}")
# # # #         return None


# # # # def _get_processed_keys(db) -> set:
# # # #     """Return set of (company_lower, role_lower) already in MongoDB."""
# # # #     if db is None:
# # # #         return set()
# # # #     keys = set()
# # # #     try:
# # # #         log.info("[Dedup] Checking existing jobs in MongoDB…")
# # # #         for doc in db.jobs.find({}, {"company": 1, "role": 1, "_id": 0}):
# # # #             co   = (doc.get("company") or "").strip().lower()
# # # #             role = (doc.get("role")    or "").strip().lower()
# # # #             if co and role:
# # # #                 keys.add((co, role))
# # # #         log.info(f"[Dedup] ✅ Found {len(keys)} existing jobs in MongoDB")
# # # #     except Exception as e:
# # # #         log.warning(f"[Dedup] ⚠️ Warning reading existing jobs: {e}")
# # # #     return keys


# # # # def _job_key(job: dict) -> tuple:
# # # #     """Canonical dedup key for a scraped job."""
# # # #     company = (
# # # #         job.get("company") or job.get("organization") or
# # # #         job.get("company_name") or "unknown"
# # # #     ).strip().lower()
# # # #     role = (
# # # #         job.get("title") or job.get("role") or
# # # #         job.get("job_title") or "unknown"
# # # #     ).strip().lower()
# # # #     return (company, role)


# # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # #  STEP 1 — RUN THE SCRAPER
# # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # def _run_scraper() -> bool:
# # # #     """Execute scraper as subprocess. Returns True if JOB_FILE was produced."""
# # # #     log.info("=" * 62)
# # # #     log.info("STEP 1 — Running Job Scraper")
# # # #     log.info("=" * 62)
# # # #     log.info(f"Script: {SCRAPER_SCRIPT}")
# # # #     log.info(f"Output: {JOB_FILE}")

# # # #     if not Path(SCRAPER_SCRIPT).exists():
# # # #         log.error(f"❌ Scraper not found: {SCRAPER_SCRIPT}")
# # # #         return False

# # # #     log.info("⏳ Starting scraper subprocess…")
# # # #     t0 = time.time()
# # # #     try:
# # # #         result = subprocess.run(
# # # #             [PYTHON_EXE, SCRAPER_SCRIPT],
# # # #             timeout=3600,
# # # #             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
# # # #             capture_output=True,
# # # #             text=True,
# # # #         )
# # # #         elapsed = round(time.time() - t0, 1)
        
# # # #         if result.returncode != 0:
# # # #             err = (result.stderr or result.stdout or "No output")[-1000:]
# # # #             log.error(f"❌ Scraper exited with code {result.returncode} after {elapsed}s")
# # # #             log.error(f"Error output: {err}")
# # # #         else:
# # # #             log.info(f"✅ Scraper finished in {elapsed}s")
# # # #             if result.stdout:
# # # #                 lines = result.stdout.strip().split('\n')
# # # #                 for line in lines[-5:]:
# # # #                     if line.strip():
# # # #                         log.info(f"  {line}")
# # # #     except subprocess.TimeoutExpired:
# # # #         log.error("❌ Scraper timed out (> 60 min)")
# # # #         return False
# # # #     except Exception as e:
# # # #         log.error(f"❌ Scraper crashed: {e}")
# # # #         return False

# # # #     if not Path(JOB_FILE).exists():
# # # #         log.error(f"❌ {JOB_FILE} not found after scraper ran")
# # # #         return False

# # # #     try:
# # # #         raw  = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # # #         jobs = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# # # #         log.info(f"✅ Scraped {len(jobs)} total jobs from {JOB_FILE}")
# # # #     except Exception as e:
# # # #         log.warning(f"⚠️ Could not count scraped jobs: {e}")
# # # #         return False

# # # #     return True


# # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # #  STEP 2 — DEDUP: KEEP ONLY NEW JOBS
# # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # def _filter_new_jobs(db) -> list:
# # # #     """Load scraped jobs, remove duplicates and already-processed ones."""
# # # #     log.info("=" * 62)
# # # #     log.info("STEP 2 — Deduplication & Filtering")
# # # #     log.info("=" * 62)

# # # #     if not Path(JOB_FILE).exists():
# # # #         log.error(f"❌ {JOB_FILE} missing — run scraper first")
# # # #         return []

# # # #     raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # # #     all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw

# # # #     existing   = _get_processed_keys(db)
# # # #     seen_batch = set()
# # # #     new_jobs   = []
# # # #     skip_db    = 0
# # # #     skip_dup   = 0

# # # #     log.info(f"🔍 Processing {len(all_scraped)} scraped jobs…")

# # # #     for i, job in enumerate(all_scraped, 1):
# # # #         key = _job_key(job)
# # # #         if key in existing:
# # # #             skip_db += 1
# # # #             if i % 5 == 0:
# # # #                 log.info(f"  [{i}/{len(all_scraped)}] Checking for duplicates…")
# # # #             continue
# # # #         if key in seen_batch:
# # # #             skip_dup += 1
# # # #             continue
# # # #         seen_batch.add(key)
# # # #         new_jobs.append(job)

# # # #     log.info("")
# # # #     log.info("📊 Deduplication Results:")
# # # #     log.info(f"  Total scraped        : {len(all_scraped)}")
# # # #     log.info(f"  Already in MongoDB   : {skip_db}  ← SKIPPED")
# # # #     log.info(f"  Duplicates in file   : {skip_dup}  ← SKIPPED")
# # # #     log.info(f"  🆕 NEW jobs         : {len(new_jobs)}")

# # # #     if len(new_jobs) > MAX_JOBS:
# # # #         log.warning(f"⚠️  Capping to first {MAX_JOBS} new jobs (limit set)")
# # # #         new_jobs = new_jobs[:MAX_JOBS]

# # # #     return new_jobs


# # # # def _write_temp_file(new_jobs: list):
# # # #     """Write new-only jobs to TEMP_JOB_FILE for the agent script."""
# # # #     payload = {
# # # #         "metadata": {
# # # #             "description": "New unique jobs for this pipeline run",
# # # #             "total_jobs":  len(new_jobs),
# # # #             "created_at":  datetime.now(timezone.utc).isoformat(),
# # # #         },
# # # #         "jobs": new_jobs,
# # # #     }
# # # #     Path(TEMP_JOB_FILE).write_text(
# # # #         json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
# # # #     )
# # # #     log.info(f"✅ Written {len(new_jobs)} new jobs → {TEMP_JOB_FILE}")


# # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # #  STEP 3 — RUN THE AGENT PIPELINE
# # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # def _patch_agent_script() -> tuple:
# # # #     """Temporarily rewrite JOB_FILE in final.py to read from TEMP_JOB_FILE."""
# # # #     content  = Path(AGENT_SCRIPT).read_text(encoding="utf-8")
# # # #     original = content

# # # #     patched = re.sub(
# # # #         r'(JOB_FILE\s*=\s*)["\']cybersecurity_remote_jobs\.json["\']',
# # # #         f'\\1"{TEMP_JOB_FILE}"',
# # # #         content,
# # # #     )
# # # #     if patched == content:
# # # #         log.warning("⚠️ Could not patch JOB_FILE in agent script — will process all scraped jobs")
# # # #         return original, False

# # # #     Path(AGENT_SCRIPT).write_text(patched, encoding="utf-8")
# # # #     log.info(f"✅ Patched {AGENT_SCRIPT} → reads from {TEMP_JOB_FILE}")
# # # #     return original, True


# # # # def _restore_agent_script(original_content: str):
# # # #     Path(AGENT_SCRIPT).write_text(original_content, encoding="utf-8")
# # # #     log.info(f"✅ Restored {AGENT_SCRIPT} to original")


# # # # def _run_agents(new_jobs: list) -> bool:
# # # #     """Write new_jobs to temp file, patch agent script, run it, then restore."""
# # # #     log.info("=" * 62)
# # # #     log.info(f"STEP 3 — Running AI Agent Pipeline")
# # # #     log.info(f"Jobs to process: {len(new_jobs)}")
# # # #     log.info("=" * 62)

# # # #     if not Path(AGENT_SCRIPT).exists():
# # # #         log.error(f"❌ Agent script not found: {AGENT_SCRIPT}")
# # # #         return False

# # # #     _write_temp_file(new_jobs)

# # # #     original_content, patched = _patch_agent_script()
# # # #     t0      = time.time()
# # # #     success = False

# # # #     log.info("⏳ Starting agent pipeline subprocess…")
# # # #     log.info("(This may take several minutes depending on the number of jobs)")
# # # #     log.info("")

# # # #     try:
# # # #         result = subprocess.run(
# # # #             [PYTHON_EXE, AGENT_SCRIPT],
# # # #             timeout=7200,
# # # #             capture_output=True,
# # # #             text=True,
# # # #             encoding="utf-8",
# # # #             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
# # # #         )
# # # #         elapsed = round((time.time() - t0) / 60, 1)

# # # #         if result.returncode != 0:
# # # #             err_output = (result.stderr or result.stdout or "No output captured")[-2000:]
# # # #             log.info("")
# # # #             log.error(f"❌ Agent script FAILED with code {result.returncode} after {elapsed} min")
# # # #             log.error("Error details:")
# # # #             log.error(err_output)
# # # #             success = False
# # # #         else:
# # # #             log.info("")
# # # #             log.info(f"✅ Agent pipeline completed successfully in {elapsed} min")
# # # #             if result.stdout:
# # # #                 lines = (result.stdout or "")[-1000:].split('\n')
# # # #                 log.info("Pipeline output (last lines):")
# # # #                 for line in lines[-10:]:
# # # #                     if line.strip():
# # # #                         log.info(f"  {line}")
# # # #             success = True

# # # #     except subprocess.TimeoutExpired:
# # # #         log.error("❌ Agent pipeline timed out (> 2 hours)")
# # # #         success = False
# # # #     except Exception as e:
# # # #         log.error(f"❌ Agent pipeline crashed: {e}")
# # # #         success = False
# # # #     finally:
# # # #         if patched:
# # # #             _restore_agent_script(original_content)
# # # #         try:
# # # #             Path(TEMP_JOB_FILE).unlink(missing_ok=True)
# # # #         except Exception:
# # # #             pass

# # # #     return success


# # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # #  PUBLIC ENTRY POINT
# # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # def run_pipeline(progress_callback=None) -> dict:
# # # #     """
# # # #     Full orchestrated run:
# # # #       1. Scrape jobs
# # # #       2. Dedup against MongoDB
# # # #       3. Run agents on new jobs only
    
# # # #     Args:
# # # #         progress_callback: Function to receive log messages in real-time
    
# # # #     Returns:
# # # #         dict with pipeline results
# # # #     """
# # # #     global _progress_cb
# # # #     _progress_cb = progress_callback

# # # #     result = {
# # # #         "success":    False,
# # # #         "scraped":    0,
# # # #         "new_jobs":   0,
# # # #         "skipped_db": 0,
# # # #         "processed":  0,
# # # #         "started_at": datetime.now(timezone.utc).isoformat(),
# # # #         "error":      None,
# # # #     }

# # # #     log.info("")
# # # #     log.info("╔" + "═" * 60 + "╗")
# # # #     log.info("║  SecureITLab Pipeline Runner                              ║")
# # # #     log.info("║  Scraper → Dedup → 14-Agent Pipeline → MongoDB            ║")
# # # #     log.info("╚" + "═" * 60 + "╝")
# # # #     log.info(f"Run started: {result['started_at']}")
# # # #     log.info("")

# # # #     db = _get_db()
# # # #     if db is None:
# # # #         result["error"] = "Failed to connect to MongoDB"
# # # #         log.error(f"❌ Stopping: {result['error']}")
# # # #         _print_summary(result)
# # # #         return result

# # # #     log.info("")

# # # #     # STEP 1: Scraper
# # # #     ok = _run_scraper()
# # # #     if not ok:
# # # #         result["error"] = "Scraper failed"
# # # #         log.error(f"❌ Stopping: {result['error']}")
# # # #         _print_summary(result)
# # # #         return result

# # # #     try:
# # # #         raw = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # # #         all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# # # #         result["scraped"] = len(all_scraped)
# # # #     except Exception:
# # # #         pass

# # # #     log.info("")

# # # #     # STEP 2: Dedup
# # # #     new_jobs             = _filter_new_jobs(db)
# # # #     result["new_jobs"]   = len(new_jobs)
# # # #     result["skipped_db"] = result["scraped"] - len(new_jobs)

# # # #     if not new_jobs:
# # # #         log.info("")
# # # #         log.info("⏭️ No new jobs to process — all already exist in MongoDB")
# # # #         result["success"]   = True
# # # #         result["processed"] = 0
# # # #         _print_summary(result)
# # # #         return result

# # # #     log.info("")

# # # #     # STEP 3: Agent pipeline
# # # #     ok                   = _run_agents(new_jobs)
# # # #     result["processed"]  = len(new_jobs) if ok else 0
# # # #     result["success"]    = ok

# # # #     log.info("")
# # # #     _print_summary(result)
# # # #     return result


# # # # def _print_summary(r: dict):
# # # #     log.info("=" * 62)
# # # #     log.info("  PIPELINE SUMMARY")
# # # #     log.info("=" * 62)
# # # #     log.info(f"  📦 Jobs scraped         : {r['scraped']}")
# # # #     log.info(f"  ⏭️ Already in MongoDB   : {r['skipped_db']}  ← SKIPPED")
# # # #     log.info(f"  🆕 New jobs found      : {r['new_jobs']}")
# # # #     log.info(f"  ✅ Processed by AI      : {r['processed']}")
# # # #     log.info(f"  Status                  : {'✅ SUCCESS' if r['success'] else '❌ FAILED'}")
# # # #     if r.get("error"):
# # # #         log.info(f"  Error                   : {r['error']}")
# # # #     log.info("=" * 62)
# # # #     log.info("")


# # # # # ══════════════════════════════════════════════════════════════════════════════
# # # # #  CLI ENTRY POINT
# # # # # ══════════════════════════════════════════════════════════════════════════════

# # # # if __name__ == "__main__":
# # # #     result = run_pipeline()
# # # #     sys.exit(0 if result["success"] else 1)
























# # # """
# # # ╔══════════════════════════════════════════════════════════════════╗
# # # ║   main.py — SecureITLab Unified Runner                          ║
# # # ║                                                                  ║
# # # ║   FLOW:                                                          ║
# # # ║   STEP 1 → Run cyber_job_scraper_v5.py                          ║
# # # ║             → produces cybersecurity_remote_jobs.json           ║
# # # ║   STEP 2 → Compare scraped jobs vs MongoDB (dedup check)        ║
# # # ║             → skip jobs already stored                          ║
# # # ║             → write only NEW jobs to new_jobs_temp.json         ║
# # # ║   STEP 3 → Run final.py on new jobs only                        ║
# # # ║             → 15 agents per job + contacts + MongoDB store      ║
# # # ║                                                                  ║
# # # ║   AUTO-SCHEDULER:                                                ║
# # # ║   • When triggered from Streamlit, pipeline runs immediately    ║
# # # ║   • Then repeats automatically every 12 hours in background     ║
# # # ║   • Scheduler state persisted in scheduler_state.json           ║
# # # ║   • Manual "Find Jobs" button always works (resets 12hr timer)  ║
# # # ║                                                                  ║
# # # ║   Called by: streamlit_dashboard.py  "🔍 Find Jobs" button      ║
# # # ║   OR direct: py -3.12 main.py                                   ║
# # # ╚══════════════════════════════════════════════════════════════════╝
# # # """

# # # import os
# # # import sys
# # # import json
# # # import re
# # # import time
# # # import logging
# # # import subprocess
# # # import threading
# # # from datetime import datetime, timezone
# # # from pathlib import Path

# # # from dotenv import load_dotenv
# # # from pymongo import MongoClient
# # # from pymongo.errors import ConnectionFailure

# # # load_dotenv()

# # # # ── Global callback for Streamlit ─────────────────────────────────────────────
# # # _progress_cb = None

# # # # ── Scheduler state (module-level, shared across calls) ──────────────────────
# # # _scheduler_thread   = None          # the running Thread object
# # # _scheduler_stop_evt = threading.Event()   # set this to stop the scheduler
# # # _scheduler_lock     = threading.Lock()

# # # SCHEDULER_STATE_FILE = "scheduler_state.json"   # persists last_run / next_run
# # # SCHEDULE_INTERVAL_HRS = 12                       # change if you want a different interval


# # # # ── Custom logging handler that sends to callback ─────────────────────────────
# # # class StreamlitLogHandler(logging.Handler):
# # #     """Custom handler that sends logs to Streamlit callback"""
# # #     def emit(self, record):
# # #         try:
# # #             msg = self.format(record)
# # #             if _progress_cb:
# # #                 _progress_cb(msg)
# # #         except Exception:
# # #             pass


# # # # ── Setup logging ─────────────────────────────────────────────────────────────
# # # logging.basicConfig(
# # #     level=logging.INFO,
# # #     format="%(asctime)s | %(levelname)s | %(message)s",
# # # )

# # # log = logging.getLogger("main")
# # # log.setLevel(logging.INFO)

# # # # Avoid adding duplicate handlers on repeated imports
# # # if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, StreamlitLogHandler)
# # #            for h in log.handlers):
# # #     console_handler = logging.StreamHandler(sys.stdout)
# # #     console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# # #     log.addHandler(console_handler)

# # # if not any(isinstance(h, logging.FileHandler) for h in log.handlers):
# # #     file_handler = logging.FileHandler("main_run.log", encoding="utf-8")
# # #     file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# # #     log.addHandler(file_handler)

# # # _streamlit_log_handler = StreamlitLogHandler()
# # # _streamlit_log_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# # # if not any(isinstance(h, StreamlitLogHandler) for h in log.handlers):
# # #     log.addHandler(_streamlit_log_handler)


# # # # ── Config ────────────────────────────────────────────────────────────────────
# # # MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# # # MONGO_DB       = os.getenv("MONGO_DB",  "secureitlab_job_pipeline")

# # # SCRAPER_SCRIPT = "cyber_job_scraper_v5.py"
# # # AGENT_SCRIPT   = "final.py"
# # # PYTHON_EXE     = r"C:\Users\DELL\AppData\Local\Programs\Python\Python312\python.exe"
# # # JOB_FILE       = "cybersecurity_remote_jobs.json"
# # # TEMP_JOB_FILE  = "new_jobs_temp.json"
# # # MAX_JOBS       = 20


# # # # ══════════════════════════════════════════════════════════════════════════════
# # # #  SCHEDULER STATE HELPERS
# # # # ══════════════════════════════════════════════════════════════════════════════

# # # def _read_scheduler_state() -> dict:
# # #     """Read persisted scheduler state from disk."""
# # #     try:
# # #         if Path(SCHEDULER_STATE_FILE).exists():
# # #             return json.loads(Path(SCHEDULER_STATE_FILE).read_text(encoding="utf-8"))
# # #     except Exception:
# # #         pass
# # #     return {"last_run": None, "next_run": None, "run_count": 0, "active": False}


# # # def _write_scheduler_state(state: dict):
# # #     """Persist scheduler state to disk so Streamlit can read it."""
# # #     try:
# # #         Path(SCHEDULER_STATE_FILE).write_text(
# # #             json.dumps(state, indent=2, default=str), encoding="utf-8"
# # #         )
# # #     except Exception as e:
# # #         log.warning(f"[Scheduler] Could not write state file: {e}")


# # # def get_scheduler_status() -> dict:
# # #     """
# # #     Public helper — called by Streamlit to display scheduler status in the UI.
# # #     Returns dict with: active, last_run, next_run, run_count, seconds_until_next
# # #     """
# # #     state = _read_scheduler_state()
# # #     now   = datetime.now(timezone.utc).timestamp()

# # #     seconds_until_next = None
# # #     if state.get("next_run"):
# # #         try:
# # #             next_ts = datetime.fromisoformat(state["next_run"]).timestamp()
# # #             seconds_until_next = max(0, int(next_ts - now))
# # #         except Exception:
# # #             pass

# # #     return {
# # #         "active":             state.get("active", False),
# # #         "last_run":           state.get("last_run"),
# # #         "next_run":           state.get("next_run"),
# # #         "run_count":          state.get("run_count", 0),
# # #         "seconds_until_next": seconds_until_next,
# # #     }


# # # def stop_scheduler():
# # #     """
# # #     Public helper — called by Streamlit if user wants to stop auto-scheduling.
# # #     Sets the stop event; the background thread will exit after its current sleep.
# # #     """
# # #     global _scheduler_thread
# # #     _scheduler_stop_evt.set()
# # #     state = _read_scheduler_state()
# # #     state["active"] = False
# # #     _write_scheduler_state(state)
# # #     log.info("[Scheduler] ⏹️  Stop requested — scheduler will exit after current sleep.")


# # # # ══════════════════════════════════════════════════════════════════════════════
# # # #  MONGODB HELPERS
# # # # ══════════════════════════════════════════════════════════════════════════════

# # # def _get_db():
# # #     """Connect to MongoDB. Returns db object or None on failure."""
# # #     try:
# # #         log.info("[MongoDB] Connecting…")
# # #         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
# # #         client.admin.command("ping")
# # #         db = client[MONGO_DB]
# # #         log.info(f"[MongoDB] ✅ Connected to {MONGO_DB}")
# # #         return db
# # #     except ConnectionFailure as e:
# # #         log.error(f"[MongoDB] ❌ Cannot connect: {e}")
# # #         return None


# # # def _get_processed_keys(db) -> set:
# # #     """Return set of (company_lower, role_lower) already in MongoDB."""
# # #     if db is None:
# # #         return set()
# # #     keys = set()
# # #     try:
# # #         log.info("[Dedup] Checking existing jobs in MongoDB…")
# # #         for doc in db.jobs.find({}, {"company": 1, "role": 1, "_id": 0}):
# # #             co   = (doc.get("company") or "").strip().lower()
# # #             role = (doc.get("role")    or "").strip().lower()
# # #             if co and role:
# # #                 keys.add((co, role))
# # #         log.info(f"[Dedup] ✅ Found {len(keys)} existing jobs in MongoDB")
# # #     except Exception as e:
# # #         log.warning(f"[Dedup] ⚠️ Warning reading existing jobs: {e}")
# # #     return keys


# # # def _job_key(job: dict) -> tuple:
# # #     """Canonical dedup key for a scraped job."""
# # #     company = (
# # #         job.get("company") or job.get("organization") or
# # #         job.get("company_name") or "unknown"
# # #     ).strip().lower()
# # #     role = (
# # #         job.get("title") or job.get("role") or
# # #         job.get("job_title") or "unknown"
# # #     ).strip().lower()
# # #     return (company, role)


# # # # ══════════════════════════════════════════════════════════════════════════════
# # # #  STEP 1 — RUN THE SCRAPER
# # # # ══════════════════════════════════════════════════════════════════════════════

# # # def _run_scraper() -> bool:
# # #     """Execute scraper as subprocess. Returns True if JOB_FILE was produced."""
# # #     log.info("=" * 62)
# # #     log.info("STEP 1 — Running Job Scraper")
# # #     log.info("=" * 62)

# # #     if not Path(SCRAPER_SCRIPT).exists():
# # #         log.error(f"❌ Scraper not found: {SCRAPER_SCRIPT}")
# # #         return False

# # #     log.info("⏳ Starting scraper subprocess…")
# # #     t0 = time.time()
# # #     try:
# # #         result = subprocess.run(
# # #             [PYTHON_EXE, SCRAPER_SCRIPT],
# # #             timeout=3600,
# # #             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
# # #             capture_output=True,
# # #             text=True,
# # #         )
# # #         elapsed = round(time.time() - t0, 1)

# # #         if result.returncode != 0:
# # #             err = (result.stderr or result.stdout or "No output")[-1000:]
# # #             log.error(f"❌ Scraper exited with code {result.returncode} after {elapsed}s")
# # #             log.error(f"Error output: {err}")
# # #         else:
# # #             log.info(f"✅ Scraper finished in {elapsed}s")
# # #             if result.stdout:
# # #                 for line in result.stdout.strip().split('\n')[-5:]:
# # #                     if line.strip():
# # #                         log.info(f"  {line}")
# # #     except subprocess.TimeoutExpired:
# # #         log.error("❌ Scraper timed out (> 60 min)")
# # #         return False
# # #     except Exception as e:
# # #         log.error(f"❌ Scraper crashed: {e}")
# # #         return False

# # #     if not Path(JOB_FILE).exists():
# # #         log.error(f"❌ {JOB_FILE} not found after scraper ran")
# # #         return False

# # #     try:
# # #         raw  = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # #         jobs = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# # #         log.info(f"✅ Scraped {len(jobs)} total jobs from {JOB_FILE}")
# # #     except Exception as e:
# # #         log.warning(f"⚠️ Could not count scraped jobs: {e}")
# # #         return False

# # #     return True


# # # # ══════════════════════════════════════════════════════════════════════════════
# # # #  STEP 2 — DEDUP: KEEP ONLY NEW JOBS
# # # # ══════════════════════════════════════════════════════════════════════════════

# # # def _filter_new_jobs(db) -> list:
# # #     """Load scraped jobs, remove duplicates and already-processed ones."""
# # #     log.info("=" * 62)
# # #     log.info("STEP 2 — Deduplication & Filtering")
# # #     log.info("=" * 62)

# # #     if not Path(JOB_FILE).exists():
# # #         log.error(f"❌ {JOB_FILE} missing — run scraper first")
# # #         return []

# # #     raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # #     all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw

# # #     existing   = _get_processed_keys(db)
# # #     seen_batch = set()
# # #     new_jobs   = []
# # #     skip_db    = 0
# # #     skip_dup   = 0

# # #     log.info(f"🔍 Processing {len(all_scraped)} scraped jobs…")

# # #     for i, job in enumerate(all_scraped, 1):
# # #         key = _job_key(job)
# # #         if key in existing:
# # #             skip_db += 1
# # #             continue
# # #         if key in seen_batch:
# # #             skip_dup += 1
# # #             continue
# # #         seen_batch.add(key)
# # #         new_jobs.append(job)

# # #     log.info("")
# # #     log.info("📊 Deduplication Results:")
# # #     log.info(f"  Total scraped        : {len(all_scraped)}")
# # #     log.info(f"  Already in MongoDB   : {skip_db}  ← SKIPPED")
# # #     log.info(f"  Duplicates in file   : {skip_dup}  ← SKIPPED")
# # #     log.info(f"  🆕 NEW jobs         : {len(new_jobs)}")

# # #     if len(new_jobs) > MAX_JOBS:
# # #         log.warning(f"⚠️  Capping to first {MAX_JOBS} new jobs (limit set)")
# # #         new_jobs = new_jobs[:MAX_JOBS]

# # #     return new_jobs


# # # def _write_temp_file(new_jobs: list):
# # #     """Write new-only jobs to TEMP_JOB_FILE for the agent script."""
# # #     payload = {
# # #         "metadata": {
# # #             "description": "New unique jobs for this pipeline run",
# # #             "total_jobs":  len(new_jobs),
# # #             "created_at":  datetime.now(timezone.utc).isoformat(),
# # #         },
# # #         "jobs": new_jobs,
# # #     }
# # #     Path(TEMP_JOB_FILE).write_text(
# # #         json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
# # #     )
# # #     log.info(f"✅ Written {len(new_jobs)} new jobs → {TEMP_JOB_FILE}")


# # # # ══════════════════════════════════════════════════════════════════════════════
# # # #  STEP 3 — RUN THE AGENT PIPELINE
# # # # ══════════════════════════════════════════════════════════════════════════════

# # # def _patch_agent_script() -> tuple:
# # #     """Temporarily rewrite JOB_FILE in final.py to read from TEMP_JOB_FILE."""
# # #     content  = Path(AGENT_SCRIPT).read_text(encoding="utf-8")
# # #     original = content

# # #     patched = re.sub(
# # #         r'(JOB_FILE\s*=\s*)["\']cybersecurity_remote_jobs\.json["\']',
# # #         f'\\1"{TEMP_JOB_FILE}"',
# # #         content,
# # #     )
# # #     if patched == content:
# # #         log.warning("⚠️ Could not patch JOB_FILE in agent script — will process all scraped jobs")
# # #         return original, False

# # #     Path(AGENT_SCRIPT).write_text(patched, encoding="utf-8")
# # #     log.info(f"✅ Patched {AGENT_SCRIPT} → reads from {TEMP_JOB_FILE}")
# # #     return original, True


# # # def _restore_agent_script(original_content: str):
# # #     Path(AGENT_SCRIPT).write_text(original_content, encoding="utf-8")
# # #     log.info(f"✅ Restored {AGENT_SCRIPT} to original")


# # # def _run_agents(new_jobs: list) -> bool:
# # #     """Write new_jobs to temp file, patch agent script, run it, then restore."""
# # #     log.info("=" * 62)
# # #     log.info(f"STEP 3 — Running AI Agent Pipeline")
# # #     log.info(f"Jobs to process: {len(new_jobs)}")
# # #     log.info("=" * 62)

# # #     if not Path(AGENT_SCRIPT).exists():
# # #         log.error(f"❌ Agent script not found: {AGENT_SCRIPT}")
# # #         return False

# # #     _write_temp_file(new_jobs)

# # #     original_content, patched = _patch_agent_script()
# # #     t0      = time.time()
# # #     success = False

# # #     log.info("⏳ Starting agent pipeline subprocess…")

# # #     try:
# # #         result = subprocess.run(
# # #             [PYTHON_EXE, AGENT_SCRIPT],
# # #             timeout=7200,
# # #             capture_output=True,
# # #             text=True,
# # #             encoding="utf-8",
# # #             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
# # #         )
# # #         elapsed = round((time.time() - t0) / 60, 1)

# # #         if result.returncode != 0:
# # #             err_output = (result.stderr or result.stdout or "No output captured")[-2000:]
# # #             log.error(f"❌ Agent script FAILED with code {result.returncode} after {elapsed} min")
# # #             log.error(err_output)
# # #             success = False
# # #         else:
# # #             log.info(f"✅ Agent pipeline completed in {elapsed} min")
# # #             if result.stdout:
# # #                 for line in (result.stdout or "")[-1000:].split('\n')[-10:]:
# # #                     if line.strip():
# # #                         log.info(f"  {line}")
# # #             success = True

# # #     except subprocess.TimeoutExpired:
# # #         log.error("❌ Agent pipeline timed out (> 2 hours)")
# # #         success = False
# # #     except Exception as e:
# # #         log.error(f"❌ Agent pipeline crashed: {e}")
# # #         success = False
# # #     finally:
# # #         if patched:
# # #             _restore_agent_script(original_content)
# # #         try:
# # #             Path(TEMP_JOB_FILE).unlink(missing_ok=True)
# # #         except Exception:
# # #             pass

# # #     return success


# # # # ══════════════════════════════════════════════════════════════════════════════
# # # #  CORE PIPELINE LOGIC (single run)
# # # # ══════════════════════════════════════════════════════════════════════════════

# # # def _execute_pipeline_once(progress_callback=None) -> dict:
# # #     """
# # #     Run one full pipeline cycle: scrape → dedup → agents.
# # #     Returns result dict. Does NOT touch scheduler state.
# # #     """
# # #     global _progress_cb
# # #     _progress_cb = progress_callback

# # #     result = {
# # #         "success":    False,
# # #         "scraped":    0,
# # #         "new_jobs":   0,
# # #         "skipped_db": 0,
# # #         "processed":  0,
# # #         "started_at": datetime.now(timezone.utc).isoformat(),
# # #         "error":      None,
# # #     }

# # #     db = _get_db()
# # #     if db is None:
# # #         result["error"] = "Failed to connect to MongoDB"
# # #         log.error(f"❌ Stopping: {result['error']}")
# # #         return result

# # #     # STEP 1
# # #     ok = _run_scraper()
# # #     if not ok:
# # #         result["error"] = "Scraper failed"
# # #         log.error(f"❌ Stopping: {result['error']}")
# # #         return result

# # #     try:
# # #         raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# # #         all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# # #         result["scraped"] = len(all_scraped)
# # #     except Exception:
# # #         pass

# # #     # STEP 2
# # #     new_jobs             = _filter_new_jobs(db)
# # #     result["new_jobs"]   = len(new_jobs)
# # #     result["skipped_db"] = result["scraped"] - len(new_jobs)

# # #     if not new_jobs:
# # #         log.info("⏭️ No new jobs to process — all already exist in MongoDB")
# # #         result["success"]   = True
# # #         result["processed"] = 0
# # #         return result

# # #     # STEP 3
# # #     ok                   = _run_agents(new_jobs)
# # #     result["processed"]  = len(new_jobs) if ok else 0
# # #     result["success"]    = ok
# # #     return result


# # # # ══════════════════════════════════════════════════════════════════════════════
# # # #  SCHEDULER — background thread that repeats every 12 hours
# # # # ══════════════════════════════════════════════════════════════════════════════

# # # def _scheduler_loop(progress_callback=None):
# # #     """
# # #     Background thread body.
# # #     Runs pipeline immediately, then sleeps SCHEDULE_INTERVAL_HRS hours and repeats.
# # #     Stops when _scheduler_stop_evt is set.
# # #     """
# # #     interval_secs = SCHEDULE_INTERVAL_HRS * 3600
# # #     run_count     = _read_scheduler_state().get("run_count", 0)

# # #     while not _scheduler_stop_evt.is_set():

# # #         # ── Run now ───────────────────────────────────────────────────────────
# # #         now_iso  = datetime.now(timezone.utc).isoformat()
# # #         run_count += 1
# # #         log.info("")
# # #         log.info("╔" + "═" * 60 + "╗")
# # #         log.info(f"║  AUTO-SCHEDULER — Run #{run_count}  ({now_iso[:19]} UTC){'':>10}║")
# # #         log.info("╚" + "═" * 60 + "╝")

# # #         # Update state: mark as running
# # #         _write_scheduler_state({
# # #             "active":    True,
# # #             "last_run":  now_iso,
# # #             "next_run":  None,   # will be set after run
# # #             "run_count": run_count,
# # #             "status":    "running",
# # #         })

# # #         result = _execute_pipeline_once(progress_callback)
# # #         _print_summary(result)

# # #         # ── Schedule next run ─────────────────────────────────────────────────
# # #         if _scheduler_stop_evt.is_set():
# # #             break

# # #         next_run_iso = datetime.fromtimestamp(
# # #             datetime.now(timezone.utc).timestamp() + interval_secs,
# # #             tz=timezone.utc
# # #         ).isoformat()

# # #         _write_scheduler_state({
# # #             "active":    True,
# # #             "last_run":  now_iso,
# # #             "next_run":  next_run_iso,
# # #             "run_count": run_count,
# # #             "status":    "sleeping",
# # #             "last_result": {
# # #                 "success":   result.get("success"),
# # #                 "scraped":   result.get("scraped"),
# # #                 "new_jobs":  result.get("new_jobs"),
# # #                 "processed": result.get("processed"),
# # #             },
# # #         })

# # #         log.info(f"[Scheduler] ✅ Run #{run_count} complete.")
# # #         log.info(f"[Scheduler] 💤 Sleeping {SCHEDULE_INTERVAL_HRS}h — next run at {next_run_iso[:19]} UTC")

# # #         # ── Sleep in small chunks so stop_evt is checked frequently ──────────
# # #         slept = 0
# # #         chunk = 60   # check every 60 seconds
# # #         while slept < interval_secs and not _scheduler_stop_evt.is_set():
# # #             time.sleep(min(chunk, interval_secs - slept))
# # #             slept += chunk

# # #     # Scheduler exited
# # #     state = _read_scheduler_state()
# # #     state["active"] = False
# # #     state["status"] = "stopped"
# # #     _write_scheduler_state(state)
# # #     log.info("[Scheduler] ⏹️  Scheduler stopped.")


# # # # ══════════════════════════════════════════════════════════════════════════════
# # # #  PUBLIC ENTRY POINT — called by Streamlit "Find Jobs" button
# # # # ══════════════════════════════════════════════════════════════════════════════

# # # def run_pipeline(progress_callback=None) -> dict:
# # #     """
# # #     Called by Streamlit's Find Jobs button (and also by CLI).

# # #     Behaviour:
# # #     • If no scheduler is running → starts it (runs immediately + every 12h after).
# # #     • If scheduler is already running → runs one immediate extra cycle
# # #       and resets the 12-hour countdown.

# # #     Returns the result of the FIRST (immediate) pipeline run so Streamlit
# # #     can display success/failure straight away.
# # #     """
# # #     global _scheduler_thread, _progress_cb
# # #     _progress_cb = progress_callback

# # #     log.info("")
# # #     log.info("╔" + "═" * 60 + "╗")
# # #     log.info("║  SecureITLab Pipeline Runner                              ║")
# # #     log.info("║  Scraper → Dedup → 15-Agent Pipeline → MongoDB            ║")
# # #     log.info(f"║  Auto-repeats every {SCHEDULE_INTERVAL_HRS}h via background scheduler         ║")
# # #     log.info("╚" + "═" * 60 + "╝")
# # #     log.info(f"Run triggered: {datetime.now(timezone.utc).isoformat()}")
# # #     log.info("")

# # #     with _scheduler_lock:
# # #         scheduler_alive = _scheduler_thread is not None and _scheduler_thread.is_alive()

# # #         if scheduler_alive:
# # #             # Already scheduled — just log and let the running thread continue.
# # #             # We still do an immediate run below so the user gets fresh results NOW.
# # #             log.info("[Scheduler] ℹ️  Scheduler already running — triggering an extra immediate run.")
# # #             log.info(f"[Scheduler] ⏱️  Auto-repeat every {SCHEDULE_INTERVAL_HRS}h will continue as before.")
# # #         else:
# # #             # Start a fresh scheduler (stop any stale stop event first)
# # #             _scheduler_stop_evt.clear()
# # #             log.info(f"[Scheduler] 🚀 Starting auto-scheduler (every {SCHEDULE_INTERVAL_HRS}h)")

# # #     # ── If scheduler is NOT already alive, launch it in the background.
# # #     # The scheduler loop itself runs the first cycle immediately, so we
# # #     # do NOT do a separate _execute_pipeline_once here in that case —
# # #     # we just return a "scheduled" result and let the thread do the work.
# # #     # However, to keep the Streamlit UI informed in real-time we run the
# # #     # first cycle here (in this call) and let subsequent cycles happen
# # #     # in the background thread.

# # #     with _scheduler_lock:
# # #         scheduler_alive = _scheduler_thread is not None and _scheduler_thread.is_alive()

# # #     if not scheduler_alive:
# # #         # Run first cycle synchronously so we can return the result to Streamlit
# # #         log.info("[Scheduler] ▶ Running first cycle now (synchronous)…")
# # #         result = _execute_pipeline_once(progress_callback)
# # #         _print_summary(result)

# # #         # Schedule the NEXT run (and all subsequent) in a daemon thread
# # #         now_ts       = datetime.now(timezone.utc).timestamp()
# # #         next_run_iso = datetime.fromtimestamp(
# # #             now_ts + SCHEDULE_INTERVAL_HRS * 3600, tz=timezone.utc
# # #         ).isoformat()

# # #         _write_scheduler_state({
# # #             "active":    True,
# # #             "last_run":  datetime.now(timezone.utc).isoformat(),
# # #             "next_run":  next_run_iso,
# # #             "run_count": 1,
# # #             "status":    "sleeping",
# # #             "last_result": {
# # #                 "success":   result.get("success"),
# # #                 "scraped":   result.get("scraped"),
# # #                 "new_jobs":  result.get("new_jobs"),
# # #                 "processed": result.get("processed"),
# # #             },
# # #         })

# # #         log.info(f"[Scheduler] 💤 Next auto-run scheduled for {next_run_iso[:19]} UTC")

# # #         def _bg_loop():
# # #             """Background loop: sleep 12h then repeat indefinitely."""
# # #             interval_secs = SCHEDULE_INTERVAL_HRS * 3600
# # #             run_count     = 1

# # #             while not _scheduler_stop_evt.is_set():
# # #                 # Sleep first (already ran once above)
# # #                 slept = 0
# # #                 chunk = 60
# # #                 while slept < interval_secs and not _scheduler_stop_evt.is_set():
# # #                     time.sleep(min(chunk, interval_secs - slept))
# # #                     slept += chunk

# # #                 if _scheduler_stop_evt.is_set():
# # #                     break

# # #                 run_count += 1
# # #                 now_iso = datetime.now(timezone.utc).isoformat()
# # #                 log.info("")
# # #                 log.info("╔" + "═" * 60 + "╗")
# # #                 log.info(f"║  AUTO-SCHEDULER — Run #{run_count}  ({now_iso[:19]} UTC){'':>10}║")
# # #                 log.info("╚" + "═" * 60 + "╝")

# # #                 _write_scheduler_state({
# # #                     "active":    True,
# # #                     "last_run":  now_iso,
# # #                     "next_run":  None,
# # #                     "run_count": run_count,
# # #                     "status":    "running",
# # #                 })

# # #                 bg_result = _execute_pipeline_once(progress_callback)
# # #                 _print_summary(bg_result)

# # #                 if _scheduler_stop_evt.is_set():
# # #                     break

# # #                 next_iso = datetime.fromtimestamp(
# # #                     datetime.now(timezone.utc).timestamp() + interval_secs,
# # #                     tz=timezone.utc
# # #                 ).isoformat()

# # #                 _write_scheduler_state({
# # #                     "active":    True,
# # #                     "last_run":  now_iso,
# # #                     "next_run":  next_iso,
# # #                     "run_count": run_count,
# # #                     "status":    "sleeping",
# # #                     "last_result": {
# # #                         "success":   bg_result.get("success"),
# # #                         "scraped":   bg_result.get("scraped"),
# # #                         "new_jobs":  bg_result.get("new_jobs"),
# # #                         "processed": bg_result.get("processed"),
# # #                     },
# # #                 })
# # #                 log.info(f"[Scheduler] 💤 Next auto-run at {next_iso[:19]} UTC")

# # #             # Mark stopped
# # #             st = _read_scheduler_state()
# # #             st["active"] = False
# # #             st["status"] = "stopped"
# # #             _write_scheduler_state(st)
# # #             log.info("[Scheduler] ⏹️  Background scheduler stopped.")

# # #         with _scheduler_lock:
# # #             _scheduler_thread = threading.Thread(target=_bg_loop, daemon=True, name="scheduler-bg")
# # #             _scheduler_thread.start()

# # #         log.info(f"[Scheduler] ✅ Background scheduler thread started (daemon)")
# # #         return result

# # #     else:
# # #         # Scheduler already alive — run one extra immediate cycle and return
# # #         result = _execute_pipeline_once(progress_callback)
# # #         _print_summary(result)
# # #         return result


# # # def _print_summary(r: dict):
# # #     log.info("=" * 62)
# # #     log.info("  PIPELINE SUMMARY")
# # #     log.info("=" * 62)
# # #     log.info(f"  📦 Jobs scraped         : {r.get('scraped', 0)}")
# # #     log.info(f"  ⏭️ Already in MongoDB   : {r.get('skipped_db', 0)}  ← SKIPPED")
# # #     log.info(f"  🆕 New jobs found      : {r.get('new_jobs', 0)}")
# # #     log.info(f"  ✅ Processed by AI      : {r.get('processed', 0)}")
# # #     log.info(f"  Status                  : {'✅ SUCCESS' if r.get('success') else '❌ FAILED'}")
# # #     if r.get("error"):
# # #         log.info(f"  Error                   : {r['error']}")
# # #     log.info("=" * 62)
# # #     log.info("")


# # # # ══════════════════════════════════════════════════════════════════════════════
# # # #  CLI ENTRY POINT
# # # # ══════════════════════════════════════════════════════════════════════════════

# # # if __name__ == "__main__":
# # #     result = run_pipeline()
# # #     sys.exit(0 if result["success"] else 1)








































# # """
# # ╔══════════════════════════════════════════════════════════════════╗
# # ║   main.py — SecureITLab Unified Runner (CRON VERSION)           ║
# # ║                                                                  ║
# # ║   FLOW:                                                          ║
# # ║   STEP 1 → Run cyber_job_scraper_v5.py                          ║
# # ║             → produces cybersecurity_remote_jobs.json           ║
# # ║   STEP 2 → Compare scraped jobs vs MongoDB (dedup check)        ║
# # ║             → skip jobs already stored                          ║
# # ║             → write only NEW jobs to new_jobs_temp.json         ║
# # ║   STEP 3 → Run final.py on new jobs only                        ║
# # ║             → 15 agents per job + contacts + MongoDB store      ║
# # ║                                                                  ║
# # ║   AUTO-SCHEDULER:                                                ║
# # ║   • CRON runs this script every 12 hours (not Python threading) ║
# # ║   • Each run updates scheduler_state.json with status           ║
# # ║   • Streamlit reads scheduler_state.json to show status         ║
# # ║   • Manual "Find Jobs" button triggers manual immediate run     ║
# # ║                                                                  ║
# # ║   CRON SETUP (Linux/Mac):                                        ║
# # ║   0 */12 * * * cd /path/to/project && python main.py            ║
# # ║                                                                  ║
# # ║   TASK SCHEDULER (Windows):                                      ║
# # ║   Every 12 hours → python main.py                               ║
# # ║                                                                  ║
# # ║   Called by: CRON/Task Scheduler automatically                  ║
# # ║   OR Streamlit: streamlit_dashboard.py "🔍 Find Jobs" button    ║
# # ║   OR direct: py -3.12 main.py                                   ║
# # ╚══════════════════════════════════════════════════════════════════╝
# # """

# # import os
# # import sys
# # import json
# # import re
# # import time
# # import logging
# # import subprocess
# # from datetime import datetime, timezone, timedelta
# # from pathlib import Path

# # from dotenv import load_dotenv
# # from pymongo import MongoClient
# # from pymongo.errors import ConnectionFailure

# # load_dotenv()

# # # ── Global callback for Streamlit ─────────────────────────────────────────────
# # _progress_cb = None

# # SCHEDULER_STATE_FILE = "scheduler_state.json"
# # SCHEDULE_INTERVAL_HRS = 12


# # # ── Custom logging handler that sends to callback ─────────────────────────────
# # class StreamlitLogHandler(logging.Handler):
# #     """Custom handler that sends logs to Streamlit callback"""
# #     def emit(self, record):
# #         try:
# #             msg = self.format(record)
# #             if _progress_cb:
# #                 _progress_cb(msg)
# #         except Exception:
# #             pass


# # # ── Setup logging ─────────────────────────────────────────────────────────────
# # logging.basicConfig(
# #     level=logging.INFO,
# #     format="%(asctime)s | %(levelname)s | %(message)s",
# # )

# # log = logging.getLogger("main")
# # log.setLevel(logging.INFO)

# # # Avoid adding duplicate handlers on repeated imports
# # if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, StreamlitLogHandler)
# #            for h in log.handlers):
# #     console_handler = logging.StreamHandler(sys.stdout)
# #     console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# #     log.addHandler(console_handler)

# # if not any(isinstance(h, logging.FileHandler) for h in log.handlers):
# #     file_handler = logging.FileHandler("main_run.log", encoding="utf-8")
# #     file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# #     log.addHandler(file_handler)

# # _streamlit_log_handler = StreamlitLogHandler()
# # _streamlit_log_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# # if not any(isinstance(h, StreamlitLogHandler) for h in log.handlers):
# #     log.addHandler(_streamlit_log_handler)


# # # ── Config ────────────────────────────────────────────────────────────────────
# # MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# # MONGO_DB       = os.getenv("MONGO_DB",  "secureitlab_job_pipeline")

# # SCRAPER_SCRIPT = "cyber_job_scraper_v5.py"
# # AGENT_SCRIPT   = "final.py"
# # PYTHON_EXE     = r"C:\Users\DELL\AppData\Local\Programs\Python\Python312\python.exe"
# # JOB_FILE       = "cybersecurity_remote_jobs.json"
# # TEMP_JOB_FILE  = "new_jobs_temp.json"
# # MAX_JOBS       = 20


# # # ══════════════════════════════════════════════════════════════════════════════
# # #  SCHEDULER STATE HELPERS
# # # ══════════════════════════════════════════════════════════════════════════════

# # def _read_scheduler_state() -> dict:
# #     """Read persisted scheduler state from disk."""
# #     try:
# #         if Path(SCHEDULER_STATE_FILE).exists():
# #             return json.loads(Path(SCHEDULER_STATE_FILE).read_text(encoding="utf-8"))
# #     except Exception:
# #         pass
# #     return {
# #         "active": False,
# #         "last_run": None,
# #         "next_run": None,
# #         "run_count": 0,
# #         "status": "idle"
# #     }


# # def _write_scheduler_state(state: dict):
# #     """Persist scheduler state to disk so Streamlit can read it."""
# #     try:
# #         Path(SCHEDULER_STATE_FILE).write_text(
# #             json.dumps(state, indent=2, default=str), encoding="utf-8"
# #         )
# #     except Exception as e:
# #         log.warning(f"[State] Could not write state file: {e}")


# # def get_scheduler_status() -> dict:
# #     """
# #     Public helper — called by Streamlit to display scheduler status in the UI.
# #     Returns dict with: active, last_run, next_run, run_count, seconds_until_next
    
# #     ⚠️ IMPORTANT: With CRON, we CALCULATE next_run based on last_run + 12h
# #     """
# #     state = _read_scheduler_state()
# #     now   = datetime.now(timezone.utc).timestamp()

# #     seconds_until_next = None
# #     next_run_iso = None

# #     if state.get("last_run"):
# #         try:
# #             last_ts = datetime.fromisoformat(state["last_run"]).timestamp()
# #             # Next run = last run + 12 hours
# #             next_ts = last_ts + (SCHEDULE_INTERVAL_HRS * 3600)
# #             next_run_iso = datetime.fromtimestamp(next_ts, tz=timezone.utc).isoformat()
# #             seconds_until_next = max(0, int(next_ts - now))
# #         except Exception:
# #             pass

# #     return {
# #         "active":             True,  # Always True with CRON (cron handles it)
# #         "last_run":           state.get("last_run"),
# #         "next_run":           next_run_iso,  # CALCULATED, not stored
# #         "run_count":          state.get("run_count", 0),
# #         "seconds_until_next": seconds_until_next,
# #         "status":             state.get("status", "running"),
# #     }


# # # ══════════════════════════════════════════════════════════════════════════════
# # #  MONGODB HELPERS
# # # ══════════════════════════════════════════════════════════════════════════════

# # def _get_db():
# #     """Connect to MongoDB. Returns db object or None on failure."""
# #     try:
# #         log.info("[MongoDB] Connecting…")
# #         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
# #         client.admin.command("ping")
# #         db = client[MONGO_DB]
# #         log.info(f"[MongoDB] ✅ Connected to {MONGO_DB}")
# #         return db
# #     except ConnectionFailure as e:
# #         log.error(f"[MongoDB] ❌ Cannot connect: {e}")
# #         return None


# # def _get_processed_keys(db) -> set:
# #     """Return set of (company_lower, role_lower) already in MongoDB."""
# #     if db is None:
# #         return set()
# #     keys = set()
# #     try:
# #         log.info("[Dedup] Checking existing jobs in MongoDB…")
# #         for doc in db.jobs.find({}, {"company": 1, "role": 1, "_id": 0}):
# #             co   = (doc.get("company") or "").strip().lower()
# #             role = (doc.get("role")    or "").strip().lower()
# #             if co and role:
# #                 keys.add((co, role))
# #         log.info(f"[Dedup] ✅ Found {len(keys)} existing jobs in MongoDB")
# #     except Exception as e:
# #         log.warning(f"[Dedup] ⚠️ Warning reading existing jobs: {e}")
# #     return keys


# # def _job_key(job: dict) -> tuple:
# #     """Canonical dedup key for a scraped job."""
# #     company = (
# #         job.get("company") or job.get("organization") or
# #         job.get("company_name") or "unknown"
# #     ).strip().lower()
# #     role = (
# #         job.get("title") or job.get("role") or
# #         job.get("job_title") or "unknown"
# #     ).strip().lower()
# #     return (company, role)


# # # ══════════════════════════════════════════════════════════════════════════════
# # #  STEP 1 — RUN THE SCRAPER
# # # ══════════════════════════════════════════════════════════════════════════════

# # def _run_scraper() -> bool:
# #     """Execute scraper as subprocess. Returns True if JOB_FILE was produced."""
# #     log.info("=" * 62)
# #     log.info("STEP 1 — Running Job Scraper")
# #     log.info("=" * 62)

# #     if not Path(SCRAPER_SCRIPT).exists():
# #         log.error(f"❌ Scraper not found: {SCRAPER_SCRIPT}")
# #         return False

# #     log.info("⏳ Starting scraper subprocess…")
# #     t0 = time.time()
# #     try:
# #         result = subprocess.run(
# #             [PYTHON_EXE, SCRAPER_SCRIPT],
# #             timeout=3600,
# #             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
# #             capture_output=True,
# #             text=True,
# #         )
# #         elapsed = round(time.time() - t0, 1)

# #         if result.returncode != 0:
# #             err = (result.stderr or result.stdout or "No output")[-1000:]
# #             log.error(f"❌ Scraper exited with code {result.returncode} after {elapsed}s")
# #             log.error(f"Error output: {err}")
# #         else:
# #             log.info(f"✅ Scraper finished in {elapsed}s")
# #             if result.stdout:
# #                 for line in result.stdout.strip().split('\n')[-5:]:
# #                     if line.strip():
# #                         log.info(f"  {line}")
# #     except subprocess.TimeoutExpired:
# #         log.error("❌ Scraper timed out (> 60 min)")
# #         return False
# #     except Exception as e:
# #         log.error(f"❌ Scraper crashed: {e}")
# #         return False

# #     if not Path(JOB_FILE).exists():
# #         log.error(f"❌ {JOB_FILE} not found after scraper ran")
# #         return False

# #     try:
# #         raw  = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# #         jobs = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# #         log.info(f"✅ Scraped {len(jobs)} total jobs from {JOB_FILE}")
# #     except Exception as e:
# #         log.warning(f"⚠️ Could not count scraped jobs: {e}")
# #         return False

# #     return True


# # # ══════════════════════════════════════════════════════════════════════════════
# # #  STEP 2 — DEDUP: KEEP ONLY NEW JOBS
# # # ══════════════════════════════════════════════════════════════════════════════

# # def _filter_new_jobs(db) -> list:
# #     """Load scraped jobs, remove duplicates and already-processed ones."""
# #     log.info("=" * 62)
# #     log.info("STEP 2 — Deduplication & Filtering")
# #     log.info("=" * 62)

# #     if not Path(JOB_FILE).exists():
# #         log.error(f"❌ {JOB_FILE} missing — run scraper first")
# #         return []

# #     raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# #     all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw

# #     existing   = _get_processed_keys(db)
# #     seen_batch = set()
# #     new_jobs   = []
# #     skip_db    = 0
# #     skip_dup   = 0

# #     log.info(f"🔍 Processing {len(all_scraped)} scraped jobs…")

# #     for i, job in enumerate(all_scraped, 1):
# #         key = _job_key(job)
# #         if key in existing:
# #             skip_db += 1
# #             continue
# #         if key in seen_batch:
# #             skip_dup += 1
# #             continue
# #         seen_batch.add(key)
# #         new_jobs.append(job)

# #     log.info("")
# #     log.info("📊 Deduplication Results:")
# #     log.info(f"  Total scraped        : {len(all_scraped)}")
# #     log.info(f"  Already in MongoDB   : {skip_db}  ← SKIPPED")
# #     log.info(f"  Duplicates in file   : {skip_dup}  ← SKIPPED")
# #     log.info(f"  🆕 NEW jobs         : {len(new_jobs)}")

# #     if len(new_jobs) > MAX_JOBS:
# #         log.warning(f"⚠️  Capping to first {MAX_JOBS} new jobs (limit set)")
# #         new_jobs = new_jobs[:MAX_JOBS]

# #     return new_jobs


# # def _write_temp_file(new_jobs: list):
# #     """Write new-only jobs to TEMP_JOB_FILE for the agent script."""
# #     payload = {
# #         "metadata": {
# #             "description": "New unique jobs for this pipeline run",
# #             "total_jobs":  len(new_jobs),
# #             "created_at":  datetime.now(timezone.utc).isoformat(),
# #         },
# #         "jobs": new_jobs,
# #     }
# #     Path(TEMP_JOB_FILE).write_text(
# #         json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
# #     )
# #     log.info(f"✅ Written {len(new_jobs)} new jobs → {TEMP_JOB_FILE}")


# # # ══════════════════════════════════════════════════════════════════════════════
# # #  STEP 3 — RUN THE AGENT PIPELINE
# # # ══════════════════════════════════════════════════════════════════════════════

# # def _patch_agent_script() -> tuple:
# #     """Temporarily rewrite JOB_FILE in final.py to read from TEMP_JOB_FILE."""
# #     content  = Path(AGENT_SCRIPT).read_text(encoding="utf-8")
# #     original = content

# #     patched = re.sub(
# #         r'(JOB_FILE\s*=\s*)["\']cybersecurity_remote_jobs\.json["\']',
# #         f'\\1"{TEMP_JOB_FILE}"',
# #         content,
# #     )
# #     if patched == content:
# #         log.warning("⚠️ Could not patch JOB_FILE in agent script — will process all scraped jobs")
# #         return original, False

# #     Path(AGENT_SCRIPT).write_text(patched, encoding="utf-8")
# #     log.info(f"✅ Patched {AGENT_SCRIPT} → reads from {TEMP_JOB_FILE}")
# #     return original, True


# # def _restore_agent_script(original_content: str):
# #     Path(AGENT_SCRIPT).write_text(original_content, encoding="utf-8")
# #     log.info(f"✅ Restored {AGENT_SCRIPT} to original")


# # def _run_agents(new_jobs: list) -> bool:
# #     """Write new_jobs to temp file, patch agent script, run it, then restore."""
# #     log.info("=" * 62)
# #     log.info(f"STEP 3 — Running AI Agent Pipeline")
# #     log.info(f"Jobs to process: {len(new_jobs)}")
# #     log.info("=" * 62)

# #     if not Path(AGENT_SCRIPT).exists():
# #         log.error(f"❌ Agent script not found: {AGENT_SCRIPT}")
# #         return False

# #     _write_temp_file(new_jobs)

# #     original_content, patched = _patch_agent_script()
# #     t0      = time.time()
# #     success = False

# #     log.info("⏳ Starting agent pipeline subprocess…")

# #     try:
# #         result = subprocess.run(
# #             [PYTHON_EXE, AGENT_SCRIPT],
# #             timeout=7200,
# #             capture_output=True,
# #             text=True,
# #             encoding="utf-8",
# #             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
# #         )
# #         elapsed = round((time.time() - t0) / 60, 1)

# #         if result.returncode != 0:
# #             err_output = (result.stderr or result.stdout or "No output captured")[-2000:]
# #             log.error(f"❌ Agent script FAILED with code {result.returncode} after {elapsed} min")
# #             log.error(err_output)
# #             success = False
# #         else:
# #             log.info(f"✅ Agent pipeline completed in {elapsed} min")
# #             if result.stdout:
# #                 for line in (result.stdout or "")[-1000:].split('\n')[-10:]:
# #                     if line.strip():
# #                         log.info(f"  {line}")
# #             success = True

# #     except subprocess.TimeoutExpired:
# #         log.error("❌ Agent pipeline timed out (> 2 hours)")
# #         success = False
# #     except Exception as e:
# #         log.error(f"❌ Agent pipeline crashed: {e}")
# #         success = False
# #     finally:
# #         if patched:
# #             _restore_agent_script(original_content)
# #         try:
# #             Path(TEMP_JOB_FILE).unlink(missing_ok=True)
# #         except Exception:
# #             pass

# #     return success


# # # ══════════════════════════════════════════════════════════════════════════════
# # #  CORE PIPELINE LOGIC (single run)
# # # ══════════════════════════════════════════════════════════════════════════════

# # def _execute_pipeline_once(progress_callback=None) -> dict:
# #     """
# #     Run one full pipeline cycle: scrape → dedup → agents.
# #     Returns result dict.
    
# #     This is called by:
# #     1. CRON/Task Scheduler automatically every 12h
# #     2. Streamlit "Find Jobs" button for immediate manual run
# #     """
# #     global _progress_cb
# #     _progress_cb = progress_callback

# #     result = {
# #         "success":    False,
# #         "scraped":    0,
# #         "new_jobs":   0,
# #         "skipped_db": 0,
# #         "processed":  0,
# #         "started_at": datetime.now(timezone.utc).isoformat(),
# #         "error":      None,
# #     }

# #     db = _get_db()
# #     if db is None:
# #         result["error"] = "Failed to connect to MongoDB"
# #         log.error(f"❌ Stopping: {result['error']}")
# #         return result

# #     # STEP 1
# #     ok = _run_scraper()
# #     if not ok:
# #         result["error"] = "Scraper failed"
# #         log.error(f"❌ Stopping: {result['error']}")
# #         return result

# #     try:
# #         raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
# #         all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw
# #         result["scraped"] = len(all_scraped)
# #     except Exception:
# #         pass

# #     # STEP 2
# #     new_jobs             = _filter_new_jobs(db)
# #     result["new_jobs"]   = len(new_jobs)
# #     result["skipped_db"] = result["scraped"] - len(new_jobs)

# #     if not new_jobs:
# #         log.info("⏭️ No new jobs to process — all already exist in MongoDB")
# #         result["success"]   = True
# #         result["processed"] = 0
# #         return result

# #     # STEP 3
# #     ok                   = _run_agents(new_jobs)
# #     result["processed"]  = len(new_jobs) if ok else 0
# #     result["success"]    = ok
# #     return result


# # # ══════════════════════════════════════════════════════════════════════════════
# # #  PUBLIC ENTRY POINT
# # # ══════════════════════════════════════════════════════════════════════════════

# # def run_pipeline(progress_callback=None) -> dict:
# #     """
# #     SIMPLIFIED FOR CRON:
    
# #     Called by:
# #     1. CRON/Task Scheduler (automatic every 12h) → just run once
# #     2. Streamlit button (manual trigger) → just run once
    
# #     ✅ NO MORE THREADING!
# #     ✅ NO MORE BACKGROUND LOOPS!
# #     ✅ Just run the pipeline once and exit
# #     """
# #     global _progress_cb
# #     _progress_cb = progress_callback

# #     log.info("")
# #     log.info("╔" + "═" * 60 + "╗")
# #     log.info("║  SecureITLab Pipeline Runner (CRON VERSION)           ║")
# #     log.info("║  Scraper → Dedup → 15-Agent Pipeline → MongoDB        ║")
# #     log.info("║  Scheduled by: OS CRON / Task Scheduler                ║")
# #     log.info("╚" + "═" * 60 + "╝")
    
# #     now_iso = datetime.now(timezone.utc).isoformat()
# #     log.info(f"Run triggered: {now_iso}")
# #     log.info("")

# #     # Run the pipeline once
# #     result = _execute_pipeline_once(progress_callback)
# #     _print_summary(result)

# #     # Update scheduler state for Streamlit to read
# #     state = _read_scheduler_state()
# #     new_run_count = state.get("run_count", 0) + 1
# #     next_run_iso = (
# #         datetime.now(timezone.utc) + 
# #         timedelta(hours=SCHEDULE_INTERVAL_HRS)
# #     ).isoformat()

# #     _write_scheduler_state({
# #         "active":     True,  # CRON keeps it active
# #         "last_run":   now_iso,
# #         "next_run":   next_run_iso,
# #         "run_count":  new_run_count,
# #         "status":     "idle",  # Waiting for next CRON run
# #         "last_result": {
# #             "success":   result.get("success"),
# #             "scraped":   result.get("scraped"),
# #             "new_jobs":  result.get("new_jobs"),
# #             "processed": result.get("processed"),
# #         },
# #     })

# #     log.info(f"[State] ✅ Saved scheduler state to {SCHEDULER_STATE_FILE}")
# #     log.info(f"[CRON]  💤 Next run scheduled for: {next_run_iso[:19]} UTC")
# #     log.info("")

# #     return result


# # def _print_summary(r: dict):
# #     log.info("=" * 62)
# #     log.info("  PIPELINE SUMMARY")
# #     log.info("=" * 62)
# #     log.info(f"  📦 Jobs scraped         : {r.get('scraped', 0)}")
# #     log.info(f"  ⏭️ Already in MongoDB   : {r.get('skipped_db', 0)}  ← SKIPPED")
# #     log.info(f"  🆕 New jobs found      : {r.get('new_jobs', 0)}")
# #     log.info(f"  ✅ Processed by AI      : {r.get('processed', 0)}")
# #     log.info(f"  Status                  : {'✅ SUCCESS' if r.get('success') else '❌ FAILED'}")
# #     if r.get("error"):
# #         log.info(f"  Error                   : {r['error']}")
# #     log.info("=" * 62)
# #     log.info("")


# # # ══════════════════════════════════════════════════════════════════════════════
# # #  CLI ENTRY POINT (called by CRON or manual execution)
# # # ══════════════════════════════════════════════════════════════════════════════

# # if __name__ == "__main__":
# #     result = run_pipeline()
# #     sys.exit(0 if result["success"] else 1)





























# """
# ╔══════════════════════════════════════════════════════════════════╗
# ║   main.py — SecureITLab Unified Runner (CRON VERSION)           ║
# ║                                                                  ║
# ║   FLOW:                                                          ║
# ║   STEP 1 → Run cyber_job_scraper_v5.py                          ║
# ║             → produces cybersecurity_remote_jobs.json           ║
# ║   STEP 2 → Compare scraped jobs vs MongoDB (dedup check)        ║
# ║             → skip jobs already stored                          ║
# ║             → write only NEW jobs to new_jobs_temp.json         ║
# ║   STEP 3 → Run final.py on new jobs only                        ║
# ║             → 15 agents per job + contacts + MongoDB store      ║
# ║                                                                  ║
# ║   AUTO-SCHEDULER:                                                ║
# ║   • CRON runs this script every 12 hours (not Python threading) ║
# ║   • Each run updates scheduler_state.json with status           ║
# ║   • Streamlit reads scheduler_state.json to show status         ║
# ║   • Manual "Find Jobs" button triggers manual immediate run     ║
# ║                                                                  ║
# ║   CRON SETUP (Linux/Mac):                                        ║
# ║   0 */12 * * * cd /path/to/project && python main.py            ║
# ║                                                                  ║
# ║   TASK SCHEDULER (Windows):                                      ║
# ║   Every 12 hours → python main.py                               ║
# ║                                                                  ║
# ║   Called by: CRON/Task Scheduler automatically                  ║
# ║   OR Streamlit: streamlit_dashboard.py "🔍 Find Jobs" button    ║
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
# from datetime import datetime, timezone, timedelta
# from pathlib import Path

# from dotenv import load_dotenv
# from pymongo import MongoClient
# from pymongo.errors import ConnectionFailure

# load_dotenv()

# # ── Global callback for Streamlit ─────────────────────────────────────────────
# _progress_cb = None

# SCHEDULER_STATE_FILE = "scheduler_state.json"
# SCHEDULE_INTERVAL_HRS = 12


# # ── Custom logging handler that sends to callback ─────────────────────────────
# class StreamlitLogHandler(logging.Handler):
#     """Custom handler that sends logs to Streamlit callback"""
#     def emit(self, record):
#         try:
#             msg = self.format(record)
#             if _progress_cb:
#                 _progress_cb(msg)
#         except Exception:
#             pass


# # ── Setup logging ─────────────────────────────────────────────────────────────
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s",
# )

# log = logging.getLogger("main")
# log.setLevel(logging.INFO)

# # Avoid adding duplicate handlers on repeated imports
# if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, StreamlitLogHandler)
#            for h in log.handlers):
#     console_handler = logging.StreamHandler(sys.stdout)
#     console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
#     log.addHandler(console_handler)

# if not any(isinstance(h, logging.FileHandler) for h in log.handlers):
#     file_handler = logging.FileHandler("main_run.log", encoding="utf-8")
#     file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
#     log.addHandler(file_handler)

# _streamlit_log_handler = StreamlitLogHandler()
# _streamlit_log_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
# if not any(isinstance(h, StreamlitLogHandler) for h in log.handlers):
#     log.addHandler(_streamlit_log_handler)


# # ── Config ────────────────────────────────────────────────────────────────────
# MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
# MONGO_DB       = os.getenv("MONGO_DB",  "secureitlab_job_pipeline")

# SCRAPER_SCRIPT = "cyber_job_scraper_v5.py"
# AGENT_SCRIPT   = "final.py"
# PYTHON_EXE     = r"C:\Users\DELL\AppData\Local\Programs\Python\Python312\python.exe"
# JOB_FILE       = "cybersecurity_remote_jobs.json"
# TEMP_JOB_FILE  = "new_jobs_temp.json"
# MAX_JOBS       = 20


# # ══════════════════════════════════════════════════════════════════════════════
# #  SCHEDULER STATE HELPERS
# # ══════════════════════════════════════════════════════════════════════════════

# def _read_scheduler_state() -> dict:
#     """Read persisted scheduler state from disk."""
#     try:
#         if Path(SCHEDULER_STATE_FILE).exists():
#             return json.loads(Path(SCHEDULER_STATE_FILE).read_text(encoding="utf-8"))
#     except Exception:
#         pass
#     return {
#         "active": False,
#         "last_run": None,
#         "next_run": None,
#         "run_count": 0,
#         "status": "idle"
#     }


# def _write_scheduler_state(state: dict):
#     """Persist scheduler state to disk so Streamlit can read it."""
#     try:
#         Path(SCHEDULER_STATE_FILE).write_text(
#             json.dumps(state, indent=2, default=str), encoding="utf-8"
#         )
#     except Exception as e:
#         log.warning(f"[State] Could not write state file: {e}")


# def get_scheduler_status() -> dict:
#     """
#     Public helper — called by Streamlit to display scheduler status in the UI.
#     Returns dict with: active, last_run, next_run, run_count, seconds_until_next
    
#     ⚠️ IMPORTANT: With CRON, we CALCULATE next_run based on last_run + 12h
#     """
#     state = _read_scheduler_state()
#     now   = datetime.now(timezone.utc).timestamp()

#     seconds_until_next = None
#     next_run_iso = None

#     if state.get("last_run"):
#         try:
#             last_ts = datetime.fromisoformat(state["last_run"]).timestamp()
#             # Next run = last run + 12 hours
#             next_ts = last_ts + (SCHEDULE_INTERVAL_HRS * 3600)
#             next_run_iso = datetime.fromtimestamp(next_ts, tz=timezone.utc).isoformat()
#             seconds_until_next = max(0, int(next_ts - now))
#         except Exception:
#             pass

#     return {
#         "active":             True,  # Always True with CRON (cron handles it)
#         "last_run":           state.get("last_run"),
#         "next_run":           next_run_iso,  # CALCULATED, not stored
#         "run_count":          state.get("run_count", 0),
#         "seconds_until_next": seconds_until_next,
#         "status":             state.get("status", "running"),
#     }


# # ══════════════════════════════════════════════════════════════════════════════
# #  MONGODB HELPERS
# # ══════════════════════════════════════════════════════════════════════════════

# def _get_db():
#     """Connect to MongoDB. Returns db object or None on failure."""
#     try:
#         log.info("[MongoDB] Connecting…")
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
#         client.admin.command("ping")
#         db = client[MONGO_DB]
#         log.info(f"[MongoDB] ✅ Connected to {MONGO_DB}")
#         return db
#     except ConnectionFailure as e:
#         log.error(f"[MongoDB] ❌ Cannot connect: {e}")
#         return None


# def _get_processed_keys(db) -> set:
#     """Return set of (company_lower, role_lower) already in MongoDB."""
#     if db is None:
#         return set()
#     keys = set()
#     try:
#         log.info("[Dedup] Checking existing jobs in MongoDB…")
#         for doc in db.jobs.find({}, {"company": 1, "role": 1, "_id": 0}):
#             co   = (doc.get("company") or "").strip().lower()
#             role = (doc.get("role")    or "").strip().lower()
#             if co and role:
#                 keys.add((co, role))
#         log.info(f"[Dedup] ✅ Found {len(keys)} existing jobs in MongoDB")
#     except Exception as e:
#         log.warning(f"[Dedup] ⚠️ Warning reading existing jobs: {e}")
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
#     """Execute scraper as subprocess. Returns True if JOB_FILE was produced."""
#     log.info("=" * 62)
#     log.info("STEP 1 — Running Job Scraper")
#     log.info("=" * 62)

#     if not Path(SCRAPER_SCRIPT).exists():
#         log.error(f"❌ Scraper not found: {SCRAPER_SCRIPT}")
#         return False

#     log.info("⏳ Starting scraper subprocess…")
#     t0 = time.time()
#     try:
#         result = subprocess.run(
#             [PYTHON_EXE, SCRAPER_SCRIPT],
#             timeout=3600,
#             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
#             capture_output=True,
#             text=True,
#         )
#         elapsed = round(time.time() - t0, 1)

#         if result.returncode != 0:
#             err = (result.stderr or result.stdout or "No output")[-1000:]
#             log.error(f"❌ Scraper exited with code {result.returncode} after {elapsed}s")
#             log.error(f"Error output: {err}")
#         else:
#             log.info(f"✅ Scraper finished in {elapsed}s")
#             if result.stdout:
#                 for line in result.stdout.strip().split('\n')[-5:]:
#                     if line.strip():
#                         log.info(f"  {line}")
#     except subprocess.TimeoutExpired:
#         log.error("❌ Scraper timed out (> 60 min)")
#         return False
#     except Exception as e:
#         log.error(f"❌ Scraper crashed: {e}")
#         return False

#     if not Path(JOB_FILE).exists():
#         log.error(f"❌ {JOB_FILE} not found after scraper ran")
#         return False

#     try:
#         raw  = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
#         jobs = raw.get("jobs", raw) if isinstance(raw, dict) else raw
#         log.info(f"✅ Scraped {len(jobs)} total jobs from {JOB_FILE}")
#     except Exception as e:
#         log.warning(f"⚠️ Could not count scraped jobs: {e}")
#         return False

#     return True


# # ══════════════════════════════════════════════════════════════════════════════
# #  STEP 2 — DEDUP: KEEP ONLY NEW JOBS
# # ══════════════════════════════════════════════════════════════════════════════

# def _filter_new_jobs(db) -> list:
#     """Load scraped jobs, remove duplicates and already-processed ones."""
#     log.info("=" * 62)
#     log.info("STEP 2 — Deduplication & Filtering")
#     log.info("=" * 62)

#     if not Path(JOB_FILE).exists():
#         log.error(f"❌ {JOB_FILE} missing — run scraper first")
#         return []

#     raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
#     all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw

#     existing   = _get_processed_keys(db)
#     seen_batch = set()
#     new_jobs   = []
#     skip_db    = 0
#     skip_dup   = 0

#     log.info(f"🔍 Processing {len(all_scraped)} scraped jobs…")

#     for i, job in enumerate(all_scraped, 1):
#         key = _job_key(job)
#         if key in existing:
#             skip_db += 1
#             continue
#         if key in seen_batch:
#             skip_dup += 1
#             continue
#         seen_batch.add(key)
#         new_jobs.append(job)

#     log.info("")
#     log.info("📊 Deduplication Results:")
#     log.info(f"  Total scraped        : {len(all_scraped)}")
#     log.info(f"  Already in MongoDB   : {skip_db}  ← SKIPPED")
#     log.info(f"  Duplicates in file   : {skip_dup}  ← SKIPPED")
#     log.info(f"  🆕 NEW jobs         : {len(new_jobs)}")

#     if len(new_jobs) > MAX_JOBS:
#         log.warning(f"⚠️  Capping to first {MAX_JOBS} new jobs (limit set)")
#         new_jobs = new_jobs[:MAX_JOBS]

#     return new_jobs


# def _write_temp_file(new_jobs: list):
#     """Write new-only jobs to TEMP_JOB_FILE for the agent script."""
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
#     log.info(f"✅ Written {len(new_jobs)} new jobs → {TEMP_JOB_FILE}")


# # ══════════════════════════════════════════════════════════════════════════════
# #  STEP 3 — RUN THE AGENT PIPELINE
# # ══════════════════════════════════════════════════════════════════════════════

# def _patch_agent_script() -> tuple:
#     """Temporarily rewrite JOB_FILE in final.py to read from TEMP_JOB_FILE."""
#     content  = Path(AGENT_SCRIPT).read_text(encoding="utf-8")
#     original = content

#     patched = re.sub(
#         r'(JOB_FILE\s*=\s*)["\']cybersecurity_remote_jobs\.json["\']',
#         f'\\1"{TEMP_JOB_FILE}"',
#         content,
#     )
#     if patched == content:
#         log.warning("⚠️ Could not patch JOB_FILE in agent script — will process all scraped jobs")
#         return original, False

#     Path(AGENT_SCRIPT).write_text(patched, encoding="utf-8")
#     log.info(f"✅ Patched {AGENT_SCRIPT} → reads from {TEMP_JOB_FILE}")
#     return original, True


# def _restore_agent_script(original_content: str):
#     Path(AGENT_SCRIPT).write_text(original_content, encoding="utf-8")
#     log.info(f"✅ Restored {AGENT_SCRIPT} to original")


# def _run_agents(new_jobs: list) -> bool:
#     """Write new_jobs to temp file, patch agent script, run it, then restore."""
#     log.info("=" * 62)
#     log.info(f"STEP 3 — Running AI Agent Pipeline")
#     log.info(f"Jobs to process: {len(new_jobs)}")
#     log.info("=" * 62)

#     if not Path(AGENT_SCRIPT).exists():
#         log.error(f"❌ Agent script not found: {AGENT_SCRIPT}")
#         return False

#     _write_temp_file(new_jobs)

#     original_content, patched = _patch_agent_script()
#     t0      = time.time()
#     success = False

#     log.info("⏳ Starting agent pipeline subprocess…")

#     try:
#         result = subprocess.run(
#             [PYTHON_EXE, AGENT_SCRIPT],
#             timeout=7200,
#             capture_output=True,
#             text=True,
#             encoding="utf-8",
#             env={**os.environ, "PYTHONIOENCODING": "utf-8"},
#         )
#         elapsed = round((time.time() - t0) / 60, 1)

#         if result.returncode != 0:
#             err_output = (result.stderr or result.stdout or "No output captured")[-2000:]
#             log.error(f"❌ Agent script FAILED with code {result.returncode} after {elapsed} min")
#             log.error(err_output)
#             success = False
#         else:
#             log.info(f"✅ Agent pipeline completed in {elapsed} min")
#             if result.stdout:
#                 for line in (result.stdout or "")[-1000:].split('\n')[-10:]:
#                     if line.strip():
#                         log.info(f"  {line}")
#             success = True

#     except subprocess.TimeoutExpired:
#         log.error("❌ Agent pipeline timed out (> 2 hours)")
#         success = False
#     except Exception as e:
#         log.error(f"❌ Agent pipeline crashed: {e}")
#         success = False
#     finally:
#         if patched:
#             _restore_agent_script(original_content)
#         try:
#             Path(TEMP_JOB_FILE).unlink(missing_ok=True)
#         except Exception:
#             pass

#     return success


# # ══════════════════════════════════════════════════════════════════════════════
# #  CORE PIPELINE LOGIC (single run)
# # ══════════════════════════════════════════════════════════════════════════════

# def _execute_pipeline_once(progress_callback=None) -> dict:
#     """
#     Run one full pipeline cycle: scrape → dedup → agents.
#     Returns result dict.
    
#     This is called by:
#     1. CRON/Task Scheduler automatically every 12h
#     2. Streamlit "Find Jobs" button for immediate manual run
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

#     db = _get_db()
#     if db is None:
#         result["error"] = "Failed to connect to MongoDB"
#         log.error(f"❌ Stopping: {result['error']}")
#         return result

#     # STEP 1
#     ok = _run_scraper()
#     if not ok:
#         result["error"] = "Scraper failed"
#         log.error(f"❌ Stopping: {result['error']}")
#         return result

#     try:
#         raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
#         all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw
#         result["scraped"] = len(all_scraped)
#     except Exception:
#         pass

#     # STEP 2
#     new_jobs             = _filter_new_jobs(db)
#     result["new_jobs"]   = len(new_jobs)
#     result["skipped_db"] = result["scraped"] - len(new_jobs)

#     if not new_jobs:
#         log.info("⏭️ No new jobs to process — all already exist in MongoDB")
#         result["success"]   = True
#         result["processed"] = 0
#         return result

#     # STEP 3
#     ok                   = _run_agents(new_jobs)
#     result["processed"]  = len(new_jobs) if ok else 0
#     result["success"]    = ok
#     return result


# # ══════════════════════════════════════════════════════════════════════════════
# #  PUBLIC ENTRY POINT
# # ══════════════════════════════════════════════════════════════════════════════

# def run_pipeline(progress_callback=None) -> dict:
#     """
#     SIMPLIFIED FOR CRON:
    
#     Called by:
#     1. CRON/Task Scheduler (automatic every 12h) → just run once
#     2. Streamlit button (manual trigger) → just run once
    
#     ✅ NO MORE THREADING!
#     ✅ NO MORE BACKGROUND LOOPS!
#     ✅ Just run the pipeline once and exit
#     """
#     global _progress_cb
#     _progress_cb = progress_callback

#     log.info("")
#     log.info("╔" + "═" * 60 + "╗")
#     log.info("║  SecureITLab Pipeline Runner (CRON VERSION)           ║")
#     log.info("║  Scraper → Dedup → 15-Agent Pipeline → MongoDB        ║")
#     log.info("║  Scheduled by: OS CRON / Task Scheduler                ║")
#     log.info("╚" + "═" * 60 + "╝")
    
#     now_iso = datetime.now(timezone.utc).isoformat()
#     log.info(f"Run triggered: {now_iso}")
#     log.info("")

#     # Run the pipeline once
#     result = _execute_pipeline_once(progress_callback)
#     _print_summary(result)

#     # Update scheduler state for Streamlit to read
#     state = _read_scheduler_state()
#     new_run_count = state.get("run_count", 0) + 1
#     next_run_iso = (
#         datetime.now(timezone.utc) + 
#         timedelta(hours=SCHEDULE_INTERVAL_HRS)
#     ).isoformat()

#     _write_scheduler_state({
#         "active":     True,  # CRON keeps it active
#         "last_run":   now_iso,
#         "next_run":   next_run_iso,
#         "run_count":  new_run_count,
#         "status":     "idle",  # Waiting for next CRON run
#         "last_result": {
#             "success":   result.get("success"),
#             "scraped":   result.get("scraped"),
#             "new_jobs":  result.get("new_jobs"),
#             "processed": result.get("processed"),
#         },
#     })

#     log.info(f"[State] ✅ Saved scheduler state to {SCHEDULER_STATE_FILE}")
#     log.info(f"[CRON]  💤 Next run scheduled for: {next_run_iso[:19]} UTC")
#     log.info("")

#     return result


# def _print_summary(r: dict):
#     log.info("=" * 62)
#     log.info("  PIPELINE SUMMARY")
#     log.info("=" * 62)
#     log.info(f"  📦 Jobs scraped         : {r.get('scraped', 0)}")
#     log.info(f"  ⏭️ Already in MongoDB   : {r.get('skipped_db', 0)}  ← SKIPPED")
#     log.info(f"  🆕 New jobs found      : {r.get('new_jobs', 0)}")
#     log.info(f"  ✅ Processed by AI      : {r.get('processed', 0)}")
#     log.info(f"  Status                  : {'✅ SUCCESS' if r.get('success') else '❌ FAILED'}")
#     if r.get("error"):
#         log.info(f"  Error                   : {r['error']}")
#     log.info("=" * 62)
#     log.info("")


# # ══════════════════════════════════════════════════════════════════════════════
# #  CLI ENTRY POINT (called by CRON or manual execution)
# # ══════════════════════════════════════════════════════════════════════════════

# if __name__ == "__main__":
#     result = run_pipeline()
#     sys.exit(0 if result["success"] else 1)































"""
╔══════════════════════════════════════════════════════════════════╗
║   main.py — SecureITLab Unified Runner (WINDOWS AUTO VERSION)   ║
║                                                                  ║
║   ✅ RUNS EVERY 12 HOURS ON ITS OWN (NO SETUP NEEDED)           ║
║                                                                  ║
║   Just run once: python main.py                                 ║
║   It will keep running and work every 12 hours automatically     ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import json
import re
import time
import logging
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

load_dotenv()

# ── Global callback for Streamlit ─────────────────────────────────────────────
_progress_cb = None

SCHEDULER_STATE_FILE = "scheduler_state.json"
SCHEDULE_INTERVAL_HRS = 12


# ── Custom logging handler that sends to callback ─────────────────────────────
class StreamlitLogHandler(logging.Handler):
    """Custom handler that sends logs to Streamlit callback"""
    def emit(self, record):
        try:
            msg = self.format(record)
            if _progress_cb:
                _progress_cb(msg)
        except Exception:
            pass


# ── Setup logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("main")
log.setLevel(logging.INFO)

# Avoid adding duplicate handlers on repeated imports
if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, StreamlitLogHandler)
           for h in log.handlers):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    log.addHandler(console_handler)

if not any(isinstance(h, logging.FileHandler) for h in log.handlers):
    file_handler = logging.FileHandler("main_run.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    log.addHandler(file_handler)

_streamlit_log_handler = StreamlitLogHandler()
_streamlit_log_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
if not any(isinstance(h, StreamlitLogHandler) for h in log.handlers):
    log.addHandler(_streamlit_log_handler)


# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB       = os.getenv("MONGO_DB",  "secureitlab_job_pipeline")

SCRAPER_SCRIPT = "cyber_job_scraper_v5.py"
AGENT_SCRIPT   = "final.py"
PYTHON_EXE     = r"C:\Users\DELL\AppData\Local\Programs\Python\Python312\python.exe"
JOB_FILE       = "cybersecurity_remote_jobs.json"
TEMP_JOB_FILE  = "new_jobs_temp.json"
MAX_JOBS       = 20


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEDULER STATE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _read_scheduler_state() -> dict:
    """Read persisted scheduler state from disk."""
    try:
        if Path(SCHEDULER_STATE_FILE).exists():
            return json.loads(Path(SCHEDULER_STATE_FILE).read_text(encoding="utf-8"))
    except Exception:
        pass
    return {
        "active": False,
        "last_run": None,
        "next_run": None,
        "run_count": 0,
        "status": "idle"
    }


def _write_scheduler_state(state: dict):
    """Persist scheduler state to disk so Streamlit can read it."""
    try:
        Path(SCHEDULER_STATE_FILE).write_text(
            json.dumps(state, indent=2, default=str), encoding="utf-8"
        )
    except Exception as e:
        log.warning(f"[State] Could not write state file: {e}")


def get_scheduler_status() -> dict:
    """
    Public helper — called by Streamlit to display scheduler status in the UI.
    Returns dict with: active, last_run, next_run, run_count, seconds_until_next
    """
    state = _read_scheduler_state()
    now   = datetime.now(timezone.utc).timestamp()

    seconds_until_next = None
    next_run_iso = None

    if state.get("last_run"):
        try:
            last_ts = datetime.fromisoformat(state["last_run"]).timestamp()
            # Next run = last run + 12 hours
            next_ts = last_ts + (SCHEDULE_INTERVAL_HRS * 3600)
            next_run_iso = datetime.fromtimestamp(next_ts, tz=timezone.utc).isoformat()
            seconds_until_next = max(0, int(next_ts - now))
        except Exception:
            pass

    return {
        "active":             True,
        "last_run":           state.get("last_run"),
        "next_run":           next_run_iso,
        "run_count":          state.get("run_count", 0),
        "seconds_until_next": seconds_until_next,
        "status":             state.get("status", "running"),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  MONGODB HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_db():
    """Connect to MongoDB. Returns db object or None on failure."""
    try:
        log.info("[MongoDB] Connecting…")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        log.info(f"[MongoDB] ✅ Connected to {MONGO_DB}")
        return db
    except ConnectionFailure as e:
        log.error(f"[MongoDB] ❌ Cannot connect: {e}")
        return None


def _get_processed_keys(db) -> set:
    """Return set of (company_lower, role_lower) already in MongoDB."""
    if db is None:
        return set()
    keys = set()
    try:
        log.info("[Dedup] Checking existing jobs in MongoDB…")
        for doc in db.jobs.find({}, {"company": 1, "role": 1, "_id": 0}):
            co   = (doc.get("company") or "").strip().lower()
            role = (doc.get("role")    or "").strip().lower()
            if co and role:
                keys.add((co, role))
        log.info(f"[Dedup] ✅ Found {len(keys)} existing jobs in MongoDB")
    except Exception as e:
        log.warning(f"[Dedup] ⚠️ Warning reading existing jobs: {e}")
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
    log.info("=" * 62)
    log.info("STEP 1 — Running Job Scraper")
    log.info("=" * 62)

    if not Path(SCRAPER_SCRIPT).exists():
        log.error(f"❌ Scraper not found: {SCRAPER_SCRIPT}")
        return False

    log.info("⏳ Starting scraper subprocess…")
    t0 = time.time()
    try:
        result = subprocess.run(
            [PYTHON_EXE, SCRAPER_SCRIPT],
            timeout=3600,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
            capture_output=True,
            text=True,
        )
        elapsed = round(time.time() - t0, 1)

        if result.returncode != 0:
            err = (result.stderr or result.stdout or "No output")[-1000:]
            log.error(f"❌ Scraper exited with code {result.returncode} after {elapsed}s")
            log.error(f"Error output: {err}")
        else:
            log.info(f"✅ Scraper finished in {elapsed}s")
            if result.stdout:
                for line in result.stdout.strip().split('\n')[-5:]:
                    if line.strip():
                        log.info(f"  {line}")
    except subprocess.TimeoutExpired:
        log.error("❌ Scraper timed out (> 60 min)")
        return False
    except Exception as e:
        log.error(f"❌ Scraper crashed: {e}")
        return False

    if not Path(JOB_FILE).exists():
        log.error(f"❌ {JOB_FILE} not found after scraper ran")
        return False

    try:
        raw  = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
        jobs = raw.get("jobs", raw) if isinstance(raw, dict) else raw
        log.info(f"✅ Scraped {len(jobs)} total jobs from {JOB_FILE}")
    except Exception as e:
        log.warning(f"⚠️ Could not count scraped jobs: {e}")
        return False

    return True


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — DEDUP: KEEP ONLY NEW JOBS
# ══════════════════════════════════════════════════════════════════════════════

def _filter_new_jobs(db) -> list:
    """Load scraped jobs, remove duplicates and already-processed ones."""
    log.info("=" * 62)
    log.info("STEP 2 — Deduplication & Filtering")
    log.info("=" * 62)

    if not Path(JOB_FILE).exists():
        log.error(f"❌ {JOB_FILE} missing — run scraper first")
        return []

    raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
    all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw

    existing   = _get_processed_keys(db)
    seen_batch = set()
    new_jobs   = []
    skip_db    = 0
    skip_dup   = 0

    log.info(f"🔍 Processing {len(all_scraped)} scraped jobs…")

    for i, job in enumerate(all_scraped, 1):
        key = _job_key(job)
        if key in existing:
            skip_db += 1
            continue
        if key in seen_batch:
            skip_dup += 1
            continue
        seen_batch.add(key)
        new_jobs.append(job)

    log.info("")
    log.info("📊 Deduplication Results:")
    log.info(f"  Total scraped        : {len(all_scraped)}")
    log.info(f"  Already in MongoDB   : {skip_db}  ← SKIPPED")
    log.info(f"  Duplicates in file   : {skip_dup}  ← SKIPPED")
    log.info(f"  🆕 NEW jobs         : {len(new_jobs)}")

    if len(new_jobs) > MAX_JOBS:
        log.warning(f"⚠️  Capping to first {MAX_JOBS} new jobs (limit set)")
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
    log.info(f"✅ Written {len(new_jobs)} new jobs → {TEMP_JOB_FILE}")


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
        log.warning("⚠️ Could not patch JOB_FILE in agent script — will process all scraped jobs")
        return original, False

    Path(AGENT_SCRIPT).write_text(patched, encoding="utf-8")
    log.info(f"✅ Patched {AGENT_SCRIPT} → reads from {TEMP_JOB_FILE}")
    return original, True


def _restore_agent_script(original_content: str):
    Path(AGENT_SCRIPT).write_text(original_content, encoding="utf-8")
    log.info(f"✅ Restored {AGENT_SCRIPT} to original")


def _run_agents(new_jobs: list) -> bool:
    """Write new_jobs to temp file, patch agent script, run it, then restore."""
    log.info("=" * 62)
    log.info(f"STEP 3 — Running AI Agent Pipeline")
    log.info(f"Jobs to process: {len(new_jobs)}")
    log.info("=" * 62)

    if not Path(AGENT_SCRIPT).exists():
        log.error(f"❌ Agent script not found: {AGENT_SCRIPT}")
        return False

    _write_temp_file(new_jobs)

    original_content, patched = _patch_agent_script()
    t0      = time.time()
    success = False

    log.info("⏳ Starting agent pipeline subprocess…")

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
            err_output = (result.stderr or result.stdout or "No output captured")[-2000:]
            log.error(f"❌ Agent script FAILED with code {result.returncode} after {elapsed} min")
            log.error(err_output)
            success = False
        else:
            log.info(f"✅ Agent pipeline completed in {elapsed} min")
            if result.stdout:
                for line in (result.stdout or "")[-1000:].split('\n')[-10:]:
                    if line.strip():
                        log.info(f"  {line}")
            success = True

    except subprocess.TimeoutExpired:
        log.error("❌ Agent pipeline timed out (> 2 hours)")
        success = False
    except Exception as e:
        log.error(f"❌ Agent pipeline crashed: {e}")
        success = False
    finally:
        if patched:
            _restore_agent_script(original_content)
        try:
            Path(TEMP_JOB_FILE).unlink(missing_ok=True)
        except Exception:
            pass

    return success


# ══════════════════════════════════════════════════════════════════════════════
#  CORE PIPELINE LOGIC (single run)
# ══════════════════════════════════════════════════════════════════════════════

def _execute_pipeline_once(progress_callback=None) -> dict:
    """
    Run one full pipeline cycle: scrape → dedup → agents.
    Returns result dict.
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

    db = _get_db()
    if db is None:
        result["error"] = "Failed to connect to MongoDB"
        log.error(f"❌ Stopping: {result['error']}")
        return result

    # STEP 1
    ok = _run_scraper()
    if not ok:
        result["error"] = "Scraper failed"
        log.error(f"❌ Stopping: {result['error']}")
        return result

    try:
        raw         = json.loads(Path(JOB_FILE).read_text(encoding="utf-8"))
        all_scraped = raw.get("jobs", raw) if isinstance(raw, dict) else raw
        result["scraped"] = len(all_scraped)
    except Exception:
        pass

    # STEP 2
    new_jobs             = _filter_new_jobs(db)
    result["new_jobs"]   = len(new_jobs)
    result["skipped_db"] = result["scraped"] - len(new_jobs)

    if not new_jobs:
        log.info("⏭️ No new jobs to process — all already exist in MongoDB")
        result["success"]   = True
        result["processed"] = 0
        return result

    # STEP 3
    ok                   = _run_agents(new_jobs)
    result["processed"]  = len(new_jobs) if ok else 0
    result["success"]    = ok
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(progress_callback=None) -> dict:
    """
    Run the pipeline once.
    """
    global _progress_cb
    _progress_cb = progress_callback

    log.info("")
    log.info("╔" + "═" * 60 + "╗")
    log.info("║  SecureITLab Pipeline Runner                         ║")
    log.info("║  Scraper → Dedup → 15-Agent Pipeline → MongoDB        ║")
    log.info("╚" + "═" * 60 + "╝")
    
    now_iso = datetime.now(timezone.utc).isoformat()
    log.info(f"Run triggered: {now_iso}")
    log.info("")

    # Run the pipeline once
    result = _execute_pipeline_once(progress_callback)
    _print_summary(result)

    # Update scheduler state for Streamlit to read
    state = _read_scheduler_state()
    new_run_count = state.get("run_count", 0) + 1
    next_run_iso = (
        datetime.now(timezone.utc) + 
        timedelta(hours=SCHEDULE_INTERVAL_HRS)
    ).isoformat()

    _write_scheduler_state({
        "active":     True,
        "last_run":   now_iso,
        "next_run":   next_run_iso,
        "run_count":  new_run_count,
        "status":     "idle",
        "last_result": {
            "success":   result.get("success"),
            "scraped":   result.get("scraped"),
            "new_jobs":  result.get("new_jobs"),
            "processed": result.get("processed"),
        },
    })

    log.info(f"[State] ✅ Saved scheduler state to {SCHEDULER_STATE_FILE}")
    log.info(f"[AUTO]  💤 Next run in 12 hours: {next_run_iso[:19]} UTC")
    log.info("")

    return result


def _print_summary(r: dict):
    log.info("=" * 62)
    log.info("  PIPELINE SUMMARY")
    log.info("=" * 62)
    log.info(f"  📦 Jobs scraped         : {r.get('scraped', 0)}")
    log.info(f"  ⏭️ Already in MongoDB   : {r.get('skipped_db', 0)}  ← SKIPPED")
    log.info(f"  🆕 New jobs found      : {r.get('new_jobs', 0)}")
    log.info(f"  ✅ Processed by AI      : {r.get('processed', 0)}")
    log.info(f"  Status                  : {'✅ SUCCESS' if r.get('success') else '❌ FAILED'}")
    if r.get("error"):
        log.info(f"  Error                   : {r['error']}")
    log.info("=" * 62)
    log.info("")


# ══════════════════════════════════════════════════════════════════════════════
#  ⭐ AUTO-SCHEDULER (RUNS EVERY 12 HOURS AUTOMATICALLY)
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import atexit
    import schedule
    
    def job():
        """This runs every 12 hours"""
        try:
            log.info("\n" + "🔄 " * 20)
            log.info("AUTOMATIC 12-HOUR CYCLE TRIGGERED")
            log.info("🔄 " * 20)
            run_pipeline()
        except Exception as e:
            log.error(f"Pipeline error: {e}")
    
    # Schedule the job to run every 12 hours
    schedule.every(12).hours.do(job)
    
    log.info("")
    log.info("╔" + "═" * 60 + "╗")
    log.info("║  ✅ AUTO-SCHEDULER STARTED                            ║")
    log.info("║  Pipeline will run every 12 hours automatically       ║")
    log.info("║  Running first cycle immediately...                  ║")
    log.info("╚" + "═" * 60 + "╝")
    
    # Run first cycle immediately
    job()
    
    # Now keep running and wait for scheduled jobs
    try:
        log.info("")
        log.info("💤 Scheduler waiting... (will run again in 12 hours)")
        log.info("Press Ctrl+C to stop")
        log.info("")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every 60 seconds
            
    except KeyboardInterrupt:
        log.info("\n[STOP] Scheduler stopped by user")
        sys.exit(0)