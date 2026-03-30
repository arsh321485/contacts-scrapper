"""
═══════════════════════════════════════════════════════════════════
EMAIL QUEUE SYSTEM - Rate Limited Email Sender
═══════════════════════════════════════════════════════════════════

PROBLEM: Pipeline gets 60-120+ emails per day
         cPanel only allows 15 emails/hour max
         
SOLUTION: Queue emails and send 2-3 per 5 minutes (safe rate)
          Spreads emails across entire day

FLOW:
1. Pipeline adds emails to QUEUE (doesn't send)
2. Separate queue worker sends 2 emails every 5 minutes
3. Queue persists to MongoDB so no loss on restart
4. Streamlit shows queue status

RATES (SAFE FOR cPANEL):
- 2 emails / 5 min = 24 emails/hour ✅ SAFE
- 120 emails will take ~5 hours to send
- Spread across entire day = NO SPAM MARK
═══════════════════════════════════════════════════════════════════
"""

import time
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("email_queue")
log.setLevel(logging.INFO)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://arshmittal740:fG8DWhdrvdlmtmji@cluster0.tf5hxs5.mongodb.net/")
MONGO_DB = os.getenv("MONGO_DB", "secureitlab_job_pipeline")

# 4 EMAIL ACCOUNTS (rotate between them)
EMAIL_ACCOUNTS = [
    {
        "address": "ashish@secureitlab.org",
        "name": "Ashish",
        "password": os.getenv("EMAIL_PASSWORD_1"),
        "smtp_server": "mail.secureitlab.org",
        "smtp_port": 587,
  
    },
    {
        "address": "yash.rohatgi@secureitlab.org",
        "name": "Yash",
        "password": os.getenv("EMAIL_PASSWORD_2"),
        "smtp_server": "mail.secureitlab.org",
        "smtp_port": 465,
    },
    {
        "address": "abdu.nafih@secureitlab.org",
        "name": "Abdu",
        "password": os.getenv("EMAIL_PASSWORD_3"),
        "smtp_server":"mail.secureitlab.org",
        "smtp_port": 465,
    },
    {
        "address": "subeer@secureitlab.org",
        "name": "Subeer",
        "password": os.getenv("EMAIL_PASSWORD_4"),
        "smtp_server": "mail.secureitlab.org",
        "smtp_port": 465,
    },
]

# RATE LIMITING (SAFE FOR cPANEL)
EMAILS_PER_BATCH = 1  # Send 2 emails at a time
BATCH_INTERVAL_SECONDS = 300  # Wait 5 minutes between batches
# = 2 emails every 5 min = 24 emails/hour ✅ SAFE


# ══════════════════════════════════════════════════════════════════════════════
#  MONGODB EMAIL QUEUE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_db():
    """Connect to MongoDB"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        log.info("[Queue] ✅ Connected to MongoDB")
        return db
    except Exception as e:
        log.error(f"[Queue] ❌ MongoDB connection failed: {e}")
        return None


def add_email_to_queue(
    recipient_email: str,
    recipient_name: str,
    subject: str,
    body: str,
    company: str = "",
    job_title: str = "",
    contact_id: str = "",
    db=None,
    reply_to: str = None
) -> bool:
    """
    Add email to MongoDB queue.
    Called by agent pipeline when finding contacts.
    """
    if db is None:
        db = _get_db()
    
    if db is None:
        log.error("[Queue] ❌ Cannot add to queue - no MongoDB")
        return False
    
    try:
        email_doc = {
            "recipient_email": recipient_email,
            "recipient_name": recipient_name,
            "subject": subject,
            "body": body,
            "company": company,
            "job_title": job_title,
            "contact_id": contact_id,
            "status": "pending",  # pending, sent, failed
            "created_at": datetime.now(timezone.utc),
            "sent_at": None,
            "error": None,
            "sender_email": None,
            "reply_to": reply_to,
        }
        
        result = db.email_queue.insert_one(email_doc)
        log.info(f"[Queue] ✅ Added email to queue for {recipient_email}")
        return True
    except Exception as e:
        log.error(f"[Queue] ❌ Failed to add email to queue: {e}")
        return False


def get_pending_emails(db, limit: int = 2) -> List[Dict]:
    """Get next batch of pending emails from queue"""
    try:
        emails = list(db.email_queue.find(
            {"status": "pending"},
            sort=[("created_at", 1)]
        ).limit(limit))
        return emails
    except Exception as e:
        log.error(f"[Queue] ❌ Failed to get pending emails: {e}")
        return []


def mark_email_sent(db, email_id: str, sender_email: str):
    """Mark email as sent"""
    try:
        db.email_queue.update_one(
            {"_id": email_id},
            {
                "$set": {
                    "status": "sent",
                    "sent_at": datetime.now(timezone.utc),
                    "sender_email": sender_email,
                }
            }
        )
        log.info(f"[Queue] ✅ Marked email {email_id} as sent")
    except Exception as e:
        log.error(f"[Queue] ❌ Failed to mark email as sent: {e}")


def mark_email_failed(db, email_id: str, error: str):
    """Mark email as failed"""
    try:
        db.email_queue.update_one(
            {"_id": email_id},
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                }
            }
        )
        log.error(f"[Queue] ❌ Marked email {email_id} as failed: {error}")
    except Exception as e:
        log.error(f"[Queue] ❌ Failed to mark email as failed: {e}")


def get_queue_status(db) -> Dict:
    """Get queue statistics"""
    try:
        pending = db.email_queue.count_documents({"status": "pending"})
        sent = db.email_queue.count_documents({"status": "sent"})
        failed = db.email_queue.count_documents({"status": "failed"})
        
        return {
            "pending": pending,
            "sent": sent,
            "failed": failed,
            "total": pending + sent + failed,
        }
    except Exception as e:
        log.error(f"[Queue] ❌ Failed to get queue status: {e}")
        return {"pending": 0, "sent": 0, "failed": 0, "total": 0}


# ══════════════════════════════════════════════════════════════════════════════
#  EMAIL SENDING
# ══════════════════════════════════════════════════════════════════════════════

def send_email(sender_account: Dict, recipient_email: str, subject: str, body: str, reply_to: str = None) -> bool:
    """
    Send email using specified sender account
    Returns True if success, False if failed
    """
    try:
        # Create message
        msg = MIMEMultipart()
        msg["From"] = f"{sender_account['name']} <{sender_account['address']}>"
        msg["To"] = recipient_email
        msg["Subject"] = subject

        # Add Reply-To header
        if reply_to:
            msg["Reply-To"] = reply_to

        msg.attach(MIMEText(body, "html"))
        
        # Connect and send
        with smtplib.SMTP(sender_account["smtp_server"], sender_account["smtp_port"]) as server:
            server.starttls()
            server.login(sender_account["address"], sender_account["password"])
            server.send_message(msg)
        
        log.info(f"[Email] ✅ Sent from {sender_account['name']} to {recipient_email}")
        return True
        
    except Exception as e:
        log.error(f"[Email] ❌ Failed to send: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  QUEUE WORKER (Main Loop - runs continuously)
# ══════════════════════════════════════════════════════════════════════════════

def run_queue_worker():
    """
    Main queue worker loop.
    
    Sends emails at rate-limited speed:
    - 2 emails every 5 minutes = 24 emails/hour (SAFE)
    - Prevents cPanel spam marking
    - Rotates between 4 email accounts
    
    Run this as separate process/thread all day long.
    """
    db = _get_db()
    if db is None:
        log.error("[Queue] ❌ Cannot start worker - no MongoDB")
        return
    
    sender_index = 0  # Track which email account to use
    
    log.info("")
    log.info("╔" + "═" * 60 + "╗")
    log.info("║  EMAIL QUEUE WORKER STARTED                          ║")
    log.info("║  Sending 2 emails every 5 minutes (SAFE RATE)        ║")
    log.info("║  Rotating between 4 email accounts                   ║")
    log.info("╚" + "═" * 60 + "╝")
    log.info("")
    
    try:
        while True:
            # Get next batch of emails
            pending_emails = get_pending_emails(db, limit=EMAILS_PER_BATCH)
            
            if pending_emails:
                log.info(f"\n[Queue] Processing {len(pending_emails)} emails...")
                
                for email_doc in pending_emails:
                    # Rotate sender account
                    sender = EMAIL_ACCOUNTS[sender_index % len(EMAIL_ACCOUNTS)]
                    sender_index += 1
                    
                    # Send email
                    success = send_email(
                        sender,
                        email_doc["recipient_email"],
                        email_doc["subject"],
                        email_doc["body"],
                        email_doc.get("reply_to")  # ← ADD THIS
                    )
                    
                    if success:
                        mark_email_sent(db, email_doc["_id"], sender["address"])
                    else:
                        mark_email_failed(db, email_doc["_id"], "SMTP send failed")
                
                # Log queue status
                status = get_queue_status(db)
                log.info(f"[Queue] Status: {status['pending']} pending, {status['sent']} sent, {status['failed']} failed")
            
            else:
                log.debug("[Queue] No pending emails, waiting...")
            
            # Wait before next batch (IMPORTANT: prevents spam)
            log.info(f"[Queue] Waiting {BATCH_INTERVAL_SECONDS}s before next batch...")
            time.sleep(BATCH_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        log.info("[Queue] ⏹️ Queue worker stopped")
    except Exception as e:
        log.error(f"[Queue] ❌ Worker crashed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Setup logging
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    log.addHandler(handler)
    
    file_handler = logging.FileHandler("email_queue.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    log.addHandler(file_handler)
    
    # Start queue worker
    run_queue_worker()





























# # TESTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT























# """
# ═══════════════════════════════════════════════════════════════════
# EMAIL QUEUE SYSTEM - Rate Limited Email Sender
# ═══════════════════════════════════════════════════════════════════

# PROBLEM: Pipeline gets 60-120+ emails per day
#          cPanel only allows 15 emails/hour max
         
# SOLUTION: Queue emails and send 2-3 per 5 minutes (safe rate)
#           Spreads emails across entire day

# FLOW:
# 1. Pipeline adds emails to QUEUE (doesn't send)
# 2. Separate queue worker sends 2 emails every 5 minutes
# 3. Queue persists to MongoDB so no loss on restart
# 4. Streamlit shows queue status

# RATES (SAFE FOR cPANEL):
# - 2 emails / 5 min = 24 emails/hour ✅ SAFE
# - 120 emails will take ~5 hours to send
# - Spread across entire day = NO SPAM MARK
# ═══════════════════════════════════════════════════════════════════
# """

# import time
# import json
# import logging
# from datetime import datetime, timezone
# from pathlib import Path
# from typing import List, Dict
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from pymongo import MongoClient
# import os
# from dotenv import load_dotenv
# DRY_RUN = True
# load_dotenv()

# log = logging.getLogger("email_queue")
# log.setLevel(logging.INFO)

# # ══════════════════════════════════════════════════════════════════════════════
# #  CONFIG
# # ══════════════════════════════════════════════════════════════════════════════

# MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://arshmittal740:fG8DWhdrvdlmtmji@cluster0.tf5hxs5.mongodb.net/")
# MONGO_DB = os.getenv("MONGO_DB", "secureitlab_job_pipeline")

# # 4 EMAIL ACCOUNTS (rotate between them)
# EMAIL_ACCOUNTS = [
#     {
#         "address": "ashish@secureitlab.org",
#         "name": "Ashish",
#         "password": os.getenv("EMAIL_PASSWORD_1"),
#         "smtp_server": "mail.secureitlab.org",
#         "smtp_port": 465,
  
#     },
#     {
#         "address": "yash.rohatgi@secureitlab.org",
#         "name": "Yash",
#         "password": os.getenv("EMAIL_PASSWORD_2"),
#         "smtp_server": "mail.secureitlab.org",
#         "smtp_port": 465,
#     },
#     {
#         "address": "abdu.nafih@secureitlab.org",
#         "name": "Abdu",
#         "password": os.getenv("EMAIL_PASSWORD_3"),
#         "smtp_server":"mail.secureitlab.org",
#         "smtp_port": 465,
#     },
#     {
#         "address": "subeer@secureitlab.org",
#         "name": "Subeer",
#         "password": os.getenv("EMAIL_PASSWORD_4"),
#         "smtp_server": "mail.secureitlab.org",
#         "smtp_port": 465,
#     },
# ]

# # RATE LIMITING (SAFE FOR cPANEL)
# EMAILS_PER_BATCH = 1  # Send 2 emails at a time
# BATCH_INTERVAL_SECONDS = 300  # Wait 5 minutes between batches
# # = 2 emails every 5 min = 24 emails/hour ✅ SAFE


# # ══════════════════════════════════════════════════════════════════════════════
# #  MONGODB EMAIL QUEUE HELPERS
# # ══════════════════════════════════════════════════════════════════════════════

# def _get_db():
#     """Connect to MongoDB"""
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
#         client.admin.command("ping")
#         db = client[MONGO_DB]
#         log.info("[Queue] ✅ Connected to MongoDB")
#         return db
#     except Exception as e:
#         log.error(f"[Queue] ❌ MongoDB connection failed: {e}")
#         return None


# def add_email_to_queue(
#     recipient_email: str,
#     recipient_name: str,
#     subject: str,
#     body: str,
#     company: str = "",
#     job_title: str = "",
#     contact_id: str = "",
#     db=None,
#     reply_to: str = None
# ) -> bool:
#     """
#     Add email to MongoDB queue.
#     Called by agent pipeline when finding contacts.
#     """
#     if db is None:
#         db = _get_db()
    
#     if db is None:
#         log.error("[Queue] ❌ Cannot add to queue - no MongoDB")
#         return False
    
#     try:
#         email_doc = {
#             "recipient_email": recipient_email,
#             "recipient_name": recipient_name,
#             "subject": subject,
#             "body": body,
#             "company": company,
#             "job_title": job_title,
#             "contact_id": contact_id,
#             "status": "pending",  # pending, sent, failed
#             "created_at": datetime.now(timezone.utc),
#             "sent_at": None,
#             "error": None,
#             "sender_email": None,
#             "reply_to": reply_to,
#         }
        
#         result = db.email_queue.insert_one(email_doc)
#         log.info(f"[Queue] ✅ Added email to queue for {recipient_email}")
#         return True
#     except Exception as e:
#         log.error(f"[Queue] ❌ Failed to add email to queue: {e}")
#         return False


# def get_pending_emails(db, limit: int = 2) -> List[Dict]:
#     """Get next batch of pending emails from queue"""
#     try:
#         emails = list(db.email_queue.find(
#             {"status": "pending"},
#             sort=[("created_at", 1)]
#         ).limit(limit))
#         return emails
#     except Exception as e:
#         log.error(f"[Queue] ❌ Failed to get pending emails: {e}")
#         return []


# def mark_email_sent(db, email_id: str, sender_email: str):
#     """Mark email as sent"""
#     try:
#         db.email_queue.update_one(
#             {"_id": email_id},
#             {
#                 "$set": {
#                     "status": "sent",
#                     "sent_at": datetime.now(timezone.utc),
#                     "sender_email": sender_email,
#                 }
#             }
#         )
#         log.info(f"[Queue] ✅ Marked email {email_id} as sent")
#     except Exception as e:
#         log.error(f"[Queue] ❌ Failed to mark email as sent: {e}")


# def mark_email_failed(db, email_id: str, error: str):
#     """Mark email as failed"""
#     try:
#         db.email_queue.update_one(
#             {"_id": email_id},
#             {
#                 "$set": {
#                     "status": "failed",
#                     "error": error,
#                 }
#             }
#         )
#         log.error(f"[Queue] ❌ Marked email {email_id} as failed: {error}")
#     except Exception as e:
#         log.error(f"[Queue] ❌ Failed to mark email as failed: {e}")


# def get_queue_status(db) -> Dict:
#     """Get queue statistics"""
#     try:
#         pending = db.email_queue.count_documents({"status": "pending"})
#         sent = db.email_queue.count_documents({"status": "sent"})
#         failed = db.email_queue.count_documents({"status": "failed"})
        
#         return {
#             "pending": pending,
#             "sent": sent,
#             "failed": failed,
#             "total": pending + sent + failed,
#         }
#     except Exception as e:
#         log.error(f"[Queue] ❌ Failed to get queue status: {e}")
#         return {"pending": 0, "sent": 0, "failed": 0, "total": 0}


# # ══════════════════════════════════════════════════════════════════════════════
# #  EMAIL SENDING
# # ══════════════════════════════════════════════════════════════════════════════

# def send_email(sender_account, recipient_email, subject, body, reply_to: str = None) -> bool:
#     """
#     Send email using specified sender account
#     Returns True if success, False if failed
#     """
#     try:
#         # Create message
#         msg = MIMEMultipart()
#         msg["From"] = f"{sender_account['name']} <{sender_account['email']}>"
#         msg["To"] = recipient_email
#         msg["Subject"] = subject
        
#         if reply_to:
#             msg["Reply-To"] = reply_to
        
#         msg.attach(MIMEText(body, "html"))
        
#         # ── DRY RUN MODE: Don't actually send ──
#         if DRY_RUN:
#             print(f"[DRY RUN] 🧪 Would send from {sender_account['name']} to {recipient_email}")
#             print(f"[DRY RUN] Subject: {subject}")
#             print(f"[DRY RUN] Reply-To: {reply_to}")
#             print(f"[DRY RUN] Body preview: {body[:100]}...")
#             return True
        
#         # ── REAL MODE: Actually send via SMTP ──
#         with smtplib.SMTP(sender_account["smtp_server"], sender_account["smtp_port"]) as server:
#             server.starttls()
#             server.login(sender_account["address"], sender_account["password"])
#             server.send_message(msg)
        
#         print(f"[Email] ✅ Sent from {sender_account['name']} to {recipient_email}")
#         return True
        
#     except Exception as e:
#         print(f"[Email] ❌ Failed: {e}")
#         return False


# # ══════════════════════════════════════════════════════════════════════════════
# #  QUEUE WORKER (Main Loop - runs continuously)
# # ══════════════════════════════════════════════════════════════════════════════

# def run_queue_worker():
#     """
#     Main queue worker loop.
    
#     Sends emails at rate-limited speed:
#     - 2 emails every 5 minutes = 24 emails/hour (SAFE)
#     - Prevents cPanel spam marking
#     - Rotates between 4 email accounts
    
#     Run this as separate process/thread all day long.
#     """
#     db = _get_db()
#     if db is None:
#         log.error("[Queue] ❌ Cannot start worker - no MongoDB")
#         return
    
#     sender_index = 0  # Track which email account to use
    
#     log.info("")
#     log.info("╔" + "═" * 60 + "╗")
#     log.info("║  EMAIL QUEUE WORKER STARTED                          ║")
#     log.info("║  Sending 2 emails every 5 minutes (SAFE RATE)        ║")
#     log.info("║  Rotating between 4 email accounts                   ║")
#     log.info("╚" + "═" * 60 + "╝")
#     log.info("")
    
#     try:
#         while True:
#             # Get next batch of emails
#             pending_emails = get_pending_emails(db, limit=EMAILS_PER_BATCH)
            
#             if pending_emails:
#                 log.info(f"\n[Queue] Processing {len(pending_emails)} emails...")
                
#                 for email_doc in pending_emails:
#                     # Rotate sender account
#                     sender = EMAIL_ACCOUNTS[sender_index % len(EMAIL_ACCOUNTS)]
#                     sender_index += 1
                    
#                     # Send email
#                     success = send_email(
#                         sender,
#                         email_doc["recipient_email"],
#                         email_doc["subject"],
#                         email_doc["body"],
#                         email_doc.get("reply_to")  # ← ADD THIS
#                     )
                    
#                     if success:
#                         mark_email_sent(db, email_doc["_id"], sender["address"])
#                     else:
#                         mark_email_failed(db, email_doc["_id"], "SMTP send failed")
                
#                 # Log queue status
#                 status = get_queue_status(db)
#                 log.info(f"[Queue] Status: {status['pending']} pending, {status['sent']} sent, {status['failed']} failed")
            
#             else:
#                 log.debug("[Queue] No pending emails, waiting...")
            
#             # Wait before next batch (IMPORTANT: prevents spam)
#             log.info(f"[Queue] Waiting {BATCH_INTERVAL_SECONDS}s before next batch...")
#             time.sleep(BATCH_INTERVAL_SECONDS)
            
#     except KeyboardInterrupt:
#         log.info("[Queue] ⏹️ Queue worker stopped")
#     except Exception as e:
#         log.error(f"[Queue] ❌ Worker crashed: {e}")


# # ══════════════════════════════════════════════════════════════════════════════
# #  ENTRY POINT
# # ══════════════════════════════════════════════════════════════════════════════

# if __name__ == "__main__":
#     # Setup logging
#     handler = logging.StreamHandler()
#     handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
#     log.addHandler(handler)
    
#     file_handler = logging.FileHandler("email_queue.log", encoding="utf-8")
#     file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
#     log.addHandler(file_handler)
    
#     # Start queue worker
#     run_queue_worker()