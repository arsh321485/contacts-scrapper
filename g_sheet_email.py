# #!/usr/bin/env python
# # ═══════════════════════════════════════════════════════════════════════════════
# # google_sheet_email_sender.py
# # ═══════════════════════════════════════════════════════════════════════════════
# # 
# # Reads contacts from Google Sheet and sends personalized emails in batches
# # Features:
# #   ✅ Read from Google Sheet (900+ rows)
# #   ✅ Sender rotation (Ashish, Yash, Abdu, Subeer)
# #   ✅ Placeholder replacement ([Hiring Manager's Name], [Your Name], etc)
# #   ✅ Rate limiting (1 email per 5 minutes = safe)
# #   ✅ Batch processing
# #   ✅ Error handling & retries
# #   ✅ MongoDB logging
# # ═══════════════════════════════════════════════════════════════════════════════

# import os
# import time
# import smtplib
# import logging
# from datetime import datetime, timezone
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from dotenv import load_dotenv
# from pymongo import MongoClient
# from google.oauth2.service_account import Credentials
# from google.auth.transport.requests import Request
# import gspread

# load_dotenv()

# # ═══════════════════════════════════════════════════════════════════════════════
# # CONFIGURATION
# # ═══════════════════════════════════════════════════════════════════════════════

# # Google Sheet Config
# GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1bVvT1LYqXZEIgijRnZf8Y6rBcGAfbMaWuDgDTkRe80c/edit?usp=sharing"
# SHEET_ID = "1bVvT1LYqXZEIgijRnZf8Y6rBcGAfbMaWuDgDTkRe80c"
# SHEET_TAB = "master_contacts"

# # MongoDB Config
# MONGO_URI = os.getenv("MONGO_URI")
# MONGO_DB = os.getenv("MONGO_DB", "secureitlab_job_pipeline")

# # Email Config
# SMTP_SERVER = "mail.secureitlab.org"
# SMTP_PORT = 587

# # Rate Limiting (1 email every 5 minutes = safe)
# BATCH_SIZE = 1
# BATCH_INTERVAL_SECONDS = 300

# # Senders (rotate through these)
# SENDER_ACCOUNTS = [
#     {
#         "name": "Ashish",
#         "email": "ashish@secureitlab.org",
#         "reply_to": "ashish@secureitlabservices.com",
#         "password": os.getenv("EMAIL_PASSWORD_1")
#     },
#     {
#         "name": "Yash",
#         "email": "yash.rohatgi@secureitlab.org",
#         "reply_to": "yash.rohatgi@secureitlabservices.com",
#         "password": os.getenv("EMAIL_PASSWORD_2")
#     },
#     {
#         "name": "Abdu",
#         "email": "abdu.nafih@secureitlab.org",
#         "reply_to": "abdu.nafih@secureitlabservices.com",
#         "password": os.getenv("EMAIL_PASSWORD_3")
#     },
#     {
#         "name": "Subeer",
#         "email": "subeer@secureitlab.org",
#         "reply_to": "subeer@secureitlabservices.com",
#         "password": os.getenv("EMAIL_PASSWORD_4")
#     },
# ]

# # ═══════════════════════════════════════════════════════════════════════════════
# # LOGGING SETUP
# # ═══════════════════════════════════════════════════════════════════════════════

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s | %(levelname)s | %(message)s',
#     handlers=[
#         logging.FileHandler('google_sheet_sender.log'),
#         logging.StreamHandler()
#     ]
# )
# log = logging.getLogger(__name__)

# # ═══════════════════════════════════════════════════════════════════════════════
# # GOOGLE SHEET FUNCTIONS
# # ═══════════════════════════════════════════════════════════════════════════════

# def authenticate_google_sheets():
#     """Authenticate with Google Sheets API using service account"""
#     try:
#         # Try using service account JSON file first
#         if os.path.exists("service_account.json"):
#             credentials = Credentials.from_service_account_file(
#                 "service_account.json",
#                 scopes=['https://www.googleapis.com/auth/spreadsheets']
#             )
#             log.info("✅ Authenticated with service account")
#             return gspread.authorize(credentials)
#         else:
#             log.error("❌ service_account.json not found")
#             log.error("   To use Google Sheets:")
#             log.error("   1. Go to https://console.cloud.google.com")
#             log.error("   2. Create a service account")
#             log.error("   3. Download service_account.json")
#             log.error("   4. Place it in your project folder")
#             return None
#     except Exception as e:
#         log.error(f"❌ Google Sheets authentication failed: {e}")
#         return None

# def read_contacts_from_sheet(gc):
#     """Read all contacts from Google Sheet"""
#     try:
#         log.info(f"📖 Opening Google Sheet: {SHEET_TAB}")
#         spreadsheet = gc.open_by_key(SHEET_ID)
#         worksheet = spreadsheet.worksheet(SHEET_TAB)
        
#         # Get all values
#         all_values = worksheet.get_all_values()
        
#         if not all_values:
#             log.error("❌ Sheet is empty")
#             return []
        
#         # First row is headers
#         headers = all_values[0]
#         contacts = []
        
#         # Map column indices
#         try:
#             job_role_idx = headers.index("Job Role")
#             company_idx = headers.index("Company")
#             priority_idx = headers.index("Priority")
#             name_idx = headers.index("Name")
#             title_idx = headers.index("Title / Role")
#             email_idx = headers.index("Contact Email")
#             linkedin_idx = headers.index("LinkedIn URL") if "LinkedIn URL" in headers else -1
#             job_score_idx = headers.index("Job Score") if "Job Score" in headers else -1
#             outreach_idx = headers.index("Outreach Email (Agent)")
#             sent_idx = headers.index("Sent") if "Sent" in headers else -1
#         except ValueError as e:
#             log.error(f"❌ Column not found: {e}")
#             log.error(f"   Available columns: {headers}")
#             return []
        
#         # Read rows (skip header)
#         for row_idx, row in enumerate(all_values[1:], start=2):
#             try:
#                 if len(row) < max(email_idx, outreach_idx) + 1:
#                     continue
                
#                 email = row[email_idx].strip() if email_idx < len(row) else ""
                
#                 # Skip if email already sent or email is empty
#                 if not email or "@" not in email:
#                     continue
                
#                 if sent_idx >= 0 and sent_idx < len(row):
#                     sent_status = row[sent_idx].strip().lower()
#                     if sent_status in ("yes", "true", "sent", "✅"):
#                         continue
                
#                 contact = {
#                     "row_number": row_idx,
#                     "job_role": row[job_role_idx].strip() if job_role_idx < len(row) else "",
#                     "company": row[company_idx].strip() if company_idx < len(row) else "",
#                     "priority": row[priority_idx].strip() if priority_idx < len(row) else "",
#                     "name": row[name_idx].strip() if name_idx < len(row) else "",
#                     "title": row[title_idx].strip() if title_idx < len(row) else "",
#                     "email": email,
#                     "linkedin_url": row[linkedin_idx].strip() if linkedin_idx >= 0 and linkedin_idx < len(row) else "",
#                     "job_score": row[job_score_idx].strip() if job_score_idx >= 0 and job_score_idx < len(row) else "",
#                     "outreach_email": row[outreach_idx].strip() if outreach_idx < len(row) else ""
#                 }
                
#                 contacts.append(contact)
#             except Exception as e:
#                 log.warning(f"⚠️  Skipped row {row_idx}: {e}")
#                 continue
        
#         log.info(f"✅ Loaded {len(contacts)} contacts from sheet")
#         return contacts
        
#     except Exception as e:
#         log.error(f"❌ Failed to read sheet: {e}")
#         return []

# # ═══════════════════════════════════════════════════════════════════════════════
# # EMAIL PERSONALIZATION
# # ═══════════════════════════════════════════════════════════════════════════════

# def personalize_email(subject, body, contact, sender):
#     """Replace placeholders with actual values"""
    
#     subject = subject.replace("[Hiring Manager's Name]", contact["name"])
#     subject = subject.replace("[Your Name]", sender["name"])
#     subject = subject.replace("[Your Position]", "Consultant")
#     subject = subject.replace("[Company]", contact["company"])
    
#     body = body.replace("[Hiring Manager's Name]", contact["name"])
#     body = body.replace("[Your Name]", sender["name"])
#     body = body.replace("[Your Position]", "Consultant")
#     body = body.replace("[Company]", contact["company"])
#     body = body.replace("[Your Contact Information]", sender["email"])
#     body = body.replace("[Job Title]", contact["job_role"])
    
#     return subject, body

# # ═══════════════════════════════════════════════════════════════════════════════
# # EMAIL SENDING
# # ═══════════════════════════════════════════════════════════════════════════════

# def send_email(sender, recipient_email, subject, body, reply_to):
#     """Send email via SMTP"""
#     try:
#         msg = MIMEMultipart()
#         msg["From"] = f"{sender['name']} <{sender['email']}>"
#         msg["To"] = recipient_email
#         msg["Subject"] = subject
#         if reply_to:
#             msg["Reply-To"] = reply_to
        
#         msg.attach(MIMEText(body, "html"))
        
#         with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=15) as server:
#             server.starttls()
#             server.login(sender["email"], sender["password"])
#             server.send_message(msg)
        
#         return True, None
        
#     except Exception as e:
#         return False, str(e)

# # ═══════════════════════════════════════════════════════════════════════════════
# # MONGODB LOGGING
# # ═══════════════════════════════════════════════════════════════════════════════

# def get_mongodb():
#     """Connect to MongoDB"""
#     try:
#         client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
#         client.admin.command("ping")
#         db = client[MONGO_DB]
#         return db
#     except Exception as e:
#         log.warning(f"⚠️  MongoDB unavailable: {e}")
#         return None

# def log_to_mongodb(db, contact, sender, subject, body, success, error):
#     """Log email send attempt to MongoDB"""
#     if not db:
#         return
    
#     try:
#         doc = {
#             "timestamp": datetime.now(timezone.utc),
#             "contact_name": contact["name"],
#             "contact_email": contact["email"],
#             "company": contact["company"],
#             "sender_name": sender["name"],
#             "sender_email": sender["email"],
#             "subject": subject[:100],
#             "body_preview": body[:200],
#             "status": "sent" if success else "failed",
#             "error": error,
#             "sheet_row": contact["row_number"]
#         }
        
#         db.google_sheet_emails.insert_one(doc)
#     except Exception as e:
#         log.warning(f"⚠️  MongoDB logging failed: {e}")

# # ═══════════════════════════════════════════════════════════════════════════════
# # MARK SENT IN SHEET
# # ═══════════════════════════════════════════════════════════════════════════════

# def mark_sent_in_sheet(worksheet, row_number, success):
#     """Mark email as sent in the sheet"""
#     try:
#         # Find "Sent" column
#         headers = worksheet.row_values(1)
#         if "Sent" in headers:
#             sent_col = headers.index("Sent") + 1
#             status = "✅ Sent" if success else "❌ Failed"
#             worksheet.update_cell(row_number, sent_col, status)
#     except Exception as e:
#         log.warning(f"⚠️  Could not mark sent in sheet: {e}")

# # ═══════════════════════════════════════════════════════════════════════════════
# # MAIN FUNCTION
# # ═══════════════════════════════════════════════════════════════════════════════

# def main():
#     log.info("\n" + "="*70)
#     log.info("📧 GOOGLE SHEET EMAIL SENDER")
#     log.info("="*70 + "\n")
    
#     # Authenticate with Google Sheets
#     gc = authenticate_google_sheets()
#     if not gc:
#         log.error("❌ Cannot proceed without Google Sheets access")
#         return
    
#     # Connect to MongoDB
#     db = get_mongodb()
#     if db:
#         log.info("✅ MongoDB connected")
#     else:
#         log.warning("⚠️  MongoDB unavailable (will continue without logging)")
    
#     # Read contacts from sheet
#     contacts = read_contacts_from_sheet(gc)
#     if not contacts:
#         log.error("❌ No contacts to send")
#         return
    
#     # Get worksheet for marking sent
#     try:
#         spreadsheet = gc.open_by_key(SHEET_ID)
#         worksheet = spreadsheet.worksheet(SHEET_TAB)
#     except Exception as e:
#         log.warning(f"⚠️  Could not open worksheet for updates: {e}")
#         worksheet = None
    
#     log.info(f"\n{'='*70}")
#     log.info(f"📊 SENDING STATS")
#     log.info(f"{'='*70}")
#     log.info(f"Total contacts: {len(contacts)}")
#     log.info(f"Batch size: {BATCH_SIZE} email(s)")
#     log.info(f"Batch interval: {BATCH_INTERVAL_SECONDS}s ({BATCH_INTERVAL_SECONDS/60:.1f}min)")
#     log.info(f"Estimated time: {len(contacts) * BATCH_INTERVAL_SECONDS / 3600:.1f} hours")
#     log.info(f"\n{'='*70}\n")
    
#     # Send emails in batches
#     sender_index = 0
#     sent_count = 0
#     failed_count = 0
    
#     for batch_start in range(0, len(contacts), BATCH_SIZE):
#         batch = contacts[batch_start:batch_start + BATCH_SIZE]
        
#         for contact in batch:
#             try:
#                 # Get sender
#                 sender = SENDER_ACCOUNTS[sender_index % len(SENDER_ACCOUNTS)]
#                 sender_index += 1
                
#                 # Get email content from sheet
#                 subject = contact["outreach_email"].split("\n")[0] if contact["outreach_email"] else "Security Opportunity"
#                 body = contact["outreach_email"]
                
#                 # Personalize
#                 subject, body = personalize_email(subject, body, contact, sender)
                
#                 # Send email
#                 log.info(f"\n📧 Sending to: {contact['name']} <{contact['email']}>")
#                 log.info(f"   From: {sender['name']}")
#                 log.info(f"   Company: {contact['company']}")
                
#                 success, error = send_email(
#                     sender,
#                     contact["email"],
#                     subject,
#                     body,
#                     sender["reply_to"]
#                 )
                
#                 if success:
#                     log.info(f"   ✅ SENT")
#                     sent_count += 1
                    
#                     # Mark in sheet
#                     if worksheet:
#                         mark_sent_in_sheet(worksheet, contact["row_number"], True)
#                 else:
#                     log.error(f"   ❌ FAILED: {error}")
#                     failed_count += 1
                    
#                     # Mark in sheet
#                     if worksheet:
#                         mark_sent_in_sheet(worksheet, contact["row_number"], False)
                
#                 # Log to MongoDB
#                 if db:
#                     log_to_mongodb(db, contact, sender, subject, body, success, error)
                
#             except Exception as e:
#                 log.error(f"❌ Error processing contact: {e}")
#                 failed_count += 1
        
#         # Wait before next batch
#         if batch_start + BATCH_SIZE < len(contacts):
#             log.info(f"\n⏳ Waiting {BATCH_INTERVAL_SECONDS}s before next batch...")
#             time.sleep(BATCH_INTERVAL_SECONDS)
    
#     # Final summary
#     log.info(f"\n{'='*70}")
#     log.info(f"✅ COMPLETE")
#     log.info(f"{'='*70}")
#     log.info(f"📊 Results:")
#     log.info(f"   ✅ Sent: {sent_count}")
#     log.info(f"   ❌ Failed: {failed_count}")
#     log.info(f"   📈 Total: {sent_count + failed_count}")
#     log.info(f"\n{'='*70}\n")

# if __name__ == "__main__":
#     main()
















#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ═══════════════════════════════════════════════════════════════════════════════
# google_sheet_email_sender.py - CORRECTED VERSION
# ═══════════════════════════════════════════════════════════════════════════════

import os
import sys
import time
import smtplib
import logging
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

# Handle Unicode on Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

try:
    from pymongo import MongoClient
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

try:
    from google.oauth2.service_account import Credentials
    import gspread
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION - CORRECTED
# ═══════════════════════════════════════════════════════════════════════════════

GOOGLE_SHEET_NAME = "linkedin_msg"  # Sheet name (NOT ID)
SHEET_TAB = "master_contacts"       # Tab name

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "secureitlab_job_pipeline")

SMTP_SERVER = "mail.secureitlab.org"
SMTP_PORT = 587

BATCH_SIZE = 1
BATCH_INTERVAL_SECONDS = 300

SENDER_ACCOUNTS = [
    {
        "name": "Ashish",
        "email": "ashish@secureitlab.org",
        "reply_to": "ashish@secureitlabservices.com",
        "password": os.getenv("EMAIL_PASSWORD_1")
    },
    {
        "name": "Yash",
        "email": "yash.rohatgi@secureitlab.org",
        "reply_to": "yash.rohatgi@secureitlabservices.com",
        "password": os.getenv("EMAIL_PASSWORD_2")
    },
    {
        "name": "Abdu",
        "email": "abdu.nafih@secureitlab.org",
        "reply_to": "abdu.nafih@secureitlabservices.com",
        "password": os.getenv("EMAIL_PASSWORD_3")
    },
    {
        "name": "Subeer",
        "email": "subeer@secureitlab.org",
        "reply_to": "subeer@secureitlabservices.com",
        "password": os.getenv("EMAIL_PASSWORD_4")
    },
]

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('google_sheet_sender.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════════════════════

def authenticate_google_sheets():
    """Authenticate with Google Sheets API"""
    if not GSHEETS_AVAILABLE:
        log.error("[ERROR] gspread not installed. Run: pip install gspread google-auth")
        return None
    
    try:
        if os.path.exists("service_account.json"):
            credentials = Credentials.from_service_account_file(
                "service_account.json",
                scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
            )
            log.info("[OK] Authenticated with Google service account")
            return gspread.authorize(credentials)
        else:
            log.error("[ERROR] service_account.json not found in current folder")
            return None
    except Exception as e:
        log.error(f"[ERROR] Google Sheets auth failed: {e}")
        return None

def read_contacts_from_sheet(gc):
    """Read contacts from Google Sheet - CORRECTED"""
    try:
        log.info(f"[INFO] Opening Google Sheet: {GOOGLE_SHEET_NAME}")
        log.info(f"[INFO] Opening tab: {SHEET_TAB}")
        
        # Open by sheet NAME (not ID)
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        worksheet = spreadsheet.worksheet(SHEET_TAB)
        
        all_values = worksheet.get_all_values()
        
        if not all_values:
            log.error("[ERROR] Sheet is empty")
            return []
        
        log.info(f"[INFO] Sheet has {len(all_values)} rows total")
        headers = all_values[2]
        log.info(f"[INFO] Columns found: {headers}")
        
        contacts = []
        
        try:
            job_role_idx = headers.index("Job Role")
            company_idx = headers.index("Company")
            priority_idx = headers.index("Priority")
            name_idx = headers.index("Name")
            title_idx = headers.index("Title / Role")
            email_idx = headers.index("Contact Email")
            outreach_idx = headers.index("Outreach Email (Agent)")
            sent_idx = headers.index("Sent") if "Sent" in headers else -1
        except ValueError as e:
            log.error(f"[ERROR] Required column not found: {e}")
            log.error(f"[ERROR] Available columns: {headers}")
            return []
        
        for row_idx, row in enumerate(all_values[1:], start=2):
            try:
                if len(row) < max(email_idx, outreach_idx) + 1:
                    continue
                
                email = row[email_idx].strip() if email_idx < len(row) else ""
                
                # Skip if no email
                if not email or "@" not in email:
                    continue
                
                # Skip if already sent
                if sent_idx >= 0 and sent_idx < len(row):
                    sent_status = row[sent_idx].strip().lower()
                    if sent_status in ("yes", "true", "sent", "[sent]"):
                        continue
                
                contact = {
                    "row_number": row_idx,
                    "job_role": row[job_role_idx].strip() if job_role_idx < len(row) else "",
                    "company": row[company_idx].strip() if company_idx < len(row) else "",
                    "priority": row[priority_idx].strip() if priority_idx < len(row) else "",
                    "name": row[name_idx].strip() if name_idx < len(row) else "",
                    "title": row[title_idx].strip() if title_idx < len(row) else "",
                    "email": email,
                    "outreach_email": row[outreach_idx].strip() if outreach_idx < len(row) else ""
                }
                
                contacts.append(contact)
            except Exception as e:
                log.warning(f"[SKIP] Row {row_idx}: {str(e)[:60]}")
                continue
        
        log.info(f"[OK] Loaded {len(contacts)} contacts ready to send")
        return contacts
        
    except Exception as e:
        log.error(f"[ERROR] Failed to read sheet: {e}")
        import traceback
        log.error(f"[ERROR] Traceback: {traceback.format_exc()}")
        return []

# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL PERSONALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

def personalize_email(subject, body, contact, sender):
    """Replace placeholders"""
    
    subject = subject.replace("[Hiring Manager's Name]", contact["name"])
    subject = subject.replace("[Your Name]", sender["name"])
    subject = subject.replace("[Your Position]", "Consultant")
    subject = subject.replace("[Company]", contact["company"])
    
    body = body.replace("[Hiring Manager's Name]", contact["name"])
    body = body.replace("[Your Name]", sender["name"])
    body = body.replace("[Your Position]", "Consultant")
    body = body.replace("[Company]", contact["company"])
    
    return subject, body

# ═══════════════════════════════════════════════════════════════════════════════
# SMTP
# ═══════════════════════════════════════════════════════════════════════════════

def send_email(sender, recipient_email, subject, body, reply_to):
    """Send email"""
    try:
        msg = MIMEMultipart()
        msg["From"] = f"{sender['name']} <{sender['email']}>"
        msg["To"] = recipient_email
        msg["Subject"] = subject
        if reply_to:
            msg["Reply-To"] = reply_to
        
        msg.attach(MIMEText(body, "html"))
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=15) as server:
            server.starttls()
            server.login(sender["email"], sender["password"])
            server.send_message(msg)
        
        return True, None
        
    except Exception as e:
        return False, str(e)

# ═══════════════════════════════════════════════════════════════════════════════
# MONGODB
# ═══════════════════════════════════════════════════════════════════════════════

def get_mongodb():
    """Connect to MongoDB"""
    if not MONGODB_AVAILABLE:
        return None
    
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        return db
    except Exception as e:
        log.warning(f"[WARNING] MongoDB unavailable: {str(e)[:60]}")
        return None

def log_to_mongodb(db, contact, sender, subject, body, success, error):
    """Log to MongoDB"""
    if db is None:
        return
    
    try:
        doc = {
            "timestamp": datetime.now(timezone.utc),
            "contact_name": contact["name"],
            "contact_email": contact["email"],
            "company": contact["company"],
            "sender_name": sender["name"],
            "sender_email": sender["email"],
            "subject": subject[:100],
            "status": "sent" if success else "failed",
            "error": error,
            "sheet_row": contact["row_number"]
        }
        
        db.google_sheet_emails.insert_one(doc)
    except Exception as e:
        log.warning(f"[WARNING] MongoDB log failed: {str(e)[:60]}")

# ═══════════════════════════════════════════════════════════════════════════════
# MARK SENT
# ═══════════════════════════════════════════════════════════════════════════════

def mark_sent_in_sheet(worksheet, row_number, success):
    """Mark as sent in sheet"""
    try:
        headers = worksheet.row_values(1)
        if "Sent" in headers:
            sent_col = headers.index("Sent") + 1
            status = "[SENT]" if success else "[FAILED]"
            worksheet.update_cell(row_number, sent_col, status)
    except Exception as e:
        log.warning(f"[WARNING] Could not mark in sheet: {str(e)[:60]}")

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("\n" + "="*70)
    log.info("[START] GOOGLE SHEET EMAIL SENDER")
    log.info("="*70 + "\n")
    
    # Authenticate
    gc = authenticate_google_sheets()
    if gc is None:
        log.error("[ERROR] Cannot proceed without Google Sheets")
        return
    
    # MongoDB
    db = get_mongodb()
    if db is not None:
        log.info("[OK] MongoDB connected")
    else:
        log.warning("[WARNING] MongoDB unavailable")
    
    # Read contacts
    contacts = read_contacts_from_sheet(gc)
    if not contacts:
        log.error("[ERROR] No contacts to send")
        return
    
    # Get worksheet
    try:
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        worksheet = spreadsheet.worksheet(SHEET_TAB)
    except Exception as e:
        log.warning(f"[WARNING] Could not open worksheet for updates: {str(e)[:60]}")
        worksheet = None
    
    # Stats
    log.info("\n" + "="*70)
    log.info("[STATS] SENDING CONFIGURATION")
    log.info("="*70)
    log.info(f"Total contacts: {len(contacts)}")
    log.info(f"Batch size: {BATCH_SIZE} email(s)")
    log.info(f"Batch interval: {BATCH_INTERVAL_SECONDS}s ({BATCH_INTERVAL_SECONDS/60:.1f}min)")
    total_hours = (len(contacts) * BATCH_INTERVAL_SECONDS) / 3600
    log.info(f"Estimated time: {total_hours:.1f} hours ({total_hours/24:.1f} days)")
    log.info("="*70 + "\n")
    
    # Send emails
    sender_index = 0
    sent_count = 0
    failed_count = 0
    
    for batch_start in range(0, len(contacts), BATCH_SIZE):
        batch = contacts[batch_start:batch_start + BATCH_SIZE]
        
        for contact in batch:
            try:
                sender = SENDER_ACCOUNTS[sender_index % len(SENDER_ACCOUNTS)]
                sender_index += 1
                
                subject = contact["outreach_email"].split("\n")[0] if contact["outreach_email"] else "Security Opportunity"
                body = contact["outreach_email"]
                
                subject, body = personalize_email(subject, body, contact, sender)
                
                log.info(f"\n[EMAIL] Sending to: {contact['name']} <{contact['email']}>")
                log.info(f"        From: {sender['name']}")
                log.info(f"        Company: {contact['company']}")
                
                success, error = send_email(
                    sender,
                    contact["email"],
                    subject,
                    body,
                    sender["reply_to"]
                )
                
                if success:
                    log.info(f"        Status: [OK] SENT")
                    sent_count += 1
                    
                    if worksheet:
                        mark_sent_in_sheet(worksheet, contact["row_number"], True)
                else:
                    log.error(f"        Status: [FAILED] {error[:60]}")
                    failed_count += 1
                    
                    if worksheet:
                        mark_sent_in_sheet(worksheet, contact["row_number"], False)
                
                if db is not None:
                    log_to_mongodb(db, contact, sender, subject, body, success, error)
                
            except Exception as e:
                log.error(f"[ERROR] Processing contact: {str(e)[:60]}")
                failed_count += 1
        
        # Wait
        if batch_start + BATCH_SIZE < len(contacts):
            remaining = len(contacts) - (batch_start + BATCH_SIZE)
            log.info(f"\n[WAIT] {BATCH_INTERVAL_SECONDS}s until next... ({remaining} remaining)")
            time.sleep(BATCH_INTERVAL_SECONDS)
    
    # Summary
    log.info("\n" + "="*70)
    log.info("[COMPLETE] ALL EMAILS PROCESSED")
    log.info("="*70)
    log.info(f"[RESULT] Sent: {sent_count}")
    log.info(f"[RESULT] Failed: {failed_count}")
    log.info(f"[RESULT] Total: {sent_count + failed_count}")
    if sent_count + failed_count > 0:
        rate = (sent_count / (sent_count + failed_count)) * 100
        log.info(f"[RESULT] Success Rate: {rate:.1f}%")
    log.info("="*70 + "\n")

if __name__ == "__main__":
    main()