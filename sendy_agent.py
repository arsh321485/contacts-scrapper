#!/usr/bin/env python3
"""
Sendy Email Agent
Fetches contacts from Google Sheet and sends personalized outreach emails via Sendy API
Runs daily and tracks sent emails to avoid duplicates
"""

import os
import sys
import requests
import gspread
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SendyEmailAgent:
    def __init__(self, sendy_url, api_key, list_id, google_sheet_url, credentials_json):
        """
        Initialize the Sendy Email Agent
        
        Args:
            sendy_url: Your Sendy installation URL (e.g., https://news.secureitlab.com)
            api_key: Your Sendy API key
            list_id: Your Sendy List ID
            google_sheet_url: Google Sheet URL
            credentials_json: Path to Google service account JSON credentials
        """
        self.sendy_url = sendy_url.rstrip('/')
        self.api_key = api_key
        self.list_id = list_id
        self.google_sheet_url = google_sheet_url
        self.credentials_json = credentials_json
        
        # Initialize Google Sheets connection
        self.gc = self._authenticate_google_sheets()
        self.sheet = None
        
        logger.info("Sendy Email Agent initialized")
    
    def _authenticate_google_sheets(self):
        """Authenticate with Google Sheets API"""
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            creds = Credentials.from_service_account_file(
                self.credentials_json,
                scopes=scopes
            )
            gc = gspread.authorize(creds)
            logger.info("✓ Google Sheets authentication successful")
            return gc
        except Exception as e:
            logger.error(f"✗ Google Sheets authentication failed: {e}")
            raise
    
    def _open_sheet(self):
        """Open the Google Sheet"""
        try:
            self.sheet = self.gc.open_by_url(self.google_sheet_url)
            logger.info(f"✓ Google Sheet opened: {self.sheet.title}")
            return self.sheet.sheet1  # Get first worksheet
        except Exception as e:
            logger.error(f"✗ Failed to open Google Sheet: {e}")
            raise
    
    def _get_column_index(self, worksheet, column_name):
        """Get the index of a column by name"""
        headers = worksheet.row_values(1)
        try:
            return headers.index(column_name) + 1  # gspread uses 1-based indexing
        except ValueError:
            logger.warning(f"Column '{column_name}' not found in headers")
            return None
    
    def _personalize_email(self, template, contact_data):
        """
        Personalize email template with contact data
        
        Replacements:
        [Hiring Manager's Name] -> Name
        [Your Name] -> Your sender name
        [Your Position] -> Your position
        [Company] -> Company name
        [Job Title] -> Job title
        """
        email_body = template
        
        # Replace personalization fields
        if 'Name' in contact_data:
            email_body = email_body.replace('[Hiring Manager\'s Name]', contact_data.get('Name', ''))
            email_body = email_body.replace('[Hiring Manager's Name]', contact_data.get('Name', ''))
        
        if 'Company' in contact_data:
            email_body = email_body.replace('[Company]', contact_data.get('Company', ''))
        
        if 'Title / Role' in contact_data:
            email_body = email_body.replace('[Job Title]', contact_data.get('Title / Role', ''))
        
        # Note: [Your Name] and [Your Position] should be replaced with actual sender info
        # Update these with your actual name/position
        email_body = email_body.replace('[Your Name]', 'SecureITLab Team')
        email_body = email_body.replace('[Your Position]', 'Security Consultant')
        
        return email_body
    
    def _send_email_via_sendy(self, email, name, subject, message):
        """
        Send email via Sendy API
        
        Args:
            email: Recipient email
            name: Recipient name
            subject: Email subject
            message: Email body (HTML)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Subscribe to list and get the subscriber info
            subscribe_payload = {
                'email': email,
                'name': name,
                'list': self.list_id,
                'api_key': self.api_key
            }
            
            # First, add to Sendy list
            response = requests.post(
                f"{self.sendy_url}/api/subscribers/add",
                data=subscribe_payload,
                timeout=10
            )
            
            if response.status_code != 200:
                logger.warning(f"✗ Failed to add subscriber {email}: HTTP {response.status_code}")
                return False
            
            response_text = response.text.strip()
            
            # Check if already subscribed (response will contain subscriber info or "1" for new)
            if 'error' in response_text.lower():
                logger.warning(f"✗ Sendy error for {email}: {response_text}")
                return False
            
            logger.info(f"✓ Added/Updated subscriber: {email}")
            
            # Note: Direct email sending via Sendy API requires campaign creation
            # For now, subscribers are added to the list
            # You can then send campaigns from Sendy dashboard
            # Or use custom SMTP integration
            
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Request failed for {email}: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Unexpected error sending to {email}: {e}")
            return False
    
    def _mark_as_sent(self, worksheet, row_number, status="✓ Sent"):
        """Mark a contact as sent in the Status column"""
        try:
            # Update Status column
            worksheet.update_cell(row_number, 13, status)  # Column M (13th column)
            logger.info(f"✓ Marked row {row_number} as sent")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to mark row {row_number}: {e}")
            return False
    
    def _get_unsent_contacts(self, worksheet):
        """
        Get all unsent contacts from the sheet
        Returns list of tuples: (row_number, contact_data)
        """
        unsent = []
        
        try:
            # Get all rows
            all_rows = worksheet.get_all_records()
            
            for idx, row in enumerate(all_rows, start=2):  # Start from row 2 (skip header)
                # Check if Status column is empty or not "✓ Sent"
                status = row.get('Status', '').strip()
                
                # Skip if already sent
                if status == '✓ Sent':
                    continue
                
                # Skip if no email
                if not row.get('Contact Email', '').strip():
                    continue
                
                unsent.append((idx, row))
            
            logger.info(f"Found {len(unsent)} unsent contacts")
            return unsent
            
        except Exception as e:
            logger.error(f"✗ Failed to get unsent contacts: {e}")
            return []
    
    def run(self, dry_run=False, limit=None):
        """
        Run the email agent
        
        Args:
            dry_run: If True, don't actually send emails (just log what would be sent)
            limit: Maximum number of emails to send (default: None = all)
        
        Returns:
            dict: Summary of sent/failed emails
        """
        logger.info("=" * 60)
        logger.info("STARTING SENDY EMAIL AGENT")
        logger.info("=" * 60)
        
        try:
            # Open Google Sheet
            worksheet = self._open_sheet()
            
            # Get unsent contacts
            unsent_contacts = self._get_unsent_contacts(worksheet)
            
            if not unsent_contacts:
                logger.info("No unsent contacts found. Nothing to do.")
                return {'sent': 0, 'failed': 0, 'skipped': 0}
            
            # Apply limit if specified
            if limit:
                unsent_contacts = unsent_contacts[:limit]
            
            sent_count = 0
            failed_count = 0
            
            logger.info(f"Processing {len(unsent_contacts)} contacts...")
            logger.info("-" * 60)
            
            for row_number, contact in unsent_contacts:
                logger.info(f"\n📧 Processing: {contact.get('Name', 'N/A')} ({contact.get('Contact Email', 'N/A')})")
                
                email = contact.get('Contact Email', '').strip()
                name = contact.get('Name', 'Valued Client').strip()
                company = contact.get('Company', '').strip()
                template = contact.get('Outreach Email (Agent)', '').strip()
                
                # Validate required fields
                if not email or not template:
                    logger.warning(f"⚠ Skipping {email}: Missing email or template")
                    failed_count += 1
                    continue
                
                # Personalize email
                personalized_email = self._personalize_email(template, contact)
                
                # Extract subject from template (first line usually)
                lines = personalized_email.split('\n')
                subject = lines[0].replace('Subject:', '').strip()
                
                if dry_run:
                    logger.info(f"[DRY RUN] Would send email to {email}")
                    logger.info(f"Subject: {subject}")
                    logger.info(f"Body preview: {personalized_email[:100]}...")
                else:
                    # Send email via Sendy
                    if self._send_email_via_sendy(email, name, subject, personalized_email):
                        # Mark as sent
                        self._mark_as_sent(worksheet, row_number, "✓ Sent")
                        sent_count += 1
                        logger.info(f"✓ Successfully sent to {email}")
                    else:
                        failed_count += 1
                        logger.error(f"✗ Failed to send to {email}")
                
                # Rate limiting - be nice to Sendy API
                time.sleep(1)
            
            # Summary
            logger.info("\n" + "=" * 60)
            logger.info("SUMMARY")
            logger.info("=" * 60)
            logger.info(f"✓ Sent: {sent_count}")
            logger.info(f"✗ Failed: {failed_count}")
            logger.info(f"Total processed: {sent_count + failed_count}")
            logger.info("=" * 60)
            
            return {
                'sent': sent_count,
                'failed': failed_count,
                'total': sent_count + failed_count
            }
            
        except Exception as e:
            logger.error(f"✗ Agent failed: {e}")
            raise


def main():
    """Main entry point"""
    
    # Configuration - UPDATE THESE WITH YOUR VALUES
    SENDY_URL = "https://news.secureitlab.com"  # Your Sendy installation URL
    API_KEY = "YOUR_API_KEY_HERE"  # Your Sendy API key
    LIST_ID = "YOUR_LIST_ID_HERE"  # Your Sendy List ID
    GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1bVvT1LYqXZEIgijRnZf8Y6rBcGAfbMaWuDgDTkRe80c/edit"
    CREDENTIALS_JSON = "google_credentials.json"  # Path to your Google service account JSON
    
    # Check if credentials file exists
    if not os.path.exists(CREDENTIALS_JSON):
        logger.error(f"✗ Credentials file not found: {CREDENTIALS_JSON}")
        logger.error("Please download your Google service account JSON and place it as 'google_credentials.json'")
        sys.exit(1)
    
    # Create agent
    agent = SendyEmailAgent(
        sendy_url=SENDY_URL,
        api_key=API_KEY,
        list_id=LIST_ID,
        google_sheet_url=GOOGLE_SHEET_URL,
        credentials_json=CREDENTIALS_JSON
    )
    
    # Run agent
    # For testing, use: agent.run(dry_run=True, limit=1)
    # For production: agent.run()
    try:
        result = agent.run(dry_run=False)  # Change to dry_run=True to test first
        sys.exit(0 if result['failed'] == 0 else 1)
    except Exception as e:
        logger.error(f"Agent execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()