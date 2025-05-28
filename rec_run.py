import io, base64, mimetypes, time, sys, re
from pathlib import Path
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from pyresparser import ResumeParser
import pandas as pd
import docx2txt
from pdfminer.high_level import extract_text as extract_pdf_text
from email.mime.text import MIMEText

TOKEN_FILE = "token.json"
SERVICE_ACCOUNT_FILE = "credentials.json"
SHEET_ID = "1BsCSX_JfuXNrVh1YfrGSq2X9rGvedOL3tcBYFfZYgSY"
TAB_NAME = "Applicants"
DRIVE_FOLDER_ID = "1DFM6XOBG8w26_91ttdAbOG9DyXTTtdBd"

ALLOWED_EXT = {".pdf", ".doc", ".docx"}
QUERY = 'has:attachment (filename:pdf OR filename:doc OR filename:docx) (cv OR resume)'
CHECK_EVERY = 300
MAX_FETCH = 50

AUTO_REPLY_SUBJECT = "Application Received - [Your Company Name]"
AUTO_REPLY_BODY_PLAIN = """
Dear {applicant_name},

Thank you for your application. Your resume is currently being reviewed.

We will contact you if your qualifications match our requirements.

Sincerely,
The [Your Company Name] Hiring Team
"""

JOB_KEYWORDS_MAPPING = {
    "Software Engineer": ["software", "dev", "engineer", "backend", "frontend", "developer", "javascript", "python"],
    "Marketing Manager": ["marketing", "manager", "digital marketing", "seo"],
    "Data Analyst": ["data", "analyst", "analytics", "sql", "python"],
}

def gmail_creds():
    scopes = ["https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.send"]
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, scopes)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        Path(TOKEN_FILE).write_text(creds.to_json())
    return creds

def svc_creds(scopes):
    return service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)

def send_acknowledgment_email(gmail_service, recipient_email, subject, body):
    message = MIMEText(body, 'plain')
    message['to'] = recipient_email
    message['from'] = 'me'
    message['subject'] = subject
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    gmail_service.users().messages().send(userId="me", body={'raw': raw_message}).execute()

def get_email_body_text(message_payload) -> str:
    if 'parts' in message_payload:
        for part in message_payload['parts']:
            if part.get('mimeType') == 'text/plain' and 'data' in part['body']:
                return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
    elif 'data' in message_payload.get('body', {}):
        return base64.urlsafe_b64decode(message_payload['body']['data']).decode('utf-8')
    return ""

def infer_job_title(subject_line, email_body, cv_content):
    combined_text = (subject_line + " " + email_body + " " + cv_content).lower()
    for job_title, keywords in JOB_KEYWORDS_MAPPING.items():
        for keyword in keywords:
            if keyword in combined_text:
                return job_title
    return "General Application"

def extract_phone_number(text):
    pattern = r'(\+?[\d\s\-\(\)._]{7,}\d)'
    candidates = re.findall(pattern, text)
    for c in candidates:
        p = re.sub(r'(?!^\+)\D', '', c)
        if 7 <= len(p) <= 15:
            return p
    return ""

def parse_resume(bytes_, filename):
    temp_file = Path("_temp_resume" + Path(filename).suffix)
    temp_file.write_bytes(bytes_)
    try:
        parsed_data = ResumeParser(str(temp_file)).get_extracted_data() or {}
        full_text_content = extract_pdf_text(str(temp_file)) if filename.lower().endswith(".pdf") else docx2txt.process(str(temp_file))
    finally:
        temp_file.unlink(missing_ok=True)

    final_phone = extract_phone_number(full_text_content)

    skills_raw = parsed_data.get("skills", [])
    skills = ", ".join(sorted(set(skills_raw)))

    return {
        "name": parsed_data.get("name", ""),
        "email_cv": parsed_data.get("email", ""),
        "phone": final_phone,
        "skills": skills,
        "full_text_content": full_text_content,
    }

def run_once():
    gmail = build("gmail", "v1", credentials=gmail_creds())
    drive = build("drive", "v3", credentials=svc_creds(["https://www.googleapis.com/auth/drive.file"]))
    sh = build("sheets", "v4", credentials=svc_creds(["https://www.googleapis.com/auth/spreadsheets"]))

    msgs = gmail.users().messages().list(userId="me", q=QUERY, maxResults=MAX_FETCH).execute().get("messages", [])

    for m in msgs:
        md = gmail.users().messages().get(userId="me", id=m["id"]).execute()
        frm, sub, date = [h.get(hdr) for hdr in ["From", "Subject", "Date"] for h in md["payload"]["headers"] if h["name"] == hdr]

        attachment = next((p for p in md["payload"].get("parts", []) if p.get("filename")), None)
        if attachment:
            data = gmail.users().messages().attachments().get(userId="me", messageId=m["id"], id=attachment["body"]["attachmentId"]).execute()
            resume_bytes = base64.urlsafe_b64decode(data["data"])

            parsed = parse_resume(resume_bytes, attachment["filename"])
            cv_link = upload_cv_to_drive(drive, resume_bytes, attachment["filename"])

            send_acknowledgment_email(gmail, parsed["email_cv"], AUTO_REPLY_SUBJECT, AUTO_REPLY_BODY_PLAIN.format(applicant_name=parsed["name"]))

if __name__ == "__main__":
    run_once()
