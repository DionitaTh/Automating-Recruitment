import io
import base64
import mimetypes
import time
import sys
import re
from pathlib import Path
from google.oauth2 import service_account, Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from pyresparser import ResumeParser
import docx2txt
from pdfminer.high_level import extract_text as extract_pdf_text
from email.mime.text import MIMEText

TOKEN_FILE = "token.json"
CLIENT_SECRET_FILE = "client_secret.json"
SERVICE_ACCOUNT_FILE = "credentials.json"

SHEET_ID = "1BsCSX_JfuXNrVh1YfrGSq2X9rGvedOL3tcBYFfZYgSY"
TAB_NAME = "Applicants"
DRIVE_FOLDER_ID = "1DFM6XOBG8w26_91ttdAbOG9DyXTTtdBd"

ALLOWED_EXT = {".pdf", ".doc", ".docx"}
QUERY = (
    'has:attachment (filename:pdf OR filename:doc OR filename:docx) '
    '(cv OR resume)'
)
CHECK_EVERY = 300
MAX_FETCH = 50

AUTO_REPLY_SUBJECT = "Application Received - [Your Company Name]"
AUTO_REPLY_BODY_PLAIN = """Dear {applicant_name},

Thank you for your interest in [Job Title/Our Company] and for submitting your resume.
We have successfully received your application and it is currently being reviewed.

We appreciate you taking the time to apply. If your qualifications match our requirements,
we will be in touch regarding the next steps in the hiring process.

Due to the high volume of applications, we are unable to respond to each candidate individually
unless they are selected for an interview. We appreciate your understanding.

Sincerely,

The [Your Company Name] Hiring Team
[Your Website/Contact Info - Optional]
"""

JOB_KEYWORDS_MAPPING = {
    "Software Engineer": ["software", "dev", "engineer", "backend", "frontend", "fullstack", "developer", "javascript", "python", "java", "c++", "react", "angular", "node.js"],
    "Marketing Manager": ["marketing", "manager", "campaign", "growth", "brand", "digital marketing", "seo", "sem", "content marketing", "social media"],
    "Product Manager": ["product", "pm", "product management", "roadmap", "ux/ui", "strategy", "agile", "scrum", "product owner"],
    "Data Analyst": ["data", "analyst", "science", "bi", "analytics", "sql", "python", "r", "excel", "tableau", "power bi", "statistics"],
    "Human Resources": ["hr", "human resources", "recruiter", "talent acquisition", "people operations", "onboarding", "employee relations", "benefits"],
    "Sales Representative": ["sales", "rep", "business development", "account executive", "client relations", "crm", "lead generation", "negotiation"],
}


def gmail_creds():
    scopes = [
        "https://www.googleapis.com/auth/gmail.readonly",
        "https://www.googleapis.com/auth/gmail.send",
    ]
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, scopes)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        Path(TOKEN_FILE).write_text(creds.to_json())
    return creds


def svc_creds(scopes):
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=scopes
    )


def send_acknowledgment_email(service, recipient, subject, body):
    message = MIMEText(body, 'plain')
    message['to'] = recipient
    message['from'] = 'me'
    message['subject'] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(
        userId="me", body={'raw': raw}
    ).execute()


def get_email_body_text(payload) -> str:
    if 'parts' in payload:
        for part in payload['parts']:
            if part.get('mimeType') == 'text/plain' and 'data' in part.get('body', {}):
                return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
    body = payload.get('body', {})
    if 'data' in body:
        return base64.urlsafe_b64decode(body['data']).decode('utf-8')
    return ""


def infer_job_title(subject, body, cv_text) -> str:
    text = f"{subject} {body} {cv_text}".lower()
    for title, keywords in JOB_KEYWORDS_MAPPING.items():
        if any(k in text for k in keywords):
            return title
    return "General Application"


def extract_phone_number(text: str) -> str:
    matches = re.findall(r'(\+?[\d\s\-\(\)._]{7,}\d)', text)
    numbers = [re.sub(r'(?!^\+)\D', '', m) for m in matches]
    valid = [n for n in numbers if 7 <= len(n) <= 15]
    for n in valid:
        if n.startswith('+'):
            return n
    return max(valid, key=len) if valid else ""


def parse_resume(data: bytes, filename: str) -> dict:
    temp = Path("_temp" + Path(filename).suffix)
    temp.write_bytes(data)
    try:
        info = ResumeParser(str(temp)).get_extracted_data() or {}
        text = extract_pdf_text(str(temp)) if filename.lower().endswith('.pdf') else docx2txt.process(str(temp))
    finally:
        temp.unlink(missing_ok=True)

    phone = extract_phone_number(text)
    skills = info.get('skills') or []
    if isinstance(skills, str):
        skills = [s.strip() for s in skills.split(',')]
    skills_text = ", ".join(sorted(set(skills)))

    education_entries = info.get('education') or []
    if isinstance(education_entries, str):
        education_entries = [e.strip() for e in education_entries.split(',')]
    latest_edu = max(education_entries, key=lambda e: re.search(r'(19|20)\d{2}', e).group(0) if re.search(r'(19|20)\d{2}', e) else 0, default="")

    return {
        'name': info.get('name', ''),
        'email_cv': info.get('email', ''),
        'phone': phone,
        'skills': skills_text,
        'education': latest_edu,
        'full_text_content': text,
    }


def fetch_messages(service):
    return service.users().messages().list(
        userId="me", q=QUERY, maxResults=MAX_FETCH
    ).execute().get('messages', [])


def read_headers(md):
    hdr = {h['name']: h['value'] for h in md['payload']['headers']}
    return hdr.get('From', ''), hdr.get('Subject', ''), hdr.get('Date', '')


def upload_cv_to_drive(service, data: bytes, filename: str) -> str:
    mime = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime)
    file = service.files().create(
        media_body=media,
        body={'name': filename, 'parents': [DRIVE_FOLDER_ID]},
        fields='webViewLink'
    ).execute()
    return file['webViewLink']


def get_sheet_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    return build('sheets', 'v4', credentials=svc_creds(scopes))


def ensure_header(sheet):
    header = [
        'MsgID', 'From', 'Subject', 'Date', 'CV Link', 'Name', 'Email (CV)',
        'Phone', 'Skills', 'Job Applied For', 'Status', 'Education', 'Acknowledgment Email Sent'
    ]
    resp = sheet.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{TAB_NAME}!A1:Z1"
    ).execute()
    if 'values' not in resp or len(resp['values'][0]) < len(header):
        sheet.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{TAB_NAME}!A1",
            valueInputOption='USER_ENTERED',
            body={'values': [header]}
        ).execute()


def existing_applicants_data(sheet):
    resp = sheet.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{TAB_NAME}!A2:M"
    ).execute()
    ids, email_links = set(), {}
    for row in resp.get('values', []):
        msg_id = row[0] if len(row) > 0 else ''
        name = row[5].strip().lower() if len(row) > 5 else ''
        email = row[6].strip().lower() if len(row) > 6 else ''
        phone = row[7].strip().lower() if len(row) > 7 else ''
        link = row[4] if len(row) > 4 else ''
        if msg_id:
            ids.add((msg_id, 'msg_id'))
        if name and email:
            ids.add((name, email, 'name_email'))
            if email not in email_links and link:
                email_links[email] = link
        if phone:
            ids.add((phone, 'phone_only'))
    return ids, email_links


def append_rows(sheet, rows):
    if rows:
        sheet.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{TAB_NAME}!A1",
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={'values': rows}
        ).execute()


def run_once():
    gmail = build('gmail', 'v1', credentials=gmail_creds())
    drive = build('drive', 'v3', credentials=svc_creds([
        'https://www.googleapis.com/auth/drive.file'
    ]))
    sheet = get_sheet_service()
    ensure_header(sheet)
    ids, links = existing_applicants_data(sheet)
    seen = {i[0] for i in ids if i[1] == 'msg_id'}
    rows = []
    for msg in fetch_messages(gmail):
        if msg['id'] in seen:
            continue
        md = gmail.users().messages().get(userId='me', id=msg['id']).execute()
        frm, sub, date = read_headers(md)
        body_text = get_email_body_text(md['payload'])
        attachment_bytes, filename = None, None
        for part in md['payload'].get('parts', []):
            if part.get('filename') and Path(part['filename']).suffix.lower() in ALLOWED_EXT:
                att = gmail.users().messages().attachments().get(
                    userId='me', messageId=msg['id'], id=part['body']['attachmentId']
                ).execute()
                attachment_bytes = base64.urlsafe_b64decode(att['data'])
                filename = part['filename']
                break
        if not attachment_bytes:
            continue
        parsed = parse_resume(attachment_bytes, filename)
        email = parsed['email_cv'] or re.search(r'<([^>]+)>', frm).group(1) if '<' in frm else frm
        name_lower = parsed['name'].strip().lower()
        if (name_lower, email.lower(), 'name_email') in ids or (parsed['phone'], 'phone_only') in ids:
            continue
        link = upload_cv_to_drive(drive, attachment_bytes, filename)
        status = 'Yes' if send_acknowledgment_email(gmail, email, AUTO_REPLY_SUBJECT,
                                                 AUTO_REPLY_BODY_PLAIN.format(applicant_name=parsed['name'].split()[0])) else 'No'
        job = infer_job_title(sub, body_text, parsed['full_text_content'])
        phone = parsed['phone']
        phone = f"'{phone}" if phone and not phone.startswith("'") else phone
        rows.append([
            msg['id'], frm, sub, date, link,
            parsed['name'], parsed['email_cv'], phone,
            parsed['skills'], job, 'New Application',
            parsed['education'], status
        ])
        seen.add(msg['id'])
    append_rows(sheet, rows)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        run_once()
        sys.exit()
    while True:
        try:
            run_once()
        except HttpError:
            pass
        except Exception:
            pass
        time.sleep(CHECK_EVERY)
