import os
from google.oauth2 import service_account, Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

SHEET_ID = "1BsCSX_JfuXNrVh1YfrGSq2X9rGvedOL3tcBYFfZYgSY"
TAB_NAME = "Applicants"
MAX_EMAILS = 20
QUERY = "CV OR Resume OR Application"

TOKEN_FILE = "token.json"
SERVICE_ACCOUNT_FILE = "credentials.json"


def gmail_creds(scopes):
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, scopes)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds


def sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def fetch_email_rows(max_results=MAX_EMAILS, query=QUERY):
    service = build(
        "gmail", "v1", credentials=gmail_creds(["https://www.googleapis.com/auth/gmail.readonly"]),
    )
    messages = (
        service.users()
        .messages()
        .list(userId="me", q=query, maxResults=max_results)
        .execute()
        .get("messages", [])
    )
    rows = []
    for msg in messages:
        data = service.users().messages().get(userId="me", id=msg["id"]).execute()
        headers = data["payload"]["headers"]
        sender = next((h["value"] for h in headers if h["name"] == "From"), "")
        subject = next((h["value"] for h in headers if h["name"] == "Subject"), "")
        date = next((h["value"] for h in headers if h["name"] == "Date"), "")
        rows.append([sender, subject, date])
    return rows


def ensure_header(service):
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"{TAB_NAME}!A1:C1"
    ).execute()
    if "values" not in result:
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{TAB_NAME}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [["From", "Subject", "Date"]]},
        ).execute()


def append_rows(service, rows):
    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


if __name__ == "__main__":
    email_rows = fetch_email_rows()
    if email_rows:
        sheet = sheets_service()
        meta = sheet.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        titles = [s["properties"]["title"] for s in meta["sheets"]]
        if TAB_NAME not in titles:
            sheet.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": TAB_NAME}}}]},
            ).execute()
        ensure_header(sheet)
        append_rows(sheet, email_rows)
        print(f"Added {len(email_rows)} rows to '{TAB_NAME}'.")
    else:
        print("No matching emails found.")
