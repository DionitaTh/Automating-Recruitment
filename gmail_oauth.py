import os
import base64
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd
from google.oauth2.credentials import Credentials

SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
]

def authenticate_gmail():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token_file:
            token_file.write(creds.to_json())
    return creds

def fetch_emails(creds, query='CV OR Resume OR Application', max_results=10):
    service = build('gmail', 'v1', credentials=creds)
    response = service.users().messages().list(
        userId='me', q=query, maxResults=max_results
    ).execute()
    messages = response.get('messages', [])
    rows = []
    for message in messages:
        msg = service.users().messages().get(
            userId='me', id=message['id']
        ).execute()
        headers = msg['payload']['headers']
        sender = next((h['value'] for h in headers if h['name'] == 'From'), '')
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '')
        date = next((h['value'] for h in headers if h['name'] == 'Date'), '')
        rows.append([sender, subject, date])
    return rows

if __name__ == '__main__':
    print('Authenticating Gmail...')
    credentials = authenticate_gmail()
    print('Fetching emails...')
    email_rows = fetch_emails(credentials)
    df = pd.DataFrame(email_rows, columns=['From', 'Subject', 'Date'])
    print(df)
