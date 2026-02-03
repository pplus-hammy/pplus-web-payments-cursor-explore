#!/usr/bin/env python3
"""
Send email from gregory.hamilton@paramount.com via Gmail SMTP.
App password read from /Users/gregory.hamilton/Desktop/Creds/gmail_app_pw.txt
"""
import smtplib
from email.mime.text import MIMEText

# Config
SENDER = "gregory.hamilton@paramount.com"
APP_PASSWORD_PATH = "/Users/gregory.hamilton/Desktop/Creds/gmail_app_pw.txt"
TO = "gregory.hamilton@paramount.com"
SUBJECT = "Test from send_email.py"
BODY = "Hello,\n\nThis is a test message sent via Gmail SMTP.\n\nRegards"

# Load app password (strip stray non-ASCII e.g. BOM or \xa0 from copy-paste)
with open(APP_PASSWORD_PATH, encoding="utf-8") as f:
    app_password = f.read().strip().replace("\xa0", "")

# Build message (explicit utf-8 so no ASCII codec error)
msg = MIMEText(BODY, "plain", "utf-8")
msg["From"] = SENDER
msg["To"] = TO
msg["Subject"] = SUBJECT

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(SENDER, app_password)
        server.sendmail(SENDER, TO, msg.as_string())
    print("Email sent successfully.")
except smtplib.SMTPAuthenticationError as e:
    print("Login failed:", e)
    exit(1)
except Exception as e:
    print(type(e).__name__, ":", e)
    exit(1)
