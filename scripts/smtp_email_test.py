# import smtplib
# from email.mime.text import MIMEText
# import os


# pw = open(f'/Users/gregory.hamilton/Desktop/Creds/gmail_app_pw.txt').read()


# sender    = "gregory.hamilton@paramount.com"
# app_pass  = pw
# receiver  = "gregory.hamilton@paramount.com"

# msg = MIMEText("This is a test message.\nSent from Python.")
# msg["Subject"] = "Quick test"
# msg["From"] = sender
# msg["To"] = receiver

# with smtplib.SMTP("smtp.gmail.com", 587) as s:
#     s.starttls()
#     s.login(sender, app_pass)
#     s.send_message(msg)
    

# print("Sent!")




import smtplib
from email.message import EmailMessage
from getpass import getpass

pw = open(f'/Users/gregory.hamilton/Desktop/Creds/gmail_app_pw.txt').read()

# ────────────────────────────────────────────────
# Only change these 4 lines
SENDER      = "gregory.hamilton@paramount.com"                  # ← your gmail
APP_PASSWORD = pw.strip()
RECIPIENTS  = ["gregory.hamilton@paramount.com"]               # ← list, even if one person
SUBJECT     = "Test 2026 — café naïve résumé €"     # ← deliberately dirty
BODY        = "Hello,\n\nNon-breaking space here: <--\nand another: \xa0\n\nRegards"
# ────────────────────────────────────────────────

# Clean addresses & subject aggressively
SENDER = SENDER.strip().encode('ascii', 'ignore').decode('ascii')
RECIPIENTS = [r.strip().encode('ascii', 'ignore').decode('ascii') for r in RECIPIENTS]
SUBJECT = SUBJECT.encode('ascii', 'ignore').decode('ascii')   # fallback — remove fancy chars

msg = EmailMessage()
msg['From']    = SENDER
msg['To']      = ", ".join(RECIPIENTS)
msg['Subject'] = SUBJECT

msg.set_content(BODY)                # UTF-8 by default

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        pw = APP_PASSWORD or getpass("Gmail App Password: ")
        server.login(SENDER, pw)

        # The important part: ALWAYS use send_message with EmailMessage
        server.send_message(msg)

    print("Email sent successfully.")

except UnicodeEncodeError as e:
    print("Still UnicodeEncodeError →", e)
    print("Likely cause: non-ASCII still in From / To addresses or bug in send method")
except smtplib.SMTPAuthenticationError:
    print("Login failed → wrong app password or 2FA not set up correctly")
except Exception as e:
    print(type(e).__name__, "→", str(e))