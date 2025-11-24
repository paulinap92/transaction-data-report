from flask import Flask, render_template
from flask_mail import Mail, Message
import json

class MailSender:
    _mail = None
    _sender = None
    _app = None  # ⬅️ dodajemy to

    def __init__(self, app: Flask, sender: str) -> None:
        MailSender._mail = Mail(app)
        MailSender._sender = sender
        MailSender._app = app  # ⬅️ zapamiętujemy app do kontekstu

    @classmethod
    def send(cls, email: str, subject: str, file: str) -> None:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Error reading or parsing the file {file}: {e}")

        # ✅ Jeden szablon dla maila i WWW
        with cls._app.app_context():
            mail_content = render_template("report.html", data=json_data, as_email=True)

        try:
            message = Message(
                subject=subject,
                sender=cls._sender,
                recipients=[email],
                html=mail_content
            )
            cls._mail.send(message)
        except Exception as e:
            raise RuntimeError(f"Error sending email to {email}: {e}")
