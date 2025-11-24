import pytest
import json
from flask import Flask
from unittest.mock import patch
from pathlib import Path

from app.mail.configuration import MailSender


@pytest.fixture
def app_with_templates(tmp_path):
    """Creates a Flask app with a real temporary template folder."""
    templates = tmp_path / "templates"
    templates.mkdir()

    # Create a minimal real template used in test
    (templates / "report.html").write_text(
        "<html><h1>Report</h1> {{ data.a }} {{ data.b }} </html>",
        encoding="utf-8"
    )

    app = Flask(__name__, template_folder=str(templates))
    app.config["MAIL_SERVER"] = "smtp.example.com"
    app.config["MAIL_PORT"] = 25
    return app


@pytest.fixture
def valid_json(tmp_path):
    f = tmp_path / "report.json"
    f.write_text(json.dumps({"a": 10, "b": 20}), encoding="utf-8")
    return str(f)


@pytest.fixture
def invalid_json(tmp_path):
    f = tmp_path / "broken.json"
    f.write_text("{ bad json }", encoding="utf-8")
    return str(f)


# =====================================================
# SUCCESS CASE — REAL TEMPLATE, NO MOCK render_template
# =====================================================
@patch("app.mail.configuration.Mail.send")   # only mail sending is mocked
def test_send_success(mock_mail_send, app_with_templates, valid_json):
    MailSender(app_with_templates, sender="sender@example.com")

    MailSender.send(
        email="target@example.com",
        subject="Monthly report",
        file=valid_json
    )

    # ensure mail was sent exactly once
    assert mock_mail_send.call_count == 1

    # extract Message object
    message = mock_mail_send.call_args[0][0]

    assert message.subject == "Monthly report"
    assert "Report" in message.html      # our template content
    assert "10" in message.html          # JSON value rendered
    assert "20" in message.html          # another JSON value rendered


# =====================================================
# JSON DECODE ERROR
# =====================================================
def test_send_json_error(app_with_templates, invalid_json):
    MailSender(app_with_templates, sender="sender@example.com")

    with pytest.raises(RuntimeError):
        MailSender.send(
            email="x@example.com",
            subject="report",
            file=invalid_json
        )


# =====================================================
# FILE NOT FOUND
# =====================================================
def test_send_file_missing(app_with_templates):
    MailSender(app_with_templates, sender="sender@example.com")

    with pytest.raises(RuntimeError):
        MailSender.send(
            email="x@example.com",
            subject="report",
            file="missing.json"
        )


# =====================================================
# MAIL SEND ERROR
# =====================================================
@patch("app.mail.configuration.Mail.send", side_effect=Exception("SMTP fail"))
def test_send_mail_error(mock_mail_send, app_with_templates, valid_json):
    MailSender(app_with_templates, sender="sender@example.com")

    with pytest.raises(RuntimeError):
        MailSender.send(
            email="x@example.com",
            subject="report",
            file=valid_json
        )
