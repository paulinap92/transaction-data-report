from flask import Flask, render_template, request, jsonify
from flask_apscheduler import APScheduler
from dotenv import load_dotenv
from pathlib import Path
from os import getenv
from datetime import datetime
import logging
import time
import json

from app.file_process.sales_monitor import SalesMonitor
from app.file_process.file_manager import FileManager
from app.mail.configuration import MailSender
from app.model.model_trainer import ModelTrainer


# -----------------------------------------------------------------------------------------
# Logging configuration
# -----------------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def maybe_retrain_models():
    """
    Check whether the ML models require retraining.

    The system maintains a file (`data/last_retrain.txt`) storing the date of the last
    retraining. If 7 or more days have passed since the previous retrain, all models are
    retrained and the date is updated.

    Why this exists:
        - Ensures long-running application deployments keep models up to date.
        - Avoids unnecessary retraining on every startup.
    """
    today = datetime.now().date()
    last_path = Path("data/last_retrain.txt")

    try:
        if last_path.exists():
            # Load last retrain date and calculate elapsed days
            last_date = datetime.strptime(last_path.read_text().strip(), "%Y-%m-%d").date()
            days_passed = (today - last_date).days
            logger.info(f"🧠 Last retrain was {days_passed} days ago ({last_date}).")
        else:
            # No retrain history — force an initial run
            logger.info("📄 No last_retrain.txt found — initial retrain required.")
            days_passed = 999

        # Retrain models only if sufficient time has elapsed
        if days_passed >= 7:
            logger.info("🚀 Retraining models for all categories...")
            trainer = ModelTrainer()
            trainer.retrain_all_categories()

            # Persist the new retrain date
            last_path.parent.mkdir(parents=True, exist_ok=True)
            last_path.write_text(str(today))
            logger.info(f"✅ Retraining complete. Date saved to {last_path}")
        else:
            logger.info("⏳ Retraining skipped — threshold not met.")

    except Exception as e:
        logger.exception(f"❌ Error while checking retraining logic: {e}")


# -----------------------------------------------------------------------------------------
# Files and directories configuration
# -----------------------------------------------------------------------------------------
DIRECTORY_TO_WATCH = "app/data/Sabores Ibéricos Company Transaction Data"
LAST_FILE_RECORD = "app/data/Sabores Ibéricos Company Transaction Data/last_processed.txt"
REPORT_FILE = "sales_report.csv"
REPORT_JSON_PATH = "sales_report.json"


def create_app() -> Flask:
    """
    Factory function that creates and configures the Flask application.

    Responsibilities:
        - Load environment variables and configure mail service.
        - Initialize scheduled background tasks via APScheduler.
        - Register health and reporting endpoints.
        - Monitor a directory for incoming sales transaction files.
        - Periodically send generated sales reports via email.
        - Trigger weekly model retraining.

    Returns:
        Flask: Fully configured Flask app instance ready for deployment.
    """
    # Initialize Flask application and Jinja templates path
    app = Flask(__name__, template_folder="app/templates")

    @app.route("/health", methods=["GET"])
    def health():
        """Simple health check endpoint used by monitoring systems."""
        return {"status": "ok"}

    @app.get("/report")
    def report_page():
        """
        Render the latest sales report.

        Implements a short retry mechanism to avoid race conditions when
        the report file is being written at the time of access.

        Returns:
            HTML page with the report, or a JSON error if the report is not ready.
        """
        p = Path(REPORT_JSON_PATH)

        if not p.exists():
            return jsonify({"error": f"report not found: {p}"}), 404

        attempts, last_err = 3, None
        for _ in range(attempts):
            try:
                if p.stat().st_size == 0:
                    raise ValueError("empty file")

                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                return render_template(
                    "report.html",
                    data=data,
                    as_email=False,
                    generation_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )

            except (json.JSONDecodeError, ValueError) as e:
                last_err = e
                time.sleep(0.15)  # allow file writing to complete

        return jsonify({"error": f"report not ready: {last_err}"}), 503

    # ------------------------------------------------------------------------------
    # Application context initialization
    # ------------------------------------------------------------------------------
    with app.app_context():
        ENV_PATH = Path.cwd().absolute().joinpath('.env')
        load_dotenv(ENV_PATH)

        # Mail configuration from .env
        mail_settings = {
            'MAIL_SERVER': getenv('MAIL_SERVER', 'smtp.gmail.com'),
            'MAIL_PORT': int(getenv('MAIL_PORT', 465)),
            'MAIL_USE_SSL': bool(getenv('MAIL_USE_SSL', True)),
            'MAIL_USERNAME': getenv('MAIL_USERNAME'),
            'MAIL_PASSWORD': getenv('MAIL_PASSWORD'),
        }

        if not mail_settings['MAIL_USERNAME'] or not mail_settings['MAIL_PASSWORD']:
            raise ValueError("Email credentials must be defined in the .env file.")

        logging.info(mail_settings)
        app.config.update(mail_settings)
        mail = MailSender(app, getenv('MAIL_USERNAME'))

        recipient_email = getenv('RECIPIENT_EMAIL')
        if not recipient_email:
            raise ValueError("RECIPIENT_EMAIL must be set in the .env file.")

        def send_transaction_report():
            """
            Send the latest sales report via email.

            Attached report is generated independently by the scheduled data-processing job.
            """
            with app.app_context():
                mail.send(recipient_email, 'Transaction Report', 'sales_report.json')

        # ------------------------------------------------------------------------------
        # Scheduler initialization and job registration
        # ------------------------------------------------------------------------------
        scheduler = APScheduler()
        scheduler.init_app(app)
        scheduler.start()

        sales_monitor = SalesMonitor(DIRECTORY_TO_WATCH, LAST_FILE_RECORD)

        scheduler.add_job(id='monitor_files', func=sales_monitor.process_new_files,
                          trigger='interval', seconds=60)

        scheduler.add_job(id='send_report', func=send_transaction_report,
                          trigger='interval', seconds=360)

        scheduler.add_job(id='retrain', func=maybe_retrain_models,
                          trigger='cron', day_of_week='sat', hour=10)

        return app
