# src/nexrad_backend/api/app_factory.py

import logging
from flask import Flask
from flask_cors import CORS

# Import config if needed for CORS settings, otherwise defaults are used
# from nexrad_backend import config

log = logging.getLogger(__name__)


def create_app() -> Flask:
    """
    Factory function to create and configure the Flask application instance.

    Returns:
        The configured Flask application.
    """
    app = Flask(__name__)
    log.info("Flask app created.")

    # Configure Cross-Origin Resource Sharing (CORS)
    CORS(
        app,
        origins="*",
        methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type"],
        max_age=86400,
    )
    log.info("CORS configured for the app.")

    # Import and REGISTER API routes AFTER the app instance is created
    with app.app_context():
        from .routes import api_bp  # Import the blueprint defined in routes.py

        app.register_blueprint(api_bp)  # <<< **** ENSURE THIS LINE EXISTS ****
        log.info("Registered API blueprint.")

    log.info("Flask app configuration complete.")
    return app
