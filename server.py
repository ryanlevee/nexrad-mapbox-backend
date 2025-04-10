import logging
import os
import sys

# --- Imports ---
try:
    from waitress import serve
    from nexrad_backend.api.app_factory import create_app
    from nexrad_backend import config  # To get host and port
except ImportError as e:
    logging.basicConfig(
        level=logging.CRITICAL
    )  # Setup basic logging just to show the error
    logging.exception("ImportError: Failed to import necessary modules.")
    logging.critical(
        "Ensure 'waitress' is installed (`pip install waitress`) and you have run 'pip install -e .' from the project root in your virtual environment."
    )
    sys.exit(1)

# --- Logging Configuration ---
# Might want more sophisticated logging config here eventually
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# --- Server Execution ---
if __name__ == "__main__":
    try:
        app = create_app()

        host = config.API_HOST
        port = config.API_PORT

        # Define Waitress options (add others as needed)
        # See: https://docs.pylonsproject.org/projects/waitress/en/stable/arguments.html
        waitress_options = {
            "backlog": 2048, 
            "connection_limit": 400, 
            "threads": 4,  # Example: Number of worker threads (adjust as needed)
            # 'url_scheme': 'https'   # If behind a reverse proxy handling HTTPS
        }

        log.info(f"Starting Waitress server for nexrad-mapbox-backend...")
        log.info(f"Listening on http://{host}:{port}")
        log.info(f"Waitress options: {waitress_options}")

        # Pass options to serve using dictionary unpacking (**)
        serve(app, host=host, port=port, **waitress_options)

    except NameError as e:
        # Catch potential NameError if imports failed but weren't caught by initial check
        log.critical(f"A required module might be missing or not importable: {e}")
        log.critical("Ensure 'waitress' is installed and `pip install -e .` was run.")
    except Exception as e:
        log.exception("An unexpected error occurred while trying to start the server.")
