# src/nexrad_backend/api/routes.py

import io
import json
import logging
import os
from flask import Blueprint, jsonify, abort, request, send_file, current_app

# Import services and config
from nexrad_backend import config
from nexrad_backend.services import s3_service, metadata_service

log = logging.getLogger(__name__)

# Using a Blueprint is good for organizing routes
api_bp = Blueprint(
    "api", __name__, url_prefix=""
)  # No prefix needed if mounted at root


# --- API Endpoints ---
@api_bp.route("/code/", methods=["GET"])
def handle_code_get():
    """Retrieves the product code options JSON."""
    log.info("Request received for /code/")
    s3_client = config.get_project_s3_client()
    bucket = config.PROJECT_S3_BUCKET

    code_options = metadata_service.get_product_codes(s3_client, bucket)

    if code_options:
        log.info("Product codes retrieved successfully.")
        return jsonify(code_options), 200
    else:
        log.warning(
            f"Product codes file not found or empty: s3://{bucket}/{config.S3_CODES_OPTIONS_FILE}"
        )
        abort(404, description="Product code options not found.")


@api_bp.route("/flag/", methods=["GET"])
def handle_flag_get():
    """Retrieves the update flags JSON."""
    log.info("Request received for GET /flag/")
    s3_client = config.get_project_s3_client()
    bucket = config.PROJECT_S3_BUCKET

    flags_data = metadata_service.get_flags(s3_client, bucket)

    # Even if empty (file not found by service), return it as JSON
    log.info("Update flags retrieved.")
    return jsonify(flags_data), 200


@api_bp.route("/flag/", methods=["POST"])
def handle_flag_post():
    """Updates the update flags JSON."""
    log.info("Request received for POST /flag/")
    s3_client = config.get_project_s3_client()
    bucket = config.PROJECT_S3_BUCKET

    body = request.get_json()
    if not body:
        log.error("POST /flag/ request body is empty or not valid JSON.")
        return jsonify({"error": "Request body is empty or not valid JSON"}), 400

    try:
        success = metadata_service.update_flags(s3_client, bucket, body)
        if success:
            log.info(
                f"Flags file updated successfully: s3://{bucket}/{config.S3_FLAGS_FILE}"
            )
            return (
                jsonify(
                    {
                        "updated": True,
                        "message": f"'{config.S3_FLAGS_FILE}' updated in S3!",
                    }
                ),
                200,
            )
        else:
            log.error(f"Failed to update flags file via metadata_service.")
            # Error logged in service layer
            return jsonify({"error": "Failed to update flags file in S3"}), 500
    except Exception as e:
        log.exception(f"Unexpected error processing POST /flag/: {e}")
        return jsonify({"error": "Internal Server Error"}), 500


@api_bp.route("/list/<level>/<product>/", methods=["GET"])
def handle_list_get(level, product):
    """Retrieves the file list JSON for a specific level and product."""
    log.info(f"Request received for /list/{level}/{product}/")
    s3_client = config.get_project_s3_client()
    bucket = config.PROJECT_S3_BUCKET

    try:
        level_int = int(level)
        if level_int not in [2, 3]:
            raise ValueError("Level must be 2 or 3")
    except ValueError:
        log.error(f"Invalid level provided in request: {level}")
        abort(400, description="Invalid level provided. Must be 2 or 3.")
        return  # Make linters happy

    # Basic validation for product name (prevent path traversal etc.)
    # Allow alphanumeric and underscores
    if not product or not product.isalnum() and "_" not in product:
        log.error(f"Invalid product name provided: {product}")
        abort(400, description="Invalid product name provided.")
        return

    file_list = metadata_service.get_file_list(s3_client, bucket, level_int, product)

    if file_list:
        log.info(f"File list for level {level} product {product} retrieved.")
        return jsonify(file_list), 200
    else:
        list_key = metadata_service._get_list_file_key(
            level_int, product
        )  # Use helper for logging
        log.warning(f"File list not found or empty: s3://{bucket}/{list_key}")
        # Return 404 if specifically not found, though service currently returns {}
        # Return empty dict with 200 for consistency with original behaviour if service doesn't raise error
        return jsonify({}), 200
        # If you prefer 404 when empty:
        # abort(404, description=f"File list for level {level} product {product} not found.")


@api_bp.route("/list-all/", methods=["GET"])
def handle_list_all_get():
    """Retrieves and combines file lists for primary products."""
    log.info("Request received for /list-all/")
    s3_client = config.get_project_s3_client()
    bucket = config.PROJECT_S3_BUCKET

    try:
        # Let the service handle fetching the predefined lists
        combined_lists = metadata_service.get_all_file_lists(s3_client, bucket)
        log.info("Combined file lists retrieved.")
        return jsonify(combined_lists), 200
    except Exception as e:
        log.exception(f"Unexpected error processing /list-all/: {e}")
        # This might catch errors during S3 calls within the service if not handled there
        abort(500, description="Internal server error retrieving file lists.")


@api_bp.route("/data/<level>/<path:file_key>/<file_ext>", methods=["GET"])
def handle_data_get(level, file_key, file_ext):
    """Retrieves a specific data file (PNG image or JSON metadata)."""
    log.info(f"Request received for /data/{level}/{file_key}/{file_ext}")
    s3_client = config.get_project_s3_client()
    bucket = config.PROJECT_S3_BUCKET

    # Validate level
    try:
        level_int = int(level)
        if level_int == 2:
            plot_prefix = config.S3_PREFIX_PLOTS_L2
        elif level_int == 3:
            plot_prefix = config.S3_PREFIX_PLOTS_L3
        else:
            raise ValueError("Level must be 2 or 3")
    except ValueError:
        log.error(f"Invalid level provided in data request: {level}")
        abort(400, description="Invalid level provided. Must be 2 or 3.")
        return

    # Validate file extension
    if file_ext not in ["png", "json"]:
        log.error(f"Invalid file extension requested: {file_ext}")
        abort(400, description="Invalid file extension. Must be 'png' or 'json'.")
        return

    # Basic validation for file_key (prevent directory traversal etc.)
    # This assumes file_key itself doesn't contain '../' etc.
    # More robust validation might be needed depending on key generation.
    if not file_key or ".." in file_key:
        log.error(f"Invalid file key requested: {file_key}")
        abort(400, description="Invalid file key.")
        return

    # Construct the full S3 key
    # os.path.join might use backslashes on Windows, ensure forward slashes for S3
    object_key = os.path.join(plot_prefix, f"{file_key}.{file_ext}").replace("\\", "/")
    log.debug(f"Attempting to retrieve data object: s3://{bucket}/{object_key}")

    # Fetch the object body using the s3_service
    body_bytes = s3_service.get_s3_object_body(s3_client, bucket, object_key)

    if body_bytes is None:
        log.warning(f"Data object not found: s3://{bucket}/{object_key}")
        abort(404, description="Requested data file not found.")
        return

    # Process based on extension
    if file_ext == "json":
        try:
            # Decode and parse JSON
            file_content_str = body_bytes.decode("utf-8")
            file_content = json.loads(file_content_str)
            log.info(f"Returning JSON data for key: {object_key}")
            return jsonify(file_content), 200
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            log.exception(f"Error processing JSON data for key {object_key}: {e}")
            abort(500, description="Error processing JSON data.")
            return
    elif file_ext == "png":
        try:
            # Serve the raw bytes as a PNG image
            log.info(f"Returning PNG image data for key: {object_key}")
            return (
                send_file(
                    io.BytesIO(body_bytes),
                    mimetype="image/png",
                    # Optional: Use as_attachment=True and download_name if needed
                ),
                200,
            )
        except Exception as e:
            log.exception(f"Error sending PNG file for key {object_key}: {e}")
            abort(500, description="Error sending image file.")
            return

    # Should not be reached due to earlier validation, but acts as a safeguard
    abort(400, description="Invalid request.")
