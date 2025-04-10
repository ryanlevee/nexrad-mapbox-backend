# src/nexrad_backend/services/metadata_service.py
import os
import json
import datetime
import logging
import re
from typing import List, Dict, Optional, Any

# Import config for bucket names and path prefixes
from nexrad_backend import config

# Import the s3_service to interact with S3
from nexrad_backend.services import s3_service

# Setup logging
log = logging.getLogger(__name__)


# --- Product Code Options (codes/options.json) ---
def get_product_codes(client, bucket: str = config.PROJECT_S3_BUCKET) -> Dict[str, Any]:
    """
    Retrieves the product code options dictionary from S3.

    Args:
        client: Initialized Boto3 S3 client instance (project client).
        bucket: The name of the project S3 bucket.

    Returns:
        The dictionary parsed from codes/options.json, or an empty dict on error/not found.
    """
    key = config.S3_CODES_OPTIONS_FILE
    body_bytes = s3_service.get_s3_object_body(client, bucket, key)
    if body_bytes:
        try:
            return json.loads(body_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            log.error(
                f"Error decoding JSON from s3://{bucket}/{key}: {e}", exc_info=True
            )
    return {}  # Return empty dict if not found or error


def update_product_codes(
    client, bucket: str, code_options_data: Dict[str, Any]
) -> bool:
    """
    Updates the product code options dictionary (options.json) in S3.

    Args:
        client: Initialized Boto3 S3 client instance (project client).
        bucket: The name of the project S3 bucket.
        code_options_data: The full dictionary data to upload.

    Returns:
        True if successful, False otherwise.
    """
    key = config.S3_CODES_OPTIONS_FILE
    success = s3_service.update_json_in_s3(client, bucket, key, code_options_data)
    if success:
        log.info(f"Successfully updated product codes: s3://{bucket}/{key}")
    else:
        log.error(f"Failed to update product codes: s3://{bucket}/{key}")
    return success


# --- File Lists (lists/*.json) ---
def _get_list_file_key(level: int, product: str) -> str:
    """Helper to construct the S3 key for a file list."""
    return os.path.join(
        config.S3_PREFIX_LISTS, f"nexrad_level{level}_{product}_files.json"
    )


def get_file_list(client, bucket: str, level: int, product: str) -> Dict[str, Any]:
    """
    Retrieves a specific product/level file list dictionary from S3.

    Args:
        client: Initialized Boto3 S3 client instance (project client).
        bucket: The name of the project S3 bucket.
        level: The NEXRAD level (2 or 3).
        product: The product name (e.g., 'reflectivity', 'hydrometeor').

    Returns:
        The dictionary parsed from the list file, or an empty dict on error/not found.
    """
    key = _get_list_file_key(level, product)
    body_bytes = s3_service.get_s3_object_body(client, bucket, key)
    if body_bytes:
        try:
            return json.loads(body_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            log.error(
                f"Error decoding JSON from s3://{bucket}/{key}: {e}", exc_info=True
            )
    return {}  # Return empty dict if not found or error


def get_all_file_lists(
    client,
    bucket: str = config.PROJECT_S3_BUCKET,
    products_levels: List[Dict[str, Any]] = [
        {"product": "reflectivity", "level": 2},
        {"product": "hydrometeor", "level": 3},
        {"product": "precipitation", "level": 3},
    ],
) -> Dict[str, Dict[str, Any]]:
    """
    Retrieves and combines file lists for predefined product/level combinations.
    Mirrors the logic for the /list-all/ API endpoint.

    Args:
        client: Initialized Boto3 S3 client instance (project client).
        bucket: The name of the project S3 bucket.
        products_levels: A list of dicts specifying product and level.

    Returns:
        A dictionary where keys are product names and values are their file list dicts.
    """
    all_data = {}
    for item in products_levels:
        product = item["product"]
        level = item["level"]
        log.debug(f"Fetching file list for level {level} product {product}")
        all_data[product] = get_file_list(client, bucket, level, product)
        # Consider handling case where get_file_list returns empty if that's an error state

    log.info("Fetched all primary file lists.")
    return all_data


def _parse_timestamp_from_key(key: str) -> Optional[datetime.datetime]:
    """Attempts to parse a UTC timestamp from L2 or L3 style keys."""
    filename = key.split("/")[-1]
    # Try L2 format: KPDT<YYYYMMDD>_<HHMMSS>...
    match_l2 = re.match(r"^[A-Z]{4}(\d{8})_(\d{6})", filename)
    if match_l2:
        try:
            ts_str = match_l2.group(1) + match_l2.group(2)
            return datetime.datetime.strptime(ts_str, "%Y%m%d%H%M%S").replace(
                tzinfo=datetime.timezone.utc
            )
        except ValueError:
            pass  # Try L3 next

    # Try L3 format: KPDTYYYYMMDDHHMMSS_XXX... (normalized in processing)
    # Or original format: PDT_XXX_YYYY_MM_DD_HHMMSS -> becomes KPDTYYYYMMDDHHMMSS_XXX
    # Assuming the keys in the list *are* the normalized format K<SITE>YYYYMMDDHHMMSS_<CODE>
    match_l3 = re.match(r"^[A-Z]{4}(\d{14})", filename)
    if match_l3:
        try:
            ts_str = match_l3.group(1)
            return datetime.datetime.strptime(ts_str, "%Y%m%d%H%M%S").replace(
                tzinfo=datetime.timezone.utc
            )
        except ValueError:
            pass  # Format mismatch

    log.warning(f"Could not parse timestamp from key: {key}")
    return None


def update_file_list(
    client,
    bucket: str,
    level: int,
    product: str,
    new_files_info: Dict[str, Any],  # Dict like { 'key_base': {'sweeps': count}, ... }
    retention_minutes: int,
) -> bool:
    """
    Updates a specific product/level file list in S3.
    Adds new file info and removes entries older than the retention period.

    Args:
        client: Initialized Boto3 S3 client instance (project client).
        bucket: The name of the project S3 bucket.
        level: The NEXRAD level (2 or 3).
        product: The product name.
        new_files_info: A dictionary containing info about newly processed files.
                        Keys should be the base filename key used in the list.
        retention_minutes: How long (in minutes) to keep entries in the list.

    Returns:
        True if the list was successfully updated in S3, False otherwise.
    """
    key = _get_list_file_key(level, product)
    current_list = get_file_list(client, bucket, level, product)
    updated_list = {}

    # Determine cutoff time based on latest new file or current time
    latest_new_file_time = None
    if new_files_info:
        timestamps = [_parse_timestamp_from_key(k) for k in new_files_info.keys()]
        valid_timestamps = [ts for ts in timestamps if ts is not None]
        if valid_timestamps:
            latest_new_file_time = max(valid_timestamps)

    reference_time = latest_new_file_time or datetime.datetime.now(
        datetime.timezone.utc
    )
    cutoff_time = reference_time - datetime.timedelta(minutes=retention_minutes)
    log.info(f"Pruning file list '{key}' using cutoff time: {cutoff_time} UTC")

    # Prune old entries from the current list
    kept_count = 0
    pruned_count = 0
    for file_key, file_info in current_list.items():
        timestamp = _parse_timestamp_from_key(file_key)
        if timestamp and timestamp >= cutoff_time:
            updated_list[file_key] = file_info
            kept_count += 1
        else:
            pruned_count += 1
            log.debug(f"Pruning old entry: {file_key}")

    log.info(
        f"Kept {kept_count} entries, pruned {pruned_count} entries from existing list '{key}'."
    )

    # Merge new files info
    added_count = 0
    for file_key, file_info in new_files_info.items():
        if file_key not in updated_list:
            added_count += 1
        updated_list[file_key] = file_info  # Add or overwrite

    log.info(
        f"Added/updated {len(new_files_info)} entries (of which {added_count} were new keys) to list '{key}'."
    )

    # Sort the final list by key for consistency (optional but nice)
    sorted_list = dict(sorted(updated_list.items()))

    # Upload the updated list back to S3
    success = s3_service.update_json_in_s3(client, bucket, key, sorted_list)
    if success:
        log.info(f"Successfully updated file list: s3://{bucket}/{key}")
    else:
        log.error(f"Failed to update file list: s3://{bucket}/{key}")
    return success


# --- Update Flags (flags/update_flags.json) ---
def get_flags(client, bucket: str = config.PROJECT_S3_BUCKET) -> Dict[str, Any]:
    """
    Retrieves the update flags dictionary from S3.

    Args:
        client: Initialized Boto3 S3 client instance (project client).
        bucket: The name of the project S3 bucket.

    Returns:
        The dictionary parsed from flags/update_flags.json, or an empty dict on error/not found.
    """
    key = config.S3_FLAGS_FILE
    body_bytes = s3_service.get_s3_object_body(client, bucket, key)
    if body_bytes:
        try:
            return json.loads(body_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            log.error(
                f"Error decoding JSON from s3://{bucket}/{key}: {e}", exc_info=True
            )
    return {}  # Return empty dict if not found or error


def update_flags(client, bucket: str, flags_data: Dict[str, Any]) -> bool:
    """
    Updates the update flags dictionary (update_flags.json) in S3.

    Args:
        client: Initialized Boto3 S3 client instance (project client).
        bucket: The name of the project S3 bucket.
        flags_data: The full dictionary data to upload.

    Returns:
        True if successful, False otherwise.
    """
    key = config.S3_FLAGS_FILE
    success = s3_service.update_json_in_s3(client, bucket, key, flags_data)
    if success:
        log.info(f"Successfully updated flags: s3://{bucket}/{key}")
    else:
        log.error(f"Failed to update flags: s3://{bucket}/{key}")
    return success


def set_update_flag(client, bucket: str, product_type: str) -> bool:
    """
    Sets the update flag for a specific product type to 1 in update_flags.json.

    Args:
        client: Initialized Boto3 S3 client instance (project client).
        bucket: The name of the project S3 bucket.
        product_type: The product name (key under 'updates').

    Returns:
        True if the flags file was successfully updated, False otherwise.
    """
    flags_data = get_flags(client, bucket)
    # Ensure the 'updates' dictionary exists
    updates_dict = flags_data.setdefault("updates", {})
    updates_dict[product_type] = 1  # Set the flag
    log.info(f"Setting update flag for product '{product_type}'")
    return update_flags(client, bucket, flags_data)


# --- Level 3 Code Count Update --- (Can be called after L3 list is updated) ---
def _calculate_code_counts(file_list_dict: Dict[str, Any]) -> Dict[str, int]:
    """Helper to count occurrences of L3 product codes in file list keys."""
    counts: Dict[str, int] = {}
    # Assumes key format like K<SITE>YYYYMMDDHHMMSS_<CODE>
    for key in file_list_dict.keys():
        parts = key.split("_")
        if len(parts) >= 2:
            code = parts[-1]  # Get the last part as the code
            counts[code] = counts.get(code, 0) + 1
        else:
            log.warning(f"Could not extract code from L3 file list key: {key}")
    return counts


def update_level3_product_code_counts(
    client,
    bucket: str,
    product_type: str,
    current_file_list: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Updates the file counts for Level 3 product codes in options.json based
    on the provided or fetched current file list.

    Args:
        client: Initialized Boto3 S3 client instance (project client).
        bucket: The name of the project S3 bucket.
        product_type: The Level 3 product type (e.g., 'hydrometeor').
        current_file_list: The latest file list dict for this product. If None,
                           it will be fetched using get_file_list.

    Returns:
        True if options.json was successfully updated, False otherwise.
    """
    log.info(f"Updating product code counts for '{product_type}' in options.json")
    code_options = get_product_codes(client, bucket)
    if not code_options or product_type not in code_options:
        log.error(
            f"Cannot update counts: Product type '{product_type}' not found in existing code options."
        )
        return False

    if current_file_list is None:
        log.debug(
            f"Fetching current file list for level 3 product {product_type} to calculate counts."
        )
        current_file_list = get_file_list(client, bucket, 3, product_type)

    if not current_file_list:
        log.warning(
            f"File list for {product_type} is empty or unavailable. Counts will be set to 0."
        )
        code_counts = {}
    else:
        code_counts = _calculate_code_counts(current_file_list)
        log.info(f"Calculated counts for {product_type}: {code_counts}")

    # Update counts in the options structure
    updated_count_total = 0
    if product_type in code_options:
        # Ensure options are in a list format as expected
        if isinstance(code_options[product_type], list):
            for option in code_options[product_type]:
                code_value = option.get("value")
                if code_value:
                    new_count = code_counts.get(code_value, 0)
                    option["count"] = new_count
                    updated_count_total += new_count
            log.info(
                f"Updated counts in options structure for {product_type}. Total files counted: {updated_count_total}"
            )
        else:
            log.error(
                f"Structure error: Expected a list for code_options['{product_type}']"
            )
            return False
    else:
        log.warning(
            f"Product type '{product_type}' not found in code_options structure during count update."
        )

    # Save the updated options back to S3
    return update_product_codes(client, bucket, code_options)
