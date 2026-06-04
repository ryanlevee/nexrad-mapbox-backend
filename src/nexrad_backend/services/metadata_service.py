import os
import json
import datetime
import logging
import re
from typing import List, Dict, Optional, Any

from nexrad_backend import config
from nexrad_backend.services import drive_service

log = logging.getLogger(__name__)


# --- Product Code Options (codes/options.json) ---
def get_product_codes() -> Dict[str, Any]:
    """Retrieves the product code options dictionary from Drive."""
    key = config.CODES_OPTIONS_FILE
    body_bytes = drive_service.get_object_body(key)
    if body_bytes:
        try:
            return json.loads(body_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            log.error(f"Error decoding JSON from {key}: {e}", exc_info=True)
    return {}


def update_product_codes(code_options_data: Dict[str, Any]) -> bool:
    """Updates the product code options dictionary (options.json) in Drive."""
    key = config.CODES_OPTIONS_FILE
    success = drive_service.update_json(key, code_options_data)
    if success:
        log.info(f"Successfully updated product codes: {key}")
    else:
        log.error(f"Failed to update product codes: {key}")
    return success


# --- File Lists (lists/*.json) ---
def _get_list_file_key(level: int, product: str) -> str:
    """Helper to construct the storage key for a file list."""
    return config.PREFIX_LISTS + f"nexrad_level{level}_{product}_files.json"


def get_file_list(level: int, product: str) -> Dict[str, Any]:
    """Retrieves a specific product/level file list dictionary from Drive."""
    key = _get_list_file_key(level, product)
    body_bytes = drive_service.get_object_body(key)
    if body_bytes:
        try:
            return json.loads(body_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            log.error(f"Error decoding JSON from {key}: {e}", exc_info=True)
    return {}


def get_all_file_lists(
    products_levels: List[Dict[str, Any]] = [
        {"product": "reflectivity", "level": 2},
        {"product": "hydrometeor", "level": 3},
        {"product": "precipitation", "level": 3},
    ],
) -> Dict[str, Dict[str, Any]]:
    """Retrieves and combines file lists for predefined product/level combinations."""
    all_data = {}
    for item in products_levels:
        product = item["product"]
        level = item["level"]
        log.debug(f"Fetching file list for level {level} product {product}")
        all_data[product] = get_file_list(level, product)

    log.info("Fetched all primary file lists.")
    return all_data


def _parse_timestamp_from_key(key: str) -> Optional[datetime.datetime]:
    """Attempts to parse a UTC timestamp from L2 or L3 style keys."""
    filename = key.split("/")[-1]

    # THREDDS L2 format: Level2_KPDT_20260604_2005.ar2v
    match_thredds = re.match(r"^Level2_[A-Z]{4}_(\d{8})_(\d{4})", filename)
    if match_thredds:
        try:
            ts_str = match_thredds.group(1) + match_thredds.group(2)
            return datetime.datetime.strptime(ts_str, "%Y%m%d%H%M").replace(
                tzinfo=datetime.timezone.utc
            )
        except ValueError:
            pass

    # Old S3 L2 format: KPDT20250409_123456_V06
    match_l2 = re.match(r"^[A-Z]{4}(\d{8})_(\d{6})", filename)
    if match_l2:
        try:
            ts_str = match_l2.group(1) + match_l2.group(2)
            return datetime.datetime.strptime(ts_str, "%Y%m%d%H%M%S").replace(
                tzinfo=datetime.timezone.utc
            )
        except ValueError:
            pass

    # L3 format: KPDTYYYYMMDDHHMMSS_XXX
    match_l3 = re.match(r"^[A-Z]{4}(\d{14})", filename)
    if match_l3:
        try:
            ts_str = match_l3.group(1)
            return datetime.datetime.strptime(ts_str, "%Y%m%d%H%M%S").replace(
                tzinfo=datetime.timezone.utc
            )
        except ValueError:
            pass

    log.warning(f"Could not parse timestamp from key: {key}")
    return None


def update_file_list(
    level: int,
    product: str,
    new_files_info: Dict[str, Any],
    retention_minutes: int,
) -> bool:
    """Updates a specific product/level file list in Drive.
    Adds new file info and removes entries older than the retention period."""
    key = _get_list_file_key(level, product)
    current_list = get_file_list(level, product)
    updated_list = {}

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

    added_count = 0
    for file_key, file_info in new_files_info.items():
        if file_key not in updated_list:
            added_count += 1
        updated_list[file_key] = file_info

    log.info(
        f"Added/updated {len(new_files_info)} entries (of which {added_count} were new keys) to list '{key}'."
    )

    sorted_list = dict(sorted(updated_list.items()))

    success = drive_service.update_json(key, sorted_list)
    if success:
        log.info(f"Successfully updated file list: {key}")
    else:
        log.error(f"Failed to update file list: {key}")
    return success


# --- Update Flags (flags/update_flags.json) ---
def get_flags() -> Dict[str, Any]:
    """Retrieves the update flags dictionary from Drive."""
    key = config.FLAGS_FILE
    body_bytes = drive_service.get_object_body(key)
    if body_bytes:
        try:
            return json.loads(body_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            log.error(f"Error decoding JSON from {key}: {e}", exc_info=True)
    return {}


def update_flags(flags_data: Dict[str, Any]) -> bool:
    """Updates the update flags dictionary (update_flags.json) in Drive."""
    key = config.FLAGS_FILE
    success = drive_service.update_json(key, flags_data)
    if success:
        log.info(f"Successfully updated flags: {key}")
    else:
        log.error(f"Failed to update flags: {key}")
    return success


def set_update_flag(product_type: str) -> bool:
    """Sets the update flag for a specific product type to 1."""
    flags_data = get_flags()
    updates_dict = flags_data.setdefault("updates", {})
    updates_dict[product_type] = 1
    log.info(f"Setting update flag for product '{product_type}'")
    return update_flags(flags_data)


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
    product_type: str,
    current_file_list: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Updates the file counts for Level 3 product codes in options.json based
    on the provided or fetched current file list.

    Args:
        product_type: The Level 3 product type (e.g., 'hydrometeor').
        current_file_list: The latest file list dict for this product. If None,
                           it will be fetched using get_file_list.

    Returns:
        True if options.json was successfully updated, False otherwise.
    """
    log.info(f"Updating product code counts for '{product_type}' in options.json")
    code_options = get_product_codes()
    if not code_options or product_type not in code_options:
        log.error(
            f"Cannot update counts: Product type '{product_type}' not found in existing code options."
        )
        return False

    if current_file_list is None:
        log.debug(
            f"Fetching current file list for level 3 product {product_type} to calculate counts."
        )
        current_file_list = get_file_list(3, product_type)

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

    return update_product_codes(code_options)
