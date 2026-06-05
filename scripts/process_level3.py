import asyncio
import os
import sys
import time
import datetime
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Any

# --- Pre-computation Setup ---
import os
os.environ["PYART_QUIET"] = "1"

try:
    import matplotlib

    matplotlib.use("Agg")
except ImportError:
    logging.warning("Matplotlib not found. Plotting will not function.")

# --- Application Imports ---
try:
    from nexrad_backend import config
    from nexrad_backend.services import drive_service, nexrad_fetcher, metadata_service
    from nexrad_backend.processing import level3 as level3_processor
    from nexrad_backend.processing.level3 import (
        _normalize_l3_filename_key as normalize_l3_filename_key,
    )
    from nexrad_backend.utils import list_helpers

except ImportError as e:
    logging.exception("ImportError: Failed to import backend modules.")
    logging.critical(
        "Ensure you have run 'pip install -e .' from the project root in your virtual environment."
    )
    sys.exit(1)

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# --- Constants / Mappings ---
PRODUCT_TYPE_TO_FIELD_MAP = {
    "hydrometeor": "radar_echo_classification",
    "precipitation": "radar_estimated_rain_rate",
}

PRODUCT_TYPES_TO_PROCESS = list(PRODUCT_TYPE_TO_FIELD_MAP.keys())


# --- Main Processing Logic ---
async def main(loop):
    """Main asynchronous function to orchestrate Level 3 processing."""
    log.info("--- Starting NEXRAD Level 3 Processing ---")
    start_time = time.time()

    # 1. Load Config & Initialize Clients
    log.info("Loading configuration and initializing clients...")
    try:
        config._validate_r2_config()
        site = config.RADAR_SITE_L3
        level = 3
        download_dir = os.path.abspath(config.DOWNLOAD_FOLDER)
        os.makedirs(download_dir, exist_ok=True)
    except Exception as e:
        log.exception("Failed to load configuration or initialize clients.")
        return

    # 2. Determine Time Window
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    end_time_utc = now_utc
    start_time_utc = now_utc - datetime.timedelta(
        minutes=config.PROCESSING_WINDOW_MINUTES
    )
    log.info(f"Processing window: {start_time_utc} UTC to {end_time_utc} UTC")

    # 3. Get L3 Product Code Options
    log.info("Fetching Level 3 product code options...")
    try:
        code_options_all = metadata_service.get_product_codes()
        if not code_options_all:
            log.error(
                "Failed to retrieve product code options (options.json). Cannot determine which codes to process."
            )
            return
    except Exception as e:
        log.exception("Error fetching product code options.")
        return

    # 4. Process Each Product Type
    with ProcessPoolExecutor() as executor:
        for product_type in PRODUCT_TYPES_TO_PROCESS:
            log.info(f"--- Processing Product Type: {product_type} ---")

            product_codes_info = code_options_all.get(product_type, [])
            if not product_codes_info:
                log.warning(
                    f"No product codes found for type '{product_type}' in options.json. Skipping."
                )
                continue
            specific_codes = [
                item["value"] for item in product_codes_info if "value" in item
            ]
            if not specific_codes:
                log.warning(
                    f"No 'value' found in code options for type '{product_type}'. Skipping."
                )
                continue

            field_name = PRODUCT_TYPE_TO_FIELD_MAP.get(product_type)
            if not field_name:
                log.error(
                    f"No Py-ART field mapping found for product type '{product_type}'. Skipping."
                )
                continue

            log.info(f"Target codes for {product_type}: {specific_codes}")
            log.info(f"Py-ART field for {product_type}: {field_name}")

            # 5. Find Recent Files from Public Source (Unidata)
            log.info(
                f"Finding recent Level 3 files for site {site} codes {specific_codes}..."
            )
            try:
                found_original_keys = nexrad_fetcher.find_level3_keys(
                    None, site, specific_codes, start_time_utc, end_time_utc
                )
            except Exception as e:
                log.exception(f"Failed to find Level 3 files for {product_type}.")
                found_original_keys = []

            if not found_original_keys:
                log.info(
                    f"No new Level 3 files found for {product_type} in the public bucket within the time window."
                )
                continue

            log.info(
                f"Found {len(found_original_keys)} potential {product_type} file(s)."
            )

            # 6. Get Existing Processed Files List
            log.info(
                f"Retrieving existing processed file list for L{level}/{product_type}..."
            )
            try:
                existing_files_dict = metadata_service.get_file_list(level, product_type)
                existing_normalized_keys = set(existing_files_dict.keys())
                log.info(
                    f"Found {len(existing_normalized_keys)} existing processed file entries for {product_type}."
                )
            except Exception as e:
                log.exception(
                    f"Failed to retrieve existing file list for {product_type}. Assuming none exist."
                )
                existing_normalized_keys = set()

            # 7. Filter Files to Process
            keys_to_process_info = []
            for original_key in found_original_keys:
                filename = original_key.split("/")[-1]
                normalized_key = normalize_l3_filename_key(filename)
                if normalized_key and normalized_key not in existing_normalized_keys:
                    keys_to_process_info.append(
                        {"original_key": original_key, "normalized_key": normalized_key}
                    )

            if not keys_to_process_info:
                log.info(
                    f"No *new* Level 3 files to process for {product_type} (all found files seem to be listed already)."
                )
                continue

            log.info(
                f"Filtered down to {len(keys_to_process_info)} new {product_type} file(s) to download and process."
            )

            # 8. Download Files Concurrently
            log.info(
                f"Downloading {len(keys_to_process_info)} {product_type} files to {download_dir}..."
            )
            download_tasks = [
                loop.run_in_executor(
                    executor,
                    nexrad_fetcher.download_s3_file,
                    None,
                    config.UNIDATA_L3_BUCKET,
                    file_info["original_key"],
                    download_dir,
                )
                for file_info in keys_to_process_info
            ]
            download_results = await asyncio.gather(*download_tasks)
            log.info(f"Download phase complete for {product_type}.")

            files_to_process_locally = []
            for file_info, local_path in zip(keys_to_process_info, download_results):
                if local_path:
                    file_info["local_path"] = local_path
                    files_to_process_locally.append(file_info)
                else:
                    log.warning(
                        f"Download failed for key: {file_info['original_key']}. Skipping processing."
                    )

            if not files_to_process_locally:
                log.warning(
                    f"No {product_type} files were successfully downloaded. Aborting processing phase for this type."
                )
                continue

            # 9. Process Downloaded Files Concurrently
            log.info(
                f"Processing {len(files_to_process_locally)} downloaded {product_type} files..."
            )
            process_tasks = [
                loop.run_in_executor(
                    executor,
                    level3_processor.process_level3_file,
                    file_info["local_path"],
                    file_info["original_key"].split("/")[-1],
                    product_type,
                    field_name,
                    config.PREFIX_PLOTS_L3,
                )
                for file_info in files_to_process_locally
            ]
            processed_normalized_keys = await asyncio.gather(*process_tasks)
            log.info(f"Processing phase complete for {product_type}.")

            # 10. Aggregate Results for Metadata Update
            new_files_metadata: Dict[str, Dict[str, int]] = {}
            successful_files = 0
            failed_files = len(files_to_process_locally)
            for normalized_key in processed_normalized_keys:
                if normalized_key:
                    new_files_metadata[normalized_key] = {"sweeps": 1}
                    successful_files += 1

            failed_files -= successful_files
            log.info(
                f"Aggregation complete for {product_type}. Successfully processed: {successful_files}, Failed: {failed_files}"
            )

            # 11. Update Metadata
            if new_files_metadata:
                log.info(f"Updating metadata for {product_type}...")
                try:
                    list_update_success = metadata_service.update_file_list(
                        level,
                        product_type,
                        new_files_metadata,
                        config.PROCESSING_WINDOW_MINUTES,
                    )
                    if list_update_success:
                        log.info(
                            f"File list metadata updated successfully for {product_type}."
                        )

                        count_update_success = (
                            metadata_service.update_level3_product_code_counts(
                                product_type,
                                current_file_list=None,
                            )
                        )
                        if count_update_success:
                            log.info(
                                f"Product code counts updated successfully for {product_type}."
                            )
                        else:
                            log.warning(
                                f"Failed to update product code counts for {product_type}."
                            )

                        flag_success = metadata_service.set_update_flag(product_type)
                        if flag_success:
                            log.info(
                                f"Update flag set successfully for {product_type}."
                            )
                        else:
                            log.warning(
                                f"Failed to set update flag for {product_type}."
                            )
                    else:
                        log.warning(
                            f"Failed to update file list metadata for {product_type}."
                        )
                except Exception as e:
                    log.exception(f"Error during metadata update for {product_type}.")
            else:
                log.info(
                    f"No new successfully processed {product_type} files to add to metadata list."
                )

            log.info(f"--- Finished Product Type: {product_type} ---")

    # 12. Cleanup Old Processed L3 Files
    try:
        log.info("Running cleanup for old Level 3 processed files...")
        deleted_count = drive_service.delete_old_files(
            config.PREFIX_PLOTS_L3,
            config.CLEANUP_WINDOW_MINUTES,
        )
        log.info(f"Cleanup finished. Deleted {deleted_count} old L3 files.")
    except Exception as e:
        log.exception("Error during L3 cleanup.")

    # --- End Script ---
    end_time = time.time()
    log.info(f"--- NEXRAD Level 3 Processing Finished ---")
    log.info(f"Total execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(main(event_loop))
    except Exception as e:
        log.exception("An unhandled error occurred during script execution.")
    finally:
        log.info("Script finished.")
