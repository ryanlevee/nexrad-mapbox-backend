import asyncio
import os
import sys
import time
import datetime
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Any

# --- Pre-computation Setup ---
try:
    import matplotlib

    matplotlib.use("Agg")
    logging.info("Matplotlib backend set to 'Agg'.")
except ImportError:
    logging.warning("Matplotlib not found. Plotting will not function.")

# --- Path Setup & Application Imports ---
# Recommended: Run `pip install -e .` from project root in virtualenv
# Alternatively, uncomment and adjust sys.path modification:
# script_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.abspath(os.path.join(script_dir, '..'))
# src_path = os.path.join(project_root, 'src')
# if src_path not in sys.path:
#     print(f"Adding to sys.path: {src_path}")
#     sys.path.insert(0, src_path)

try:
    from nexrad_backend import config
    from nexrad_backend.services import s3_service, nexrad_fetcher, metadata_service

    # Import the L3 processing function and the normalization helper
    from nexrad_backend.processing import level3 as level3_processor

    # Import the normalization function specifically if needed for filtering
    from nexrad_backend.processing.level3 import (
        _normalize_l3_filename_key as normalize_l3_filename_key,
    )
    from nexrad_backend.utils import list_helpers  # For flatten_list if needed

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
# Map general product types to the specific Py-ART field names expected by the processor
# This could also live in config.py
PRODUCT_TYPE_TO_FIELD_MAP = {
    "hydrometeor": "radar_echo_classification",  # Check Py-ART docs for exact field name
    "precipitation": "radar_estimated_rain_rate",  # Check Py-ART docs for exact field name
    # Add other L3 products here if needed
}

# Define which product types to process
PRODUCT_TYPES_TO_PROCESS = list(PRODUCT_TYPE_TO_FIELD_MAP.keys())


# --- Main Processing Logic ---
async def main(loop):
    """Main asynchronous function to orchestrate Level 3 processing."""
    log.info("--- Starting NEXRAD Level 3 Processing ---")
    start_time = time.time()

    # 1. Load Config & Initialize Clients
    log.info("Loading configuration and initializing S3 clients...")
    try:
        s3_project_client = config.get_project_s3_client()
        s3_public_client = config.get_public_s3_client()
        site = config.RADAR_SITE_L3  # e.g., PDT
        level = 3
        bucket = config.PROJECT_S3_BUCKET
        download_dir = os.path.abspath(config.DOWNLOAD_FOLDER)
        os.makedirs(download_dir, exist_ok=True)
    except Exception as e:
        log.exception("Failed to load configuration or initialize S3 clients.")
        return

    # 2. Determine Time Window
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    end_time_utc = now_utc
    start_time_utc = now_utc - datetime.timedelta(
        minutes=config.PROCESSING_WINDOW_MINUTES
    )
    log.info(f"Processing window: {start_time_utc} UTC to {end_time_utc} UTC")

    # 3. Get L3 Product Code Options from Project S3
    log.info("Fetching Level 3 product code options...")
    try:
        code_options_all = metadata_service.get_product_codes(s3_project_client, bucket)
        if not code_options_all:
            log.error(
                "Failed to retrieve product code options (options.json). Cannot determine which codes to process."
            )
            return
    except Exception as e:
        log.exception("Error fetching product code options.")
        return

    # 4. Process Each Product Type (Hydrometeor, Precipitation, etc.)
    with ProcessPoolExecutor() as executor:
        for product_type in PRODUCT_TYPES_TO_PROCESS:
            log.info(f"--- Processing Product Type: {product_type} ---")

            # Get specific codes and field name for this type
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
                # Note: nexrad_fetcher handles iterating codes internally if needed based on its design
                # Here we pass the list of codes for this product type
                found_original_keys = nexrad_fetcher.find_level3_keys(
                    s3_public_client, site, specific_codes, start_time_utc, end_time_utc
                )
            except Exception as e:
                log.exception(f"Failed to find Level 3 files for {product_type}.")
                found_original_keys = []  # Continue to next product type

            if not found_original_keys:
                log.info(
                    f"No new Level 3 files found for {product_type} in the public bucket within the time window."
                )
                continue  # Move to the next product type

            log.info(
                f"Found {len(found_original_keys)} potential {product_type} file(s)."
            )

            # 6. Get Existing Processed Files List from Project Bucket
            log.info(
                f"Retrieving existing processed file list for L{level}/{product_type}..."
            )
            try:
                existing_files_dict = metadata_service.get_file_list(
                    s3_project_client, bucket, level, product_type
                )
                existing_normalized_keys = set(existing_files_dict.keys())
                log.info(
                    f"Found {len(existing_normalized_keys)} existing processed file entries for {product_type}."
                )
            except Exception as e:
                log.exception(
                    f"Failed to retrieve existing file list for {product_type}. Assuming none exist."
                )
                existing_normalized_keys = set()

            # 7. Filter Files to Process (Compare normalized keys)
            keys_to_process_info = (
                []
            )  # Store dicts {'original_key': ..., 'normalized_key': ...}
            for original_key in found_original_keys:
                filename = original_key.split("/")[-1]
                normalized_key = normalize_l3_filename_key(filename)  # Use the helper
                if normalized_key and normalized_key not in existing_normalized_keys:
                    keys_to_process_info.append(
                        {"original_key": original_key, "normalized_key": normalized_key}
                    )

            if not keys_to_process_info:
                log.info(
                    f"No *new* Level 3 files to process for {product_type} (all found files seem to be listed already)."
                )
                continue  # Move to next product type

            log.info(
                f"Filtered down to {len(keys_to_process_info)} new {product_type} file(s) to download and process."
            )
            log.debug(
                f"Files to process for {product_type}: {[info['original_key'] for info in keys_to_process_info]}"
            )

            # 8. Download Files Concurrently
            log.info(
                f"Downloading {len(keys_to_process_info)} {product_type} files to {download_dir}..."
            )
            download_tasks = [
                loop.run_in_executor(
                    executor,
                    nexrad_fetcher.download_s3_file,
                    s3_public_client,
                    config.UNIDATA_L3_BUCKET,
                    file_info["original_key"],  # Download using original key
                    download_dir,
                )
                for file_info in keys_to_process_info
            ]
            download_results = await asyncio.gather(
                *download_tasks
            )  # Results are local paths or None
            log.info(f"Download phase complete for {product_type}.")

            # Filter out failed downloads and prepare for processing
            files_to_process_locally = []
            for file_info, local_path in zip(keys_to_process_info, download_results):
                if local_path:
                    # Add normalized key here for process step if needed, though process func re-calculates it
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
                continue  # Move to next product type

            # 9. Process Downloaded Files Concurrently
            log.info(
                f"Processing {len(files_to_process_locally)} downloaded {product_type} files..."
            )
            process_tasks = [
                loop.run_in_executor(
                    executor,
                    level3_processor.process_level3_file,
                    file_info["local_path"],
                    file_info["original_key"].split("/")[-1],  # Pass original filename
                    product_type,
                    field_name,  # Pass the specific Py-ART field
                    s3_project_client,
                    bucket,
                    config.S3_PREFIX_PLOTS_L3,
                )
                for file_info in files_to_process_locally
            ]
            # Results are normalized key prefixes or None
            processed_normalized_keys = await asyncio.gather(*process_tasks)
            log.info(f"Processing phase complete for {product_type}.")

            # 10. Aggregate Results for Metadata Update
            new_files_metadata: Dict[str, Dict[str, int]] = {}
            successful_files = 0
            failed_files = len(
                files_to_process_locally
            )  # Start assuming all failed initially
            for normalized_key in processed_normalized_keys:
                if normalized_key:
                    # L3 files effectively have 1 sweep relevant to the list
                    new_files_metadata[normalized_key] = {"sweeps": 1}
                    successful_files += 1
                    log.debug(f"Aggregated successful result for {normalized_key}")

            failed_files -= successful_files  # Adjust based on success
            log.info(
                f"Aggregation complete for {product_type}. Successfully processed: {successful_files}, Failed: {failed_files}"
            )

            # 11. Update Metadata (List, Counts, Flag) if new files were processed
            if new_files_metadata:
                log.info(f"Updating metadata for {product_type}...")
                try:
                    # Update File List
                    list_update_success = metadata_service.update_file_list(
                        s3_project_client,
                        bucket,
                        level,
                        product_type,
                        new_files_metadata,
                        config.PROCESSING_WINDOW_MINUTES,
                    )
                    if list_update_success:
                        log.info(
                            f"File list metadata updated successfully for {product_type}."
                        )

                        # Update Code Counts (needs the latest list state)
                        count_update_success = (
                            metadata_service.update_level3_product_code_counts(
                                s3_project_client,
                                bucket,
                                product_type,
                                current_file_list=None,  # Let service fetch latest list
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

                        # Set Update Flag
                        flag_success = metadata_service.set_update_flag(
                            s3_project_client, bucket, product_type
                        )
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
            # End of loop for product_type

    # 12. Cleanup Old Processed L3 Files from Project S3 (Run once after all types)
    try:
        log.info("Running S3 cleanup for old Level 3 processed files...")
        deleted_count = s3_service.delete_old_files(
            s3_project_client,
            bucket,
            config.S3_PREFIX_PLOTS_L3,
            config.CLEANUP_WINDOW_MINUTES,
        )
        log.info(f"S3 cleanup finished. Deleted {deleted_count} old L3 files.")
    except Exception as e:
        log.exception("Error during L3 S3 cleanup.")

    # --- End Script ---
    end_time = time.time()
    log.info(f"--- NEXRAD Level 3 Processing Finished ---")
    log.info(f"Total execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    # Set backend if needed
    # matplotlib.use('Agg')

    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(main(event_loop))
    except Exception as e:
        log.exception("An unhandled error occurred during script execution.")
    finally:
        log.info("Script finished.")
