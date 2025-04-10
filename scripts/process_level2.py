import asyncio
import os
import sys
import time
import datetime
import logging
from concurrent.futures import ProcessPoolExecutor

# --- Pre-computation Setup ---
# Setting Matplotlib backend *before* importing Py-ART or processing modules,
# especially useful when using multiprocessing.
try:
    import matplotlib

    matplotlib.use("Agg")
    logging.info("Matplotlib backend set to 'Agg'.")
except ImportError:
    logging.warning("Matplotlib not found. Plotting will not function.")

# --- Application Imports ---
try:
    from nexrad_backend import config
    from nexrad_backend.services import s3_service, nexrad_fetcher, metadata_service
    from nexrad_backend.processing import level2 as level2_processor
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


# --- Main Processing Logic ---
async def main(loop):
    """Main asynchronous function to orchestrate Level 2 processing."""
    log.info("--- Starting NEXRAD Level 2 Processing ---")
    start_time = time.time()

    # 1. Load Config & Initialize Clients
    log.info("Loading configuration and initializing S3 clients...")
    try:
        # Ensure AWS keys are loaded if validation added in config
        # config._validate_config() # Or call directly if needed
        s3_project_client = config.get_project_s3_client()
        s3_public_client = config.get_public_s3_client()
        site = config.RADAR_SITE_L2
        product = "reflectivity"  # L2 processing is focused on this
        level = 2
        bucket = config.PROJECT_S3_BUCKET
        download_dir = os.path.abspath(config.DOWNLOAD_FOLDER)  # Ensure absolute path
        os.makedirs(download_dir, exist_ok=True)  # Ensure download dir exists
    except Exception as e:
        log.exception("Failed to load configuration or initialize S3 clients.")
        return  # Cannot proceed

    # 2. Determine Time Window
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    end_time_utc = now_utc
    start_time_utc = now_utc - datetime.timedelta(
        minutes=config.PROCESSING_WINDOW_MINUTES
    )
    log.info(f"Processing window: {start_time_utc} UTC to {end_time_utc} UTC")

    # 3. Find Recent Files from Public Source
    log.info(
        f"Finding recent Level 2 files for site {site} in {config.NOAA_L2_BUCKET}..."
    )
    try:
        found_keys = nexrad_fetcher.find_level2_keys(
            s3_public_client, site, start_time_utc, end_time_utc
        )
    except Exception as e:
        log.exception("Failed to find Level 2 files from public bucket.")
        found_keys = []  # Continue if possible, maybe log error and exit?

    if not found_keys:
        log.info(
            "No new Level 2 files found in the public bucket within the time window."
        )
        # Optionally run cleanup even if no new files found
        try:
            log.info("Running S3 cleanup for old processed files...")
            deleted_count = s3_service.delete_old_files(
                s3_project_client,
                bucket,
                config.S3_PREFIX_PLOTS_L2,
                config.CLEANUP_WINDOW_MINUTES,
            )
            log.info(f"S3 cleanup finished. Deleted {deleted_count} old files.")
        except Exception as e:
            log.exception("Error during S3 cleanup.")
        return  # Exit script

    log.info(f"Found {len(found_keys)} potential Level 2 file(s).")

    # 4. Get Existing Processed Files List from Project Bucket
    log.info(f"Retrieving existing processed file list for L{level}/{product}...")
    try:
        existing_files_dict = metadata_service.get_file_list(
            s3_project_client, bucket, level, product
        )
        existing_key_prefixes = set(existing_files_dict.keys())
        log.info(f"Found {len(existing_key_prefixes)} existing processed file entries.")
    except Exception as e:
        log.exception("Failed to retrieve existing file list. Assuming none exist.")
        existing_key_prefixes = set()

    # 5. Filter Files to Process (Find keys not already processed)
    keys_to_process = []
    for key in found_keys:
        key_prefix = key.split("/")[-1]  # Extract base filename like KPDT..._V06
        if key_prefix not in existing_key_prefixes:
            keys_to_process.append(key)

    if not keys_to_process:
        log.info(
            "No *new* Level 2 files to process (all found files seem to be listed already)."
        )
        # Optionally run cleanup even if no new files processed
        try:
            log.info("Running S3 cleanup for old processed files...")
            deleted_count = s3_service.delete_old_files(
                s3_project_client,
                bucket,
                config.S3_PREFIX_PLOTS_L2,
                config.CLEANUP_WINDOW_MINUTES,
            )
            log.info(f"S3 cleanup finished. Deleted {deleted_count} old files.")
        except Exception as e:
            log.exception("Error during S3 cleanup.")
        return  # Exit script

    log.info(
        f"Filtered down to {len(keys_to_process)} new Level 2 file(s) to download and process."
    )
    log.debug(f"Files to process: {keys_to_process}")

    # Use ProcessPoolExecutor for CPU-bound tasks (PyART/Matplotlib) and potentially I/O
    processed_results = []
    with ProcessPoolExecutor() as executor:
        # 6. Download Files Concurrently
        log.info(f"Downloading {len(keys_to_process)} files to {download_dir}...")
        download_tasks = [
            loop.run_in_executor(
                executor,
                nexrad_fetcher.download_s3_file,
                s3_public_client,
                config.NOAA_L2_BUCKET,
                key,
                download_dir,
            )
            for key in keys_to_process
        ]
        # Results are local file paths or None
        download_results = await asyncio.gather(*download_tasks)
        log.info("Download phase complete.")

        # Filter out failed downloads and prepare for processing
        files_to_process_locally = []
        for key, local_path in zip(keys_to_process, download_results):
            if local_path:
                files_to_process_locally.append({"key": key, "local_path": local_path})
            else:
                log.warning(f"Download failed for key: {key}. Skipping processing.")

        if not files_to_process_locally:
            log.warning(
                "No files were successfully downloaded. Aborting processing phase."
            )
            # Optionally run cleanup
            return

        # 7. Process Downloaded Files Concurrently
        log.info(f"Processing {len(files_to_process_locally)} downloaded files...")
        process_tasks = [
            loop.run_in_executor(
                executor,
                level2_processor.process_level2_file,
                file_info["local_path"],
                file_info["key"],  # Pass original key for naming prefix
                product,
                s3_project_client,
                bucket,
                config.S3_PREFIX_PLOTS_L2,
            )
            for file_info in files_to_process_locally
        ]
        # Results are summary dictionaries from process_level2_file or None
        processed_results = await asyncio.gather(*process_tasks)
        log.info("Processing phase complete.")

    # 8. Aggregate Results for Metadata Update
    new_files_metadata = {}
    successful_files = 0
    failed_files = 0
    for result in processed_results:
        if result and result.get("processed_key_prefix"):
            # Store info needed for update_file_list: { key_prefix: {sweeps: count} }
            key_prefix = result["processed_key_prefix"]
            # Use total_sweeps from the radar object for the count
            new_files_metadata[key_prefix] = {"sweeps": result.get("total_sweeps", 0)}
            successful_files += 1
            log.debug(
                f"Aggregated result for {key_prefix} with {result.get('total_sweeps')} sweeps."
            )
        else:
            # Processing failed for this file (error already logged in processor)
            failed_files += 1

    log.info(
        f"Aggregation complete. Successfully processed: {successful_files}, Failed: {failed_files}"
    )

    # 9. Update Metadata File List in Project S3 (if new files were processed)
    if new_files_metadata:
        log.info(f"Updating file list metadata for L{level}/{product}...")
        try:
            update_success = metadata_service.update_file_list(
                s3_project_client,
                bucket,
                level,
                product,
                new_files_metadata,
                config.PROCESSING_WINDOW_MINUTES,  # Use same window for retention
            )
            if update_success:
                log.info("File list metadata updated successfully.")
                # 10. Set Update Flag
                log.info(f"Setting update flag for product '{product}'...")
                flag_success = metadata_service.set_update_flag(
                    s3_project_client, bucket, product
                )
                if flag_success:
                    log.info("Update flag set successfully.")
                else:
                    log.warning("Failed to set update flag.")
            else:
                log.warning("Failed to update file list metadata.")
        except Exception as e:
            log.exception("Error during metadata update or flag setting.")
    else:
        log.info("No new successfully processed files to add to metadata list.")

    # 11. Cleanup Old Processed Files from Project S3
    try:
        log.info("Running S3 cleanup for old processed files...")
        deleted_count = s3_service.delete_old_files(
            s3_project_client,
            bucket,
            config.S3_PREFIX_PLOTS_L2,
            config.CLEANUP_WINDOW_MINUTES,
        )
        log.info(f"S3 cleanup finished. Deleted {deleted_count} old files.")
    except Exception as e:
        log.exception("Error during S3 cleanup.")

    # --- End Script ---
    end_time = time.time()
    log.info(f"--- NEXRAD Level 2 Processing Finished ---")
    log.info(f"Total execution time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    # Run the main async function
    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(main(event_loop))
    except Exception as e:
        log.exception("An unhandled error occurred during script execution.")
    finally:
        # Optionally close the loop if not running continuously
        # event_loop.close()
        log.info("Script finished.")
