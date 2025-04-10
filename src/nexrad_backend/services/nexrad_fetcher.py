import os
import re
import datetime
import logging
from typing import List, Optional, Set
from botocore.exceptions import ClientError

# Import config for bucket names, chunk size, etc.
from nexrad_backend import config

# Setup logging
log = logging.getLogger(__name__)


# --- Level 2 Fetching ---
def find_level2_keys(
    client,
    site: str,
    start_time_utc: datetime.datetime,
    end_time_utc: datetime.datetime,
) -> List[str]:
    """
    Finds NEXRAD Level 2 V06 file keys for a given site and time window
    from the NOAA public S3 bucket.

    Args:
        client: Initialized Boto3 S3 client instance (unsigned, public access).
        site: The 4-letter radar site identifier (e.g., "KPDT").
        start_time_utc: The beginning of the time window (UTC).
        end_time_utc: The end of the time window (UTC).

    Returns:
        A list of S3 keys for matching Level 2 files. Returns empty list on error.
    """
    keys_found: List[str] = []
    bucket = config.NOAA_L2_BUCKET
    current_date_utc = start_time_utc.date()
    end_date_utc = end_time_utc.date()

    log.info(
        f"Searching for L2 keys for site {site} from {start_time_utc} to {end_time_utc}"
    )

    while current_date_utc <= end_date_utc:
        year = current_date_utc.year
        month = current_date_utc.month
        day = current_date_utc.day
        prefix = f"{year}/{month:02d}/{day:02d}/{site}/"
        paginator = client.get_paginator("list_objects_v2")

        try:
            log.debug(f"Checking L2 prefix: s3://{bucket}/{prefix}")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            for page in pages:
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    key = obj["Key"]
                    # Filter for V06 files, excluding metadata files
                    if not key.endswith("V06") or key.endswith("_MDM"):
                        continue

                    # Attempt to parse timestamp and filter
                    try:
                        # Extract YYYYMMDD_HHMMSS from key like YYYY/MM/DD/SITE/SITE_YYYYMMDD_HHMMSS_V06
                        filename = key.split("/")[-1]
                        timestamp_str = (
                            filename.split("_")[1] + "_" + filename.split("_")[2]
                        )
                        file_datetime_utc = datetime.datetime.strptime(
                            timestamp_str, "%Y%m%d_%H%M%S"
                        ).replace(tzinfo=datetime.timezone.utc)

                        if start_time_utc <= file_datetime_utc <= end_time_utc:
                            keys_found.append(key)
                            log.debug(f"Found L2 key in time window: {key}")
                        # else: # Can be very noisy
                        #     log.debug(f"Skipping L2 key (out of time window): {key}")

                    except (ValueError, IndexError) as e:
                        log.warning(
                            f"Could not parse timestamp for L2 key: {key}. Error: {e}"
                        )
                        continue

        except ClientError as e:
            log.error(
                f"Error listing L2 objects in s3://{bucket}/{prefix}: {e}",
                exc_info=True,
            )
            # Continue to next day if one day fails
        except Exception as e:
            log.error(
                f"Unexpected error listing L2 objects in s3://{bucket}/{prefix}: {e}",
                exc_info=True,
            )

        current_date_utc += datetime.timedelta(days=1)

    log.info(f"Found {len(keys_found)} total L2 keys for site {site} in time window.")
    return sorted(list(set(keys_found)))  # Return sorted unique keys


# --- Level 3 Fetching ---
def find_level3_keys(
    client,
    site: str,
    product_codes: List[str],
    start_time_utc: datetime.datetime,
    end_time_utc: datetime.datetime,
) -> List[str]:
    """
    Finds NEXRAD Level 3 file keys for a given site, product codes, and time window
    from the Unidata public S3 bucket.

    Args:
        client: Initialized Boto3 S3 client instance (unsigned, public access).
        site: The 3-letter radar site identifier (e.g., "PDT").
        product_codes: A list of 3-letter product codes (e.g., ["N0Q", "HHC"]).
        start_time_utc: The beginning of the time window (UTC).
        end_time_utc: The end of the time window (UTC).

    Returns:
        A list of S3 keys for matching Level 3 files. Returns empty list on error.
    """
    keys_found: List[str] = []
    bucket = config.UNIDATA_L3_BUCKET
    # Use a set for efficient lookup in the matching function
    product_codes_set: Set[str] = set(product_codes)
    # Ensure site matches the expected 3-letter format for L3 prefixes
    site_prefix = site.upper()[:3]

    log.info(
        f"Searching for L3 keys for site {site_prefix}, codes {product_codes} from {start_time_utc} to {end_time_utc}"
    )

    current_hour_utc = start_time_utc.replace(minute=0, second=0, microsecond=0)
    end_hour_utc = end_time_utc  # Check includes the end hour

    while current_hour_utc <= end_hour_utc:
        for code in product_codes_set:
            prefix = f"{site_prefix}/{code}/{current_hour_utc.strftime('%Y/%m/%d')}/"
            # Unidata filenames seem to be like SITE_CODE_YYYY_MM_DD_HH_MM_SS
            # Listing by SITE/CODE/YYYY/MM/DD/ seems efficient enough
            paginator = client.get_paginator("list_objects_v2")

            try:
                log.debug(f"Checking L3 prefix: s3://{bucket}/{prefix}")
                pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
                for page in pages:
                    if "Contents" not in page:
                        continue
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        matched_key = _match_level3_file(
                            key,
                            product_codes_set,
                            site_prefix,
                            start_time_utc,
                            end_time_utc,
                        )
                        if matched_key:
                            keys_found.append(matched_key)
                            log.debug(f"Found L3 key in time window: {key}")

            except ClientError as e:
                log.error(
                    f"Error listing L3 objects in s3://{bucket}/{prefix}: {e}",
                    exc_info=True,
                )
                # Continue to next code/hour if one prefix fails
            except Exception as e:
                log.error(
                    f"Unexpected error listing L3 objects in s3://{bucket}/{prefix}: {e}",
                    exc_info=True,
                )

        current_hour_utc += datetime.timedelta(hours=1)

    log.info(
        f"Found {len(keys_found)} total L3 keys for site {site_prefix} in time window."
    )
    return sorted(list(set(keys_found)))  # Return sorted unique keys


def _match_level3_file(
    key: str,
    product_codes_set: Set[str],
    site: str,
    start_time_utc: datetime.datetime,
    end_time_utc: datetime.datetime,
) -> Optional[str]:
    """
    Helper to validate a potential Level 3 filename against criteria.
    Expected format: SITE/CODE/YYYY/MM/DD/SITE_CODE_YYYY_MM_DD_HH_MM_SS
    """
    filename = key.split("/")[-1]
    # Regex to match the expected filename format more robustly
    match = re.match(
        r"^(?P<site>[A-Z]{3})_(?P<product>[A-Z0-9]{3})_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})$",
        filename,
    )

    if not match:
        # log.debug(f"L3 key '{key}' does not match expected filename pattern.")
        return None

    details = match.groupdict()

    # Check if site and product code match
    if details["site"] != site or details["product"] not in product_codes_set:
        # log.debug(f"L3 key '{key}' site/product mismatch ({details['site']}/{details['product']}).")
        return None

    # Check if timestamp is within the window
    try:
        timestamp_str = f"{details['year']}-{details['month']}-{details['day']} {details['hour']}:{details['minute']}:{details['second']}"
        file_datetime_utc = datetime.datetime.strptime(
            timestamp_str, "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)

        if start_time_utc <= file_datetime_utc <= end_time_utc:
            return key  # Return the full key if it matches
        else:
            # log.debug(f"L3 key '{key}' timestamp out of window.")
            return None
    except ValueError:
        log.warning(f"Could not parse timestamp from L3 filename: {filename}")
        return None


# --- Generic Download ---
def download_s3_file(
    client,
    bucket: str,
    key: str,
    target_dir: str,
    chunk_size: int = config.DOWNLOAD_CHUNK_SIZE,
) -> Optional[str]:
    """
    Downloads a file from S3 to a local directory.

    Args:
        client: Initialized Boto3 S3 client instance.
        bucket: The S3 bucket name.
        key: The S3 object key.
        target_dir: The local directory to save the file in.
        chunk_size: The chunk size for downloading.

    Returns:
        The full path to the downloaded local file if successful, otherwise None.
    """
    filename = key.split("/")[-1]
    local_filepath = os.path.join(target_dir, filename)

    try:
        # Ensure target directory exists
        os.makedirs(target_dir, exist_ok=True)

        log.info(f"Downloading s3://{bucket}/{key} to {local_filepath}...")
        response = client.get_object(Bucket=bucket, Key=key)

        # Stream download in chunks
        with open(local_filepath, "wb") as f:
            body = response["Body"]
            while chunk := body.read(chunk_size):
                f.write(chunk)

        log.info(f"Successfully downloaded {local_filepath}")
        return local_filepath

    except ClientError as e:
        log.error(f"Error downloading s3://{bucket}/{key}: {e}", exc_info=True)
        # Clean up partial download if it exists
        if os.path.exists(local_filepath):
            try:
                os.remove(local_filepath)
            except OSError as rm_err:
                log.warning(
                    f"Could not remove partial download {local_filepath}: {rm_err}"
                )
        return None
    except OSError as e:
        log.error(f"Error writing downloaded file {local_filepath}: {e}", exc_info=True)
        return None
    except Exception as e:
        log.error(
            f"Unexpected error downloading s3://{bucket}/{key}: {e}", exc_info=True
        )
        return None
