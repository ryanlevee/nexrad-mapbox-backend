import os
import re
import datetime
import logging
import xml.etree.ElementTree as ET
from typing import List, Optional, Set

import requests

from nexrad_backend import config

log = logging.getLogger(__name__)

THREDDS_CATALOG_URL = "https://thredds.ucar.edu/thredds/catalog/nexrad/level2/{site}/{date}/catalog.xml"
THREDDS_DOWNLOAD_URL = "https://thredds.ucar.edu/thredds/fileServer/nexrad/level2/{site}/{date}/{filename}"

UNIDATA_L3_BASE = "https://unidata-nexrad-level3.s3.amazonaws.com"


# --- Level 2 Fetching (via UCAR THREDDS) ---
def find_level2_keys(
    _client,
    site: str,
    start_time_utc: datetime.datetime,
    end_time_utc: datetime.datetime,
) -> List[str]:
    """
    Finds NEXRAD Level 2 file keys for a given site and time window
    from UCAR THREDDS. The `client` param is accepted for interface
    compatibility but not used.

    Returns:
        A list of download URLs for matching Level 2 files.
    """
    keys_found: List[str] = []
    current_date_utc = start_time_utc.date()
    end_date_utc = end_time_utc.date()

    log.info(
        f"Searching for L2 files for site {site} from {start_time_utc} to {end_time_utc} (THREDDS)"
    )

    while current_date_utc <= end_date_utc:
        date_str = current_date_utc.strftime("%Y%m%d")
        catalog_url = THREDDS_CATALOG_URL.format(site=site, date=date_str)

        try:
            log.debug(f"Fetching THREDDS catalog: {catalog_url}")
            resp = requests.get(catalog_url, timeout=30)
            if resp.status_code == 404:
                log.debug(f"No THREDDS catalog for {site}/{date_str}")
                current_date_utc += datetime.timedelta(days=1)
                continue
            resp.raise_for_status()

            root = ET.fromstring(resp.content)

            for dataset in root.iter("{http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0}dataset"):
                name = dataset.get("name", "")
                if not name or not name.startswith("Level2_"):
                    continue

                file_dt = _parse_thredds_l2_timestamp(name)
                if file_dt and start_time_utc <= file_dt <= end_time_utc:
                    download_url = THREDDS_DOWNLOAD_URL.format(
                        site=site, date=date_str, filename=name
                    )
                    keys_found.append(download_url)
                    log.debug(f"Found L2 file in time window: {name}")

        except requests.RequestException as e:
            log.error(f"Error fetching THREDDS catalog for {site}/{date_str}: {e}")
        except ET.ParseError as e:
            log.error(f"Error parsing THREDDS XML for {site}/{date_str}: {e}")

        current_date_utc += datetime.timedelta(days=1)

    log.info(f"Found {len(keys_found)} total L2 files for site {site} in time window.")
    return sorted(list(set(keys_found)))


def _parse_thredds_l2_timestamp(filename: str) -> Optional[datetime.datetime]:
    """Parse timestamp from THREDDS L2 filename like Level2_KPDT_20250601_1234.ar2v"""
    match = re.match(
        r"^Level2_[A-Z]{4}_(\d{8})_(\d{4})\.ar2v$", filename
    )
    if not match:
        return None
    try:
        dt = datetime.datetime.strptime(
            match.group(1) + match.group(2), "%Y%m%d%H%M"
        ).replace(tzinfo=datetime.timezone.utc)
        return dt
    except ValueError:
        return None


# --- Level 3 Fetching (via S3 HTTPS endpoint) ---
def find_level3_keys(
    _client,
    site: str,
    product_codes: List[str],
    start_time_utc: datetime.datetime,
    end_time_utc: datetime.datetime,
) -> List[str]:
    """
    Finds NEXRAD Level 3 file keys via HTTPS to the Unidata S3 bucket.
    The `client` param is accepted for interface compatibility but not used.

    Returns:
        A list of S3 keys for matching Level 3 files.
    """
    keys_found: List[str] = []
    site_prefix = site.upper()[:3]
    product_codes_set: Set[str] = set(product_codes)

    log.info(
        f"Searching for L3 keys for site {site_prefix}, codes {product_codes} "
        f"from {start_time_utc} to {end_time_utc} (HTTPS)"
    )

    current_date = start_time_utc.date()
    end_date = end_time_utc.date()

    while current_date <= end_date:
        for code in product_codes_set:
            prefix = f"{site_prefix}_{code}_{current_date.strftime('%Y_%m_%d')}"
            continuation_token = None

            while True:
                params = {
                    "list-type": "2",
                    "prefix": prefix,
                    "max-keys": "1000",
                }
                if continuation_token:
                    params["continuation-token"] = continuation_token

                try:
                    resp = requests.get(UNIDATA_L3_BASE, params=params, timeout=30)
                    resp.raise_for_status()

                    root = ET.fromstring(resp.content)
                    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

                    for key_elem in root.findall("s3:Contents/s3:Key", ns):
                        key = key_elem.text
                        if not key:
                            continue
                        matched = _match_level3_file(
                            key, product_codes_set, site_prefix,
                            start_time_utc, end_time_utc,
                        )
                        if matched:
                            keys_found.append(matched)

                    is_truncated = root.findtext("s3:IsTruncated", namespaces=ns)
                    if is_truncated == "true":
                        continuation_token = root.findtext(
                            "s3:NextContinuationToken", namespaces=ns
                        )
                    else:
                        break

                except requests.RequestException as e:
                    log.error(f"Error listing L3 files for prefix {prefix}: {e}")
                    break
                except ET.ParseError as e:
                    log.error(f"Error parsing L3 XML for prefix {prefix}: {e}")
                    break

        current_date += datetime.timedelta(days=1)

    log.info(
        f"Found {len(keys_found)} total L3 keys for site {site_prefix} in time window."
    )
    return sorted(list(set(keys_found)))


def _match_level3_file(
    key: str,
    product_codes_set: Set[str],
    site: str,
    start_time_utc: datetime.datetime,
    end_time_utc: datetime.datetime,
) -> Optional[str]:
    """Validate a Level 3 filename. Format: SITE_CODE_YYYY_MM_DD_HHMMSS"""
    filename = key.split("/")[-1]
    match = re.match(
        r"^(?P<site>[A-Z]{3})_(?P<product>[A-Z0-9]{3})_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})$",
        filename,
    )

    if not match:
        return None

    details = match.groupdict()

    if details["site"] != site or details["product"] not in product_codes_set:
        return None

    try:
        timestamp_str = f"{details['year']}-{details['month']}-{details['day']} {details['hour']}:{details['minute']}:{details['second']}"
        file_datetime_utc = datetime.datetime.strptime(
            timestamp_str, "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)

        if start_time_utc <= file_datetime_utc <= end_time_utc:
            return key
        return None
    except ValueError:
        log.warning(f"Could not parse timestamp from L3 filename: {filename}")
        return None


# --- Download ---
def download_s3_file(
    _client,
    bucket: str,
    key: str,
    target_dir: str,
    chunk_size: int = config.DOWNLOAD_CHUNK_SIZE,
) -> Optional[str]:
    """
    Downloads a file via HTTPS. If `key` is a full URL (THREDDS),
    downloads directly. Otherwise downloads from the S3 bucket HTTPS endpoint.
    The `client` param is kept for interface compatibility.
    """
    if key.startswith("https://"):
        url = key
        filename = key.split("/")[-1]
    else:
        url = f"https://{bucket}.s3.amazonaws.com/{key}"
        filename = key.split("/")[-1]

    local_filepath = os.path.join(target_dir, filename)

    try:
        os.makedirs(target_dir, exist_ok=True)

        log.info(f"Downloading {url} to {local_filepath}...")
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        with open(local_filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)

        log.info(f"Successfully downloaded {local_filepath}")
        return local_filepath

    except requests.RequestException as e:
        log.error(f"Error downloading {url}: {e}", exc_info=True)
        if os.path.exists(local_filepath):
            try:
                os.remove(local_filepath)
            except OSError as rm_err:
                log.warning(f"Could not remove partial download {local_filepath}: {rm_err}")
        return None
    except OSError as e:
        log.error(f"Error writing file {local_filepath}: {e}", exc_info=True)
        return None
