import asyncio
import datetime
import json
import os
import re
import shutil
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta, timezone
import sys
from time import time

import boto3
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.client import Config
from pytz import UTC

from helpers import delete_old_s3_files
from read_and_plot_nexrad_level3 import read_and_plot_nexrad_level3_data
from utils import Utl

BUCKET_PATH_PLOTS = "plots_level3/"
BUCKET_PATH_LISTS = "lists/"
BUCKET_PATH_CODES = "codes/"
BUCKET_FLAG_LISTS = "flags/"
MY_BUCKET_NAME = "nexrad-mapbox"
DOWNLOAD_FOLDER = "public/nexrad_level3_data"
CHUNK_SIZE = 1024 * 1024 * 2

TRANSFER_CONFIG = TransferConfig(max_concurrency=50)
CONFIG = Config(signature_version=UNSIGNED, s3={"transfer_config": TRANSFER_CONFIG})
SESSION = boto3.session.Session()
unidata_s3_client = SESSION.client("s3", config=CONFIG, region_name="us-east-1")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)


def update_metadata(
    plotted_product_files, product_type, radar_site, product_file_list, code_options
):
    filtered_file_list = {}

    list_filename = f"nexrad_level3_{product_type}_files.json"
    s3_list_key = BUCKET_PATH_LISTS + list_filename

    plotted_product_files.sort()
    latest_file_datetime = plotted_product_files[-1][4:19].replace("_", " ")
    format_string = "%Y%m%d %H%M%S"
    datetime_object = datetime.datetime.strptime(latest_file_datetime, format_string)
    three_hours_ago = datetime_object - timedelta(minutes=180)

    min_file_datetime = (
        str(three_hours_ago).replace("-", "").replace(":", "").replace(" ", "_")
    )

    min_prefix = f"K{radar_site}{min_file_datetime}"

    filtered_product_list = {
        k: v for k, v in product_file_list.items() if k >= min_prefix
    }

    for file in plotted_product_files:
        if file >= min_prefix:
            filtered_product_list.update({file: {"sweeps": 1}})

    json_list_string = json.dumps(filtered_product_list)
    json_list_bytes = json_list_string.encode("utf-8")
    try:
        response = s3_client.put_object(
            Bucket=MY_BUCKET_NAME,
            Key=s3_list_key,
            Body=json_list_bytes,
            ContentType="application/json",
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"Updated s3://{MY_BUCKET_NAME}/{s3_list_key}")
        else:
            print(
                f"Failed to update s3://{MY_BUCKET_NAME}/{s3_list_key}. Response: {response}"
            )
    except Exception as e:
        print(f"Error writing file list to S3: {e}")

    [filtered_file_list.update({k: v}) for k, v in filtered_product_list.items()]

    flags_filename = "update_flags.json"
    s3_flags_key = BUCKET_FLAG_LISTS + flags_filename

    try:
        response = s3_client.get_object(Bucket=MY_BUCKET_NAME, Key=s3_flags_key)
        flag_file = json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print(
            f"Warning: S3 key '{s3_flags_key}' not found. Starting with an empty list."
        )
        flag_file = {}
    except Exception as e:
        print(f"Error reading file list from S3: {e}")
        return

    flag_file["updates"][product_type] = 1

    json_flag_string = json.dumps(flag_file)
    json_flag_bytes = json_flag_string.encode("utf-8")
    try:
        response = s3_client.put_object(
            Bucket=MY_BUCKET_NAME,
            Key=s3_flags_key,
            Body=json_flag_bytes,
            ContentType="application/json",
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"Updated s3://{MY_BUCKET_NAME}/{s3_flags_key}")
        else:
            print(
                f"Failed to update s3://{MY_BUCKET_NAME}/{s3_flags_key}. Response: {response}"
            )
    except Exception as e:
        print(f"Error writing file list to S3: {e}")

    print(f"nexrad_level3_{product_type}_files.json updated")
    s3_codes_key = os.path.join(BUCKET_PATH_CODES, "options.json")

    print(f"Generating {product_type} code options for options.json")

    product_codes = code_options[product_type]
    jcodes = [jk[-3:] for jk in filtered_file_list]

    for i, codes in enumerate(product_codes):
        code = codes["value"]
        product_codes[i]["count"] = jcodes.count(code)

    code_options[product_type] = product_codes
    print(f"updating options.json for {product_type}")

    json_code_string = json.dumps(code_options)
    json_code_bytes = json_code_string.encode("utf-8")
    try:
        response = s3_client.put_object(
            Bucket=MY_BUCKET_NAME,
            Key=s3_codes_key,
            Body=json_code_bytes,
            ContentType="application/json",
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"Updated s3://{MY_BUCKET_NAME}/{s3_codes_key}")
        else:
            print(
                f"Failed to update s3://{MY_BUCKET_NAME}/{s3_codes_key}. Response: {response}"
            )
    except Exception as e:
        print(f"Error writing file list to S3: {e}")


def download_nexrad_level3_data(
    filename,
    existing_files,
    bucket_name="unidata-nexrad-level3",
):
    fns = filename.split("_")
    normalized_filename = (
        f"K{''.join([fns[0], *fns[2:5]])}_{''.join(fns[5:8])}_{fns[1]}"
    )

    if normalized_filename in existing_files:
        print(f"File {filename} already exists, skipping.")
        return False

    current_path = os.getcwd()
    file_path = os.path.join(current_path, DOWNLOAD_FOLDER)

    if not os.path.exists(file_path):
        os.makedirs(file_path)

    download_path = os.path.join(DOWNLOAD_FOLDER, filename)
    print(f"Downloading {filename} to {download_path}")

    response = unidata_s3_client.get_object(Bucket=bucket_name, Key=filename)
    parts = []
    body = response["Body"]
    while data := body.read(CHUNK_SIZE):
        parts.append(data)

    content = b"".join(parts)

    with open(download_path, "wb") as f:
        f.write(content)

    print(f"Downloaded {filename} successfully.")

    return filename


def fetch_nexrad_level3_data(
    product_code,
    radar_site,
    bucket_name,
    start_time,
    end_time,
    max_keys=1000,
):
    all_files_list = []
    current_time = start_time

    while current_time <= end_time:
        code = product_code["value"]
        prefix = f"{radar_site}_{code}_{current_time.strftime('%Y_%m_%d_%H')}"

        continuation_token = None

        while True:
            list_kwargs = {
                "Bucket": bucket_name,
                "Prefix": prefix,
                "MaxKeys": max_keys,
            }
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            response = unidata_s3_client.list_objects_v2(**list_kwargs)

            for obj in response.get("Contents", []):
                if matched_file := _match_file(code, obj):
                    all_files_list.append(matched_file)

            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break

        current_time += timedelta(hours=1)

        print(f"Files found for product code {code}: {all_files_list}")

    return all_files_list


def _match_file(product_code_prefix, obj):
    filename = obj["Key"]
    print(f"Processing filename: {filename}")

    match = re.match(
        r"^(?P<site>[A-Z]{3})_(?P<product>[A-Z0-9]{3})_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<hour>\d{2})_(?P<minute>\d{2})_(?P<second>\d{2})$",
        filename,
    )

    if not match:
        print(f"SKIPPING: Filename pattern mismatch: {filename}")
        return False

    match_details = match.groupdict()
    radar_site_file = match_details["site"]
    product_code_file = match_details["product"]

    file_datetime_str = f"{match_details['year']}-{match_details['month']}-{match_details['day']} {match_details['hour']}:{match_details['minute']}:{match_details['second']}"
    try:
        file_datetime = datetime.datetime.strptime(
            file_datetime_str, "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=UTC)
    except ValueError as e:
        print(f"ERROR parsing timestamp: {filename} - {e}")

    print(
        f"MATCHED: {filename}, Product Prefix: {product_code_prefix}, Filename Product: {product_code_file}, Site: {radar_site_file}, Datetime: {file_datetime}"
    )

    return filename


def get_product_codes():
    s3_codes_key = os.path.join(BUCKET_PATH_CODES, "options.json")
    code_options = {}
    try:
        response = s3_client.get_object(Bucket=MY_BUCKET_NAME, Key=s3_codes_key)
        code_options = json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print(
            f"Warning: S3 key '{s3_codes_key}' not found. Starting with an empty codes."
        )
        code_options = {}
    except Exception as e:
        print(f"Error reading file list from S3: {e}")
        return

    return code_options


async def main(loop):
    minutes = 180
    now_utc = datetime.datetime.now(timezone.utc)
    end_time_utc = now_utc
    start_time_utc = now_utc - timedelta(minutes=minutes)
    bucket_name = "unidata-nexrad-level3"
    radar_site = "PDT"

    products = [
        {"type": "hydrometeor", "field": "radar_echo_classification"},
        {
            "type": "precipitation",
            "field": "radar_estimated_rain_rate",
        },
    ]

    executor = ProcessPoolExecutor()

    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    current_path = os.getcwd()
    file_path = os.path.join(current_path, DOWNLOAD_FOLDER)
    plotted_files = {}
    code_options = get_product_codes()

    existing_files = {}
    for product in products:
        product_type = product["type"]
        product_codes = code_options[product_type]

        nested_files_to_download = await asyncio.gather(
            *(
                loop.run_in_executor(
                    executor,
                    fetch_nexrad_level3_data,
                    codes,
                    radar_site,
                    bucket_name,
                    start_time_utc,
                    end_time_utc,
                )
                for codes in product_codes
            ),
        )

        files_to_download = Utl.flatten_list(
            nested_files_to_download, flat=[], remove_falsey=True
        )

        print(f"Total Files found for {product_type}: {files_to_download}")
        if not files_to_download:
            continue

        list_filename = f"nexrad_level3_{product_type}_files.json"
        s3_list_key = BUCKET_PATH_LISTS + list_filename
        try:
            response = s3_client.get_object(Bucket=MY_BUCKET_NAME, Key=s3_list_key)
            existing_files[product_type] = json.loads(
                response["Body"].read().decode("utf-8")
            )
        except s3_client.exceptions.NoSuchKey:
            print(
                f"Warning: S3 key '{s3_list_key}' not found. Starting with an empty existing files list."
            )

        if not existing_files:
            print(f"No existing files found for {product}.")
            continue

        downloaded_files = []
        if files_to_download:
            downloaded_files = await asyncio.gather(
                *(
                    loop.run_in_executor(
                        executor,
                        download_nexrad_level3_data,
                        filename,
                        existing_files[product_type],
                    )
                    for filename in files_to_download
                ),
            )

        else:
            print(f"No {product_type} files to download.")
            continue

        downloaded_files = [d for d in downloaded_files if d]

        print("downloaded_files:", downloaded_files)

        if downloaded_files:
            plotted_files[product_type] = await asyncio.gather(
                *(
                    loop.run_in_executor(
                        executor,
                        read_and_plot_nexrad_level3_data,
                        file,
                        file_path,
                        product_type,
                        product["field"],
                    )
                    for file in downloaded_files
                ),
            )
        else:
            print(f"No files downloaded for {product}.")
            continue

        plotted_files[product_type] = [f for f in plotted_files[product_type] if f]

        if not plotted_files[product_type]:
            print(f"No plotted files for {product_type}.")
            continue

        update_metadata(
            plotted_files[product_type],
            product_type,
            radar_site,
            existing_files[product_type],
            code_options,
        )

    delete_old_s3_files(MY_BUCKET_NAME, BUCKET_PATH_PLOTS, s3_client, 180)

    for root, dirs, files in os.walk(file_path):
        print(f"Removing all temp files")
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

    print(f"Level 3 data processing and image creation complete.")


if __name__ == "__main__":
    start = time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    end = time()
    print(
        f"get_rexrad_data_level3.py completed in {round((end - start)/60, 2)} minutes "
        f"on {datetime.datetime.now()}."
    )
