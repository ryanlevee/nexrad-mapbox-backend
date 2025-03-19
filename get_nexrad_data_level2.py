import asyncio
import datetime
import io
import json
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
from time import time

import boto3
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pyart
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.client import Config
from pyart.core import transforms

from helpers import delete_old_s3_files

BUCKET_PATH_PLOTS = "plots_level2/"
BUCKET_PATH_LISTS = "lists/"
BUCKET_PATH_FLAG = "flags/"
MY_BUCKET_NAME = "nexrad-mapbox"
DOWNLOAD_FOLDER = "public/nexrad_level2_data"
CHUNK_SIZE = 1024 * 1024 * 2

TRANSFER_CONFIG = TransferConfig(max_concurrency=50)
CONFIG = Config(signature_version=UNSIGNED, s3={"transfer_config": TRANSFER_CONFIG})
SESSION = boto3.session.Session()
noaa_s3_client = SESSION.client("s3", config=CONFIG, region_name="us-east-1")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)


def update_metadata(plotted_files, product_type, radar_site):
    filtered_file_list = {}
    product_file_list = {}
    print(f"Generating file list for nexrad_level2_{product_type}_files.json")
    list_filename = f"nexrad_level2_{product_type}_files.json"
    s3_list_key = BUCKET_PATH_LISTS + list_filename

    try:
        response = s3_client.get_object(Bucket=MY_BUCKET_NAME, Key=s3_list_key)
        product_file_list = json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print(
            f"Warning: S3 key '{s3_list_key}' not found. Starting with an empty list."
        )
        product_file_list = {}
    except Exception as e:
        print(f"Error reading file list from S3: {e}")
        return

    plotted_file_list = list(plotted_files[product_type].keys())
    plotted_file_list.sort()

    latest_file_datetime = plotted_file_list[-1][4:19].replace("_", " ")
    format_string = "%Y%m%d %H%M%S"
    datetime_object = datetime.datetime.strptime(latest_file_datetime, format_string)
    three_hours_ago = datetime_object - timedelta(minutes=180)

    min_file_datetime = (
        str(three_hours_ago).replace("-", "").replace(":", "").replace(" ", "_")
    )

    min_prefix = f"{radar_site}{min_file_datetime}"  # do NOT add "K" for level2

    filtered_product_list = {
        k: v for k, v in product_file_list.items() if k >= min_prefix
    }

    filtered_product_list.update(plotted_files[product_type])

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

    print(f"nexrad_level2_{product_type}_files.json updated in S3")
    print(
        f"Removing old {product_type} pngs and jsons in s3://{MY_BUCKET_NAME}/{BUCKET_PATH_PLOTS}"
    )

    flag_filename = f"update_flags.json"
    s3_flag_key = BUCKET_PATH_FLAG + flag_filename

    try:
        response = s3_client.get_object(Bucket=MY_BUCKET_NAME, Key=s3_flag_key)
        flag_file = json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print(
            f"Warning: S3 key '{s3_flag_key}' not found. Starting with an empty list."
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
            Key=s3_flag_key,
            Body=json_flag_bytes,
            ContentType="application/json",
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"Updated s3://{MY_BUCKET_NAME}/{s3_flag_key}")
        else:
            print(
                f"Failed to update s3://{MY_BUCKET_NAME}/{s3_flag_key}. Response: {response}"
            )
    except Exception as e:
        print(f"Error writing file list to S3: {e}")


def calculate_file_index(radar, sweep_num):
    """Calculates a unique file index based on elevation and azimuth."""
    sweep_data = []
    for i in range(radar.nsweeps):
        start = radar.sweep_start_ray_index["data"][i]
        sweep_data.append(
            (radar.elevation["data"][start], radar.azimuth["data"][start], i)
        )

    sorted_sweeps = sorted(sweep_data)

    for index, (elev, az, num) in enumerate(sorted_sweeps):
        if num == sweep_num:
            return index


def generate_colorbar(ax, product_name, file_base):
    """
    Generates and saves a separate colorbar image in a *dedicated figure*.

    Args:
        ax: The Matplotlib Axes object where the radar plot is drawn.
        product_name (str): Name of the radar product (e.g., 'reflectivity').
        file_base (str): Base filename of the radar data.
    """
    plot_obj = ax.collections[0]
    fig_colorbar = plt.figure(figsize=(0.5, 7))
    ax_colorbar = fig_colorbar.add_axes([0.2, 0.05, 0.6, 0.9])

    fig_colorbar.colorbar(
        mappable=plot_obj,
        cax=ax_colorbar,
        orientation="vertical",
        label=product_name.capitalize() + " (dBZ)",
    )

    ax_colorbar.yaxis.set_label_position("right")
    ax_colorbar.yaxis.label.set_rotation(270)
    ax_colorbar.yaxis.label.set_verticalalignment("bottom")

    colorbar_image_name = f"{file_base}_{product_name}_colorbar.png"
    s3_image_key = BUCKET_PATH_PLOTS + colorbar_image_name

    buffer = io.BytesIO()
    fig_colorbar.savefig(buffer, bbox_inches="tight", format="png", transparent=True)
    buffer.seek(0)

    try:
        response = s3_client.upload_fileobj(
            buffer, MY_BUCKET_NAME, s3_image_key, ExtraArgs={"ContentType": "image/png"}
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"Saved colorbar image to: s3://{MY_BUCKET_NAME}/{s3_image_key}")
        else:
            print(f"Failed to save colorbar image to S3. Response: {response}")
    except Exception as e:
        print(f"Error saving colorbar image to S3: {e}")

    plt.close(fig_colorbar)


def process_single_sweep(radar, sweep_num, file_key, product):
    """Processes a single sweep of radar data and creates an image and JSON."""
    file_index = calculate_file_index(radar, sweep_num)
    sweep_start = radar.sweep_start_ray_index["data"][sweep_num]

    elevation_angle = radar.elevation["data"][sweep_start]
    elevation_list = []
    for s in range(radar.nsweeps):
        start_index = radar.sweep_start_ray_index["data"][s]
        elevation_list.append(radar.elevation["data"][start_index])
    unique_elevations = sorted(list(set(elevation_list)))
    elevation_to_index = {
        elevation: index for index, elevation in enumerate(unique_elevations)
    }
    elevation_index = elevation_to_index[elevation_angle]

    x, y, z = radar.get_gate_x_y_z(sweep_num, edges=True)

    min_x_km = np.min(x) / 1000.0
    max_x_km = np.max(x) / 1000.0
    min_y_km = np.min(y) / 1000.0
    max_y_km = np.max(y) / 1000.0

    corners_xy_km = [
        (min_x_km, max_y_km),
        (max_x_km, max_y_km),
        (max_x_km, min_y_km),
        (min_x_km, min_y_km),
    ]

    all_lons = []
    all_lats = []
    radar_lat = radar.latitude["data"][0]
    radar_lon = radar.longitude["data"][0]
    for corner_x_km, corner_y_km in corners_xy_km:
        corner_lon_np, corner_lat_np = transforms.cartesian_to_geographic_aeqd(
            corner_x_km * 1000.0,
            corner_y_km * 1000.0,
            radar_lon,
            radar_lat,
            R=6370997.0,
        )
        corner_lon = float(corner_lon_np)
        corner_lat = float(corner_lat_np)
        all_lons.append(corner_lon)
        all_lats.append(corner_lat)

    min_lon = min(all_lons)
    max_lon = max(all_lons)
    min_lat = min(all_lats)
    max_lat = max(all_lats)

    bbox = {
        "nw": [min_lon, max_lat],
        "ne": [max_lon, max_lat],
        "se": [max_lon, min_lat],
        "sw": [min_lon, min_lat],
    }

    azimuth_angle = radar.azimuth["data"][sweep_start]

    bbox_json_data = {
        "original_sweep_number": sweep_num + 1,
        "elevation_index": elevation_index + 1,
        "elevation_angle_degrees": np.float64(elevation_angle),
        "azimuth_angle_degrees": np.float64(azimuth_angle),
        "bounding_box_lon_lat": bbox,
    }

    file_base = file_key.split("/")[-1]
    json_name = f"{file_base}_{product}_idx{file_index}.json"
    s3_json_key = BUCKET_PATH_PLOTS + json_name

    json_string = json.dumps(bbox_json_data)
    json_bytes = json_string.encode("utf-8")
    try:
        response = s3_client.put_object(
            Bucket=MY_BUCKET_NAME,
            Key=s3_json_key,
            Body=json_bytes,
            ContentType="application/json",
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"Saved bounding box JSON to: s3://{MY_BUCKET_NAME}/{s3_json_key}")
        else:
            print(f"Failed to save bounding box JSON to S3. Response: {response}")
    except Exception as e:
        print(f"Error saving bounding box JSON to S3: {e}")

    fig = plt.figure(
        figsize=(10, 10),
        dpi=350,
    )

    ax = plt.gca()

    ax.spines["top"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])

    display = pyart.graph.RadarDisplay(radar)
    display.plot(
        product,
        sweep=sweep_num,
        colorbar_label="",
        axislabels=("", ""),
        title_flag=False,
        axislabels_flag=False,
        colorbar_flag=False,
        raster=True,
        vmin=-20,
        vmax=60,
        fig=fig,
        ax=ax,
    )

    image_name = f"{file_base}_{product}_idx{file_index}.png"
    s3_image_key = BUCKET_PATH_PLOTS + image_name

    buffer = io.BytesIO()
    plt.savefig(
        buffer, bbox_inches="tight", pad_inches=0, format="png", transparent=True
    )
    buffer.seek(0)
    image_data = buffer.getvalue()

    try:
        response_image = s3_client.put_object(
            Bucket=MY_BUCKET_NAME,
            Key=s3_image_key,
            Body=image_data,
            ContentType="image/png",
        )
        if (
            response_image
            and response_image.get("ResponseMetadata")
            and response_image["ResponseMetadata"]["HTTPStatusCode"] == 200
        ):
            print(
                f"Created image, Elevation: {elevation_angle:.2f} degrees, Azimuth: "
                f"{azimuth_angle:.2f} degrees. Saved to s3://{MY_BUCKET_NAME}/{s3_image_key}"
            )
        else:
            print(f"Failed to save image to S3. Response: {response_image}")
    except Exception as e:
        print(f"Error saving image to S3 (put_object direct): {type(e)}, {e}")

    plt.close()


def plot_and_save_overlays(file_key, product):
    file_prefix = file_key.split("/")[-1]
    file_path = os.path.join(DOWNLOAD_FOLDER, file_prefix)

    try:
        radar = pyart.io.read(file_path)
        num_sweeps = radar.nsweeps

        for sweep_num in range(num_sweeps):
            process_single_sweep(radar, sweep_num, file_key, product)

        os.remove(file_path)
        return {"file": file_prefix, "sweeps": num_sweeps}

    except Exception as e:
        print(f"Error processing {file_key}: {e}")
        if os.path.exists(file_path):
            os.remove(file_path)
        return {"file": file_prefix, "sweeps": 0}


def get_data_and_create_radar_file(file_key, bucket_name):
    """Downloads and processes all sweeps of a single radar file."""

    current_path = os.getcwd()
    file_path = os.path.join(current_path, DOWNLOAD_FOLDER)
    file_prefix = file_key.split("/")[-1]
    download_path = os.path.join(file_path, file_prefix)
    print(f"Downloading {file_prefix} to {download_path}")

    if not os.path.exists(file_path):
        os.makedirs(file_path)

    bucket_name = "noaa-nexrad-level2"
    response = noaa_s3_client.get_object(Bucket=bucket_name, Key=file_key)
    parts = []
    body = response["Body"]

    while data := body.read(CHUNK_SIZE):
        parts.append(data)

    content = b"".join(parts)

    with open(download_path, "wb") as f:
        f.write(content)

    print(f"Downloaded {file_key} successfully.")

    return file_key


def check_for_files(radar_site, minutes):
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    three_hours_ago = now_utc - datetime.timedelta(minutes=minutes)
    start_date = three_hours_ago.date()
    end_date = now_utc.date()
    s3_resource = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
    bucket = s3_resource.Bucket("noaa-nexrad-level2")

    files_to_process = []
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        day = current_date.day
        prefix = f"{year}/{month:02d}/{day:02d}/{radar_site}/"
        all_files_current_date = bucket.objects.filter(Prefix=prefix)
        print(f"Checking prefix: {prefix}")

        for obj in all_files_current_date:
            file_key = obj.key
            if file_key.endswith("V06") and not file_key.endswith("_MDM"):
                try:
                    filename_parts = file_key.split("/")[-1].split("_")
                    timestamp_str = filename_parts[0][4:] + "_" + filename_parts[1]
                    file_datetime_utc = datetime.datetime.strptime(
                        timestamp_str, "%Y%m%d_%H%M%S"
                    ).replace(tzinfo=datetime.timezone.utc)

                    if three_hours_ago <= file_datetime_utc <= now_utc:
                        files_to_process.append(file_key)
                    else:
                        print(
                            f"Skipping file (out of {minutes}-minute window): {file_key}"
                        )

                except ValueError:
                    print(
                        f"Warning: Could not parse timestamp from filename: {file_key}. Skipping."
                    )
                    continue

        current_date += datetime.timedelta(days=1)
    return files_to_process


async def main(loop):
    radar_site = "KPDT"
    product_type = "reflectivity"
    minutes = 180
    list_file_name = f"nexrad_level2_{product_type}_files.json"
    s3_list_key = BUCKET_PATH_LISTS + list_file_name

    files_to_process = check_for_files(radar_site, minutes)
    print(f"\nFiles to process (last {minutes} minutes): {files_to_process}")

    existing_files = {}
    try:
        response = s3_client.get_object(Bucket=MY_BUCKET_NAME, Key=s3_list_key)
        existing_files = json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print(
            f"Warning: S3 key '{s3_list_key}' not found. Starting with an empty existing files list."
        )
    except Exception as e:
        print(f"Error reading existing files list from S3: {e}")
        existing_files = {}

    filtered_files = [
        file_key
        for file_key in files_to_process
        if file_key.split("/")[-1] not in existing_files
    ]
    print(f"Filtered files to process: {filtered_files}")

    plotted_files = {}
    if filtered_files:
        bucket_name = "noaa-nexrad-level2"

        executor = ProcessPoolExecutor()
        downloaded_files = await asyncio.gather(
            *(
                loop.run_in_executor(
                    executor,
                    get_data_and_create_radar_file,
                    file,
                    bucket_name,
                )
                for file in filtered_files
            )
        )

        if not downloaded_files:
            print("PROBLEM - no dl-ed files")
            return False

        plotted_file_results = await asyncio.gather(
            *(
                loop.run_in_executor(
                    executor,
                    plot_and_save_overlays,
                    file,
                    product_type,
                )
                for file in filtered_files
            ),
        )

        plotted_files[product_type] = {
            item["file"]: {"sweeps": item["sweeps"]}
            for item in plotted_file_results
            if item["file"]
        }

        if plotted_files[product_type]:
            update_metadata(plotted_files, product_type, radar_site)
            delete_old_s3_files(MY_BUCKET_NAME, BUCKET_PATH_PLOTS, s3_client, 180)
    else:
        print("No new files to process.")

    print("Finished processing.")


if __name__ == "__main__":
    start = time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    end = time()
    print(
        f"get_rexrad_data_level2.py completed in {round((end - start)/60, 2)} minutes "
        f"on {datetime.datetime.now()}."
    )
