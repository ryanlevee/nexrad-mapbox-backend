import io
import json
import os

import boto3
import matplotlib.pyplot as plt
import numpy as np
import pyart
from pyart.core import transforms

RELATIVE_PATH = "./public/"
LOCAL_COLORBAR_PATH = f"{os.path.abspath(RELATIVE_PATH)}/plots_level3/"
BUCKET_PATH_PLOTS = "plots_level3/"
MY_BUCKET_NAME = "nexrad-mapbox"
DOWNLOAD_FOLDER = "public/nexrad_level3_data"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)


def read_and_plot_nexrad_level3_data(filename, file_path, product_type, field):
    fns = filename.split("_")

    normalized_filename = (
        f"K{''.join([fns[0], *fns[2:5]])}_{''.join(fns[5:8])}_{fns[1]}"
    )

    file_index = 0
    radar_data_path = os.path.join(file_path, filename)
    radar = False

    try:
        radar = pyart.io.read(radar_data_path)
    except FileNotFoundError:
        print(f"Error: File not found at path: {radar_data_path}")
        return False
    except AssertionError as e:
        print(e)
        return False
    except Exception as e:
        print(f"General Error occurred while reading file: {radar_data_path}")
        print("Error details:", e)
        return False

    if not radar:
        print(f"Error: {product_type} radar cannot be created for: {radar_data_path}")
        return False

    display = pyart.graph.RadarDisplay(radar)
    sweep_num = 0
    sweep_start = radar.sweep_start_ray_index["data"][sweep_num]
    elevation_angle = radar.elevation["data"][sweep_start]

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
        "elevation_angle_degrees": np.float64(elevation_angle),
        "azimuth_angle_degrees": np.float64(azimuth_angle),
        "bounding_box_lon_lat": bbox,
    }

    json_ext = ".json"
    save_json_filename = (
        f"{normalized_filename}_{product_type}_idx{file_index}{json_ext}"
    )
    s3_json_key = BUCKET_PATH_PLOTS + save_json_filename

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

    fig = plt.figure(figsize=(10, 10), dpi=350)
    ax = plt.gca()

    ax.spines["top"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])

    display.plot(
        field,
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

    img_ext = ".png"
    save_img_filename = f"{normalized_filename}_{product_type}_idx{file_index}{img_ext}"

    s3_image_key = BUCKET_PATH_PLOTS + save_img_filename

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

    return normalized_filename


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
    colorbar_image_path_full = os.path.join(LOCAL_COLORBAR_PATH, colorbar_image_name)

    fig_colorbar.savefig(
        colorbar_image_path_full,
        bbox_inches="tight",
        format="png",
        transparent=True,
    )
    plt.close(fig_colorbar)
    print(f"Saved colorbar image to: {colorbar_image_path_full}")
