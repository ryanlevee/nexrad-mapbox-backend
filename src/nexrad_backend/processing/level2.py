import os
import io
import json
import logging
from typing import Dict, Optional, Any, List

# Third-party imports
import numpy as np
import pyart
import matplotlib.pyplot as plt

# Ensure matplotlib uses a non-interactive backend suitable for scripts/servers.
# This should ideally be set once at the application entry point (e.g., in the script),
# but setting it here defensively is also possible.
try:
    import matplotlib

    matplotlib.use("Agg")
except ImportError:
    logging.warning("Matplotlib not found, plotting functionality will fail.")


# Local application imports
from nexrad_backend import config
from nexrad_backend.services import s3_service
from nexrad_backend.processing import common

log = logging.getLogger(__name__)

# Default plotting parameters for reflectivity (may move to config...)
REFLECTIVITY_VMIN = -20
REFLECTIVITY_VMAX = 60
PLOT_DPI = 350


def _calculate_sweep_elevation_index(radar, sweep_num: int) -> int:
    """
    Calculates a unique 0-based index for a sweep based on its sorted elevation angle.
    Lower elevation angles get lower indices. If elevations are equal, azimuth is used
    as a secondary sort key (though unlikely needed for unique indexing usually).

    Args:
        radar: The Py-ART Radar object.
        sweep_num: The original sweep number (0 to nsweeps-1).

    Returns:
        The calculated index based on sorted elevation.
    """
    sweep_data = []
    for i in range(radar.nsweeps):
        try:
            start = radar.sweep_start_ray_index["data"][i]
            elevation = radar.elevation["data"][start]
            azimuth = radar.azimuth["data"][start]  # Secondary sort key
            sweep_data.append(
                (elevation, azimuth, i)
            )  # Store original sweep number 'i'
        except IndexError:
            log.warning(
                f"Could not access sweep data for index {i} in _calculate_sweep_elevation_index"
            )
            continue  # Skip potentially corrupt sweep data

    # Sort primarily by elevation, secondarily by azimuth
    sorted_sweeps = sorted(sweep_data)

    # Find the rank (index) of the original sweep_num in the sorted list
    for index, (elev, az, original_num) in enumerate(sorted_sweeps):
        if original_num == sweep_num:
            return index

    log.error(
        f"Could not find original sweep number {sweep_num} in sorted sweeps. Returning -1."
    )
    return -1  # Indicate error


def _process_l2_sweep_to_s3(
    radar,
    sweep_num: int,
    file_key_prefix: str,  # e.g., KPDT20250409_123456_V06
    product: str,  # e.g., 'reflectivity'
    s3_project_client,
    bucket: str,
    plot_prefix: str,
) -> Optional[Dict[str, Any]]:
    """
    Processes a single Level 2 radar sweep: calculates metadata, plots the image,
    and uploads both JSON metadata and PNG image to S3.

    Args:
        radar: The Py-ART Radar object.
        sweep_num: The sweep number to process.
        file_key_prefix: Base key for naming output files (derived from input filename).
        product: The product field name (e.g., 'reflectivity').
        s3_project_client: Initialized Boto3 S3 client for the project bucket.
        bucket: Project S3 bucket name.
        plot_prefix: S3 prefix where plots/JSON should be stored (e.g., 'plots_level2/').

    Returns:
        A dictionary with info about the processed sweep {'json_key': ..., 'png_key': ...},
        or None if processing failed.
    """
    log.info(f"Processing sweep {sweep_num} for {file_key_prefix}, product {product}")

    try:
        # 1. Calculate Sweep Index based on sorted elevation
        sweep_elevation_index = _calculate_sweep_elevation_index(radar, sweep_num)
        if sweep_elevation_index == -1:
            raise ValueError(
                f"Failed to calculate elevation index for sweep {sweep_num}"
            )

        # 2. Get Metadata (Angles, BBox)
        sweep_start_ray_index = radar.sweep_start_ray_index["data"][sweep_num]
        elevation_angle = float(radar.elevation["data"][sweep_start_ray_index])
        azimuth_angle = float(
            radar.azimuth["data"][sweep_start_ray_index]
        )  # Azimuth of the first ray

        bbox = common.calculate_geographic_bounding_box(radar, sweep_num)
        if bbox is None:
            raise ValueError(f"Failed to calculate bounding box for sweep {sweep_num}")

        # 3. Prepare JSON Metadata
        sweep_metadata = {
            "original_sweep_number": sweep_num + 1,
            "elevation_index": sweep_elevation_index + 1,  # 1-based index
            "elevation_angle_degrees": elevation_angle,
            "azimuth_angle_degrees": azimuth_angle,  # First ray azimuth
            "bounding_box_lon_lat": bbox,
        }
        json_filename = f"{file_key_prefix}_{product}_idx{sweep_elevation_index}.json"
        json_s3_key = os.path.join(plot_prefix, json_filename).replace("\\", "/")

        # 4. Upload JSON Metadata
        if not s3_service.update_json_in_s3(
            s3_project_client, bucket, json_s3_key, sweep_metadata
        ):
            # Logged error in service, maybe raise specific exception?
            raise IOError(f"Failed to upload metadata JSON to {json_s3_key}")
        log.info(f"Uploaded metadata: {json_s3_key}")

        # 5. Plotting
        fig = plt.figure(figsize=(10, 10), dpi=PLOT_DPI)
        ax = plt.gca()  # Get current axes

        # Configure plot appearance (no axes, labels, titles, colorbar)
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
            ax=ax,
            fig=fig,
            colorbar_flag=False,
            title_flag=False,
            axislabels_flag=False,
            vmin=REFLECTIVITY_VMIN,
            vmax=REFLECTIVITY_VMAX,
            raster=True,  # Use rasterized plotting for potentially better performance/smaller files
        )

        # 6. Save Plot to Buffer
        png_buffer = common.save_figure_to_buffer(fig)  # Also closes the figure
        if png_buffer is None:
            raise IOError("Failed to save plot figure to buffer.")

        # 7. Upload PNG Plot
        png_filename = f"{file_key_prefix}_{product}_idx{sweep_elevation_index}.png"
        png_s3_key = os.path.join(plot_prefix, png_filename).replace("\\", "/")

        if not s3_service.put_s3_object(
            s3_project_client, bucket, png_s3_key, png_buffer.getvalue(), "image/png"
        ):
            raise IOError(f"Failed to upload plot PNG to {png_s3_key}")
        log.info(f"Uploaded plot: {png_s3_key}")

        return {
            "sweep_num": sweep_num,
            "elevation_index": sweep_elevation_index,
            "elevation_angle": elevation_angle,
            "json_key": json_s3_key,
            "png_key": png_s3_key,
        }

    except Exception as e:
        log.error(
            f"Failed to process sweep {sweep_num} for {file_key_prefix}: {e}",
            exc_info=True,
        )
        # Ensure figure is closed if error occurred before buffer saving finished
        if "fig" in locals() and plt.fignum_exists(fig.number):
            plt.close(fig)
            log.debug(
                f"Closed figure due to error during sweep {sweep_num} processing."
            )
        return None


def process_level2_file(
    local_file_path: str,
    file_key: str,  # The original S3 key from NOAA (used for naming prefix)
    product: str,
    s3_project_client,
    bucket: str = config.PROJECT_S3_BUCKET,
    plot_prefix: str = config.S3_PREFIX_PLOTS_L2,
) -> Optional[Dict[str, Any]]:
    """
    Reads a downloaded Level 2 NEXRAD file, processes each sweep (plotting, metadata),
    uploads results to S3, and cleans up the local file.

    Args:
        local_file_path: Path to the downloaded NEXRAD Level 2 file.
        file_key: The original S3 key from the public bucket (e.g., '2025/04/09/KPDT/KPDT20250409_123456_V06').
        product: The product field name (e.g., 'reflectivity').
        s3_project_client: Initialized Boto3 S3 client for the project bucket.
        bucket: Project S3 bucket name.
        plot_prefix: S3 prefix for storing processed plots and JSON.

    Returns:
        A dictionary summarizing the processing results for the file, including
        sweep count and individual sweep results, or None if reading the file failed.
    """
    radar = None
    file_key_prefix = file_key.split("/")[-1]  # e.g., KPDT20250409_123456_V06
    log.info(
        f"Starting processing for L2 file: {local_file_path} (key prefix: {file_key_prefix})"
    )

    try:
        # 1. Read Radar File
        radar = pyart.io.read(local_file_path)
        num_sweeps = radar.nsweeps
        log.info(f"Successfully read {local_file_path}, found {num_sweeps} sweeps.")

        # 2. Process Each Sweep
        sweep_results = []
        processed_count = 0
        for sweep_num in range(num_sweeps):
            result = _process_l2_sweep_to_s3(
                radar,
                sweep_num,
                file_key_prefix,
                product,
                s3_project_client,
                bucket,
                plot_prefix,
            )
            if result:
                sweep_results.append(result)
                processed_count += 1

        log.info(
            f"Finished processing sweeps for {file_key_prefix}. Successfully processed: {processed_count}/{num_sweeps}"
        )

        # 3. Return Summary
        return {
            "original_key": file_key,
            "processed_key_prefix": file_key_prefix,  # The base key used in lists
            "total_sweeps": num_sweeps,
            "sweeps_processed": processed_count,
            "sweep_results": sweep_results,  # List of dicts from _process_l2_sweep_to_s3
        }

    except FileNotFoundError:
        log.error(f"L2 file not found at path: {local_file_path}")
        return None  # Don't cleanup if file wasn't even there
    except Exception as e:
        log.error(
            f"Failed to read or process L2 file {local_file_path}: {e}", exc_info=True
        )
        return None  # Indicate failure
    finally:
        # 4. Cleanup Local File - always attempt cleanup if path exists
        common.cleanup_local_file(local_file_path)
