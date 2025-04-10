import os
import io
import json
import logging
import re
from typing import Dict, Optional, Any

# Third-party imports
import numpy as np
import pyart
import matplotlib.pyplot as plt

# Ensure matplotlib uses a non-interactive backend
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

# Default plotting parameters (adjust or make configurable if needed)
PLOT_DPI = 350


def _normalize_l3_filename_key(original_filename: str) -> Optional[str]:
    """
    Normalizes an original Level 3 filename (e.g., PDT_HHC_2025_04_09_153000)
    into the key format used for processed files and lists
    (e.g., KPDT20250409_153000_HHC).

    Args:
        original_filename: The filename as downloaded from Unidata S3.

    Returns:
        The normalized key prefix string, or None if parsing fails.
    """
    # Regex to parse SITE_CODE_YYYY_MM_DD_HHMMSS format
    match = re.match(
        r"^(?P<site>[A-Z]{3})_(?P<product>[A-Z0-9]{3})_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})$",
        original_filename,
    )

    if not match:
        log.warning(f"Could not parse original L3 filename format: {original_filename}")
        return None

    d = match.groupdict()
    # Construct key: K<SITE><YYYYMMDD>_<HHMMSS>_<PRODUCT>
    normalized_key = f"K{d['site']}{d['year']}{d['month']}{d['day']}_{d['hour']}{d['minute']}{d['second']}_{d['product']}"
    log.debug(f"Normalized '{original_filename}' to '{normalized_key}'")
    return normalized_key


def process_level3_file(
    local_file_path: str,
    original_filename: str,  # Filename as downloaded (e.g., PDT_HHC_...)
    product_type: str,  # General type (e.g., 'hydrometeor', 'precipitation')
    field: str,  # Specific Py-ART field name (e.g., 'radar_echo_classification')
    s3_project_client,
    bucket: str = config.PROJECT_S3_BUCKET,
    plot_prefix: str = config.S3_PREFIX_PLOTS_L3,
) -> Optional[str]:
    """
    Reads a downloaded Level 3 NEXRAD file, processes it (plotting, metadata),
    uploads results to S3, and cleans up the local file.

    Args:
        local_file_path: Path to the downloaded NEXRAD Level 3 file.
        original_filename: The filename as downloaded from the public bucket.
        product_type: General category ('hydrometeor', 'precipitation'). Used for naming output.
        field: The specific Py-ART field to plot (e.g., 'reflectivity', 'radar_echo_classification').
        s3_project_client: Initialized Boto3 S3 client for the project bucket.
        bucket: Project S3 bucket name.
        plot_prefix: S3 prefix for storing processed plots and JSON.

    Returns:
        The normalized filename key prefix (e.g., KPDT20250409_153000_HHC) if processing
        and all uploads were successful, otherwise None.
    """
    radar = None
    log.info(
        f"Starting processing for L3 file: {local_file_path} (original: {original_filename})"
    )

    # 1. Normalize filename for output keys
    normalized_key_prefix = _normalize_l3_filename_key(original_filename)
    if not normalized_key_prefix:
        log.error(
            f"Failed to normalize filename: {original_filename}. Aborting processing."
        )
        common.cleanup_local_file(
            local_file_path
        )  # Cleanup even if normalization fails
        return None

    try:
        # 2. Read Radar File
        radar = pyart.io.read(local_file_path)
        log.info(f"Successfully read {local_file_path}")

        # Level 3 products typically have one sweep/elevation
        sweep_num = 0
        if radar.nsweeps > 1:
            log.warning(
                f"Expected 1 sweep for L3 file {original_filename}, but found {radar.nsweeps}. Processing sweep 0."
            )
        elif radar.nsweeps == 0:
            raise ValueError("L3 file contains no sweeps.")

        # 3. Get Metadata (Angles, BBox)
        sweep_start_ray_index = radar.sweep_start_ray_index["data"][sweep_num]
        elevation_angle = float(radar.elevation["data"][sweep_start_ray_index])
        azimuth_angle = float(
            radar.azimuth["data"][sweep_start_ray_index]
        )  # First ray azimuth

        bbox = common.calculate_geographic_bounding_box(radar, sweep_num)
        if bbox is None:
            raise ValueError(f"Failed to calculate bounding box for L3 file")

        # 4. Prepare JSON Metadata
        # L3 uses index 0 consistently, as there's only one "tilt" per product file
        file_index = 0
        metadata = {
            # No original_sweep_number or elevation_index needed like in L2
            "elevation_angle_degrees": elevation_angle,
            "azimuth_angle_degrees": azimuth_angle,
            "bounding_box_lon_lat": bbox,
        }
        json_filename = f"{normalized_key_prefix}_{product_type}_idx{file_index}.json"
        json_s3_key = os.path.join(plot_prefix, json_filename).replace("\\", "/")

        # 5. Upload JSON Metadata
        if not s3_service.update_json_in_s3(
            s3_project_client, bucket, json_s3_key, metadata
        ):
            raise IOError(f"Failed to upload metadata JSON to {json_s3_key}")
        log.info(f"Uploaded metadata: {json_s3_key}")

        # 6. Plotting
        fig = plt.figure(figsize=(10, 10), dpi=PLOT_DPI)
        ax = plt.gca()

        # Configure plot appearance
        ax.spines["top"].set_visible(False)
        ax.spines["bottom"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_xticks([])
        ax.set_yticks([])

        display = pyart.graph.RadarDisplay(radar)
        try:
            # Plot the specified field for the single sweep (sweep 0)
            # Rely on Py-ART defaults for L3 colormaps/ranges unless specified
            display.plot(
                field,
                sweep=sweep_num,
                ax=ax,
                fig=fig,
                colorbar_flag=False,
                title_flag=False,
                axislabels_flag=False,
                raster=True,
            )
        except KeyError:
            log.error(
                f"Field '{field}' not found in radar object from file {original_filename}. Available fields: {list(radar.fields.keys())}"
            )
            raise  # Re-raise the error to abort processing this file

        # 7. Save Plot to Buffer
        png_buffer = common.save_figure_to_buffer(fig)  # Also closes figure
        if png_buffer is None:
            raise IOError("Failed to save plot figure to buffer.")

        # 8. Upload PNG Plot
        png_filename = f"{normalized_key_prefix}_{product_type}_idx{file_index}.png"
        png_s3_key = os.path.join(plot_prefix, png_filename).replace("\\", "/")

        if not s3_service.put_s3_object(
            s3_project_client, bucket, png_s3_key, png_buffer.getvalue(), "image/png"
        ):
            raise IOError(f"Failed to upload plot PNG to {png_s3_key}")
        log.info(f"Uploaded plot: {png_s3_key}")

        # 9. Success: Return the key prefix used for uploads
        return normalized_key_prefix

    except FileNotFoundError:
        log.error(f"L3 file not found at path: {local_file_path}")
        return None  # Don't attempt cleanup if file never existed
    except Exception as e:
        log.error(
            f"Failed to read or process L3 file {local_file_path} (original: {original_filename}): {e}",
            exc_info=True,
        )
        # Ensure figure is closed if error occurred after figure creation but before buffer saving
        if (
            "fig" in locals()
            and isinstance(fig, plt.Figure)
            and plt.fignum_exists(fig.number)
        ):
            plt.close(fig)
            log.debug("Closed figure due to error during L3 processing.")
        return None  # Indicate failure
    finally:
        # 10. Cleanup Local File - always attempt cleanup if path exists
        common.cleanup_local_file(local_file_path)
