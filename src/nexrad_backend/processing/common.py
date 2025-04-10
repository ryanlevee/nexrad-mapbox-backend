# src/nexrad_backend/processing/common.py

import io
import os
import logging
from typing import Dict, List, Tuple, Optional

import numpy as np
import matplotlib.figure
import matplotlib.pyplot as plt  # Only needed if type hinting Figure, not for functionality here
from pyart.core import transforms  # For coordinate conversion

# Assuming Py-ART Radar object type hinting if desired
# from pyart.core.radar import Radar

log = logging.getLogger(__name__)

# Constants (could also live in config.py if preferred)
EARTH_RADIUS_METERS = 6370997.0  # Used in pyart transforms


def calculate_geographic_bounding_box(
    radar, sweep_num: int  # Expected: pyart.core.radar.Radar object
) -> Optional[Dict[str, List[float]]]:
    """
    Calculates the geographic bounding box (NW, NE, SE, SW corners)
    for a given radar sweep using AEQD projection.

    Args:
        radar: The Py-ART Radar object.
        sweep_num: The index of the sweep to calculate the bounding box for.

    Returns:
        A dictionary containing the bounding box coordinates {'nw': [lon, lat], ...}
        or None if calculation fails.
    """
    try:
        # Get Cartesian coordinates of gate boundaries for the sweep
        # edges=True gives coordinates of the corners of the gates
        x, y, _ = radar.get_gate_x_y_z(sweep_num, edges=True)

        # Find min/max Cartesian coordinates in meters
        min_x_m = np.min(x)
        max_x_m = np.max(x)
        min_y_m = np.min(y)
        max_y_m = np.max(y)

        # Define corners in Cartesian coordinates (meters)
        # (min_x, max_y) -> NW, (max_x, max_y) -> NE
        # (max_x, min_y) -> SE, (min_x, min_y) -> SW
        corners_xy_m = [
            (min_x_m, max_y_m),
            (max_x_m, max_y_m),
            (max_x_m, min_y_m),
            (min_x_m, min_y_m),
        ]

        # Get radar origin coordinates
        radar_lat_deg = radar.latitude["data"][0]
        radar_lon_deg = radar.longitude["data"][0]

        all_lons: List[float] = []
        all_lats: List[float] = []

        # Convert Cartesian corners to geographic coordinates
        for corner_x_m, corner_y_m in corners_xy_m:
            # Use Py-ART's transformation function
            corner_lon_arr, corner_lat_arr = transforms.cartesian_to_geographic_aeqd(
                corner_x_m,
                corner_y_m,
                radar_lon_deg,
                radar_lat_deg,
                R=EARTH_RADIUS_METERS,
            )
            # Convert numpy float to standard Python float if necessary
            all_lons.append(float(corner_lon_arr))
            all_lats.append(float(corner_lat_arr))

        # Find the overall min/max longitude and latitude
        # Note: This assumes the projection doesn't wrap weirdly near poles/dateline
        # which is generally fine for typical NEXRAD ranges.
        min_lon = min(all_lons)
        max_lon = max(all_lons)
        min_lat = min(all_lats)
        max_lat = max(all_lats)

        # Structure the bounding box dictionary
        bbox = {
            "nw": [min_lon, max_lat],
            "ne": [max_lon, max_lat],
            "se": [max_lon, min_lat],
            "sw": [min_lon, min_lat],
        }
        log.debug(f"Calculated BBox for sweep {sweep_num}: {bbox}")
        return bbox

    except Exception as e:
        log.error(
            f"Error calculating bounding box for sweep {sweep_num}: {e}", exc_info=True
        )
        return None


def save_figure_to_buffer(fig: matplotlib.figure.Figure) -> Optional[io.BytesIO]:
    """
    Saves a Matplotlib figure to an in-memory BytesIO buffer as a PNG image.

    Args:
        fig: The Matplotlib Figure object to save.

    Returns:
        An io.BytesIO buffer containing the PNG data, ready for reading/uploading,
        or None if saving fails. The buffer's position is reset to 0.
    """
    try:
        buffer = io.BytesIO()
        # Save with transparency, tight bounding box, and no extra padding
        fig.savefig(
            buffer,
            format="png",
            transparent=True,
            bbox_inches="tight",
            pad_inches=0,
            dpi=fig.get_dpi(),  # Use the figure's DPI setting
        )
        buffer.seek(0)  # Reset buffer position to the beginning
        log.debug("Successfully saved figure to in-memory buffer.")
        return buffer
    except Exception as e:
        log.error(f"Error saving figure to buffer: {e}", exc_info=True)
        return None
    finally:
        # Ensure the figure is closed to release memory, regardless of success/failure
        plt.close(fig)
        log.debug("Closed figure associated with buffer.")


def cleanup_local_file(file_path: str) -> None:
    """
    Safely removes a file from the local filesystem if it exists.

    Args:
        file_path: The absolute or relative path to the file to remove.
    """
    if not file_path:
        log.warning("Attempted to cleanup an empty file path.")
        return

    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            log.info(f"Successfully removed local file: {file_path}")
        else:
            log.debug(
                f"Local file not found for cleanup (already removed?): {file_path}"
            )
    except OSError as e:
        log.error(f"Error removing local file {file_path}: {e}", exc_info=True)
    except Exception as e:
        log.error(
            f"Unexpected error cleaning up local file {file_path}: {e}", exc_info=True
        )
