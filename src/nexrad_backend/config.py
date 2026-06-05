import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config as BotoConfig
from boto3.s3.transfer import TransferConfig as S3TransferConfig
from botocore import UNSIGNED

# Load environment variables from the .env file in the project root
# Ensure this runs before accessing os.getenv for variables
dotenv_path = os.path.join(
    os.path.dirname(__file__), "..", "..", ".env"
)  # Assumes config.py is in src/nexrad_backend/
load_dotenv(dotenv_path=dotenv_path)
# Alternatively, if running scripts/server from project root, load_dotenv() might just work.
# load_dotenv()


# --- Cloudflare R2 Configuration ---
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET", "nexrad-mapbox")

# --- AWS Region (for public bucket access only) ---
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Standardized path prefixes (used as Drive subfolder names)
PREFIX_PLOTS_L2 = "plots_level2/"
PREFIX_PLOTS_L3 = "plots_level3/"
PREFIX_LISTS = "lists/"
PREFIX_CODES = "codes/"
PREFIX_FLAGS = "flags/"

# Names for specific metadata files within the prefixes
CODES_OPTIONS_FILE = PREFIX_CODES + "options.json"
FLAGS_FILE = PREFIX_FLAGS + "update_flags.json"

# --- Public NEXRAD S3 Buckets (unsigned/anonymous access) ---
NOAA_L2_BUCKET = "noaa-nexrad-level2"
UNIDATA_L3_BUCKET = "unidata-nexrad-level3"

# --- Data Processing Settings ---
RADAR_SITE_L2 = os.getenv("RADAR_SITE_L2", "KPDT")  # Level 2 Site (e.g., KPDT)
RADAR_SITE_L3 = os.getenv("RADAR_SITE_L3", "PDT")  # Level 3 Site (often 3 chars)
# Time window (in minutes) for fetching recent NEXRAD data
PROCESSING_WINDOW_MINUTES = int(os.getenv("PROCESSING_WINDOW_MINUTES", "180"))
# Time window (in minutes) for deleting old processed files from project S3 bucket
CLEANUP_WINDOW_MINUTES = int(
    os.getenv("CLEANUP_WINDOW_MINUTES", str(PROCESSING_WINDOW_MINUTES + 60))
)

# --- Local File Settings ---
# Directory for temporary downloads during processing
# Consider using tempfile module for truly temporary files if appropriate
DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_FOLDER", "temp_nexrad_downloads")
# Ensure this directory exists or create it in the scripts/services

# --- Boto3 / Download Settings (for public bucket access) ---
DOWNLOAD_CHUNK_SIZE = int(
    os.getenv("DOWNLOAD_CHUNK_SIZE", str(2 * 1024 * 1024))
)  # 2MB default
S3_MAX_CONCURRENCY = int(os.getenv("S3_MAX_CONCURRENCY", "50"))

# Boto3 configuration for unsigned access to public buckets (NOAA/Unidata)
UNSIGNED_TRANSFER_CONFIG = S3TransferConfig(max_concurrency=S3_MAX_CONCURRENCY)
UNSIGNED_BOTO_CONFIG = BotoConfig(
    signature_version=UNSIGNED, s3={"transfer_config": UNSIGNED_TRANSFER_CONFIG}
)



# --- API Server Settings ---
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "4000"))
# CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*") # Example if you want configurable CORS


# --- Validation ---
def _validate_r2_config():
    """Checks for essential R2 configuration."""
    missing = []
    if not R2_ENDPOINT_URL:
        missing.append("R2_ENDPOINT_URL")
    if not R2_ACCESS_KEY_ID:
        missing.append("R2_ACCESS_KEY_ID")
    if not R2_SECRET_ACCESS_KEY:
        missing.append("R2_SECRET_ACCESS_KEY")
    if missing:
        raise EnvironmentError(
            f"Missing R2 configuration. Please set in .env: {', '.join(missing)}"
        )


def get_public_s3_client():
    """Returns a Boto3 S3 client configured for unsigned public access."""
    return boto3.client(
        "s3",
        config=UNSIGNED_BOTO_CONFIG,
        region_name="us-east-1",
        aws_access_key_id="",
        aws_secret_access_key="",
    )


import multiprocessing as _mp

if _mp.current_process().name == "MainProcess":
    print("-" * 30)
    print("Backend Configuration Loaded:")
    print(f"  R2 Bucket:         {R2_BUCKET}")
    print(f"  R2 Endpoint:       {R2_ENDPOINT_URL}")
    print(f"  L2 Radar Site:     {RADAR_SITE_L2}")
    print(f"  L3 Radar Site:     {RADAR_SITE_L3}")
    print(f"  Processing Window: {PROCESSING_WINDOW_MINUTES} mins")
    print(f"  API Host:          {API_HOST}:{API_PORT}")
    print("-" * 30)
