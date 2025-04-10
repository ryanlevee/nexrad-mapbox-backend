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


# --- AWS Credentials & Region ---
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")  # Default to us-east-1 if not set

# --- Project S3 Bucket Configuration ---
PROJECT_S3_BUCKET = os.getenv("PROJECT_S3_BUCKET", "nexrad-mapbox")

# Standardized path prefixes within the project bucket
S3_PREFIX_PLOTS_L2 = "plots_level2/"
S3_PREFIX_PLOTS_L3 = "plots_level3/"
S3_PREFIX_LISTS = "lists/"
S3_PREFIX_CODES = "codes/"
S3_PREFIX_FLAGS = "flags/"

# Names for specific metadata files within the prefixes
S3_CODES_OPTIONS_FILE = os.path.join(S3_PREFIX_CODES, "options.json")
S3_FLAGS_FILE = os.path.join(S3_PREFIX_FLAGS, "update_flags.json")

# --- Public NEXRAD S3 Buckets ---
# These are less likely to change but could be env vars if needed
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

# --- Boto3 / Download Settings ---
DOWNLOAD_CHUNK_SIZE = int(
    os.getenv("DOWNLOAD_CHUNK_SIZE", str(2 * 1024 * 1024))
)  # 2MB default
S3_MAX_CONCURRENCY = int(os.getenv("S3_MAX_CONCURRENCY", "50"))

# Boto3 configuration for unsigned access to public buckets (NOAA/Unidata)
UNSIGNED_TRANSFER_CONFIG = S3TransferConfig(max_concurrency=S3_MAX_CONCURRENCY)
UNSIGNED_BOTO_CONFIG = BotoConfig(
    signature_version=UNSIGNED, s3={"transfer_config": UNSIGNED_TRANSFER_CONFIG}
)

# Boto3 Session (can be useful for initializing clients)
# Consider if a global session is appropriate vs creating clients per request/task
BOTO3_SESSION = boto3.session.Session()

# --- API Server Settings ---
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "4000"))
# CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*") # Example if you want configurable CORS


# --- Basic Validation (Optional but recommended) ---
def _validate_config():
    """Checks for essential configuration variables."""
    required_aws = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"]
    missing = [var for var in required_aws if not globals().get(var)]
    if missing:
        raise EnvironmentError(
            f"Missing essential AWS configuration. Please set in .env: {', '.join(missing)}"
        )
    # Add more checks as needed (e.g., for PROJECT_S3_BUCKET)


# Run validation when the module is imported
# _validate_config() # Uncomment to enable validation on import


# --- Optional: Function to get configured Boto3 clients ---
# This avoids initializing clients globally if preferred
def get_project_s3_client():
    """Returns a Boto3 S3 client configured for the project bucket."""
    _validate_config()  # Ensure credentials are loaded
    return BOTO3_SESSION.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def get_public_s3_client():
    """Returns a Boto3 S3 client configured for unsigned public access."""
    # No validation needed as it's unsigned
    return BOTO3_SESSION.client(
        "s3", config=UNSIGNED_BOTO_CONFIG, region_name="us-east-1"
    )  # Public buckets often in us-east-1


print("-" * 30)
print("Backend Configuration Loaded:")
print(f"  Project S3 Bucket: {PROJECT_S3_BUCKET}")
print(f"  AWS Region:        {AWS_REGION}")
print(f"  L2 Radar Site:     {RADAR_SITE_L2}")
print(f"  L3 Radar Site:     {RADAR_SITE_L3}")
print(f"  Processing Window: {PROCESSING_WINDOW_MINUTES} mins")
print(f"  API Host:          {API_HOST}:{API_PORT}")
print("-" * 30)
