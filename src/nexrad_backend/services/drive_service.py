import json
import datetime
import logging

import boto3
from botocore.exceptions import ClientError

from nexrad_backend import config

log = logging.getLogger(__name__)

_r2_client = None


def _get_r2_client():
    global _r2_client
    if _r2_client is None:
        _r2_client = boto3.client(
            "s3",
            endpoint_url=config.R2_ENDPOINT_URL,
            aws_access_key_id=config.R2_ACCESS_KEY_ID,
            aws_secret_access_key=config.R2_SECRET_ACCESS_KEY,
            region_name="auto",
        )
    return _r2_client


def get_object_body(key: str) -> bytes | None:
    """Download file content by storage key."""
    try:
        client = _get_r2_client()
        response = client.get_object(Bucket=config.R2_BUCKET, Key=key)
        body = response["Body"].read()
        log.info(f"Successfully retrieved: {key}")
        return body
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            log.warning(f"File not found: {key}")
        else:
            log.error(f"Error retrieving {key}: {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error retrieving {key}: {e}", exc_info=True)
        return None


def put_object(key: str, body_bytes: bytes, content_type: str) -> bool:
    """Upload bytes to R2 at the given key."""
    try:
        client = _get_r2_client()
        client.put_object(
            Bucket=config.R2_BUCKET,
            Key=key,
            Body=body_bytes,
            ContentType=content_type,
        )
        log.info(f"Successfully uploaded: {key}")
        return True
    except ClientError as e:
        log.error(f"Error uploading {key}: {e}")
        return False
    except Exception as e:
        log.error(f"Unexpected error uploading {key}: {e}", exc_info=True)
        return False


def update_json(key: str, data_dict: dict) -> bool:
    """Upload a dictionary as a JSON file."""
    try:
        json_bytes = json.dumps(data_dict, indent=2).encode("utf-8")
        return put_object(key, json_bytes, "application/json")
    except TypeError as e:
        log.error(f"Error serializing dict to JSON for key {key}: {e}", exc_info=True)
        return False


def object_exists(key: str) -> bool:
    """Check if a file exists at the given key."""
    try:
        client = _get_r2_client()
        client.head_object(Bucket=config.R2_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        log.error(f"Error checking existence for {key}: {e}", exc_info=True)
        return False
    except Exception as e:
        log.error(f"Unexpected error checking existence for {key}: {e}", exc_info=True)
        return False


def list_object_keys(prefix: str) -> list[str]:
    """List all file keys under a prefix."""
    try:
        client = _get_r2_client()
        keys = []
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=config.R2_BUCKET, Prefix=prefix)
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    keys.append(obj["Key"])
        log.info(f"Listed {len(keys)} files with prefix '{prefix}'.")
        return keys
    except ClientError as e:
        log.error(f"Error listing files with prefix {prefix}: {e}")
        return []
    except Exception as e:
        log.error(f"Unexpected error listing files with prefix {prefix}: {e}", exc_info=True)
        return []


def delete_old_files(prefix: str, older_than_minutes: int) -> int:
    """Delete .png and .json files older than the specified age under a prefix."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    cutoff_time = now_utc - datetime.timedelta(minutes=older_than_minutes)

    client = _get_r2_client()
    total_deleted = 0
    objects_to_delete = []

    log.info(f"Scanning '{prefix}' for files older than {cutoff_time} UTC...")

    try:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=config.R2_BUCKET, Prefix=prefix)
        for page in pages:
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                key = obj["Key"]
                last_modified = obj["LastModified"]
                if (key.endswith(".png") or key.endswith(".json")) and last_modified < cutoff_time:
                    objects_to_delete.append({"Key": key})

                    if len(objects_to_delete) == 1000:
                        deleted = _batch_delete(client, objects_to_delete)
                        total_deleted += deleted
                        objects_to_delete = []

        if objects_to_delete:
            deleted = _batch_delete(client, objects_to_delete)
            total_deleted += deleted

        if total_deleted > 0:
            log.info(f"Cleanup complete. Deleted {total_deleted} old files from '{prefix}'.")
        else:
            log.info(f"No old files found in '{prefix}'.")

    except ClientError as e:
        log.error(f"Error during cleanup of '{prefix}': {e}")
    except Exception as e:
        log.error(f"Unexpected error during cleanup of '{prefix}': {e}", exc_info=True)

    return total_deleted


def _batch_delete(client, objects: list[dict]) -> int:
    """Perform a batch delete operation."""
    if not objects:
        return 0
    try:
        response = client.delete_objects(
            Bucket=config.R2_BUCKET,
            Delete={"Objects": objects},
        )
        deleted = len(response.get("Deleted", []))
        if "Errors" in response and response["Errors"]:
            for err in response["Errors"]:
                log.error(f"  Delete error: {err.get('Key')}: {err.get('Code')} - {err.get('Message')}")
        return deleted
    except ClientError as e:
        log.error(f"Error during batch delete: {e}")
        return 0
