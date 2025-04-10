import json
import datetime
import logging
from botocore.exceptions import ClientError

# Setup basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def get_s3_object_body(client, bucket: str, key: str) -> bytes | None:
    """
    Retrieves the body of an object from S3.

    Args:
        client: Initialized Boto3 S3 client instance.
        bucket: The name of the S3 bucket.
        key: The key (path) of the object within the bucket.

    Returns:
        The object body as bytes if successful, otherwise None.
    """
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body_bytes = response["Body"].read()
        log.info(f"Successfully retrieved object: s3://{bucket}/{key}")
        return body_bytes
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            log.warning(f"Object not found: s3://{bucket}/{key}")
        else:
            log.error(
                f"Error retrieving object s3://{bucket}/{key}: {e}", exc_info=True
            )
        return None
    except Exception as e:
        log.error(
            f"Unexpected error retrieving object s3://{bucket}/{key}: {e}",
            exc_info=True,
        )
        return None


def put_s3_object(
    client, bucket: str, key: str, body_bytes: bytes, content_type: str
) -> bool:
    """
    Uploads an object (as bytes) to S3.

    Args:
        client: Initialized Boto3 S3 client instance.
        bucket: The name of the S3 bucket.
        key: The key (path) where the object will be stored.
        body_bytes: The content of the object as bytes.
        content_type: The MIME type of the object (e.g., 'image/png', 'application/json').

    Returns:
        True if the upload was successful, False otherwise.
    """
    try:
        response = client.put_object(
            Bucket=bucket, Key=key, Body=body_bytes, ContentType=content_type
        )
        # Check if response is successful (status code 200 typically)
        status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status_code == 200:
            log.info(f"Successfully uploaded object: s3://{bucket}/{key}")
            return True
        else:
            log.error(
                f"Failed to upload object s3://{bucket}/{key}. Status: {status_code}, Response: {response}"
            )
            return False
    except ClientError as e:
        log.error(f"Error uploading object s3://{bucket}/{key}: {e}", exc_info=True)
        return False
    except Exception as e:
        log.error(
            f"Unexpected error uploading object s3://{bucket}/{key}: {e}", exc_info=True
        )
        return False


def update_json_in_s3(client, bucket: str, key: str, data_dict: dict) -> bool:
    """
    Uploads a Python dictionary as a JSON file to S3.

    Args:
        client: Initialized Boto3 S3 client instance (likely the project client).
        bucket: The name of the S3 bucket.
        key: The key (path) where the JSON file will be stored.
        data_dict: The Python dictionary to upload.

    Returns:
        True if the upload was successful, False otherwise.
    """
    try:
        json_string = json.dumps(
            data_dict, indent=2
        )  # Add indent for readability in S3 console
        json_bytes = json_string.encode("utf-8")
        return put_s3_object(client, bucket, key, json_bytes, "application/json")
    except TypeError as e:
        log.error(
            f"Error serializing dictionary to JSON for key {key}: {e}", exc_info=True
        )
        return False
    except Exception as e:
        log.error(f"Unexpected error preparing JSON for key {key}: {e}", exc_info=True)
        return False


def object_exists(client, bucket: str, key: str) -> bool:
    """
    Checks if an object exists in S3 using head_object.

    Args:
        client: Initialized Boto3 S3 client instance.
        bucket: The name of the S3 bucket.
        key: The key (path) of the object to check.

    Returns:
        True if the object exists, False otherwise.
    """
    try:
        client.head_object(Bucket=bucket, Key=key)
        # log.debug(f"Object exists: s3://{bucket}/{key}") # Can be noisy
        return True
    except ClientError as e:
        # If it was a 404 error, the object does not exist. Otherwise, it's another error.
        if e.response["Error"]["Code"] == "404":
            # log.debug(f"Object does not exist: s3://{bucket}/{key}") # Can be noisy
            return False
        else:
            # Got another error (e.g., access denied)
            log.error(
                f"Error checking existence for s3://{bucket}/{key}: {e}", exc_info=True
            )
            return False  # Treat other errors as "doesn't exist" or handle differently
    except Exception as e:
        log.error(
            f"Unexpected error checking existence for s3://{bucket}/{key}: {e}",
            exc_info=True,
        )
        return False


def list_object_keys(client, bucket: str, prefix: str = "") -> list[str]:
    """
    Lists object keys within an S3 bucket matching a prefix.

    Args:
        client: Initialized Boto3 S3 client instance.
        bucket: The name of the S3 bucket.
        prefix: The prefix to filter object keys (optional).

    Returns:
        A list of object keys matching the prefix. Returns empty list on error.
    """
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    keys.append(obj["Key"])
        log.info(
            f"Listed {len(keys)} objects with prefix '{prefix}' in bucket '{bucket}'."
        )
        return keys
    except ClientError as e:
        log.error(
            f"Error listing objects in s3://{bucket}/{prefix}: {e}", exc_info=True
        )
        return []
    except Exception as e:
        log.error(
            f"Unexpected error listing objects in s3://{bucket}/{prefix}: {e}",
            exc_info=True,
        )
        return []


def delete_old_files(client, bucket: str, prefix: str, older_than_minutes: int) -> int:
    """
    Deletes objects (matching *.png or *.json) older than a specified number
    of minutes from an S3 prefix.

    Args:
        client: Initialized Boto3 S3 client instance (likely the project client).
        bucket: The name of the S3 bucket.
        prefix: The prefix within the bucket to scan for old files.
        older_than_minutes: The age threshold in minutes. Files older than this will be deleted.

    Returns:
        The total number of files successfully deleted.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    cutoff_time = now_utc - datetime.timedelta(minutes=older_than_minutes)
    total_deleted_count = 0

    paginator = client.get_paginator("list_objects_v2")
    objects_to_delete_batch = []

    log.info(
        f"Scanning s3://{bucket}/{prefix} for files older than {cutoff_time} UTC..."
    )

    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    last_modified = obj["LastModified"]  # Already timezone-aware (UTC)

                    # Only consider PNG and JSON files for deletion
                    if (
                        key.endswith(".png") or key.endswith(".json")
                    ) and last_modified < cutoff_time:
                        objects_to_delete_batch.append({"Key": key})
                        log.debug(
                            f"Marked for deletion: {key} (LastModified: {last_modified})"
                        )

                        # Batch delete in chunks of 1000 (S3 limit)
                        if len(objects_to_delete_batch) == 1000:
                            deleted_count = _perform_batch_delete(
                                client, bucket, objects_to_delete_batch
                            )
                            total_deleted_count += deleted_count
                            objects_to_delete_batch = []  # Reset batch

        # Delete any remaining objects in the last batch
        if objects_to_delete_batch:
            deleted_count = _perform_batch_delete(
                client, bucket, objects_to_delete_batch
            )
            total_deleted_count += deleted_count

        if total_deleted_count > 0:
            log.info(
                f"Finished cleanup. Total old files deleted from s3://{bucket}/{prefix}: {total_deleted_count}"
            )
        else:
            log.info(f"No old files found matching criteria in s3://{bucket}/{prefix}.")

    except ClientError as e:
        log.error(
            f"Error listing objects during cleanup in s3://{bucket}/{prefix}: {e}",
            exc_info=True,
        )
    except Exception as e:
        log.error(
            f"Unexpected error during cleanup in s3://{bucket}/{prefix}: {e}",
            exc_info=True,
        )

    return total_deleted_count


def _perform_batch_delete(client, bucket: str, objects_to_delete: list[dict]) -> int:
    """Helper function to perform a single batch delete operation."""
    deleted_count = 0
    if not objects_to_delete:
        return 0
    try:
        log.info(
            f"Deleting batch of {len(objects_to_delete)} objects from s3://{bucket}/..."
        )
        response = client.delete_objects(
            Bucket=bucket, Delete={"Objects": objects_to_delete}
        )
        if "Deleted" in response:
            deleted_count = len(response["Deleted"])
            log.info(f"Successfully deleted batch of {deleted_count} files.")
        if "Errors" in response and response["Errors"]:
            log.error("Errors occurred during batch deletion:")
            for error in response["Errors"]:
                log.error(
                    f"  Key: {error.get('Key')}, Error: {error.get('Code')} - {error.get('Message')}"
                )
        return deleted_count
    except ClientError as e:
        log.error(
            f"Error performing batch delete in s3://{bucket}/: {e}", exc_info=True
        )
        return 0
    except Exception as e:
        log.error(
            f"Unexpected error during batch delete in s3://{bucket}/: {e}",
            exc_info=True,
        )
        return 0
