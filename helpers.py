import datetime


def delete_old_s3_files(bucket_name, prefix, s3_client, minutes):
    """Deletes PNG and JSON files older than a specified number of minutes from an S3 prefix."""
    now = datetime.datetime.now(datetime.timezone.utc)
    cutoff = now - datetime.timedelta(minutes=minutes + 60)
    deleted_count = 0

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    objects_to_delete = []

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                last_modified = obj["LastModified"]
                if (
                    key.endswith(".png") or key.endswith(".json")
                ) and last_modified < cutoff:
                    objects_to_delete.append({"Key": key})

    if objects_to_delete:
        print(
            f"Found {len(objects_to_delete)} old files to delete in s3://{bucket_name}/{prefix} older than {minutes} minutes."
        )
        while objects_to_delete:
            try:
                chunk = objects_to_delete[:1000]
                objects_to_delete = objects_to_delete[1000:]

                response = s3_client.delete_objects(
                    Bucket=bucket_name, Delete={"Objects": chunk}
                )
                if "Deleted" in response:
                    deleted_count = len(response["Deleted"])
                    print(f"Successfully deleted {deleted_count} old files.")
                if "Errors" in response:
                    print("Errors occurred during deletion:")
                    for error in response["Errors"]:
                        print(f"  Key: {error['Key']}, Error: {error['Message']}")
            except Exception as e:
                print(f"Error deleting old files from S3: {e}")
    else:
        print(
            f"No PNG or JSON files older than {minutes} minutes found in s3://{bucket_name}/{prefix}."
        )
