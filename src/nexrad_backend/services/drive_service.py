import json
import datetime
import logging
import os

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from googleapiclient.errors import HttpError

from nexrad_backend import config

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

_drive_service = None
_folder_id_cache = {}


def _get_drive_service():
    global _drive_service
    if _drive_service is None:
        token_path = config.GOOGLE_OAUTH_TOKEN_PATH
        if not token_path or not os.path.exists(token_path):
            raise EnvironmentError(
                f"OAuth token file not found at: {token_path}. "
                "Run 'python scripts/auth_google_drive.py' to generate it."
            )

        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            try:
                with open(token_path, "w") as f:
                    f.write(creds.to_json())
            except OSError:
                pass  # read-only filesystem (e.g. Render secret files)

        _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive_service


def _get_or_create_subfolder(parent_folder_id: str, folder_name: str) -> str:
    """Find or create a subfolder by name under the parent folder."""
    if folder_name in _folder_id_cache:
        return _folder_id_cache[folder_name]

    service = _get_drive_service()
    query = (
        f"name = '{folder_name}' and "
        f"'{parent_folder_id}' in parents and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"trashed = false"
    )
    results = service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
    files = results.get("files", [])

    if files:
        folder_id = files[0]["id"]
    else:
        metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        folder = service.files().create(body=metadata, fields="id").execute()
        folder_id = folder["id"]
        log.info(f"Created Drive subfolder: {folder_name} ({folder_id})")

    _folder_id_cache[folder_name] = folder_id
    return folder_id


def _resolve_key(key: str) -> tuple[str, str]:
    """Split a storage key like 'plots_level2/file.png' into (subfolder_name, filename)."""
    parts = key.split("/", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return "", parts[0]


def _get_folder_for_key(key: str) -> str:
    """Get the Drive folder ID for a given storage key."""
    subfolder_name, _ = _resolve_key(key)
    if subfolder_name:
        return _get_or_create_subfolder(config.GOOGLE_DRIVE_FOLDER_ID, subfolder_name)
    return config.GOOGLE_DRIVE_FOLDER_ID


def _find_file(folder_id: str, filename: str) -> str | None:
    """Find a file by name in a folder. Returns file ID or None."""
    service = _get_drive_service()
    query = (
        f"name = '{filename}' and "
        f"'{folder_id}' in parents and "
        f"trashed = false"
    )
    results = service.files().list(q=query, fields="files(id)", pageSize=1).execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


def get_object_body(key: str) -> bytes | None:
    """Download file content by storage key."""
    try:
        subfolder_name, filename = _resolve_key(key)
        folder_id = _get_folder_for_key(key)
        file_id = _find_file(folder_id, filename)
        if not file_id:
            log.warning(f"File not found in Drive: {key}")
            return None

        service = _get_drive_service()
        content = service.files().get_media(fileId=file_id).execute()
        log.info(f"Successfully retrieved: {key}")
        return content
    except HttpError as e:
        log.error(f"Error retrieving {key} from Drive: {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error retrieving {key}: {e}", exc_info=True)
        return None


def put_object(key: str, body_bytes: bytes, content_type: str) -> bool:
    """Upload bytes to Drive at the given key. Overwrites if exists."""
    try:
        subfolder_name, filename = _resolve_key(key)
        folder_id = _get_folder_for_key(key)

        service = _get_drive_service()
        media = MediaInMemoryUpload(body_bytes, mimetype=content_type, resumable=False)

        existing_id = _find_file(folder_id, filename)
        if existing_id:
            service.files().update(
                fileId=existing_id, media_body=media
            ).execute()
        else:
            file_metadata = {"name": filename, "parents": [folder_id]}
            service.files().create(
                body=file_metadata, media_body=media, fields="id"
            ).execute()

        log.info(f"Successfully uploaded: {key}")
        return True
    except HttpError as e:
        log.error(f"Error uploading {key} to Drive: {e}")
        return False
    except Exception as e:
        log.error(f"Unexpected error uploading {key}: {e}", exc_info=True)
        return False


def update_json(key: str, data_dict: dict) -> bool:
    """Upload a dictionary as a JSON file to Drive."""
    try:
        json_bytes = json.dumps(data_dict, indent=2).encode("utf-8")
        return put_object(key, json_bytes, "application/json")
    except TypeError as e:
        log.error(f"Error serializing dict to JSON for key {key}: {e}", exc_info=True)
        return False


def object_exists(key: str) -> bool:
    """Check if a file exists at the given key."""
    try:
        folder_id = _get_folder_for_key(key)
        _, filename = _resolve_key(key)
        return _find_file(folder_id, filename) is not None
    except Exception as e:
        log.error(f"Error checking existence for {key}: {e}", exc_info=True)
        return False


def list_object_keys(prefix: str) -> list[str]:
    """List all file keys under a prefix (subfolder)."""
    try:
        subfolder_name = prefix.rstrip("/")
        folder_id = _get_or_create_subfolder(config.GOOGLE_DRIVE_FOLDER_ID, subfolder_name)

        service = _get_drive_service()
        keys = []
        page_token = None

        while True:
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(
                q=query,
                fields="nextPageToken, files(name)",
                pageSize=1000,
                pageToken=page_token,
            ).execute()

            for f in results.get("files", []):
                keys.append(f"{subfolder_name}/{f['name']}")

            page_token = results.get("nextPageToken")
            if not page_token:
                break

        log.info(f"Listed {len(keys)} files with prefix '{prefix}'.")
        return keys
    except HttpError as e:
        log.error(f"Error listing files with prefix {prefix}: {e}")
        return []
    except Exception as e:
        log.error(f"Unexpected error listing files with prefix {prefix}: {e}", exc_info=True)
        return []


def delete_old_files(prefix: str, older_than_minutes: int) -> int:
    """Delete .png and .json files older than the specified age under a prefix."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    cutoff_time = now_utc - datetime.timedelta(minutes=older_than_minutes)
    cutoff_rfc = cutoff_time.isoformat()

    subfolder_name = prefix.rstrip("/")
    try:
        folder_id = _get_or_create_subfolder(config.GOOGLE_DRIVE_FOLDER_ID, subfolder_name)
    except Exception as e:
        log.error(f"Error resolving folder for cleanup prefix {prefix}: {e}", exc_info=True)
        return 0

    service = _get_drive_service()
    total_deleted = 0
    page_token = None

    log.info(f"Scanning Drive folder '{subfolder_name}' for files older than {cutoff_time} UTC...")

    try:
        while True:
            query = (
                f"'{folder_id}' in parents and "
                f"trashed = false and "
                f"modifiedTime < '{cutoff_rfc}'"
            )
            results = service.files().list(
                q=query,
                fields="nextPageToken, files(id, name)",
                pageSize=1000,
                pageToken=page_token,
            ).execute()

            for f in results.get("files", []):
                name = f["name"]
                if name.endswith(".png") or name.endswith(".json"):
                    try:
                        service.files().delete(fileId=f["id"]).execute()
                        total_deleted += 1
                        log.debug(f"Deleted old file: {subfolder_name}/{name}")
                    except HttpError as e:
                        log.error(f"Error deleting file {name}: {e}")

            page_token = results.get("nextPageToken")
            if not page_token:
                break

        if total_deleted > 0:
            log.info(f"Cleanup complete. Deleted {total_deleted} old files from '{subfolder_name}/'.")
        else:
            log.info(f"No old files found in '{subfolder_name}/'.")

    except HttpError as e:
        log.error(f"Error during cleanup of '{subfolder_name}/': {e}")
    except Exception as e:
        log.error(f"Unexpected error during cleanup of '{subfolder_name}/': {e}", exc_info=True)

    return total_deleted
