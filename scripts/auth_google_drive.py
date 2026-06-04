"""
One-time OAuth2 setup for Google Drive access.

Run this once to generate a token file:
    python3 scripts/auth_google_drive.py

It will open a browser for you to authorize, then save the refresh token
to the path specified in your .env as GOOGLE_OAUTH_TOKEN_PATH.

Prerequisites:
  1. Go to Google Cloud Console → APIs & Services → Credentials
  2. Create an OAuth 2.0 Client ID (type: Desktop app)
  3. Download the client secret JSON file
  4. Set GOOGLE_OAUTH_CLIENT_SECRET_PATH in .env to point at it
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from google_auth_oauthlib.flow import InstalledAppFlow
from nexrad_backend import config

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def main():
    client_secret_path = config.GOOGLE_OAUTH_CLIENT_SECRET_PATH
    token_path = config.GOOGLE_OAUTH_TOKEN_PATH

    if not client_secret_path or not os.path.exists(client_secret_path):
        print(f"ERROR: Client secret file not found at: {client_secret_path}")
        print("Download it from Google Cloud Console → Credentials → OAuth 2.0 Client IDs")
        print("Then set GOOGLE_OAUTH_CLIENT_SECRET_PATH in your .env")
        sys.exit(1)

    print(f"Starting OAuth flow using: {client_secret_path}")
    print("A browser window will open for authorization...\n")

    flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
    creds = flow.run_local_server(port=0)

    os.makedirs(os.path.dirname(token_path), exist_ok=True)
    with open(token_path, "w") as f:
        f.write(creds.to_json())

    print(f"\nToken saved to: {token_path}")
    print("You can now run the processing scripts.")


if __name__ == "__main__":
    main()
