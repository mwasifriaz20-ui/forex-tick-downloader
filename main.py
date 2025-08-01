import os
from datetime import datetime
import pandas as pd
from dukascopy import Dukascopy
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import json

# === CONFIG ===
PAIRS = ["EURUSD", "GBPUSD", "GOLD"]
START_DATE = "2015-01-01"
END_DATE = "2025-01-01"
FOLDER_ID = "1Ab46ssLiJgqmN7f3He5M643c6Z9mu7Kr"

# === Google Drive Auth ===
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Load Google credentials from Railway environment variable
creds_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
with open("credentials.json", "w") as f:
    f.write(creds_json)

flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
creds = flow.run_local_server(port=0)
drive_service = build('drive', 'v3', credentials=creds)

# === Function to upload file to Google Drive ===
def upload_to_drive(file_path, file_name):
    file_metadata = {'name': file_name, 'parents': [FOLDER_ID]}
    media = MediaFileUpload(file_path, mimetype='text/csv')
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"✅ Uploaded {file_name}")

# === Download + Upload loop ===
for pair in PAIRS:
    print(f"📥 Downloading {pair} data...")
    duka = Dukascopy(instrument=pair, timeframe="tick", start=START_DATE, end=END_DATE)
    df = duka.get_data()
    file_name = f"{pair}_tick_{START_DATE}_to_{END_DATE}.csv"
    df.to_csv(file_name, index=False)

    print(f☁️ Uploading {file_name} to Google Drive...")
    upload_to_drive(file_name, file_name)

print("🎯 All files downloaded and uploaded to Google Drive successfully!")
