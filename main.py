import os
from datetime import datetime, timedelta
import pandas as pd
from dukascopy import Dukascopy
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ==== SETTINGS ====
PAIRS = ["EURUSD", "GBPUSD", "XAUUSD"]  # Gold is XAUUSD
START_DATE = datetime(2015, 1, 1)
END_DATE = datetime(2025, 1, 1)
FOLDER_ID = "1Ab46ssLiJgqmN7f3He5M643c6Z9mu7Kr"  # Google Drive folder ID

# ==== GOOGLE DRIVE AUTH ====
SCOPES = ['https://www.googleapis.com/auth/drive.file']

creds_json = os.getenv("GDRIVE_CREDENTIALS_JSON")
with open("service_account.json", "w") as f:
    f.write(creds_json)

creds = service_account.Credentials.from_service_account_file(
    "service_account.json", scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=creds)

# ==== UPLOAD TO GOOGLE DRIVE ====
def upload_to_drive(file_path, file_name):
    file_metadata = {'name': file_name, 'parents': [FOLDER_ID]}
    media = MediaFileUpload(file_path, mimetype='text/csv')
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"✅ Uploaded {file_name} to Google Drive")

# ==== DOWNLOAD IN CHUNKS ====
def download_pair(pair):
    print(f"\n📥 Starting download for {pair}...")
    current_start = START_DATE
    all_data = []

    while current_start < END_DATE:
        current_end = current_start + timedelta(days=30)
        if current_end > END_DATE:
            current_end = END_DATE

        print(f"⏳ Downloading {pair} from {current_start.date()} to {current_end.date()}...")
        try:
            duka = Dukascopy(
                instrument=pair,
                timeframe="tick",
                start=current_start.strftime("%Y-%m-%d"),
                end=current_end.strftime("%Y-%m-%d")
            )
            df = duka.get_data()
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            print(f"⚠️ Error downloading {pair} {current_start} - {current_end}: {e}")

        current_start = current_end

    if all_data:
        final_df = pd.concat(all_data)
        file_name = f"{pair}_tick_{START_DATE.date()}_to_{END_DATE.date()}.csv"
        final_df.to_csv(file_name, index=False)
        upload_to_drive(file_name, file_name)
        print(f"✅ Finished {pair}")
    else:
        print(f"❌ No data downloaded for {pair}")

# ==== MAIN ====
def main():
    for pair in PAIRS:
        download_pair(pair)
    print("\n🎯 All downloads complete and uploaded to Google Drive.")

if __name__ == "__main__":
    main()
