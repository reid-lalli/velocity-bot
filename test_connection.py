#!/usr/bin/env python3
"""
Quick test: verifies credentials.json + Sheets access and initializes headers.
Run this first after setting up credentials to confirm everything works.
"""
import os, sys
from dotenv import load_dotenv
load_dotenv()

SPREADSHEET_ID   = os.getenv("SPREADSHEET_ID", "")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")
CLAN_NAME        = os.getenv("CLAN_NAME", "Vy")

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def main():
    print("═" * 55)
    print("  Vy Clan Tracker — Sheets Connection Test")
    print("═" * 55)

    if not os.path.exists(CREDENTIALS_FILE):
        print(f"\n❌  '{CREDENTIALS_FILE}' not found in this folder.")
        print("    See README.md Step 2 to create your Google Service Account.")
        sys.exit(1)

    if not SPREADSHEET_ID:
        print("\n❌  SPREADSHEET_ID missing from .env")
        sys.exit(1)

    print(f"\n  Spreadsheet ID : {SPREADSHEET_ID}")
    print(f"  Credentials    : {CREDENTIALS_FILE}")
    print(f"  Clan name      : {CLAN_NAME}")
    print()

    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SHEETS_SCOPES)
        gc = gspread.authorize(creds)
        print("  ✅  Google auth OK")

        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        print(f"  ✅  Spreadsheet opened: '{spreadsheet.title}'")
    except gspread.exceptions.APIError as e:
        print(f"\n❌  Google Sheets API error: {e}")
        print("    Make sure you shared the sheet with the service account email as Editor.")
        print("    The email is inside credentials.json as 'client_email'.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌  Error: {e}")
        sys.exit(1)

    # Init headers
    from race_tracker_bot import setup_headers
    wl, ts, rd, wts, nt, pick_analytics, pick_board, pick_splits = setup_headers(spreadsheet)
    _ = (wl, ts, rd, wts, nt, pick_analytics, pick_board, pick_splits)
    print(
        "  ✅  Sheet tabs verified/created: "
        "War Log, Track Stats, Race Details, War Track Summary, Net Tracker, "
        "Track Pick Analytics, Track Pick Board, Track Pick Splits"
    )
    print()
    print("Everything is working! You can now run:")
    print("  python manual_tracker.py         (paste a war result manually)")
    print("  python race_tracker_bot.py        (start the Discord bot)")


if __name__ == "__main__":
    main()
