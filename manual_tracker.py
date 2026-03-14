#!/usr/bin/env python3
"""
Vy Clan War Result Tracker — Manual Mode (no Discord bot needed)
=================================================================
Run this script, paste your Lorenzi/Quaxly war result when prompted,
and it will be logged to Google Sheets automatically.

Requires the same .env and credentials.json as the bot version.
"""

import os
import sys
from dotenv import load_dotenv

# Must load .env before importing config values from the bot module
load_dotenv()

# Reuse all parsing + Sheets logic from the bot file
from race_tracker_bot import parse_war, write_war, CLAN_NAME, SPREADSHEET_ID, CREDENTIALS_FILE


def collect_input() -> str:
    """Prompt the user to paste a war result block, ending with a blank line."""
    print()
    print("Paste the full Lorenzi/Quaxly war result below.")
    print("Press Enter on a blank line when done (or Ctrl+C to cancel).")
    print("─" * 55)

    lines = []
    try:
        while True:
            line = input()
            # Two consecutive blank lines = end of input
            if line == "" and lines and lines[-1] == "":
                break
            lines.append(line)
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        sys.exit(0)

    return "\n".join(lines)


def main():
    print("═" * 55)
    print(f"  {CLAN_NAME} Clan War Result Tracker — Manual Mode")
    print("═" * 55)

    # Sanity checks
    missing = []
    if not SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID  (add it to .env)")
    if not os.path.exists(CREDENTIALS_FILE):
        missing.append(f"credentials file '{CREDENTIALS_FILE}'")
    if missing:
        print("\nERROR: Missing required config:")
        for m in missing:
            print(f"  • {m}")
        print("\nSee README.md for setup instructions.")
        sys.exit(1)

    text = collect_input()

    if not text.strip():
        print("No input received. Exiting.")
        sys.exit(0)

    war = parse_war(text)
    if not war:
        print(
            "\nCould not parse a war result from the pasted text.\n"
            "Make sure you copied the FULL Lorenzi/Quaxly output, starting with\n"
            '  "Total Score after Race NN"'
        )
        sys.exit(1)

    # Show a summary before writing
    print()
    print("─" * 55)
    print("Parsed result:")
    print(f"  Opponent  : {war['opponent']}")
    print(f"  {CLAN_NAME:<10}: {war['vy_score']}")
    print(f"  Opp       : {war['opp_score']}")
    net = war["difference"]
    print(f"  Net       : {f'{net:+d}' if net is not None else '?'}")
    print(f"  Races     : {len(war['races'])}")
    print()
    for r in war["races"]:
        positions_str = ", ".join(str(p) for p in r["positions"])
        print(f"  Race {r['race']:>2}: {r['net']:>+4}  pos [{positions_str}]  ({r['track']})")
    print("─" * 55)
    print()

    confirm = input("Log this to Google Sheets? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted. Nothing was written.")
        sys.exit(0)

    print("\nWriting to Google Sheets…")
    try:
        write_war(war)
        print("✅  War result logged successfully!")
        print()
        print("Sheets updated:")
        print("  • War Log      — new war row added")
        print("  • Race Details — individual race rows added")
        print("  • Track Stats  — per-track net scores updated")
    except Exception as exc:
        print(f"\n❌  Failed to write to Google Sheets: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
