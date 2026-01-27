#!/usr/bin/env python3
"""
Import data from Singularity Research Fund tracker.
https://singularityresearchfund.com/opendoor-tracker

This script helps import the data tables from Singularity's tracker,
which has different data than Parcl Labs (includes realized net, etc.)

Usage:
    # Paste table data from clipboard
    python scripts/import_singularity.py --paste

    # Import from CSV
    python scripts/import_singularity.py data.csv
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd


def import_from_clipboard():
    """Import data pasted from Singularity tracker table."""
    print("\nReading from clipboard...")

    try:
        df = pd.read_clipboard()
        print(f"Found {len(df)} rows")
        print(f"Columns: {list(df.columns)}")

        if df.empty:
            print("No data found in clipboard.")
            print("\nTo use: Copy the table from the Singularity tracker website,")
            print("then run this script again.")
            return None

        # Try to detect what kind of data this is
        cols_lower = [c.lower() for c in df.columns]

        if any("realized" in c for c in cols_lower):
            data_type = "sales"
        elif any("listing" in c or "dom" in c for c in cols_lower):
            data_type = "listings"
        else:
            data_type = "unknown"

        print(f"Detected data type: {data_type}")

        # Save to imports directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(__file__).parent.parent / "data" / "imports"
        output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"singularity_{data_type}_{timestamp}.csv"
        output_path = output_dir / filename

        df.to_csv(output_path, index=False)
        print(f"\nSaved to: {output_path}")

        return df

    except Exception as e:
        print(f"Error reading clipboard: {e}")
        print("\nMake sure you've copied the table data first.")
        return None


def import_from_file(filepath: str):
    """Import from CSV file."""
    path = Path(filepath)
    if not path.exists():
        print(f"File not found: {filepath}")
        return None

    print(f"Reading: {filepath}")
    df = pd.read_csv(filepath)
    print(f"Found {len(df)} rows")
    print(f"Columns: {list(df.columns)}")

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Import Singularity Research Fund tracker data"
    )
    parser.add_argument(
        "file",
        nargs="?",
        help="CSV file to import"
    )
    parser.add_argument(
        "--paste", "-p",
        action="store_true",
        help="Import from clipboard (paste table data)"
    )
    args = parser.parse_args()

    if args.paste:
        df = import_from_clipboard()
    elif args.file:
        df = import_from_file(args.file)
    else:
        print("Usage:")
        print("  python scripts/import_singularity.py --paste   # From clipboard")
        print("  python scripts/import_singularity.py data.csv  # From file")
        return 1

    if df is not None:
        print("\nData preview:")
        print(df.head())
        print(f"\nYou can now run: python glasshouse.py --csv")

    return 0


if __name__ == "__main__":
    sys.exit(main())
