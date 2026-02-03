#!/usr/bin/env python3
"""
Sync Parcl CSV sales data to the glasshouse.db sales_log table.

This script imports records from Parcl Labs CSV exports that are not
already in the database. It handles deduplication by checking for
existing records by property_id and sale_date.

Usage:
    python scripts/sync_parcl_to_db.py
    python scripts/sync_parcl_to_db.py --dry-run
    python scripts/sync_parcl_to_db.py --csv /path/to/specific.csv
"""

import sys
import argparse
import glob
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "glasshouse.db"
PARCL_CSV_PATTERN = Path.home() / "Desktop" / "glasshouse" / "opendoor-home-sales-*.csv"


def find_latest_parcl_csv() -> Path:
    """Find the most recent Parcl sales CSV file."""
    pattern = str(PARCL_CSV_PATTERN)
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(f"No Parcl CSV files found matching: {pattern}")

    # Sort by modification time, most recent first
    files.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
    latest = Path(files[0])

    logger.info(f"Found {len(files)} Parcl CSV files, using latest: {latest.name}")
    return latest


def parse_date(date_str: str) -> str:
    """Parse date string from Parcl CSV to YYYY-MM-DD format."""
    if pd.isna(date_str) or not date_str:
        return None

    try:
        # Handle format like "Jan 29, 2026"
        dt = datetime.strptime(str(date_str).strip(), "%b %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        # Try ISO format if already in that format
        try:
            dt = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            logger.warning(f"Could not parse date: {date_str}")
            return None


def parse_currency(value) -> float:
    """Parse currency string like '$589,000' to float."""
    if pd.isna(value) or not value:
        return None

    value_str = str(value).strip()
    if not value_str or value_str == '$0':
        return None

    # Remove $ and commas, handle negative values
    cleaned = value_str.replace('$', '').replace(',', '')
    try:
        return float(cleaned)
    except ValueError:
        logger.warning(f"Could not parse currency: {value}")
        return None


def load_parcl_csv(csv_path: Path) -> pd.DataFrame:
    """Load and parse Parcl CSV file."""
    logger.info(f"Loading CSV: {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows from CSV")
    logger.info(f"Columns: {list(df.columns)}")

    # Standardize column names to lowercase with underscores
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # Parse and transform data
    records = []
    for _, row in df.iterrows():
        record = {
            'property_id': str(row.get('property_id', '')).strip() if pd.notna(row.get('property_id')) else None,
            'sale_date': parse_date(row.get('sale_date')),
            'sale_price': parse_currency(row.get('sale_price')),
            'purchase_date': parse_date(row.get('purchase_date')),
            'purchase_price': parse_currency(row.get('purchase_price')),
            'days_held': int(row.get('days_held')) if pd.notna(row.get('days_held')) else None,
            'realized_net': parse_currency(row.get('realized_net')),
            'quarter': str(row.get('quarter', '')).strip() if pd.notna(row.get('quarter')) else None,
            'year': int(row.get('year')) if pd.notna(row.get('year')) else None,
            'source': 'parcl_csv',
            'imported_at': datetime.now().isoformat(),
        }

        # Skip records without essential data
        if not record['property_id'] or not record['sale_date']:
            logger.debug(f"Skipping record with missing property_id or sale_date: {row.to_dict()}")
            continue

        records.append(record)

    result_df = pd.DataFrame(records)
    logger.info(f"Parsed {len(result_df)} valid records from CSV")
    return result_df


def get_address_mapping(conn: sqlite3.Connection) -> dict:
    """Get property_id to address mapping from property_daily_snapshot."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT property_id, address_normalized, city, state
        FROM property_daily_snapshot
        WHERE property_id IS NOT NULL AND address_normalized IS NOT NULL
    """)

    mapping = {}
    for row in cursor.fetchall():
        property_id, address, city, state = row
        mapping[str(property_id)] = {
            'address': address,
            'city': city,
            'state': state
        }

    logger.info(f"Loaded address mapping for {len(mapping)} properties")
    return mapping


def get_existing_records(conn: sqlite3.Connection) -> set:
    """Get set of (property_id, sale_date) tuples already in database."""
    cursor = conn.cursor()

    # Get records that have property_id (from parcl_csv or desktop_csv sources)
    cursor.execute("""
        SELECT property_id, sale_date
        FROM sales_log
        WHERE property_id IS NOT NULL AND property_id != ''
    """)

    existing = set()
    for row in cursor.fetchall():
        property_id, sale_date = row
        if property_id and sale_date:
            existing.add((str(property_id), sale_date))

    logger.info(f"Found {len(existing)} existing records with property_id in database")
    return existing


def sync_to_database(df: pd.DataFrame, dry_run: bool = False) -> dict:
    """
    Sync DataFrame to sales_log table.

    Returns dict with counts of added, skipped, and error records.
    """
    stats = {'added': 0, 'skipped': 0, 'errors': 0}

    conn = sqlite3.connect(str(DB_PATH))

    try:
        # Get existing records and address mapping
        existing = get_existing_records(conn)
        address_mapping = get_address_mapping(conn)

        cursor = conn.cursor()

        for _, row in df.iterrows():
            property_id = row['property_id']
            sale_date = row['sale_date']

            # Check for duplicate by property_id + sale_date
            if (property_id, sale_date) in existing:
                stats['skipped'] += 1
                continue

            # Look up address from property_daily_snapshot
            address_info = address_mapping.get(property_id, {})
            address = address_info.get('address')
            city = address_info.get('city')
            state = address_info.get('state')

            if dry_run:
                logger.info(f"Would add: property_id={property_id}, sale_date={sale_date}, "
                           f"address={address}, sale_price={row.get('sale_price')}")
                stats['added'] += 1
                continue

            try:
                cursor.execute("""
                    INSERT INTO sales_log (
                        property_id, sale_date, address, city, state,
                        sale_price, purchase_price, purchase_date,
                        days_held, realized_net, quarter, year,
                        source, imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    property_id,
                    sale_date,
                    address,
                    city,
                    state,
                    row.get('sale_price'),
                    row.get('purchase_price'),
                    row.get('purchase_date'),
                    row.get('days_held'),
                    row.get('realized_net'),
                    row.get('quarter'),
                    row.get('year'),
                    row.get('source'),
                    row.get('imported_at'),
                ))
                stats['added'] += 1

            except sqlite3.IntegrityError as e:
                # Handle unique constraint violation (sale_date, address)
                logger.debug(f"Skipping duplicate record: {e}")
                stats['skipped'] += 1
            except Exception as e:
                logger.error(f"Error inserting record {property_id}: {e}")
                stats['errors'] += 1

        if not dry_run:
            conn.commit()
            logger.info("Changes committed to database")

    finally:
        conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Sync Parcl CSV sales data to glasshouse.db"
    )
    parser.add_argument(
        "--csv",
        type=str,
        help="Path to specific CSV file (default: latest in ~/Desktop/glasshouse/)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without making changes"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Find CSV file
    if args.csv:
        csv_path = Path(args.csv)
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return 1
    else:
        try:
            csv_path = find_latest_parcl_csv()
        except FileNotFoundError as e:
            logger.error(str(e))
            return 1

    # Check database exists
    if not DB_PATH.exists():
        logger.error(f"Database not found: {DB_PATH}")
        return 1

    # Load and parse CSV
    df = load_parcl_csv(csv_path)

    if df.empty:
        logger.warning("No valid records found in CSV")
        return 0

    # Get current count before sync
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sales_log")
    count_before = cursor.fetchone()[0]
    conn.close()

    logger.info(f"Current sales_log count: {count_before}")

    # Sync to database
    if args.dry_run:
        logger.info("=== DRY RUN MODE - No changes will be made ===")

    stats = sync_to_database(df, dry_run=args.dry_run)

    # Report results
    logger.info("=" * 50)
    logger.info("SYNC COMPLETE")
    logger.info(f"  Records in CSV: {len(df)}")
    logger.info(f"  Added:   {stats['added']}")
    logger.info(f"  Skipped: {stats['skipped']} (already exist)")
    logger.info(f"  Errors:  {stats['errors']}")

    if not args.dry_run:
        # Get new count
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sales_log")
        count_after = cursor.fetchone()[0]
        conn.close()

        logger.info(f"  Total sales_log records: {count_before} -> {count_after}")

    logger.info("=" * 50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
