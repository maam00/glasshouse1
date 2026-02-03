#!/usr/bin/env python3
"""
Backfill Purchase Data
======================
Enrich sales_log records from Singularity (which lacks purchase data) with:
- purchase_price
- days_held
- cohort
- realized_net (if calculable)

Data sources:
- Parcl sales CSV: Has property_id, purchase_date, purchase_price, days_held, realized_net
- Parcl listings CSV: Has address + purchase data for fuzzy matching

Matching strategy:
1. Match by property_id (exact)
2. Match by normalized address (fuzzy, high confidence only)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import sqlite3
import pandas as pd
import re
import json
from datetime import datetime
from difflib import SequenceMatcher
import logging

from src.config import get_cohort

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def normalize_address(address: str) -> str:
    """
    Normalize address for consistent matching.
    Converts to lowercase, standardizes abbreviations, removes punctuation.
    """
    if not address or pd.isna(address):
        return ""

    addr = str(address).lower().strip()

    # Standardize direction prefixes/suffixes
    direction_map = {
        'north': 'n', 'south': 's', 'east': 'e', 'west': 'w',
        'northeast': 'ne', 'northwest': 'nw', 'southeast': 'se', 'southwest': 'sw',
    }
    for full, abbr in direction_map.items():
        addr = re.sub(rf'\b{full}\b', abbr, addr)

    # Standardize street type abbreviations
    street_types = [
        ('street', 'st'), ('avenue', 'ave'), ('boulevard', 'blvd'),
        ('drive', 'dr'), ('lane', 'ln'), ('road', 'rd'),
        ('court', 'ct'), ('place', 'pl'), ('circle', 'cir'),
        ('terrace', 'ter'), ('highway', 'hwy'), ('parkway', 'pkwy'),
        ('cove', 'cv'), ('trail', 'trl'), ('way', 'way'),
    ]
    for full, abbr in street_types:
        addr = re.sub(rf'\b{full}\b', abbr, addr)

    # Remove periods, commas, extra spaces, # symbols
    addr = addr.replace('.', '').replace(',', '').replace('#', ' ')
    addr = ' '.join(addr.split())

    return addr


def parse_price(price_str):
    """Parse price string like '$589,000' to float."""
    if pd.isna(price_str) or price_str == '':
        return None
    if isinstance(price_str, (int, float)):
        return float(price_str)
    return float(str(price_str).replace('$', '').replace(',', '').strip() or '0')


def parse_date(date_str):
    """Parse date string to YYYY-MM-DD format."""
    if pd.isna(date_str) or date_str == '':
        return None

    # Try multiple formats
    formats = [
        '%b %d, %Y',   # Jan 29, 2026
        '%Y-%m-%d',    # 2026-01-29
        '%m/%d/%Y',    # 01/29/2026
    ]

    for fmt in formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None


def fuzzy_match_score(str1: str, str2: str) -> float:
    """Calculate fuzzy match score between two strings (0-1)."""
    if not str1 or not str2:
        return 0.0
    return SequenceMatcher(None, str1, str2).ratio()


def extract_street_address(full_address: str) -> str:
    """Extract just the street address portion (before city/state)."""
    if not full_address:
        return ""

    # Split on common city/state delimiters
    # e.g., "3869 W CHARLOTTE DR GLENDALE AZ 85310" -> need to find where city starts
    parts = full_address.split()

    # Simple heuristic: look for 2-letter state code near end
    state_codes = {'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                   'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                   'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                   'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                   'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'}

    for i, part in enumerate(parts):
        if part.upper() in state_codes and i > 2:
            # City is likely the word(s) before state
            # Street address is everything before city
            # This is a rough heuristic
            return ' '.join(parts[:i-1])

    return full_address


def load_parcl_sales(csv_path: str) -> pd.DataFrame:
    """Load and process Parcl sales CSV."""
    logger.info(f"Loading Parcl sales from: {csv_path}")
    df = pd.read_csv(csv_path)

    # Normalize columns
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # Parse numeric columns
    for col in ['sale_price', 'purchase_price', 'realized_net']:
        if col in df.columns:
            df[col] = df[col].apply(parse_price)

    # Parse days_held
    if 'days_held' in df.columns:
        df['days_held'] = pd.to_numeric(df['days_held'], errors='coerce')

    # Parse dates
    for col in ['sale_date', 'purchase_date']:
        if col in df.columns:
            df[col] = df[col].apply(parse_date)

    # Ensure property_id is string
    if 'property_id' in df.columns:
        df['property_id'] = df['property_id'].astype(str)

    logger.info(f"  Loaded {len(df)} sales records")
    return df


def load_parcl_listings(csv_path: str) -> pd.DataFrame:
    """Load and process Parcl listings CSV."""
    logger.info(f"Loading Parcl listings from: {csv_path}")
    df = pd.read_csv(csv_path)

    # Normalize columns
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # Parse numeric columns
    price_cols = ['original_purchase_price', 'initial_listing_price', 'latest_listing_price']
    for col in price_cols:
        if col in df.columns:
            df[col] = df[col].apply(parse_price)

    # Parse dates
    date_cols = ['original_purchase_date', 'initial_listing_date', 'latest_listing_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(parse_date)

    # Create normalized address
    if 'address' in df.columns:
        df['address_normalized'] = df.apply(
            lambda row: normalize_address(
                f"{row['address']} {row.get('city', '')} {row.get('state', '')}"
            ), axis=1
        )

    # Ensure property_id is string
    if 'property_id' in df.columns:
        df['property_id'] = df['property_id'].astype(str)

    logger.info(f"  Loaded {len(df)} listings records")
    return df


def get_sales_log_records(db_path: str) -> list:
    """Get all sales_log records that need enrichment."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get records missing purchase data
    # We consider a record as "missing data" if:
    # - purchase_price is NULL or 0
    # - AND days_held is NULL or 0
    # - AND cohort is NULL or empty
    cursor.execute("""
        SELECT id, sale_date, address, city, state, sale_price,
               purchase_price, days_held, realized_net, cohort, property_id
        FROM sales_log
        WHERE (purchase_price IS NULL OR purchase_price = 0)
           OR (days_held IS NULL OR days_held = 0)
           OR (cohort IS NULL OR cohort = '')
    """)

    records = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return records


def update_sales_record(db_path: str, record_id: int, updates: dict):
    """Update a sales_log record with enriched data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    set_clauses = []
    values = []

    for key, value in updates.items():
        if value is not None:
            set_clauses.append(f"{key} = ?")
            values.append(value)

    if set_clauses:
        query = f"UPDATE sales_log SET {', '.join(set_clauses)} WHERE id = ?"
        values.append(record_id)
        cursor.execute(query, values)

    conn.commit()
    conn.close()


def backfill_purchase_data(
    db_path: str,
    sales_csv_path: str,
    listings_csv_path: str,
    output_dir: str
):
    """Main backfill function."""

    logger.info("=" * 60)
    logger.info("BACKFILL PURCHASE DATA")
    logger.info("=" * 60)

    # Load data sources
    parcl_sales = load_parcl_sales(sales_csv_path)
    parcl_listings = load_parcl_listings(listings_csv_path)

    # Create lookup dictionaries
    # 1. Property ID lookup from sales
    sales_by_property_id = {}
    for _, row in parcl_sales.iterrows():
        pid = str(row.get('property_id', ''))
        if pid and pid != 'nan':
            sales_by_property_id[pid] = row

    logger.info(f"  Created property_id lookup: {len(sales_by_property_id)} entries")

    # 2. Address lookup from listings
    listings_by_address = {}
    for _, row in parcl_listings.iterrows():
        addr_norm = row.get('address_normalized', '')
        if addr_norm:
            listings_by_address[addr_norm] = row

    logger.info(f"  Created address lookup: {len(listings_by_address)} entries")

    # Get records needing enrichment
    records = get_sales_log_records(db_path)
    logger.info(f"\nRecords needing enrichment: {len(records)}")

    # Tracking stats
    stats = {
        'total_records': len(records),
        'matched_by_property_id': 0,
        'matched_by_address': 0,
        'cohort_calculated': 0,
        'still_missing': 0,
        'enriched_fields': {
            'purchase_price': 0,
            'purchase_date': 0,
            'days_held': 0,
            'realized_net': 0,
            'cohort': 0,
            'property_id': 0,
        },
        'skipped_low_confidence': 0,
    }

    enriched_records = []
    still_missing_records = []

    for record in records:
        record_id = record['id']
        updates = {}
        match_method = None

        # Strategy 1: Match by property_id
        prop_id = str(record.get('property_id', '') or '')
        if prop_id and prop_id != '' and prop_id in sales_by_property_id:
            match_data = sales_by_property_id[prop_id]
            match_method = 'property_id'

            # Extract data from Parcl sales
            if not record.get('purchase_price') or record.get('purchase_price') == 0:
                pp = match_data.get('purchase_price')
                if pp and not pd.isna(pp):
                    updates['purchase_price'] = float(pp)

            if not record.get('purchase_date'):
                pd_val = match_data.get('purchase_date')
                if pd_val and not pd.isna(pd_val):
                    updates['purchase_date'] = pd_val

            if not record.get('days_held') or record.get('days_held') == 0:
                dh = match_data.get('days_held')
                if dh and not pd.isna(dh):
                    updates['days_held'] = int(dh)

            if not record.get('realized_net'):
                rn = match_data.get('realized_net')
                if rn and not pd.isna(rn):
                    updates['realized_net'] = float(rn)

        # Strategy 2: Match by normalized address (for Singularity records without property_id)
        if match_method is None and record.get('address'):
            # Normalize the address from sales_log
            addr_from_record = normalize_address(
                f"{record['address']} {record.get('city', '')} {record.get('state', '')}"
            )

            # Try exact match first
            if addr_from_record in listings_by_address:
                match_data = listings_by_address[addr_from_record]
                match_method = 'address_exact'
            else:
                # Fuzzy match - be conservative
                best_match = None
                best_score = 0

                for listings_addr, listings_row in listings_by_address.items():
                    score = fuzzy_match_score(addr_from_record, listings_addr)
                    if score > best_score and score >= 0.90:  # High confidence threshold
                        best_score = score
                        best_match = listings_row

                if best_match is not None:
                    match_data = best_match
                    match_method = f'address_fuzzy_{best_score:.2f}'
                else:
                    stats['skipped_low_confidence'] += 1

            # Extract data from listings if matched
            if match_method and match_method.startswith('address'):
                if not record.get('purchase_price') or record.get('purchase_price') == 0:
                    pp = match_data.get('original_purchase_price')
                    if pp and not pd.isna(pp):
                        updates['purchase_price'] = float(pp)

                if not record.get('purchase_date'):
                    pd_val = match_data.get('original_purchase_date')
                    if pd_val and not pd.isna(pd_val):
                        updates['purchase_date'] = pd_val

                # Get property_id from listings
                if not record.get('property_id'):
                    pid = match_data.get('property_id')
                    if pid and not pd.isna(pid):
                        updates['property_id'] = str(pid)

        # Calculate days_held if we have purchase_date but not days_held
        if ('days_held' not in updates and
            (not record.get('days_held') or record.get('days_held') == 0)):

            purchase_date = updates.get('purchase_date') or record.get('purchase_date')
            sale_date = record.get('sale_date')

            if purchase_date and sale_date:
                try:
                    pd_dt = datetime.strptime(purchase_date, '%Y-%m-%d')
                    sd_dt = datetime.strptime(sale_date, '%Y-%m-%d')
                    updates['days_held'] = (sd_dt - pd_dt).days
                except:
                    pass

        # Calculate cohort from days_held
        days_held = updates.get('days_held') or record.get('days_held')
        if days_held and (not record.get('cohort') or record.get('cohort') == ''):
            updates['cohort'] = get_cohort(int(days_held))

        # Apply updates
        if updates:
            update_sales_record(db_path, record_id, updates)

            # Track stats
            if match_method == 'property_id':
                stats['matched_by_property_id'] += 1
            elif match_method and match_method.startswith('address'):
                stats['matched_by_address'] += 1

            for field in updates:
                if field in stats['enriched_fields']:
                    stats['enriched_fields'][field] += 1

            if 'cohort' in updates:
                stats['cohort_calculated'] += 1

            enriched_records.append({
                'id': record_id,
                'address': record.get('address'),
                'match_method': match_method,
                'updates': {k: str(v) for k, v in updates.items()},
            })
        else:
            stats['still_missing'] += 1
            still_missing_records.append({
                'id': record_id,
                'address': record.get('address'),
                'sale_date': record.get('sale_date'),
                'property_id': record.get('property_id'),
            })

    # Generate report
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_records_processed': stats['total_records'],
            'records_enriched': stats['total_records'] - stats['still_missing'],
            'matched_by_property_id': stats['matched_by_property_id'],
            'matched_by_address': stats['matched_by_address'],
            'cohorts_calculated': stats['cohort_calculated'],
            'still_missing_data': stats['still_missing'],
            'skipped_low_confidence_match': stats['skipped_low_confidence'],
        },
        'enriched_fields': stats['enriched_fields'],
        'sample_enriched': enriched_records[:20],
        'sample_still_missing': still_missing_records[:20],
    }

    # Save report
    output_path = Path(output_dir) / 'backfill_report.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)

    # Log summary
    logger.info("\n" + "=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total records processed:    {stats['total_records']}")
    logger.info(f"Records enriched:           {stats['total_records'] - stats['still_missing']}")
    logger.info(f"  - Matched by property_id: {stats['matched_by_property_id']}")
    logger.info(f"  - Matched by address:     {stats['matched_by_address']}")
    logger.info(f"Cohorts calculated:         {stats['cohort_calculated']}")
    logger.info(f"Still missing data:         {stats['still_missing']}")
    logger.info(f"\nEnriched fields:")
    for field, count in stats['enriched_fields'].items():
        if count > 0:
            logger.info(f"  - {field}: {count}")
    logger.info(f"\nReport saved to: {output_path}")

    return report


if __name__ == "__main__":
    import glob

    # Paths
    project_root = Path(__file__).parent.parent
    db_path = project_root / "data" / "glasshouse.db"
    output_dir = project_root / "outputs"

    # Find CSV files
    csv_dir = Path.home() / "Desktop" / "glasshouse"
    sales_files = sorted(csv_dir.glob("opendoor-home-sales-*.csv"), reverse=True)
    listings_files = sorted(csv_dir.glob("opendoor-for-sale-listings-*.csv"), reverse=True)

    if not sales_files:
        logger.error("No sales CSV files found in ~/Desktop/glasshouse/")
        sys.exit(1)

    if not listings_files:
        logger.error("No listings CSV files found in ~/Desktop/glasshouse/")
        sys.exit(1)

    sales_path = str(sales_files[0])
    listings_path = str(listings_files[0])

    logger.info(f"Sales CSV:    {sales_path}")
    logger.info(f"Listings CSV: {listings_path}")
    logger.info(f"Database:     {db_path}")

    # Run backfill
    backfill_purchase_data(
        db_path=str(db_path),
        sales_csv_path=sales_path,
        listings_csv_path=listings_path,
        output_dir=str(output_dir)
    )
