"""
Import Historical Data from Desktop CSVs and Singularity
=========================================================
Loads listing and sales data into the GlassHouse database.
"""

import csv
import sys
from pathlib import Path
from datetime import datetime
import re

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.database import Database

# Data source directories
DESKTOP_DIR = Path("/Users/mabramsky/Desktop/glasshouse")
SINGULARITY_DIR = Path("/Users/mabramsky/glasshouse/outputs")


def parse_date(date_str: str) -> str | None:
    """Parse various date formats to YYYY-MM-DD."""
    if not date_str or date_str.strip() == '':
        return None

    date_str = date_str.strip()

    # Try different formats
    formats = [
        "%b %d, %Y",      # "Jan 27, 2026"
        "%Y-%m-%d",       # "2026-01-27"
        "%m/%d/%Y",       # "01/27/2026"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def parse_price(price_str: str) -> float | None:
    """Parse price string like '$305,000' to float."""
    if not price_str or price_str.strip() == '':
        return None

    # Remove $ and commas
    cleaned = re.sub(r'[$,]', '', str(price_str).strip())

    # Handle percentage
    if '%' in cleaned:
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int(val: str) -> int | None:
    """Parse integer, return None if invalid."""
    if not val or val.strip() == '':
        return None
    try:
        return int(float(val))
    except ValueError:
        return None


def normalize_address(address: str) -> str:
    """Normalize address for matching."""
    if not address:
        return ""
    return re.sub(r'\s+', ' ', address.upper().strip())


def ensure_schema(db: Database):
    """Ensure database has required columns."""
    conn = db._get_conn()
    cursor = conn.cursor()

    # Check and add columns to property_daily_snapshot
    cursor.execute("PRAGMA table_info(property_daily_snapshot)")
    existing_cols = {row['name'] for row in cursor.fetchall()}

    new_cols = {
        'purchase_date': 'TEXT',
        'purchase_price': 'REAL',
        'initial_list_date': 'TEXT',
        'initial_list_price': 'REAL',
        'unrealized_pnl': 'REAL',
        'unrealized_pnl_pct': 'REAL',
        'cohort': 'TEXT',  # 'new', 'mid', 'old', 'toxic'
    }

    for col, dtype in new_cols.items():
        if col not in existing_cols:
            cursor.execute(f"ALTER TABLE property_daily_snapshot ADD COLUMN {col} {dtype}")
            print(f"  Added column: {col}")

    # Check sales_log table and add missing columns
    cursor.execute("PRAGMA table_info(sales_log)")
    sales_cols = {row['name'] for row in cursor.fetchall()}

    sales_new_cols = {
        'property_id': 'TEXT',
        'purchase_date': 'TEXT',
        'quarter': 'TEXT',
        'year': 'INTEGER',
        'source': 'TEXT',
        'imported_at': 'TEXT',
    }

    for col, dtype in sales_new_cols.items():
        if col not in sales_cols:
            try:
                cursor.execute(f"ALTER TABLE sales_log ADD COLUMN {col} {dtype}")
                print(f"  Added column to sales_log: {col}")
            except Exception as e:
                print(f"  Note: {col} - {e}")

    conn.commit()
    conn.close()


def import_listings(db: Database):
    """Import listings from Desktop CSV files."""
    print("\n--- Importing Listings ---")

    conn = db._get_conn()
    cursor = conn.cursor()

    # Find all listing files
    listing_files = sorted(DESKTOP_DIR.glob("opendoor-for-sale-listings-*.csv"))

    total_imported = 0
    total_updated = 0

    for csv_file in listing_files:
        print(f"\nProcessing: {csv_file.name}")

        # Extract date from filename
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', csv_file.name)
        snapshot_date = date_match.group(1) if date_match else datetime.now().strftime("%Y-%m-%d")

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                property_id = row.get('Property ID', '')
                address = row.get('Address', '')
                city = row.get('City', '')
                state = row.get('State', '')

                if not address:
                    continue

                address_normalized = normalize_address(f"{address} {city} {state}")

                # Parse fields
                list_price = parse_price(row.get('Latest Listing Price'))
                purchase_date = parse_date(row.get('Original Purchase Date'))
                purchase_price = parse_price(row.get('Original Purchase Price'))
                initial_list_date = parse_date(row.get('Initial Listing Date'))
                initial_list_price = parse_price(row.get('Initial Listing Price'))
                days_on_market = parse_int(row.get('Days on Market'))
                price_cuts = parse_int(row.get('Price Cuts')) or 0
                unrealized_net = parse_price(row.get('Unrealized Net'))
                unrealized_pct_str = row.get('Unrealized Net %', '')
                unrealized_pct = parse_price(unrealized_pct_str.replace('%', '')) if unrealized_pct_str else None

                beds = parse_int(row.get('Bedrooms'))
                baths = parse_price(row.get('Bathrooms'))  # can be 2.5
                sqft = parse_int(row.get('Square Feet'))

                # Determine cohort based on days held
                if purchase_date:
                    try:
                        purch_dt = datetime.strptime(purchase_date, "%Y-%m-%d")
                        days_held = (datetime.now() - purch_dt).days
                        if days_held < 90:
                            cohort = 'new'
                        elif days_held < 180:
                            cohort = 'mid'
                        elif days_held < 365:
                            cohort = 'old'
                        else:
                            cohort = 'toxic'
                    except:
                        cohort = None
                else:
                    cohort = None

                # Calculate price change
                previous_price = initial_list_price
                price_change = (list_price - previous_price) if (list_price and previous_price) else None

                # Check if exists
                cursor.execute("""
                    SELECT id FROM property_daily_snapshot
                    WHERE property_id = ? AND snapshot_date = ?
                """, (property_id, snapshot_date))
                existing = cursor.fetchone()

                if existing:
                    # Update existing
                    cursor.execute("""
                        UPDATE property_daily_snapshot SET
                            list_price = ?,
                            purchase_date = ?,
                            purchase_price = ?,
                            initial_list_date = ?,
                            initial_list_price = ?,
                            days_on_market = ?,
                            price_cuts_count = ?,
                            price_change = ?,
                            unrealized_pnl = ?,
                            unrealized_pnl_pct = ?,
                            cohort = ?,
                            beds = ?,
                            baths = ?,
                            sqft = ?
                        WHERE id = ?
                    """, (
                        list_price, purchase_date, purchase_price,
                        initial_list_date, initial_list_price,
                        days_on_market, price_cuts, price_change,
                        unrealized_net, unrealized_pct, cohort,
                        beds, baths, sqft,
                        existing['id']
                    ))
                    total_updated += 1
                else:
                    # Insert new
                    cursor.execute("""
                        INSERT INTO property_daily_snapshot (
                            property_id, snapshot_date, address_normalized,
                            city, state, market, list_price, status,
                            beds, baths, sqft, first_seen_date, days_on_market,
                            previous_price, price_change, price_cuts_count,
                            purchase_date, purchase_price, initial_list_date,
                            initial_list_price, unrealized_pnl, unrealized_pnl_pct,
                            cohort, source, scrape_timestamp
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        property_id, snapshot_date, address_normalized,
                        city, state, f"{city.lower().replace(' ', '-')}-{state.lower()}",
                        list_price, 'FOR_SALE',
                        beds, baths, sqft, initial_list_date, days_on_market,
                        previous_price, price_change, price_cuts,
                        purchase_date, purchase_price, initial_list_date,
                        initial_list_price, unrealized_net, unrealized_pct,
                        cohort, 'desktop_csv', datetime.now().isoformat()
                    ))
                    total_imported += 1

        conn.commit()
        print(f"  Imported: {total_imported}, Updated: {total_updated}")

    conn.close()
    print(f"\nListings total - Imported: {total_imported}, Updated: {total_updated}")


def import_sales(db: Database):
    """Import sales from Desktop CSV files."""
    print("\n--- Importing Sales ---")

    conn = db._get_conn()
    cursor = conn.cursor()

    # Find all sales files from Desktop
    sales_files = sorted(DESKTOP_DIR.glob("opendoor-home-sales-*.csv"))

    total_imported = 0

    for csv_file in sales_files:
        print(f"\nProcessing: {csv_file.name}")

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                property_id = row.get('Property ID', '')
                sale_date = parse_date(row.get('Sale Date'))
                sale_price = parse_price(row.get('Sale Price'))
                purchase_date = parse_date(row.get('Purchase Date'))
                purchase_price = parse_price(row.get('Purchase Price'))
                days_held = parse_int(row.get('Days Held'))
                realized_net = parse_price(row.get('Realized Net'))
                quarter = row.get('Quarter', '')
                year = parse_int(row.get('Year'))

                if not sale_date:
                    continue

                # Check for duplicate by property_id or address
                cursor.execute("""
                    SELECT id FROM sales_log
                    WHERE (property_id = ? OR address = ?) AND sale_date = ?
                """, (property_id, normalize_address(row.get('Address', '')), sale_date))

                if cursor.fetchone():
                    continue  # Skip duplicate

                # Determine cohort
                if days_held:
                    if days_held < 90:
                        cohort = 'new'
                    elif days_held < 180:
                        cohort = 'mid'
                    elif days_held < 365:
                        cohort = 'old'
                    else:
                        cohort = 'toxic'
                else:
                    cohort = None

                cursor.execute("""
                    INSERT INTO sales_log (
                        property_id, address, sale_date, sale_price,
                        purchase_date, purchase_price, days_held,
                        realized_net, cohort, quarter, year,
                        source, imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    property_id, normalize_address(row.get('Address', '')),
                    sale_date, sale_price,
                    purchase_date, purchase_price, days_held,
                    realized_net, cohort, quarter, year,
                    'desktop_csv', datetime.now().isoformat()
                ))
                total_imported += 1

        conn.commit()

    print(f"\nDesktop sales imported: {total_imported}")

    # Also import from Singularity sales files (more historical data)
    print("\n--- Importing Singularity Sales ---")
    singularity_files = sorted(SINGULARITY_DIR.glob("singularity_sales_*.csv"))

    sing_imported = 0

    for csv_file in singularity_files:
        print(f"\nProcessing: {csv_file.name}")

        # Extract date from filename
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', csv_file.name)
        file_date = date_match.group(1) if date_match else None

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                # Singularity format: baths,beds,city,full_address,list_price,living_square_footage,sold_date
                address = row.get('full_address', '')
                city = row.get('city', '')
                sale_date = parse_date(row.get('sold_date'))
                list_price = parse_price(row.get('list_price'))

                if not sale_date or not address:
                    continue

                address_normalized = normalize_address(address)

                # Check for duplicate by address and date
                cursor.execute("""
                    SELECT id FROM sales_log
                    WHERE address = ? AND sale_date = ?
                """, (address_normalized, sale_date))

                if cursor.fetchone():
                    continue

                cursor.execute("""
                    INSERT INTO sales_log (
                        address, city, sale_date, sale_price,
                        source, imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    address_normalized, city, sale_date, list_price,
                    'singularity', datetime.now().isoformat()
                ))
                sing_imported += 1

        conn.commit()

    conn.close()
    print(f"\nSingularity sales imported: {sing_imported}")
    print(f"\nTotal sales imported: {total_imported + sing_imported}")


def show_summary(db: Database):
    """Show summary of imported data."""
    print("\n" + "="*60)
    print("  IMPORT SUMMARY")
    print("="*60)

    conn = db._get_conn()
    cursor = conn.cursor()

    # Listings summary
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT snapshot_date) as dates,
            COUNT(CASE WHEN purchase_price IS NOT NULL THEN 1 END) as with_purchase,
            COUNT(CASE WHEN unrealized_pnl IS NOT NULL THEN 1 END) as with_pnl,
            AVG(days_on_market) as avg_dom,
            AVG(price_cuts_count) as avg_cuts
        FROM property_daily_snapshot
    """)
    row = cursor.fetchone()
    print(f"\n  Listings:")
    print(f"    Total records: {row['total']}")
    print(f"    Snapshot dates: {row['dates']}")
    print(f"    With purchase price: {row['with_purchase']}")
    print(f"    With unrealized P&L: {row['with_pnl']}")
    print(f"    Avg DOM: {row['avg_dom']:.1f}" if row['avg_dom'] else "    Avg DOM: N/A")
    print(f"    Avg price cuts: {row['avg_cuts']:.2f}" if row['avg_cuts'] else "    Avg price cuts: N/A")

    # Cohort breakdown
    cursor.execute("""
        SELECT cohort, COUNT(*) as cnt
        FROM property_daily_snapshot
        WHERE cohort IS NOT NULL
        GROUP BY cohort
    """)
    cohorts = cursor.fetchall()
    if cohorts:
        print(f"\n  Cohort breakdown:")
        for c in cohorts:
            print(f"    {c['cohort']}: {c['cnt']}")

    # Sales summary
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT sale_date) as dates,
            COUNT(CASE WHEN realized_net IS NOT NULL THEN 1 END) as with_pnl,
            SUM(CASE WHEN realized_net > 0 THEN 1 ELSE 0 END) as profitable,
            AVG(realized_net) as avg_pnl
        FROM sales_log
    """)
    row = cursor.fetchone()
    print(f"\n  Sales:")
    print(f"    Total records: {row['total']}")
    print(f"    Sale dates: {row['dates']}")
    print(f"    With realized P&L: {row['with_pnl']}")
    if row['with_pnl'] and row['with_pnl'] > 0:
        print(f"    Profitable: {row['profitable']} ({row['profitable']/row['with_pnl']*100:.1f}%)")
        print(f"    Avg P&L: ${row['avg_pnl']:,.0f}" if row['avg_pnl'] else "    Avg P&L: N/A")

    conn.close()
    print("\n" + "="*60)


def main():
    print("="*60)
    print("  HISTORICAL DATA IMPORT")
    print("="*60)

    db = Database()

    # Ensure schema has required columns
    print("\n--- Checking Schema ---")
    ensure_schema(db)

    # Import listings
    import_listings(db)

    # Import sales
    import_sales(db)

    # Show summary
    show_summary(db)


if __name__ == "__main__":
    main()
