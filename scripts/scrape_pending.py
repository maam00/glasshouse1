#!/usr/bin/env python3
"""
Opendoor Pending Listings Scraper
=================================
Scrapes pending/under-contract listings from MLS data and cross-references
with our Opendoor inventory to track the sales funnel.

Tracks:
- Active → Pending transitions
- Pending → Sold conversions
- Fall-throughs (Pending → Back to Active)
- Toxic homes going pending (clearance progress)

Usage:
    python scripts/scrape_pending.py
    python scripts/scrape_pending.py --markets "Phoenix,Dallas,Atlanta"
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import logging

import pandas as pd
import numpy as np

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import get_config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Try to import homeharvest
try:
    from homeharvest import scrape_property
    HAS_HOMEHARVEST = True
except ImportError:
    HAS_HOMEHARVEST = False
    logger.warning("homeharvest not installed. Run: pip install homeharvest")


# Opendoor's primary markets (cities where they operate)
OPENDOOR_MARKETS = [
    # Texas
    "Austin, TX",
    "Dallas, TX",
    "Fort Worth, TX",
    "Houston, TX",
    "San Antonio, TX",
    # Arizona
    "Phoenix, AZ",
    "Tucson, AZ",
    # Florida
    "Tampa, FL",
    "Orlando, FL",
    "Jacksonville, FL",
    "Miami, FL",
    # Georgia
    "Atlanta, GA",
    # North Carolina
    "Charlotte, NC",
    "Raleigh, NC",
    # Tennessee
    "Nashville, TN",
    # Colorado
    "Denver, CO",
    # Nevada
    "Las Vegas, NV",
    # California
    "Sacramento, CA",
    "Los Angeles, CA",
    "Riverside, CA",
    # Other
    "Minneapolis, MN",
    "Portland, OR",
    "Salt Lake City, UT",
]

# Kaz era start date (CEO transition)
KAZ_ERA_START = datetime(2023, 10, 1)


def load_opendoor_inventory(output_dir: Path) -> pd.DataFrame:
    """Load current Opendoor inventory from Parcl data for cross-reference."""
    # Try to find latest listings file
    listings_patterns = [
        "unified_sales_*.csv",
        "*listing*.csv",
    ]

    for pattern in listings_patterns:
        files = list(output_dir.glob(pattern))
        if files:
            latest = max(files, key=lambda f: f.stat().st_mtime)
            logger.info(f"Loading Opendoor inventory from {latest.name}")
            return pd.read_csv(latest)

    # Also check Desktop/glasshouse
    desktop_dir = Path.home() / "Desktop" / "glasshouse"
    if desktop_dir.exists():
        files = list(desktop_dir.glob("*listing*.csv"))
        if files:
            latest = max(files, key=lambda f: f.stat().st_mtime)
            logger.info(f"Loading Opendoor inventory from {latest.name}")
            return pd.read_csv(latest)

    logger.warning("No Opendoor inventory file found")
    return pd.DataFrame()


def normalize_address(address: str) -> str:
    """Normalize address for matching."""
    if not address or pd.isna(address):
        return ""

    # Lowercase and remove common variations
    addr = str(address).lower().strip()

    # Remove unit/apt designations for matching
    for term in [' unit ', ' apt ', ' apt.', ' #', ' suite ', ' ste ']:
        if term in addr:
            addr = addr.split(term)[0]

    # Remove trailing unit numbers like ", Unit 101"
    import re
    addr = re.sub(r',?\s*(unit|apt|ste|suite|#)\s*\d+.*$', '', addr, flags=re.IGNORECASE)

    # Standardize direction prefixes/suffixes
    direction_map = {
        'north': 'n', 'south': 's', 'east': 'e', 'west': 'w',
        'northeast': 'ne', 'northwest': 'nw', 'southeast': 'se', 'southwest': 'sw',
    }
    for full, abbr in direction_map.items():
        addr = re.sub(rf'\b{full}\b', abbr, addr)

    # Remove common abbreviations differences
    replacements = [
        ('street', 'st'),
        ('avenue', 'ave'),
        ('boulevard', 'blvd'),
        ('drive', 'dr'),
        ('lane', 'ln'),
        ('road', 'rd'),
        ('court', 'ct'),
        ('place', 'pl'),
        ('circle', 'cir'),
        ('terrace', 'ter'),
        ('highway', 'hwy'),
        ('parkway', 'pkwy'),
        ('way', 'wy'),
    ]

    for full, abbr in replacements:
        addr = re.sub(rf'\b{full}\b', abbr, addr)

    # Remove periods and commas
    addr = addr.replace('.', '').replace(',', '')

    # Remove extra whitespace
    addr = ' '.join(addr.split())

    return addr


def fuzzy_address_match(addr1: str, addr2: str) -> bool:
    """Check if two addresses likely match."""
    # Handle NA/None/empty values
    if pd.isna(addr1) or pd.isna(addr2):
        return False
    if not addr1 or not addr2:
        return False

    n1 = normalize_address(str(addr1))
    n2 = normalize_address(str(addr2))

    # Exact match after normalization
    if n1 == n2:
        return True

    # Extract street number and check if it matches
    import re
    num1 = re.match(r'^(\d+)', n1)
    num2 = re.match(r'^(\d+)', n2)

    if num1 and num2 and num1.group(1) == num2.group(1):
        # Same street number - check if street name is similar
        street1 = n1[len(num1.group(1)):].strip()
        street2 = n2[len(num2.group(1)):].strip()

        # Check if one is substring of the other
        if street1 in street2 or street2 in street1:
            return True

    return False


def build_address_index(inventory_df: pd.DataFrame) -> Dict[str, dict]:
    """Build address lookup index from inventory."""
    index = {}

    if inventory_df.empty:
        return index

    # Try different address column names
    addr_cols = ['address', 'Address', 'street_address', 'full_address', 'property_address']
    addr_col = None
    for col in addr_cols:
        if col in inventory_df.columns:
            addr_col = col
            break

    if not addr_col:
        logger.warning("No address column found in inventory")
        return index

    for _, row in inventory_df.iterrows():
        norm_addr = normalize_address(row.get(addr_col, ''))
        if norm_addr:
            index[norm_addr] = {
                'property_id': row.get('property_id', ''),
                'purchase_price': row.get('purchase_price', row.get('Original Purchase Price', 0)),
                'purchase_date': row.get('purchase_date', row.get('Purchase Date', '')),
                'list_price': row.get('list_price', row.get('Latest Listing Price', 0)),
                'days_on_market': row.get('days_on_market', row.get('Days on Market', 0)),
                'state': row.get('state', row.get('State', '')),
                'city': row.get('city', row.get('City', '')),
            }

    logger.info(f"Built address index with {len(index)} properties")
    return index


def scrape_market_listings(
    market: str,
    listing_type: str = "sold",
    past_days: int = 14,
) -> pd.DataFrame:
    """Scrape listings for a single market."""
    if not HAS_HOMEHARVEST:
        logger.error("homeharvest not installed")
        return pd.DataFrame()

    try:
        logger.info(f"Scraping {listing_type} listings in {market}...")

        df = scrape_property(
            location=market,
            listing_type=listing_type,
            past_days=past_days,
            limit=500,  # Per market limit
        )

        if df is not None and not df.empty:
            df['search_market'] = market
            logger.info(f"  Found {len(df)} {listing_type} listings in {market}")
            return df
        else:
            logger.info(f"  No {listing_type} listings found in {market}")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error scraping {market}: {e}")
        return pd.DataFrame()


def identify_opendoor_listings(
    scraped_df: pd.DataFrame,
    address_index: Dict[str, dict],
) -> pd.DataFrame:
    """
    Identify which scraped listings belong to Opendoor.

    Methods:
    1. Address matching with our inventory
    2. Agent/broker name matching (Opendoor agents)
    3. Listing description keywords
    """
    if scraped_df.empty:
        return pd.DataFrame()

    matches = []

    for _, row in scraped_df.iterrows():
        # Build address from scraped data
        street = row.get('street', row.get('address', ''))
        city = row.get('city', '')

        # Try to match by address
        norm_addr = normalize_address(f"{street}")

        is_opendoor = False
        opendoor_data = {}
        match_method = None

        # Method 1: Direct address match
        if norm_addr in address_index:
            is_opendoor = True
            opendoor_data = address_index[norm_addr]
            match_method = 'address_exact'

        # Method 1b: Fuzzy address match
        if not is_opendoor:
            for idx_addr, idx_data in address_index.items():
                if fuzzy_address_match(street, idx_addr):
                    is_opendoor = True
                    opendoor_data = idx_data
                    match_method = 'address_fuzzy'
                    break

        # Method 2: Check agent/broker for Opendoor
        agent = str(row.get('agent_name', '')).lower()
        broker = str(row.get('broker_name', '')).lower()
        office = str(row.get('office_name', '')).lower()

        if any('opendoor' in x for x in [agent, broker, office]):
            is_opendoor = True
            match_method = match_method or 'agent'

        if is_opendoor:
            match_data = {
                'scraped_address': street,
                'city': city,
                'state': row.get('state', ''),
                'status': row.get('status', 'pending'),
                'list_price': row.get('list_price', 0),
                'list_date': row.get('list_date', ''),
                'pending_date': row.get('pending_date', ''),
                'beds': row.get('beds', 0),
                'baths': row.get('full_baths', 0),
                'sqft': row.get('sqft', 0),
                'year_built': row.get('year_built', 0),
                'latitude': row.get('latitude', 0),
                'longitude': row.get('longitude', 0),
                'property_url': row.get('property_url', ''),
                'mls_id': row.get('mls_id', ''),
                'agent_name': row.get('agent_name', ''),
                'broker_name': row.get('broker_name', ''),
                'match_method': match_method,
                'search_market': row.get('search_market', ''),
            }

            # Add Opendoor-specific data if matched
            if opendoor_data:
                match_data.update({
                    'od_property_id': opendoor_data.get('property_id', ''),
                    'od_purchase_price': opendoor_data.get('purchase_price', 0),
                    'od_purchase_date': opendoor_data.get('purchase_date', ''),
                    'od_days_on_market': opendoor_data.get('days_on_market', 0),
                })

            matches.append(match_data)

    if matches:
        return pd.DataFrame(matches)
    return pd.DataFrame()


def calculate_pending_metrics(pending_df: pd.DataFrame, config=None) -> Dict:
    """Calculate pending funnel metrics."""
    if pending_df.empty:
        return {}

    config = config or get_config()

    metrics = {
        'total_pending': len(pending_df),
        'scraped_at': datetime.now().isoformat(),
    }

    # Parse dates for cohort analysis
    if 'od_purchase_date' in pending_df.columns:
        pending_df['purchase_date_parsed'] = pd.to_datetime(
            pending_df['od_purchase_date'], errors='coerce'
        )

        # Kaz era vs Legacy
        kaz_mask = pending_df['purchase_date_parsed'] >= KAZ_ERA_START
        metrics['kaz_era_pending'] = int(kaz_mask.sum())
        metrics['legacy_pending'] = int((~kaz_mask & pending_df['purchase_date_parsed'].notna()).sum())

    # Days on market analysis
    if 'od_days_on_market' in pending_df.columns:
        dom = pd.to_numeric(pending_df['od_days_on_market'], errors='coerce')

        metrics['avg_dom_at_pending'] = round(dom.mean(), 1) if dom.notna().any() else 0
        metrics['max_dom_at_pending'] = int(dom.max()) if dom.notna().any() else 0

        # Cohort breakdown of pending
        cohort_boundaries = config.cohorts

        new_pending = (dom < cohort_boundaries.new_max).sum()
        mid_pending = ((dom >= cohort_boundaries.mid_min) & (dom < cohort_boundaries.mid_max)).sum()
        old_pending = ((dom >= cohort_boundaries.old_min) & (dom < cohort_boundaries.old_max)).sum()
        toxic_pending = (dom >= cohort_boundaries.toxic_min).sum()

        metrics['cohort_breakdown'] = {
            'new': int(new_pending),
            'mid': int(mid_pending),
            'old': int(old_pending),
            'toxic': int(toxic_pending),
        }

        # Toxic homes going pending is a KEY metric for clearance
        metrics['toxic_pending_count'] = int(toxic_pending)
        metrics['toxic_pending_pct'] = round(toxic_pending / len(pending_df) * 100, 1) if len(pending_df) > 0 else 0

    # Price analysis
    if 'list_price' in pending_df.columns:
        prices = pd.to_numeric(pending_df['list_price'], errors='coerce')
        metrics['avg_pending_price'] = round(prices.mean()) if prices.notna().any() else 0
        metrics['total_pending_value'] = round(prices.sum()) if prices.notna().any() else 0

    # Geographic breakdown
    if 'state' in pending_df.columns:
        state_counts = pending_df['state'].value_counts().head(10).to_dict()
        metrics['by_state'] = state_counts

    if 'search_market' in pending_df.columns:
        market_counts = pending_df['search_market'].value_counts().head(10).to_dict()
        metrics['by_market'] = market_counts

    return metrics


def load_previous_pending(output_dir: Path) -> pd.DataFrame:
    """Load previous pending data for comparison."""
    files = list(output_dir.glob("pending_*.json"))
    if not files:
        return pd.DataFrame()

    latest = max(files, key=lambda f: f.stat().st_mtime)

    try:
        with open(latest) as f:
            data = json.load(f)

        if 'listings' in data:
            return pd.DataFrame(data['listings'])
    except Exception as e:
        logger.warning(f"Could not load previous pending data: {e}")

    return pd.DataFrame()


def calculate_funnel_changes(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
) -> Dict:
    """Calculate funnel changes between snapshots."""
    changes = {
        'new_pending': 0,
        'converted_to_sold': 0,
        'fell_through': 0,
        'still_pending': 0,
    }

    if current_df.empty or previous_df.empty:
        return changes

    # Get address sets
    current_addresses = set(current_df['scraped_address'].dropna().unique())
    previous_addresses = set(previous_df['scraped_address'].dropna().unique())

    # New pending (in current but not previous)
    new_pending = current_addresses - previous_addresses
    changes['new_pending'] = len(new_pending)

    # Still pending (in both)
    still_pending = current_addresses & previous_addresses
    changes['still_pending'] = len(still_pending)

    # Left pending (in previous but not current)
    # Could be sold or fell through - we assume sold for now
    left_pending = previous_addresses - current_addresses
    changes['converted_to_sold'] = len(left_pending)

    return changes


def main():
    parser = argparse.ArgumentParser(description='Scrape Opendoor pending listings')
    parser.add_argument('--markets', type=str, help='Comma-separated list of markets to scrape')
    parser.add_argument('--all-markets', action='store_true', help='Scrape all Opendoor markets')
    parser.add_argument('--past-days', type=int, default=90, help='Look back period in days')
    parser.add_argument('--output-dir', type=str, help='Output directory')
    args = parser.parse_args()

    if not HAS_HOMEHARVEST:
        print("ERROR: homeharvest not installed. Run: pip install homeharvest")
        return 1

    project_root = Path(__file__).parent.parent
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine markets to scrape
    if args.markets:
        markets = [m.strip() for m in args.markets.split(',')]
    elif args.all_markets:
        markets = OPENDOOR_MARKETS
    else:
        # Default: top 5 markets
        markets = OPENDOOR_MARKETS[:5]

    print(f"\n{'='*60}")
    print("  OPENDOOR PENDING LISTINGS SCRAPER")
    print(f"{'='*60}")
    print(f"  Markets: {len(markets)}")
    print(f"  Lookback: {args.past_days} days")
    print(f"{'='*60}\n")

    # Load Opendoor inventory for cross-reference
    inventory_df = load_opendoor_inventory(output_dir)
    address_index = build_address_index(inventory_df)

    # Scrape sold listings from each market (pending not available via this API)
    all_scraped = []

    for market in markets:
        # Scrape recent sales to validate against our data
        sold_df = scrape_market_listings(market, "sold", min(args.past_days, 30))
        if not sold_df.empty:
            all_scraped.append(sold_df)

    if not all_scraped:
        print("No listings found across all markets")
        return 0

    # Combine all scraped data
    combined_df = pd.concat(all_scraped, ignore_index=True)
    print(f"\nTotal scraped listings: {len(combined_df)}")

    # Identify Opendoor listings
    opendoor_pending = identify_opendoor_listings(combined_df, address_index)
    print(f"Identified Opendoor listings: {len(opendoor_pending)}")

    # Calculate metrics
    metrics = calculate_pending_metrics(opendoor_pending)

    # Load previous for funnel comparison
    previous_df = load_previous_pending(output_dir)
    funnel_changes = calculate_funnel_changes(opendoor_pending, previous_df)
    metrics['funnel_changes'] = funnel_changes

    # Print summary
    print(f"\n{'='*60}")
    print("  PENDING FUNNEL SUMMARY")
    print(f"{'='*60}")
    print(f"  Total Pending: {metrics.get('total_pending', 0)}")
    print(f"  Kaz-Era Pending: {metrics.get('kaz_era_pending', 0)}")
    print(f"  Legacy Pending: {metrics.get('legacy_pending', 0)}")
    print(f"  Avg DOM at Pending: {metrics.get('avg_dom_at_pending', 0)} days")

    cohorts = metrics.get('cohort_breakdown', {})
    if cohorts:
        print(f"\n  Cohort Breakdown:")
        print(f"    New (<90d):     {cohorts.get('new', 0)}")
        print(f"    Mid (90-180d):  {cohorts.get('mid', 0)}")
        print(f"    Old (180-365d): {cohorts.get('old', 0)}")
        print(f"    Toxic (>365d):  {cohorts.get('toxic', 0)} ← KEY METRIC")

    if funnel_changes.get('new_pending', 0) > 0 or funnel_changes.get('converted_to_sold', 0) > 0:
        print(f"\n  Funnel Changes (vs last run):")
        print(f"    New Pending:    +{funnel_changes.get('new_pending', 0)}")
        print(f"    Likely Sold:    {funnel_changes.get('converted_to_sold', 0)}")
        print(f"    Still Pending:  {funnel_changes.get('still_pending', 0)}")

    print(f"{'='*60}\n")

    # Save results
    date_str = datetime.now().strftime('%Y-%m-%d')

    # Save pending listings
    if not opendoor_pending.empty:
        listings_file = output_dir / f"pending_listings_{date_str}.csv"
        opendoor_pending.to_csv(listings_file, index=False)
        print(f"Saved: {listings_file}")

    # Save metrics JSON
    output_data = {
        'scraped_at': datetime.now().isoformat(),
        'markets_scraped': markets,
        'metrics': metrics,
        'listings': opendoor_pending.to_dict('records') if not opendoor_pending.empty else [],
    }

    metrics_file = output_dir / f"pending_{date_str}.json"
    with open(metrics_file, 'w') as f:
        json.dump(output_data, f, indent=2, default=str)
    print(f"Saved: {metrics_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
