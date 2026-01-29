#!/usr/bin/env python3
"""
Daily Property Snapshot Job
============================
Scrapes Opendoor listings across all markets and upserts to property_daily_snapshot.
Tracks status transitions and price changes.

Usage:
    python scripts/daily_snapshot.py
    python scripts/daily_snapshot.py --markets phoenix,dallas
    python scripts/daily_snapshot.py --dry-run

This should be run daily via cron:
    0 12 * * * cd /path/to/glasshouse1 && source venv/bin/activate && python scripts/daily_snapshot.py
"""

import sys
import json
import argparse
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.database import Database, generate_property_id, normalize_address

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import playwright for scraping
try:
    from playwright.async_api import async_playwright, Page
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    logger.warning("playwright not installed. Run: pip install playwright && playwright install chromium")

# Market slugs
OPENDOOR_MARKETS = {
    "phoenix": "phoenix-az",
    "dallas": "dallas-tx",
    "houston": "houston-tx",
    "austin": "austin-tx",
    "san_antonio": "san-antonio-tx",
    "atlanta": "atlanta-ga",
    "charlotte": "charlotte-nc",
    "raleigh": "raleigh-nc",
    "tampa": "tampa-fl",
    "orlando": "orlando-fl",
    "jacksonville": "jacksonville-fl",
    "denver": "denver-co",
    "las_vegas": "las-vegas-nv",
    "nashville": "nashville-tn",
    "sacramento": "sacramento-ca",
    "riverside": "riverside-ca",
    "minneapolis": "minneapolis-mn",
    "portland": "portland-or",
    "salt_lake_city": "salt-lake-city-ut",
    "tucson": "tucson-az",
}


async def scrape_market(page: Page, market_slug: str) -> List[Dict]:
    """Scrape all listings from a single Opendoor market."""
    url = f"https://www.opendoor.com/homes/{market_slug}"
    listings = []

    try:
        logger.info(f"Scraping {market_slug}...")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(2000)

        # Wait for listings
        try:
            await page.wait_for_selector('a[href*="/properties/"]', timeout=30000)
        except Exception:
            logger.warning(f"  Timeout waiting for listings in {market_slug}")

        # Scroll to load lazy content
        await auto_scroll(page)

        # Extract via JavaScript
        js_code = """
        () => {
            const results = [];
            const links = document.querySelectorAll('a[href*="/properties/"]');

            links.forEach(link => {
                const href = link.getAttribute('href');
                if (!href) return;

                let card = link;
                for (let i = 0; i < 10 && card; i++) {
                    const text = card.innerText || '';
                    const priceMatch = text.match(/\\$(\\d{1,3}(?:,\\d{3})+)/);
                    if (priceMatch) {
                        const bedMatch = text.match(/(\\d+)\\s*bds?/i);
                        const bathMatch = text.match(/(\\d+(?:\\.\\d+)?)\\s*ba/i);
                        const sqftMatch = text.match(/([\\d,]+)\\s*sqft/i);

                        let address = null;
                        const addrMatch = href.match(/\\/properties\\/([^/?]+)/);
                        if (addrMatch) {
                            address = addrMatch[1].replace(/-/g, ' ');
                        }

                        let status = 'FOR_SALE';
                        const textLower = text.toLowerCase();
                        if (textLower.includes('pending')) status = 'PENDING';
                        else if (textLower.includes('under contract')) status = 'UNDER_CONTRACT';
                        else if (textLower.includes('sold')) status = 'SOLD';

                        results.push({
                            url: href.startsWith('http') ? href : 'https://www.opendoor.com' + href,
                            address: address,
                            price: parseInt(priceMatch[1].replace(/,/g, '')),
                            beds: bedMatch ? parseInt(bedMatch[1]) : null,
                            baths: bathMatch ? parseFloat(bathMatch[1]) : null,
                            sqft: sqftMatch ? parseInt(sqftMatch[1].replace(/,/g, '')) : null,
                            status: status,
                        });
                        break;
                    }
                    card = card.parentElement;
                }
            });

            // Dedupe by URL
            const seen = new Set();
            return results.filter(r => {
                if (seen.has(r.url)) return false;
                seen.add(r.url);
                return true;
            });
        }
        """

        js_listings = await page.evaluate(js_code)
        logger.info(f"  Found {len(js_listings)} listings in {market_slug}")

        # Parse city/state from market slug
        parts = market_slug.split('-')
        state = parts[-1].upper() if len(parts) > 1 else ''
        city = ' '.join(parts[:-1]).title() if len(parts) > 1 else market_slug.title()

        for item in js_listings:
            item['market'] = market_slug
            item['city'] = city
            item['state'] = state
            item['list_price'] = item.pop('price', None)
            item['opendoor_url'] = item.pop('url', None)
            listings.append(item)

    except Exception as e:
        logger.error(f"Error scraping {market_slug}: {e}")

    return listings


async def auto_scroll(page: Page, max_scrolls: int = 20):
    """Scroll to trigger lazy loading."""
    prev_height = 0
    for i in range(max_scrolls):
        await page.evaluate("window.scrollBy(0, window.innerHeight)")
        await page.wait_for_timeout(800)

        current_height = await page.evaluate("document.body.scrollHeight")
        at_bottom = await page.evaluate(
            "window.innerHeight + window.scrollY >= document.body.scrollHeight - 100"
        )
        if at_bottom and current_height == prev_height:
            break
        prev_height = current_height

    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(500)


async def scrape_all_markets(markets: List[str], headless: bool = True) -> List[Dict]:
    """Scrape listings from multiple markets."""
    all_listings = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        )
        page = await context.new_page()

        for market in markets:
            slug = OPENDOOR_MARKETS.get(market.lower().replace(' ', '_'), market)
            listings = await scrape_market(page, slug)
            all_listings.extend(listings)
            await page.wait_for_timeout(2000)  # Pause between markets

        await browser.close()

    return all_listings


def upsert_snapshots(db: Database, listings: List[Dict], dry_run: bool = False) -> Dict:
    """
    Upsert all listings to property_daily_snapshot.

    Returns summary stats.
    """
    stats = {
        'total_scraped': len(listings),
        'new_properties': 0,
        'updated_properties': 0,
        'status_transitions': [],
        'price_cuts': 0,
        'by_market': {},
        'by_status': {},
    }

    for listing in listings:
        market = listing.get('market', 'unknown')
        status = listing.get('status', 'FOR_SALE')

        # Track by market
        stats['by_market'][market] = stats['by_market'].get(market, 0) + 1

        # Track by status
        stats['by_status'][status] = stats['by_status'].get(status, 0) + 1

        if dry_run:
            continue

        # Upsert to database
        property_id, is_new, transition = db.upsert_property_snapshot(listing)

        if is_new:
            stats['new_properties'] += 1
        else:
            stats['updated_properties'] += 1

        if transition:
            stats['status_transitions'].append(transition)

    return stats


def detect_missing_properties(db: Database) -> List[Dict]:
    """
    Detect properties that were active yesterday but not scraped today.
    These may have been sold, removed, or are missing from scrape.
    """
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - __import__('datetime').timedelta(days=1)).strftime('%Y-%m-%d')

    conn = db._get_conn()
    cursor = conn.cursor()

    # Find properties active yesterday but not seen today
    cursor.execute("""
        SELECT DISTINCT p1.property_id, p1.address_normalized, p1.market, p1.list_price
        FROM property_daily_snapshot p1
        WHERE p1.snapshot_date = ?
          AND p1.status = 'FOR_SALE'
          AND NOT EXISTS (
              SELECT 1 FROM property_daily_snapshot p2
              WHERE p2.property_id = p1.property_id
                AND p2.snapshot_date = ?
          )
    """, (yesterday, today))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def print_summary(stats: Dict, transitions: List[Dict], missing: List[Dict]):
    """Print daily snapshot summary."""
    print(f"\n{'='*60}")
    print("  DAILY PROPERTY SNAPSHOT SUMMARY")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    print(f"\n  SCRAPE RESULTS")
    print(f"  {'-'*50}")
    print(f"  Total Listings Scraped:  {stats['total_scraped']:>6}")
    print(f"  New Properties:          {stats['new_properties']:>6}")
    print(f"  Updated Properties:      {stats['updated_properties']:>6}")

    print(f"\n  BY STATUS")
    print(f"  {'-'*50}")
    for status, count in sorted(stats['by_status'].items()):
        print(f"  {status:<20} {count:>6}")

    print(f"\n  BY MARKET (top 10)")
    print(f"  {'-'*50}")
    sorted_markets = sorted(stats['by_market'].items(), key=lambda x: -x[1])[:10]
    for market, count in sorted_markets:
        print(f"  {market:<20} {count:>6}")

    if transitions:
        print(f"\n  STATUS TRANSITIONS ({len(transitions)})")
        print(f"  {'-'*50}")
        transition_summary = {}
        for t in transitions:
            key = f"{t['from_status']} -> {t['to_status']}"
            transition_summary[key] = transition_summary.get(key, 0) + 1
        for transition, count in sorted(transition_summary.items(), key=lambda x: -x[1]):
            print(f"  {transition:<30} {count:>6}")

    if missing:
        print(f"\n  MISSING FROM TODAY'S SCRAPE ({len(missing)})")
        print(f"  {'-'*50}")
        print(f"  (These were active yesterday but not seen today)")
        for prop in missing[:5]:
            print(f"  - {prop['address_normalized'][:40]} ({prop['market']})")
        if len(missing) > 5:
            print(f"  ... and {len(missing) - 5} more")

    print(f"\n{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description='Daily property snapshot job')
    parser.add_argument('--markets', type=str, help='Comma-separated list of markets')
    parser.add_argument('--all-markets', action='store_true', help='Scrape all markets')
    parser.add_argument('--dry-run', action='store_true', help='Scrape but do not save to database')
    parser.add_argument('--visible', action='store_true', help='Run browser in visible mode')
    args = parser.parse_args()

    if not HAS_PLAYWRIGHT:
        print("ERROR: playwright not installed")
        print("Run: pip install playwright && playwright install chromium")
        return 1

    # Determine markets
    if args.markets:
        markets = [m.strip() for m in args.markets.split(',')]
    elif args.all_markets:
        markets = list(OPENDOOR_MARKETS.keys())
    else:
        # Default: all markets
        markets = list(OPENDOOR_MARKETS.keys())

    print(f"\n{'='*60}")
    print("  DAILY PROPERTY SNAPSHOT JOB")
    print(f"{'='*60}")
    print(f"  Markets:  {len(markets)}")
    print(f"  Dry Run:  {args.dry_run}")
    print(f"{'='*60}\n")

    # Initialize database
    db = Database()

    # Scrape
    listings = asyncio.run(scrape_all_markets(markets, headless=not args.visible))

    if not listings:
        print("No listings found")
        return 1

    # Upsert to database
    stats = upsert_snapshots(db, listings, dry_run=args.dry_run)

    # Detect missing properties
    missing = [] if args.dry_run else detect_missing_properties(db)

    # Print summary
    print_summary(stats, stats['status_transitions'], missing)

    # Save summary to outputs
    if not args.dry_run:
        output_dir = Path(__file__).parent.parent / "outputs"
        output_dir.mkdir(exist_ok=True)

        summary_file = output_dir / f"snapshot_summary_{datetime.now().strftime('%Y-%m-%d')}.json"
        summary_data = {
            'scraped_at': datetime.now().isoformat(),
            'markets': markets,
            'stats': stats,
            'transitions': stats['status_transitions'],
            'missing_count': len(missing),
        }
        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2, default=str)
        print(f"Saved: {summary_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
