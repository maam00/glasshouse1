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

# City to state mapping for known Opendoor markets
CITY_STATE_MAP = {
    "phoenix": "az", "scottsdale": "az", "mesa": "az", "chandler": "az",
    "gilbert": "az", "glendale": "az", "tucson": "az", "tempe": "az",
    "dallas": "tx", "fort worth": "tx", "plano": "tx", "arlington": "tx",
    "houston": "tx", "austin": "tx", "san antonio": "tx", "round rock": "tx",
    "atlanta": "ga", "marietta": "ga", "alpharetta": "ga", "johns creek": "ga",
    "charlotte": "nc", "raleigh": "nc", "durham": "nc", "cary": "nc",
    "tampa": "fl", "orlando": "fl", "jacksonville": "fl", "st. petersburg": "fl",
    "denver": "co", "aurora": "co", "lakewood": "co", "thornton": "co",
    "las vegas": "nv", "henderson": "nv", "north las vegas": "nv",
    "nashville": "tn", "murfreesboro": "tn", "franklin": "tn",
    "sacramento": "ca", "riverside": "ca", "san bernardino": "ca",
    "minneapolis": "mn", "st. paul": "mn", "bloomington": "mn",
    "portland": "or", "beaverton": "or", "hillsboro": "or",
    "salt lake city": "ut", "west jordan": "ut", "provo": "ut",
    "indianapolis": "in", "carmel": "in", "fishers": "in",
    "columbia": "sc", "greenville": "sc", "charleston": "sc",
    "san diego": "ca", "los angeles": "ca",
}

# Fallback hardcoded markets (used if Singularity fails)
FALLBACK_MARKETS = [
    "phoenix-az", "dallas-tx", "houston-tx", "austin-tx", "san-antonio-tx",
    "atlanta-ga", "charlotte-nc", "raleigh-nc", "tampa-fl", "orlando-fl",
    "jacksonville-fl", "denver-co", "las-vegas-nv", "nashville-tn",
    "sacramento-ca", "riverside-ca", "minneapolis-mn", "portland-or",
    "salt-lake-city-ut", "tucson-az", "indianapolis-in",
]


def get_markets_from_singularity(min_listings: int = 5) -> List[str]:
    """
    Discover active markets from Singularity map data.
    Returns list of market slugs (e.g., 'phoenix-az').
    """
    import requests

    logger.info("Fetching markets from Singularity...")

    try:
        response = requests.get(
            "https://singularityresearchfund.com/api/opendoor/map-data",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        markets = []
        for item in data:
            city = item.get('city', '').lower().strip()
            listing_count = item.get('listing_count', 0)

            if listing_count < min_listings:
                continue

            # Find state for this city
            state = CITY_STATE_MAP.get(city)
            if not state:
                # Try partial match
                for known_city, known_state in CITY_STATE_MAP.items():
                    if known_city in city or city in known_city:
                        state = known_state
                        break

            if state:
                slug = f"{city.replace(' ', '-')}-{state}"
                if slug not in markets:
                    markets.append(slug)
                    logger.debug(f"  Found market: {slug} ({listing_count} listings)")

        logger.info(f"Discovered {len(markets)} markets from Singularity")
        return markets

    except Exception as e:
        logger.warning(f"Could not fetch Singularity markets: {e}")
        logger.info("Using fallback market list")
        return FALLBACK_MARKETS


async def scrape_market(page: Page, market_slug: str) -> List[Dict]:
    """Scrape all listings from a single Opendoor market."""
    # Try both URL formats - Opendoor sometimes redirects
    urls_to_try = [
        f"https://www.opendoor.com/homes/{market_slug}",
        f"https://www.opendoor.com/homes?market={market_slug}",
    ]
    listings = []

    try:
        logger.info(f"Scraping {market_slug}...")

        # Try first URL
        url = urls_to_try[0]
        response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)  # Wait for JS to load content

        # Get the actual URL after any redirects
        final_url = page.url
        logger.debug(f"  Final URL: {final_url}")

        # Try multiple selectors for property cards
        selectors = [
            'a[href*="/properties/"]',
            '[data-testid*="property"]',
            '[class*="PropertyCard"]',
            '[class*="listing"]',
        ]

        found_selector = None
        for selector in selectors:
            try:
                await page.wait_for_selector(selector, timeout=10000)
                found_selector = selector
                logger.debug(f"  Found content with selector: {selector}")
                break
            except Exception:
                continue

        if not found_selector:
            logger.warning(f"  No listings found with any selector in {market_slug}")
            # Try to get the page title for debugging
            title = await page.title()
            logger.debug(f"  Page title: {title}")

        # Get initial count before scrolling
        initial_count = await page.evaluate("""
            () => document.querySelectorAll('a[href*="/properties/"]').length
        """)
        logger.info(f"  Initial property links: {initial_count}")

        # Check for total count displayed on page
        total_text = await page.evaluate("""
            () => {
                const text = document.body.innerText;
                const match = text.match(/(\\d+)\\s+homes?\\s+(?:for sale|available)/i);
                return match ? match[1] : null;
            }
        """)
        expected_count = int(total_text) if total_text else None
        if expected_count:
            logger.info(f"  Page shows: {expected_count} homes")

        # Load all properties by clicking "Show more" repeatedly
        final_count = await load_all_properties(page, max_clicks=30)
        logger.info(f"  Loaded {final_count} unique properties")

        # Extract all listings via JavaScript
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
                    // Match price: $XXX,XXX format
                    const priceMatch = text.match(/\\$(\\d{1,3}(?:,\\d{3})*)/);
                    if (priceMatch) {
                        const bedMatch = text.match(/(\\d+)\\s*(?:bds?|beds?|br)/i);
                        const bathMatch = text.match(/(\\d+(?:\\.\\d+)?)\\s*(?:ba|baths?)/i);
                        const sqftMatch = text.match(/([\\d,]+)\\s*(?:sqft|sq\\s*ft)/i);

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

            // Dedupe by URL (strip query params)
            const seen = new Set();
            return results.filter(r => {
                const cleanUrl = r.url.split('?')[0];
                if (seen.has(cleanUrl)) return false;
                seen.add(cleanUrl);
                return true;
            });
        }
        """

        js_listings = await page.evaluate(js_code)
        logger.info(f"  Extracted {len(js_listings)} listings")

        # Warn if we didn't get expected count
        if expected_count and len(js_listings) < expected_count * 0.9:
            logger.warning(f"  Expected ~{expected_count} but only got {len(js_listings)}")

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


async def load_all_properties(page: Page, max_clicks: int = 20) -> int:
    """
    Load all properties by clicking 'Show more' button until no more available.
    Uses JavaScript click to handle hidden button state.
    Returns the count of unique property URLs found.
    """

    get_unique_count_js = """
    () => {
        const links = document.querySelectorAll('a[href*="/properties/"]');
        const urls = new Set();
        links.forEach(l => urls.add(l.getAttribute('href').split('?')[0]));
        return urls.size;
    }
    """

    prev_count = await page.evaluate(get_unique_count_js)
    logger.info(f"    Initial unique properties: {prev_count}")

    for click_num in range(max_clicks):
        # Scroll to absolute bottom to reveal Show more button
        for _ in range(5):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(500)

        # Use JavaScript to find and click the VISIBLE "Show more" button
        # (There are multiple buttons, we need the one that's actually visible)
        click_result = await page.evaluate("""
        () => {
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                const text = (btn.innerText || '').toLowerCase().trim();
                if (text === 'show more' || text === 'load more') {
                    const style = window.getComputedStyle(btn);
                    const rect = btn.getBoundingClientRect();
                    // Check if actually visible (not hidden by CSS)
                    if (style.visibility === 'visible' &&
                        style.display !== 'none' &&
                        rect.height > 0) {
                        btn.click();
                        return {clicked: true, position: rect.top};
                    }
                }
            }
            return {clicked: false};
        }
        """)

        if click_result.get('clicked'):
            logger.info(f"    Click {click_num + 1}: Clicked 'Show more' at position {click_result.get('position', 'unknown')}")
            await page.wait_for_timeout(3000)  # Wait for content to load

            # Check new count
            current_count = await page.evaluate(get_unique_count_js)
            logger.info(f"    Now have {current_count} unique properties")

            if current_count == prev_count:
                logger.info(f"    No new properties loaded, stopping")
                break

            prev_count = current_count
        else:
            logger.debug(f"    No clickable 'Show more' button found")
            break

    # Final scroll to top
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(500)

    final_count = await page.evaluate(get_unique_count_js)
    return final_count


async def auto_scroll(page: Page, max_scrolls: int = 10):
    """
    Simple scroll to trigger initial lazy loading.
    Main loading is done via load_all_properties clicking 'Show more'.
    """
    for i in range(max_scrolls):
        await page.evaluate("window.scrollBy(0, window.innerHeight)")
        await page.wait_for_timeout(500)

    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(300)
    return await page.evaluate("""
        () => document.querySelectorAll('a[href*="/properties/"]').length
    """)


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
            # Market should already be a slug like 'phoenix-az'
            # If it's just a city name, try to convert it
            slug = market
            if '-' not in market:
                city = market.lower().replace('_', ' ')
                state = CITY_STATE_MAP.get(city)
                if state:
                    slug = f"{city.replace(' ', '-')}-{state}"

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
        # User-specified markets (can be slugs or city names)
        markets = []
        for m in args.markets.split(','):
            m = m.strip().lower()
            # If it's already a slug (has hyphen), use as-is
            if '-' in m:
                markets.append(m)
            else:
                # Convert city name to slug
                state = CITY_STATE_MAP.get(m.replace('-', ' '))
                if state:
                    markets.append(f"{m.replace(' ', '-')}-{state}")
                else:
                    markets.append(m)  # Use as-is, will likely fail
    else:
        # Default: discover markets from Singularity (primary source: MLS data)
        markets = get_markets_from_singularity(min_listings=5)

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
