#!/usr/bin/env python3
"""
Opendoor Direct Scraper
=======================
Scrapes opendoor.com directly to get all listings with their status.

Tracks:
- For Sale listings
- Pending / Under Contract listings
- Days on market
- Price and location

Usage:
    python scripts/scrape_opendoor.py
    python scripts/scrape_opendoor.py --markets "phoenix,dallas,atlanta"
    python scripts/scrape_opendoor.py --all-markets
"""

import sys
import json
import argparse
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import re

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from playwright.async_api import async_playwright, Page, Browser
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    logger.warning("playwright not installed. Run: pip install playwright && playwright install chromium")

import pandas as pd

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import KAZ_ERA_START

# Opendoor market slugs
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

# NOTE: KAZ_ERA_START is imported from src.config - single source of truth


async def scrape_market(page: Page, market_slug: str) -> List[Dict]:
    """Scrape all listings from a single Opendoor market."""
    url = f"https://www.opendoor.com/homes/{market_slug}"
    listings = []

    try:
        logger.info(f"Loading {url}...")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Wait for listings to appear
        await page.wait_for_timeout(2000)
        try:
            await page.wait_for_selector('a[href*="/homes/"]', timeout=30000)
        except Exception:
            logger.warning(f"  Timeout waiting for listings selector, continuing anyway...")
        await page.wait_for_timeout(2000)

        # Scroll to load all listings (lazy loading)
        await auto_scroll(page)

        # Extract listing data
        # Opendoor uses various selectors - try multiple approaches
        # Extract listings using JavaScript evaluation
        js_code = """
        () => {
            const results = [];

            // Find all links to property pages
            const links = document.querySelectorAll('a[href*="/properties/"]');

            links.forEach(link => {
                const href = link.getAttribute('href');
                if (!href) return;

                // Get the card container (parent elements)
                let card = link;
                for (let i = 0; i < 10 && card; i++) {
                    const text = card.innerText || '';
                    // Look for price pattern ($XXX,XXX)
                    const priceMatch = text.match(/\\$(\\d{1,3}(?:,\\d{3})+)/);
                    if (priceMatch) {
                        const bedMatch = text.match(/(\\d+)\\s*bds?/i);
                        const bathMatch = text.match(/(\\d+(?:\\.\\d+)?)\\s*ba/i);
                        const sqftMatch = text.match(/([\\d,]+)\\s*sqft/i);

                        // Extract address from URL
                        let address = null;
                        const addrMatch = href.match(/\\/properties\\/([^/?]+)/);
                        if (addrMatch) {
                            address = addrMatch[1].replace(/-/g, ' ');
                        }

                        // Check for status
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
        logger.info(f"  Found {len(js_listings)} listings via JS extraction")

        for item in js_listings:
            item['market'] = market_slug
            item['scraped_at'] = datetime.now().isoformat()
            listings.append(item)

        # If JS extraction failed, try page data extraction
        if not listings:
            listings = await extract_from_page_data(page, market_slug)

        logger.info(f"  Extracted {len(listings)} listings from {market_slug}")

    except Exception as e:
        logger.error(f"Error scraping {market_slug}: {e}")

    return listings


async def auto_scroll(page: Page, max_scrolls: int = 20):
    """Scroll page to trigger lazy loading."""
    prev_height = 0
    for i in range(max_scrolls):
        await page.evaluate("window.scrollBy(0, window.innerHeight)")
        await page.wait_for_timeout(800)

        # Check if we've reached the bottom and content stopped loading
        current_height = await page.evaluate("document.body.scrollHeight")
        at_bottom = await page.evaluate(
            "window.innerHeight + window.scrollY >= document.body.scrollHeight - 100"
        )
        if at_bottom and current_height == prev_height:
            break
        prev_height = current_height

    # Scroll back to top
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(500)


async def extract_listing_data(element) -> Optional[Dict]:
    """Extract data from a listing card element."""
    listing = {}

    try:
        # Get the link/URL
        href = await element.get_attribute('href')
        if href:
            listing['url'] = f"https://www.opendoor.com{href}" if href.startswith('/') else href

            # Extract address from URL
            # URLs look like: /homes/phoenix-az/address/123-main-st-phoenix-az-85001
            if '/address/' in href:
                addr_part = href.split('/address/')[-1]
                # Convert URL slug to address
                listing['address_slug'] = addr_part

        # Try to get text content for price, beds, baths, sqft
        text = await element.inner_text()
        if text:
            # Extract price
            price_match = re.search(r'\$[\d,]+', text)
            if price_match:
                listing['price'] = int(price_match.group().replace('$', '').replace(',', ''))

            # Extract beds/baths/sqft
            beds_match = re.search(r'(\d+)\s*(?:bed|bd)', text, re.IGNORECASE)
            if beds_match:
                listing['beds'] = int(beds_match.group(1))

            baths_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:bath|ba)', text, re.IGNORECASE)
            if baths_match:
                listing['baths'] = float(baths_match.group(1))

            sqft_match = re.search(r'([\d,]+)\s*(?:sq\s*ft|sqft)', text, re.IGNORECASE)
            if sqft_match:
                listing['sqft'] = int(sqft_match.group(1).replace(',', ''))

            # Check for status indicators
            text_lower = text.lower()
            if 'pending' in text_lower:
                listing['status'] = 'PENDING'
            elif 'under contract' in text_lower:
                listing['status'] = 'UNDER_CONTRACT'
            elif 'sold' in text_lower:
                listing['status'] = 'SOLD'
            else:
                listing['status'] = 'FOR_SALE'

        # Try to get address from child elements
        address_el = await element.query_selector('[class*="address"], [class*="Address"], [data-testid*="address"]')
        if address_el:
            listing['address'] = await address_el.inner_text()

    except Exception as e:
        logger.debug(f"Error in extract_listing_data: {e}")

    return listing if listing.get('url') or listing.get('address') else None


async def extract_from_page_data(page: Page, market_slug: str) -> List[Dict]:
    """Try to extract listing data from embedded JSON or script tags."""
    listings = []

    try:
        # Look for __NEXT_DATA__ (Next.js apps)
        next_data = await page.evaluate("""
            () => {
                const el = document.querySelector('#__NEXT_DATA__');
                return el ? el.textContent : null;
            }
        """)

        if next_data:
            data = json.loads(next_data)
            # Navigate the JSON structure to find listings
            # This depends on Opendoor's specific structure
            props = data.get('props', {}).get('pageProps', {})
            if 'listings' in props:
                for item in props['listings']:
                    listings.append(parse_listing_json(item, market_slug))
            elif 'homes' in props:
                for item in props['homes']:
                    listings.append(parse_listing_json(item, market_slug))

        # Also try to find any JSON in script tags
        scripts = await page.query_selector_all('script[type="application/json"], script[type="application/ld+json"]')
        for script in scripts:
            try:
                content = await script.inner_text()
                data = json.loads(content)
                if isinstance(data, list):
                    for item in data:
                        if 'address' in str(item).lower():
                            parsed = parse_listing_json(item, market_slug)
                            if parsed:
                                listings.append(parsed)
            except:
                continue

    except Exception as e:
        logger.debug(f"Error extracting page data: {e}")

    return listings


def parse_listing_json(data: Dict, market_slug: str) -> Optional[Dict]:
    """Parse a listing from JSON data."""
    if not isinstance(data, dict):
        return None

    listing = {
        'market': market_slug,
        'scraped_at': datetime.now().isoformat(),
    }

    # Common field mappings
    field_maps = {
        'address': ['address', 'streetAddress', 'street_address', 'full_address'],
        'price': ['price', 'listPrice', 'list_price', 'currentPrice'],
        'beds': ['beds', 'bedrooms', 'bedroom_count'],
        'baths': ['baths', 'bathrooms', 'bathroom_count'],
        'sqft': ['sqft', 'squareFeet', 'square_feet', 'livingArea'],
        'status': ['status', 'listingStatus', 'listing_status'],
        'url': ['url', 'detailUrl', 'detail_url', 'permalink'],
        'days_on_market': ['daysOnMarket', 'days_on_market', 'dom'],
    }

    for field, keys in field_maps.items():
        for key in keys:
            if key in data:
                listing[field] = data[key]
                break

    # Normalize status
    if 'status' in listing:
        status = str(listing['status']).upper()
        if 'PENDING' in status:
            listing['status'] = 'PENDING'
        elif 'CONTRACT' in status:
            listing['status'] = 'UNDER_CONTRACT'
        elif 'SOLD' in status:
            listing['status'] = 'SOLD'
        else:
            listing['status'] = 'FOR_SALE'

    return listing if listing.get('address') or listing.get('url') else None


async def scrape_all_markets(markets: List[str], headless: bool = True) -> pd.DataFrame:
    """Scrape listings from multiple markets."""
    all_listings = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = await context.new_page()

        for market in markets:
            slug = OPENDOOR_MARKETS.get(market.lower().replace(' ', '_'), market)
            listings = await scrape_market(page, slug)
            all_listings.extend(listings)

            # Brief pause between markets
            await page.wait_for_timeout(2000)

        await browser.close()

    if not all_listings:
        return pd.DataFrame()

    df = pd.DataFrame(all_listings)
    return df


def classify_listing(row) -> Dict:
    """Add cohort and era classification to a listing."""
    result = {}

    # Days on market -> cohort
    dom = row.get('days_on_market', 0) or 0
    if dom < 90:
        result['cohort'] = 'new'
    elif dom < 180:
        result['cohort'] = 'mid'
    elif dom < 365:
        result['cohort'] = 'old'
    else:
        result['cohort'] = 'toxic'

    return result


def calculate_pending_metrics(df: pd.DataFrame) -> Dict:
    """Calculate pending funnel metrics."""
    if df.empty:
        return {}

    total = len(df)

    # Status breakdown
    status_counts = df['status'].value_counts().to_dict() if 'status' in df.columns else {}

    pending_count = status_counts.get('PENDING', 0) + status_counts.get('UNDER_CONTRACT', 0)
    for_sale_count = status_counts.get('FOR_SALE', 0)

    metrics = {
        'total_listings': total,
        'for_sale': for_sale_count,
        'pending': pending_count,
        'pending_pct': round(pending_count / total * 100, 1) if total > 0 else 0,
        'status_breakdown': status_counts,
        'scraped_at': datetime.now().isoformat(),
    }

    # Cohort breakdown of pending
    if 'cohort' in df.columns:
        pending_df = df[df['status'].isin(['PENDING', 'UNDER_CONTRACT'])]
        if not pending_df.empty:
            cohort_counts = pending_df['cohort'].value_counts().to_dict()
            metrics['pending_by_cohort'] = cohort_counts
            metrics['toxic_pending'] = cohort_counts.get('toxic', 0)

    # Market breakdown
    if 'market' in df.columns:
        market_counts = df['market'].value_counts().to_dict()
        metrics['by_market'] = market_counts

        # Pending by market
        if pending_count > 0:
            pending_df = df[df['status'].isin(['PENDING', 'UNDER_CONTRACT'])]
            metrics['pending_by_market'] = pending_df['market'].value_counts().to_dict()

    # Price stats
    if 'price' in df.columns:
        prices = pd.to_numeric(df['price'], errors='coerce')
        metrics['avg_price'] = round(prices.mean()) if prices.notna().any() else 0
        metrics['total_value'] = round(prices.sum()) if prices.notna().any() else 0

    return metrics


async def main_async(args):
    """Async main function."""
    project_root = Path(__file__).parent.parent
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine markets
    if args.markets:
        markets = [m.strip() for m in args.markets.split(',')]
    elif args.all_markets:
        markets = list(OPENDOOR_MARKETS.keys())
    else:
        # Default: top 5 markets
        markets = ['phoenix', 'dallas', 'atlanta', 'houston', 'charlotte']

    print(f"\n{'='*60}")
    print("  OPENDOOR DIRECT SCRAPER")
    print(f"{'='*60}")
    print(f"  Markets: {len(markets)}")
    print(f"  Headless: {not args.visible}")
    print(f"{'='*60}\n")

    # Scrape
    df = await scrape_all_markets(markets, headless=not args.visible)

    if df.empty:
        print("No listings found")
        return 1

    # Add classifications
    if 'days_on_market' in df.columns:
        classifications = df.apply(classify_listing, axis=1, result_type='expand')
        df = pd.concat([df, classifications], axis=1)

    # Calculate metrics
    metrics = calculate_pending_metrics(df)

    # Print summary
    print(f"\n{'='*60}")
    print("  RESULTS")
    print(f"{'='*60}")
    print(f"  Total Listings: {metrics.get('total_listings', 0)}")
    print(f"  For Sale:       {metrics.get('for_sale', 0)}")
    print(f"  Pending:        {metrics.get('pending', 0)} ({metrics.get('pending_pct', 0)}%)")

    status = metrics.get('status_breakdown', {})
    if status:
        print(f"\n  Status Breakdown:")
        for s, count in status.items():
            print(f"    {s}: {count}")

    pending_cohorts = metrics.get('pending_by_cohort', {})
    if pending_cohorts:
        print(f"\n  Pending by Cohort:")
        for cohort, count in pending_cohorts.items():
            marker = " ← TOXIC CLEARING" if cohort == 'toxic' else ""
            print(f"    {cohort}: {count}{marker}")

    print(f"{'='*60}\n")

    # Save outputs
    date_str = datetime.now().strftime('%Y-%m-%d')

    # CSV of all listings
    csv_file = output_dir / f"opendoor_listings_{date_str}.csv"
    df.to_csv(csv_file, index=False)
    print(f"Saved: {csv_file}")

    # JSON metrics
    json_file = output_dir / f"opendoor_pending_{date_str}.json"
    output_data = {
        'scraped_at': datetime.now().isoformat(),
        'markets': markets,
        'metrics': metrics,
        'listings': df.to_dict('records'),
    }
    with open(json_file, 'w') as f:
        json.dump(output_data, f, indent=2, default=str)
    print(f"Saved: {json_file}")

    return 0


def main():
    parser = argparse.ArgumentParser(description='Scrape Opendoor listings directly')
    parser.add_argument('--markets', type=str, help='Comma-separated list of markets')
    parser.add_argument('--all-markets', action='store_true', help='Scrape all Opendoor markets')
    parser.add_argument('--output-dir', type=str, help='Output directory')
    parser.add_argument('--visible', action='store_true', help='Run browser in visible mode (not headless)')
    args = parser.parse_args()

    if not HAS_PLAYWRIGHT:
        print("ERROR: playwright not installed")
        print("Run: pip install playwright && playwright install chromium")
        return 1

    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
