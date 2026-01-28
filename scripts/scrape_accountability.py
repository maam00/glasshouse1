#!/usr/bin/env python3
"""
Scrape Opendoor Accountability page for acquisition contract data and product updates.
https://accountable.opendoor.com/

This gives us:
1. The "inflow" side - homes Opendoor is BUYING (acquisition contracts)
2. Product updates - features and improvements shipped by the team
"""

import json
import re
import requests
from pathlib import Path
from datetime import datetime

def extract_product_updates(html: str) -> list:
    """Extract product updates/features shipped from the accountability page."""
    products = []

    # Categories we're looking for
    categories = {
        'ai': 'AI & Automation',
        'acquisition': 'Faster Acquisitions',
        'buyer': 'Buyer Experience',
        'seller': 'Seller Experience',
        'agent': 'Agent & Partner',
        'operations': 'Operations'
    }

    # Pattern 1: Look for product entries with dates in format MM/DD/YYYY or similar
    # Common patterns: "Product Name" (date) - description
    product_pattern = r'"([^"]{5,80})"\s*\((\d{1,2}/\d{1,2}/\d{4})\)'
    matches = re.findall(product_pattern, html)
    for name, date in matches:
        # Skip if it looks like a code/JSON artifact
        if any(x in name.lower() for x in ['month', 'actual', 'high', 'low', '{', '}']):
            continue
        products.append({
            'name': name.strip(),
            'date': date,
            'category': categorize_product(name)
        })

    # Pattern 2: Look for JSON-style product data that might be embedded
    # Format: {"title":"...", "date":"...", ...}
    json_pattern = r'\{"title"\s*:\s*"([^"]+)"\s*,\s*"date"\s*:\s*"([^"]+)"'
    json_matches = re.findall(json_pattern, html)
    for title, date in json_matches:
        if title not in [p['name'] for p in products]:
            products.append({
                'name': title.strip(),
                'date': date,
                'category': categorize_product(title)
            })

    # Pattern 3: Look for specific known product formats from the page
    # These are often in structured data or specific HTML patterns
    known_products = [
        ('End-to-End AI Home Scoping', '10/17/2025', 'AI & Automation'),
        ('Universal Underwriting Ensembler', '9/25/2025', 'AI & Automation'),
        ('Automating Title & Escrow', '10/15/2025', 'AI & Automation'),
        ('In-House Vision Model', '12/12/2025', 'AI & Automation'),
        ('Cash Plus Pricing Unblocked Nationwide', '9/30/2025', 'Faster Acquisitions'),
        ('True Seller Model v2', '9/29/2025', 'Faster Acquisitions'),
        ('New Homes Directory', '11/13/2025', 'Faster Acquisitions'),
        ('Buyer Peace of Mind Guarantee', '10/1/2025', 'Buyer Experience'),
        ('Revamped Mortgage Experience', '12/8/2025', 'Buyer Experience'),
        ('USDC Payment Acceptance', '11/6/2025', 'Buyer Experience'),
        ('100% Zip Coverage in Lower 48 States', '12/19/2025', 'Seller Experience'),
        ('Market Expansion', '11/5/2025', 'Seller Experience'),
        ('Opendoor Key App on iOS & Android', '9/15/2025', 'Agent & Partner'),
        ('Cash Plus for ALL Agents', '10/14/2025', 'Agent & Partner'),
    ]

    # Check if known products are mentioned in the HTML and add them
    for name, date, category in known_products:
        # Check if the product name appears in the HTML (case-insensitive partial match)
        name_pattern = re.escape(name.split()[0]) + r'[^<]*' + re.escape(name.split()[-1]) if len(name.split()) > 1 else re.escape(name)
        if re.search(name_pattern, html, re.IGNORECASE):
            if name not in [p['name'] for p in products]:
                products.append({
                    'name': name,
                    'date': date,
                    'category': category
                })

    # Remove duplicates and sort by date (newest first)
    seen = set()
    unique_products = []
    for p in products:
        if p['name'] not in seen:
            seen.add(p['name'])
            unique_products.append(p)

    # Sort by date (newest first)
    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, '%m/%d/%Y')
        except:
            return datetime.min

    unique_products.sort(key=lambda x: parse_date(x['date']), reverse=True)

    return unique_products

def categorize_product(name: str) -> str:
    """Categorize a product based on its name."""
    name_lower = name.lower()

    if any(x in name_lower for x in ['ai', 'model', 'vision', 'automat', 'ml', 'ensemble']):
        return 'AI & Automation'
    elif any(x in name_lower for x in ['buyer', 'mortgage', 'guarantee', 'usdc', 'payment']):
        return 'Buyer Experience'
    elif any(x in name_lower for x in ['seller', 'coverage', 'zip', 'market expansion']):
        return 'Seller Experience'
    elif any(x in name_lower for x in ['agent', 'partner', 'key app']):
        return 'Agent & Partner'
    elif any(x in name_lower for x in ['acquisition', 'pricing', 'underwriting', 'cash plus', 'true seller']):
        return 'Faster Acquisitions'
    elif any(x in name_lower for x in ['title', 'escrow', 'closing']):
        return 'Operations'
    else:
        return 'Other'

def scrape_accountability():
    """Scrape acquisition data from Opendoor's accountability page."""

    url = "https://accountable.opendoor.com/"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    print(f"Fetching {url}...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    html = response.text

    acquisition_data = {
        "scraped_at": datetime.now().isoformat(),
        "source": url,
        "weekly_contracts": [],
        "product_updates": [],
        "latest": {},
    }

    # Extract product updates
    print("Extracting product updates...")
    acquisition_data['product_updates'] = extract_product_updates(html)
    print(f"Found {len(acquisition_data['product_updates'])} product updates")

    # Try multiple extraction strategies
    weekly_matches = []

    # Strategy 1: Direct JSON format (unescaped)
    weekly_pattern = r'\{"month":"(\d{4}-\d{2}-\d{2})","actual":(\d+),"high":\d+,"low":\d+,"lastYear":(\d+)\}'
    weekly_matches = re.findall(weekly_pattern, html)

    # Strategy 2: Unicode escaped quotes (common in Next.js hydration)
    if not weekly_matches:
        weekly_pattern_unicode = r'\{\\u0022month\\u0022:\\u0022(\d{4}-\d{2}-\d{2})\\u0022,\\u0022actual\\u0022:(\d+)'
        weekly_matches = re.findall(weekly_pattern_unicode, html)
        # Add placeholder for last_year since we're only capturing 2 groups
        weekly_matches = [(m[0], m[1], '0') for m in weekly_matches]

    # Strategy 3: Look for the JSON array in script tags
    if not weekly_matches:
        # Find data in __NEXT_DATA__ or similar script blocks
        script_pattern = r'<script[^>]*>([^<]*"month"[^<]*)</script>'
        script_matches = re.findall(script_pattern, html, re.IGNORECASE)
        for script_content in script_matches:
            # Try to extract month/actual pairs from script content
            inner_pattern = r'"month"\s*:\s*"(\d{4}-\d{2}-\d{2})"\s*,\s*"actual"\s*:\s*(\d+)'
            inner_matches = re.findall(inner_pattern, script_content)
            weekly_matches.extend([(m[0], m[1], '0') for m in inner_matches])

    # Strategy 4: Look for escaped JSON format
    if not weekly_matches:
        escaped_pattern = r'\\?"month\\?"\\?:\\?"(\d{4}-\d{2}-\d{2})\\?"\\?,\\?"actual\\?"\\?:(\d+)'
        weekly_matches = re.findall(escaped_pattern, html)
        weekly_matches = [(m[0], m[1], '0') for m in weekly_matches]

    print(f"Found {len(weekly_matches)} data points")

    for match in weekly_matches:
        actual_val = int(match[1])
        # Skip projection data (actual=0) - only keep real data
        if actual_val > 0:
            acquisition_data['weekly_contracts'].append({
                'week': match[0],
                'actual': actual_val,
                'last_year': int(match[2])
            })

    # Sort by date
    acquisition_data['weekly_contracts'].sort(key=lambda x: x['week'])

    # Remove duplicates
    seen = set()
    unique = []
    for item in acquisition_data['weekly_contracts']:
        if item['week'] not in seen:
            seen.add(item['week'])
            unique.append(item)
    acquisition_data['weekly_contracts'] = unique

    # Get latest data
    if acquisition_data['weekly_contracts']:
        latest = acquisition_data['weekly_contracts'][-1]
        acquisition_data['latest']['week'] = latest['week']
        acquisition_data['latest']['contracts'] = latest['actual']
        acquisition_data['latest']['last_year'] = latest['last_year']

        # Calculate WoW change
        if len(acquisition_data['weekly_contracts']) >= 2:
            current = acquisition_data['weekly_contracts'][-1]['actual']
            previous = acquisition_data['weekly_contracts'][-2]['actual']
            if previous > 0:
                wow = ((current - previous) / previous) * 100
                acquisition_data['latest']['wow_change'] = round(wow, 1)

        # Calculate 4-week average
        last_4 = acquisition_data['weekly_contracts'][-4:] if len(acquisition_data['weekly_contracts']) >= 4 else acquisition_data['weekly_contracts']
        avg_4w = sum(w['actual'] for w in last_4) / len(last_4)
        acquisition_data['latest']['avg_4w'] = round(avg_4w, 1)

        # Calculate total Q1 contracts (from 2026-01-01)
        q1_contracts = [w for w in acquisition_data['weekly_contracts'] if w['week'] >= '2026-01-01']
        acquisition_data['latest']['q1_total'] = sum(w['actual'] for w in q1_contracts)
        acquisition_data['latest']['q1_weeks'] = len(q1_contracts)

    # Also grab the +X% from the page directly as a sanity check
    wow_match = re.search(r'color:#007f5f">\+?(-?\d+)%', html)
    if wow_match:
        acquisition_data['latest']['wow_display'] = int(wow_match.group(1))

    return acquisition_data


def main():
    output_dir = Path(__file__).parent.parent / "outputs"
    output_dir.mkdir(exist_ok=True)

    print("Scraping Opendoor Accountability page...")
    data = scrape_accountability()

    # Save raw data
    timestamp = datetime.now().strftime("%Y-%m-%d")
    output_file = output_dir / f"accountability_{timestamp}.json"

    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\nSaved to: {output_file}")

    # Print summary
    print("\n" + "=" * 50)
    print("  ACQUISITION DATA SUMMARY")
    print("=" * 50)

    latest = data.get('latest', {})
    print(f"  Latest Week: {latest.get('week', 'N/A')}")
    print(f"  Contracts: {latest.get('contracts', 'N/A')}")
    print(f"  WoW Change: {latest.get('wow_change', 'N/A')}%")
    print(f"  4-Week Avg: {latest.get('avg_4w', 'N/A')}")
    print(f"  Q1 Total: {latest.get('q1_total', 'N/A')} ({latest.get('q1_weeks', 0)} weeks)")
    print(f"  Data Points: {len(data.get('weekly_contracts', []))}")
    print("=" * 50)

    # Print product updates summary
    products = data.get('product_updates', [])
    if products:
        print("\n" + "=" * 50)
        print("  PRODUCT UPDATES")
        print("=" * 50)

        # Group by category
        by_category = {}
        for p in products:
            cat = p.get('category', 'Other')
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(p)

        for category, items in by_category.items():
            print(f"\n  {category} ({len(items)}):")
            for item in items[:3]:  # Show top 3 per category
                print(f"    - {item['name']} ({item['date']})")
            if len(items) > 3:
                print(f"    ... and {len(items) - 3} more")

        print("=" * 50)

    return data


if __name__ == "__main__":
    main()
