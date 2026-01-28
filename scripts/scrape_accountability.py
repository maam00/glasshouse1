#!/usr/bin/env python3
"""
Scrape Opendoor Accountability page for acquisition contract data.
https://accountable.opendoor.com/

This gives us the "inflow" side - homes Opendoor is BUYING.
"""

import json
import re
import requests
from pathlib import Path
from datetime import datetime

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
        "latest": {},
    }

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

    return data


if __name__ == "__main__":
    main()
