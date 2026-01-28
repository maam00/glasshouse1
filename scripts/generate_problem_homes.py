#!/usr/bin/env python3
"""
Generate Problem Homes Deep Dive data.

Analyzes inventory by market to identify problem areas and generates
links to Opendoor's market pages for further investigation.
"""

import json
from pathlib import Path
from datetime import datetime

# State to Opendoor market slug mapping
STATE_TO_MARKETS = {
    'AZ': [('phoenix', 'Phoenix'), ('tucson', 'Tucson')],
    'CA': [('los-angeles', 'Los Angeles'), ('san-diego', 'San Diego'), ('sacramento', 'Sacramento'), ('riverside', 'Riverside')],
    'CO': [('denver', 'Denver'), ('colorado-springs', 'Colorado Springs')],
    'FL': [('tampa', 'Tampa'), ('orlando', 'Orlando'), ('jacksonville', 'Jacksonville'), ('miami', 'Miami')],
    'GA': [('atlanta', 'Atlanta')],
    'IN': [('indianapolis', 'Indianapolis')],
    'KS': [('kansas-city', 'Kansas City')],
    'MD': [('baltimore', 'Baltimore')],
    'MI': [('detroit', 'Detroit')],
    'MN': [('minneapolis', 'Minneapolis')],
    'MO': [('st-louis', 'St. Louis'), ('kansas-city', 'Kansas City')],
    'NC': [('charlotte', 'Charlotte'), ('raleigh', 'Raleigh')],
    'NV': [('las-vegas', 'Las Vegas')],
    'NJ': [('new-jersey', 'New Jersey')],
    'OH': [('columbus', 'Columbus'), ('cincinnati', 'Cincinnati')],
    'OK': [('oklahoma-city', 'Oklahoma City')],
    'OR': [('portland', 'Portland')],
    'SC': [('columbia', 'Columbia'), ('charleston', 'Charleston')],
    'TN': [('nashville', 'Nashville')],
    'TX': [('dallas', 'Dallas'), ('houston', 'Houston'), ('san-antonio', 'San Antonio'), ('austin', 'Austin')],
    'UT': [('salt-lake-city', 'Salt Lake City')],
    'VA': [('northern-virginia', 'Northern Virginia')],
    'WA': [('seattle', 'Seattle')],
}


def calculate_risk_score(inventory, toxic, avg_dom, clearance_pct=33.9):
    """
    Calculate a risk score for a market (0-100).

    Factors:
    - Toxic concentration (toxic/inventory ratio)
    - Average days on market
    - Implied clearance difficulty
    """
    if inventory == 0:
        return 0

    toxic_ratio = (toxic / inventory) * 100
    dom_factor = min(avg_dom / 365, 1.0) * 100  # Normalized to 365 days

    # Weight: 50% toxic ratio, 30% DOM, 20% base difficulty
    risk = (toxic_ratio * 0.5) + (dom_factor * 0.3) + 20

    return min(round(risk, 1), 100)


def get_risk_level(score):
    """Categorize risk score into levels."""
    if score >= 60:
        return 'critical'
    elif score >= 40:
        return 'high'
    elif score >= 25:
        return 'moderate'
    else:
        return 'low'


def generate_problem_homes_data():
    """Generate problem homes breakdown by market."""

    output_dir = Path(__file__).parent.parent / "outputs"

    # Load intelligence data
    today = datetime.now().strftime("%Y-%m-%d")
    intel_file = output_dir / f"intelligence_{today}.json"

    if not intel_file.exists():
        # Try yesterday
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        intel_file = output_dir / f"intelligence_{yesterday}.json"

    if not intel_file.exists():
        print("No intelligence file found")
        return None

    with open(intel_file) as f:
        intel = json.load(f)

    operational = intel.get('operational', {})
    inventory = operational.get('inventory', {})
    markets = intel.get('advanced', {}).get('markets', [])

    # Overall stats
    total_inventory = inventory.get('total', 0)
    total_toxic = inventory.get('toxic_count', 0)
    total_very_stale = inventory.get('very_stale_count', 0)
    total_stale = inventory.get('stale_count', 0)
    avg_dom = inventory.get('avg_dom', 0)
    total_unrealized_pnl = inventory.get('total_unrealized_pnl', 0)

    # Build market breakdown
    market_data = []

    for market in markets:
        state = market.get('state', '')
        inv_count = market.get('inventory_count', 0)
        toxic_count = market.get('toxic_count', 0)
        market_avg_dom = market.get('avg_dom', 0)
        concentration = market.get('concentration_pct', 0)

        if inv_count == 0:
            continue

        # Calculate toxic percentage
        toxic_pct = round((toxic_count / inv_count) * 100, 1) if inv_count > 0 else 0

        # Calculate risk score
        risk_score = calculate_risk_score(inv_count, toxic_count, market_avg_dom)
        risk_level = get_risk_level(risk_score)

        # Get Opendoor market links
        market_links = STATE_TO_MARKETS.get(state, [(state.lower(), state)])
        links = [
            {
                'slug': slug,
                'name': name,
                'url': f"https://www.opendoor.com/homes/{slug}"
            }
            for slug, name in market_links
        ]

        market_data.append({
            'state': state,
            'inventory': inv_count,
            'toxic_count': toxic_count,
            'toxic_pct': toxic_pct,
            'avg_dom': market_avg_dom,
            'concentration_pct': concentration,
            'risk_score': risk_score,
            'risk_level': risk_level,
            'opendoor_links': links
        })

    # Sort by risk score (highest first)
    market_data.sort(key=lambda x: x['risk_score'], reverse=True)

    # Identify top problem markets
    critical_markets = [m for m in market_data if m['risk_level'] == 'critical']
    high_risk_markets = [m for m in market_data if m['risk_level'] == 'high']

    # Generate summary
    problem_homes_data = {
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'total_inventory': total_inventory,
            'total_toxic': total_toxic,
            'total_very_stale': total_very_stale,
            'total_stale': total_stale,
            'toxic_pct': round((total_toxic / total_inventory) * 100, 1) if total_inventory > 0 else 0,
            'avg_dom': avg_dom,
            'total_unrealized_pnl': total_unrealized_pnl,
            'unrealized_pnl_millions': round(total_unrealized_pnl / 1_000_000, 1),
            'critical_markets_count': len(critical_markets),
            'high_risk_markets_count': len(high_risk_markets),
        },
        'cohorts': {
            'fresh': {'label': 'Fresh (<30d)', 'count': inventory.get('fresh_count', 0)},
            'normal': {'label': 'Normal (30-90d)', 'count': inventory.get('normal_count', 0)},
            'stale': {'label': 'Stale (90-180d)', 'count': total_stale},
            'very_stale': {'label': 'Very Stale (180-365d)', 'count': total_very_stale},
            'toxic': {'label': 'Toxic (>365d)', 'count': total_toxic},
        },
        'markets': market_data,
        'top_problem_markets': market_data[:5],  # Top 5 by risk
    }

    # Save output
    output_file = output_dir / f"problem_homes_{today}.json"
    with open(output_file, 'w') as f:
        json.dump(problem_homes_data, f, indent=2)

    print(f"Generated: {output_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("  PROBLEM HOMES SUMMARY")
    print("=" * 60)
    print(f"  Total Inventory: {total_inventory}")
    print(f"  Toxic (>365d): {total_toxic} ({problem_homes_data['summary']['toxic_pct']}%)")
    print(f"  Very Stale (180-365d): {total_very_stale}")
    print(f"  Unrealized P&L: ${total_unrealized_pnl / 1_000_000:.1f}M")
    print(f"  Avg Days on Market: {avg_dom}")
    print("=" * 60)

    print("\n  TOP PROBLEM MARKETS:")
    for m in market_data[:5]:
        links_str = ', '.join([l['name'] for l in m['opendoor_links'][:2]])
        print(f"    {m['state']}: {m['inventory']} homes, {m['toxic_count']} toxic ({m['toxic_pct']}%), "
              f"Risk: {m['risk_score']} ({m['risk_level'].upper()})")
        print(f"         -> {links_str}")

    print("=" * 60)

    return problem_homes_data


if __name__ == "__main__":
    generate_problem_homes_data()
