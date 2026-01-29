#!/usr/bin/env python3
"""
Update dashboard with sales funnel data.

Adds sales velocity and funnel metrics to the existing dashboard.
"""

import sys
import json
from pathlib import Path
from datetime import datetime

import pandas as pd

def find_latest_file(directory: Path, pattern: str) -> Path:
    """Find the most recent file matching pattern."""
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def main():
    project_root = Path(__file__).parent.parent
    output_dir = project_root / "outputs"

    print("=" * 60)
    print("  UPDATING DASHBOARD WITH SALES FUNNEL DATA")
    print("=" * 60)

    # Load Opendoor direct inventory (active listings)
    opendoor_file = find_latest_file(output_dir, "opendoor_listings_*.csv")
    opendoor_inventory = None
    if opendoor_file:
        opendoor_inventory = pd.read_csv(opendoor_file)
        print(f"\nLoaded {len(opendoor_inventory)} active Opendoor listings")

    # Load pending/sold listings (from MLS scraper)
    pending_csv = find_latest_file(output_dir, "pending_listings_*.csv")
    pending_listings = None
    if pending_csv:
        pending_listings = pd.read_csv(pending_csv)
        print(f"Loaded {len(pending_listings)} pending/sold listings")

    # Load pending JSON for additional metrics
    pending_json = find_latest_file(output_dir, "pending_*.json")
    pending_metrics = {}
    scraped_at = datetime.now().isoformat()
    if pending_json:
        with open(pending_json) as f:
            pending_data = json.load(f)
            pending_metrics = pending_data.get('metrics', {})
            scraped_at = pending_data.get('scraped_at', scraped_at)

    # Build sales funnel data
    sales_funnel = {
        'scraped_at': scraped_at,
        'data_sources': {
            'active_inventory': str(opendoor_file) if opendoor_file else None,
            'sales_data': str(pending_csv) if pending_csv else None,
        }
    }

    # Active inventory stats
    if opendoor_inventory is not None and len(opendoor_inventory) > 0:
        by_market = opendoor_inventory['market'].value_counts().to_dict() if 'market' in opendoor_inventory.columns else {}

        sales_funnel['active_inventory'] = {
            'total': len(opendoor_inventory),
            'total_value': int(opendoor_inventory['price'].sum()) if 'price' in opendoor_inventory.columns else 0,
            'avg_price': int(opendoor_inventory['price'].mean()) if 'price' in opendoor_inventory.columns else 0,
            'by_market': dict(list(by_market.items())[:10]),  # Top 10 markets
            'markets_count': len(by_market),
        }
        print(f"  Active inventory value: ${sales_funnel['active_inventory']['total_value']/1e6:.1f}M")

    # Sales stats
    if pending_listings is not None and len(pending_listings) > 0:
        by_market = pending_listings['search_market'].value_counts().to_dict() if 'search_market' in pending_listings.columns else {}
        by_agent = pending_listings['agent_name'].value_counts().to_dict() if 'agent_name' in pending_listings.columns else {}

        sales_funnel['recent_sales'] = {
            'total': len(pending_listings),
            'total_value': int(pending_listings['list_price'].sum()) if 'list_price' in pending_listings.columns else 0,
            'avg_price': int(pending_listings['list_price'].mean()) if 'list_price' in pending_listings.columns else 0,
            'by_market': by_market,
            'by_agent': by_agent,
        }
        print(f"  Recent sales value: ${sales_funnel['recent_sales']['total_value']/1e6:.1f}M")

        # Calculate days-to-pending
        if 'list_date' in pending_listings.columns and 'pending_date' in pending_listings.columns:
            pending_listings['list_date_dt'] = pd.to_datetime(pending_listings['list_date'], errors='coerce')
            pending_listings['pending_date_dt'] = pd.to_datetime(pending_listings['pending_date'], errors='coerce')
            pending_listings['days_to_pending'] = (pending_listings['pending_date_dt'] - pending_listings['list_date_dt']).dt.days

            valid_days = pending_listings['days_to_pending'].dropna()
            if len(valid_days) > 0:
                sales_funnel['recent_sales']['days_to_pending'] = {
                    'avg': int(valid_days.mean()),
                    'min': int(valid_days.min()),
                    'max': int(valid_days.max()),
                    'median': int(valid_days.median()),
                }

                # Speed cohorts
                def categorize_speed(days):
                    if pd.isna(days):
                        return 'unknown'
                    if days < 30:
                        return 'fast_under_30d'
                    elif days < 90:
                        return 'normal_30_90d'
                    elif days < 180:
                        return 'slow_90_180d'
                    else:
                        return 'stale_over_180d'

                pending_listings['speed_cohort'] = pending_listings['days_to_pending'].apply(categorize_speed)
                sales_funnel['recent_sales']['by_speed'] = pending_listings['speed_cohort'].value_counts().to_dict()

        # Top 5 sales by price
        if 'list_price' in pending_listings.columns:
            top_sales = pending_listings.nlargest(5, 'list_price')[
                ['scraped_address', 'city', 'state', 'list_price']
            ].to_dict('records')
            sales_funnel['recent_sales']['top_sales'] = top_sales

    # Calculate turnover metrics
    if sales_funnel.get('active_inventory') and sales_funnel.get('recent_sales'):
        active = sales_funnel['active_inventory']['total']
        sold = sales_funnel['recent_sales']['total']

        sales_funnel['turnover'] = {
            'sold_90d': sold,
            'active_inventory': active,
            'turnover_rate_90d_pct': round(sold / active * 100, 1) if active > 0 else 0,
            'monthly_velocity': round(sold / 3, 1),  # 90 days = ~3 months
            'weekly_velocity': round(sold / 13, 1),  # 90 days = ~13 weeks
            'months_of_inventory': round(active / (sold / 3), 1) if sold > 0 else 0,
        }

    # Market velocity comparison
    if sales_funnel.get('active_inventory') and sales_funnel.get('recent_sales'):
        active_by_market = sales_funnel['active_inventory'].get('by_market', {})
        sales_by_market = sales_funnel['recent_sales'].get('by_market', {})

        # Normalize market names for comparison
        def normalize_market(m):
            return m.lower().replace('-', ' ').replace(',', '').replace('  ', ' ').strip()

        market_velocity = []
        for market, active_count in active_by_market.items():
            # Find matching sales market
            norm_market = normalize_market(market)
            sales_count = 0
            for sm, sc in sales_by_market.items():
                if normalize_market(sm).startswith(norm_market.split()[0]):
                    sales_count = sc
                    break

            if active_count > 0:
                velocity = round(sales_count / active_count * 100, 1)
                market_velocity.append({
                    'market': market,
                    'active': active_count,
                    'sold_90d': sales_count,
                    'turnover_pct': velocity,
                })

        # Sort by turnover rate descending
        market_velocity.sort(key=lambda x: x['turnover_pct'], reverse=True)
        sales_funnel['market_velocity'] = market_velocity[:15]  # Top 15

    # Include funnel metrics from pending scraper
    if pending_metrics:
        sales_funnel['funnel_metrics'] = {
            'total_identified': pending_metrics.get('total_pending', 0),
            'kaz_era': pending_metrics.get('kaz_era_pending', 0),
            'legacy': pending_metrics.get('legacy_pending', 0),
            'toxic_count': pending_metrics.get('toxic_pending_count', 0),
            'toxic_pct': pending_metrics.get('toxic_pending_pct', 0),
            'by_state': pending_metrics.get('by_state', {}),
            'changes': pending_metrics.get('funnel_changes', {}),
        }

    # Load existing dashboard and update it
    dashboard_file = output_dir / "dashboard_data.json"
    unified_file = output_dir / "unified_dashboard_data.json"

    # Update both files if they exist
    for target_file in [dashboard_file, unified_file]:
        if target_file.exists():
            try:
                with open(target_file) as f:
                    dashboard = json.load(f)

                dashboard['sales_funnel'] = sales_funnel
                dashboard['sales_funnel_updated_at'] = datetime.now().isoformat()

                with open(target_file, 'w') as f:
                    json.dump(dashboard, f, indent=2, default=str)

                print(f"\nUpdated: {target_file}")
            except Exception as e:
                print(f"Warning: Could not update {target_file}: {e}")

    # Also save standalone sales funnel JSON
    funnel_file = output_dir / f"sales_funnel_{datetime.now().strftime('%Y-%m-%d')}.json"
    with open(funnel_file, 'w') as f:
        json.dump(sales_funnel, f, indent=2, default=str)
    print(f"Saved: {funnel_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("  SALES FUNNEL SUMMARY")
    print("=" * 60)

    if 'active_inventory' in sales_funnel:
        ai = sales_funnel['active_inventory']
        print(f"\n  Active Inventory:")
        print(f"    Total Listings: {ai['total']}")
        print(f"    Total Value: ${ai['total_value']/1e6:.1f}M")
        print(f"    Avg Price: ${ai['avg_price']:,}")

    if 'recent_sales' in sales_funnel:
        rs = sales_funnel['recent_sales']
        print(f"\n  Recent Sales (90 days):")
        print(f"    Total Sold: {rs['total']}")
        print(f"    Total Value: ${rs['total_value']/1e6:.1f}M")
        print(f"    Avg Price: ${rs['avg_price']:,}")

        if 'days_to_pending' in rs:
            dtp = rs['days_to_pending']
            print(f"    Days to Pending: {dtp['avg']} avg ({dtp['min']}-{dtp['max']} range)")

        if 'by_speed' in rs:
            print(f"\n  Sales by Speed:")
            for speed, count in rs['by_speed'].items():
                print(f"    {speed}: {count}")

    if 'turnover' in sales_funnel:
        t = sales_funnel['turnover']
        print(f"\n  Turnover Metrics:")
        print(f"    90-Day Turnover: {t['turnover_rate_90d_pct']}%")
        print(f"    Monthly Velocity: {t['monthly_velocity']} sales/month")
        print(f"    Weekly Velocity: {t['weekly_velocity']} sales/week")
        print(f"    Months of Inventory: {t['months_of_inventory']}")

    if 'market_velocity' in sales_funnel:
        print(f"\n  Top Markets by Velocity:")
        for mv in sales_funnel['market_velocity'][:5]:
            print(f"    {mv['market']}: {mv['sold_90d']} sold / {mv['active']} active = {mv['turnover_pct']}%")

    if 'recent_sales' in sales_funnel and 'by_agent' in sales_funnel['recent_sales']:
        print(f"\n  Top Agents:")
        agents = sales_funnel['recent_sales']['by_agent']
        for agent, count in list(agents.items())[:5]:
            print(f"    {agent}: {count} sales")

    print("\n" + "=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
