#!/usr/bin/env python3
"""
Generate dashboard data from unified dataset.

Creates a JSON file optimized for the Glass House dashboard,
using the merged Parcl + Singularity data.

Usage:
    python scripts/generate_unified_dashboard.py
"""

import sys
import json
import os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# Load .env file
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

# Add parent to path for local imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Claude API for narrative generation
try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

# Unit Economics and Market P&L
try:
    from src.metrics.unit_economics import UnitEconomicsCalculator
    from src.metrics.market_pnl import MarketPnLAnalyzer
    from src.metrics.pending_tracker import PendingTracker, PendingMetrics
    from src.config import get_config
    HAS_ANALYTICS = True
except ImportError:
    HAS_ANALYTICS = False

def find_latest_file(directory: Path, pattern: str) -> Path:
    """Find the most recent file matching pattern."""
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def main():
    project_root = Path(__file__).parent.parent
    output_dir = project_root / "outputs"

    # Load unified data
    sales_file = find_latest_file(output_dir, "unified_sales_*.csv")
    daily_file = find_latest_file(output_dir, "unified_daily_*.csv")
    metrics_file = find_latest_file(output_dir, "unified_metrics_*.json")

    # Also load Singularity map data for geographic info
    map_file = find_latest_file(output_dir, "singularity_map_*.json")
    charts_file = find_latest_file(output_dir, "singularity_charts_*.json")

    # Load market intel data (mortgage rates, Fed data, Polymarket)
    market_intel_file = find_latest_file(output_dir, "market_intel_*.json")

    # Load Parcl listings for inventory metrics
    parcl_dir = Path.home() / "Desktop" / "glasshouse"
    listings_file = find_latest_file(parcl_dir, "*listing*.csv")

    if not all([sales_file, daily_file, metrics_file]):
        print("Error: Run merge_datasets.py first to create unified data")
        return 1

    print("Loading unified data...")
    sales = pd.read_csv(sales_file)
    daily = pd.read_csv(daily_file)

    with open(metrics_file) as f:
        metrics = json.load(f)

    map_data = []
    if map_file and map_file.exists():
        with open(map_file) as f:
            map_data = json.load(f)

    charts_data = {}
    if charts_file and charts_file.exists():
        with open(charts_file) as f:
            charts_data = json.load(f)

    # Load market intel data
    market_intel_data = {}
    if market_intel_file and market_intel_file.exists():
        with open(market_intel_file) as f:
            market_intel_data = json.load(f)
        print(f"Loaded market intel data from {market_intel_file.name}")

    # Load accountability data (acquisitions)
    accountability_file = find_latest_file(output_dir, "accountability_*.json")
    accountability_data = {}
    if accountability_file and accountability_file.exists():
        with open(accountability_file) as f:
            accountability_data = json.load(f)
        print(f"Loaded accountability data from {accountability_file.name}")

    # Load careers data
    careers_file = find_latest_file(output_dir, "careers_*.json")
    careers_data = {}
    if careers_file and careers_file.exists():
        with open(careers_file) as f:
            careers_data = json.load(f)
        print(f"Loaded careers data from {careers_file.name}")

    # Load Parcl listings for inventory metrics
    listings_data = None
    if listings_file and listings_file.exists():
        listings_data = pd.read_csv(listings_file)
        print(f"Loaded {len(listings_data)} listings for inventory metrics")

    # Parse dates
    sales['sale_date'] = pd.to_datetime(sales['sale_date'])
    daily['date'] = pd.to_datetime(daily['date'])

    # Kaz-era start date (CEO change: Sep 10, 2025)
    KAZ_ERA_START = pd.Timestamp('2025-09-10')

    # Filter to Q1 2026
    q1_start = '2026-01-01'
    q1_sales = sales[sales['sale_date'] >= q1_start].copy()
    q1_daily = daily[daily['date'] >= q1_start].copy()

    # Q4 2025 data (for earnings estimates)
    q4_start = '2025-10-01'
    q4_end = '2025-12-31'
    q4_sales = sales[(sales['sale_date'] >= q4_start) & (sales['sale_date'] <= q4_end)].copy()

    q1_metrics = metrics['q1_2026']

    print(f"Q1 2026: {len(q1_sales)} sales, {len(q1_daily)} days")
    print(f"Q4 2025: {len(q4_sales)} sales")

    # =========================================================================
    # BUILD DASHBOARD DATA
    # =========================================================================

    dashboard_data = {
        "generated_at": datetime.now().isoformat(),
        "data_sources": {
            "singularity": "https://singularityresearchfund.com/opendoor-tracker",
            "parcl": "Parcl Research CSV exports",
            "note": "Unified dataset combining both sources"
        },

        # Current date
        "as_of": q1_daily['date'].max().strftime('%Y-%m-%d'),

        # ==== VELOCITY METRICS ====
        "velocity": {
            "q1_sales": int(q1_metrics['sales']['total']),
            "q1_revenue": q1_metrics['revenue']['total'],
            "daily_avg_sales": round(q1_metrics['velocity']['daily_avg'], 1),
            "daily_avg_revenue": q1_metrics['revenue']['daily_avg'],
            "avg_home_price": round(q1_metrics['revenue']['total'] / q1_metrics['sales']['total']) if q1_metrics['sales']['total'] > 0 else 0,
            "best_day": q1_metrics['velocity']['best_day'],
            "best_day_sales": q1_metrics['velocity']['best_day_count'],
        },

        # ==== P&L METRICS ====
        "pnl": {
            "win_rate": round(q1_metrics['pnl']['win_rate'], 1),
            "avg_profit": round(q1_metrics['pnl']['avg_profit']),
            "avg_loss": round(q1_metrics['pnl']['avg_loss']),
            "total_realized": q1_metrics['pnl']['total_realized'],
            "wins": q1_metrics['pnl']['wins'],
            "losses": q1_metrics['pnl']['losses'],
            "coverage_pct": round(q1_metrics['pnl']['coverage_pct'], 1),
        },

        # ==== REVENUE CHART DATA ====
        "revenue_chart": [],

        # ==== SALES CHART DATA ====
        "sales_chart": [],

        # ==== 30-DAY MOVING AVERAGE (from Singularity) ====
        "moving_avg_30d": charts_data.get('daily_sales', {}).get('moving_avg', [])[-30:] if charts_data else [],

        # ==== DATA QUALITY ====
        "data_quality": {
            "total_records": q1_metrics['sales']['total'],
            "matched_records": q1_metrics['sales']['matched'],
            "singularity_only": q1_metrics['sales']['singularity_only'],
            "parcl_only": q1_metrics['sales']['parcl_only'],
            "pnl_coverage": round(q1_metrics['pnl']['coverage_pct'], 1),
            "details_coverage": round(q1_metrics['property_details']['coverage_pct'], 1),
        },

        # ==== METHODOLOGY NOTES ====
        "methodology": {
            "margin_note": "Realized Net from Parcl includes all costs: renovation, holding costs (taxes, insurance), transaction fees, and commissions. Not simply Sale Price minus Purchase Price.",
            "win_rate_note": "Win = Realized Net > $0. Based on sales with P&L data from Parcl.",
            "guidance_target": "$1B Q1 revenue from Opendoor guidance. 29 homes/day based on $388K avg price.",
        },
    }

    # Build daily chart data using unified (merged) counts for consistency with velocity totals
    for _, row in q1_daily.iterrows():
        date_str = row['date'].strftime('%b %d')

        # Use unified counts (merged Singularity + Parcl) for chart/total consistency
        unified_sales = int(row['sales_count_unified']) if pd.notna(row.get('sales_count_unified')) else int(row['sales_count'])
        unified_revenue_millions = row['revenue_millions_unified'] if pd.notna(row.get('revenue_millions_unified')) else row['revenue_millions']

        dashboard_data['revenue_chart'].append({
            "date": date_str,
            "date_full": row['date'].strftime('%Y-%m-%d'),
            "revenue": unified_revenue_millions * 1_000_000,
            "revenue_millions": round(unified_revenue_millions, 2),
            "sales_count": unified_sales,
        })

        # Calculate avg price for the day
        daily_avg_price = (unified_revenue_millions * 1_000_000) / unified_sales if unified_sales > 0 else 0

        dashboard_data['sales_chart'].append({
            "date": date_str,
            "date_full": row['date'].strftime('%Y-%m-%d'),
            "count": unified_sales,
            "avg_price": round(daily_avg_price),
            "moving_avg": round(row['sales_moving_avg'], 1) if pd.notna(row.get('sales_moving_avg')) else None,
        })

    # ==== DAY OF WEEK ANALYSIS (Weekend/Weekday Normalization) ====
    day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    day_stats = {i: {'count': 0, 'total_revenue': 0, 'total_sales': 0} for i in range(7)}

    for entry in dashboard_data['revenue_chart']:
        date_str = entry.get('date_full')
        if date_str:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            dow = dt.weekday()  # 0=Monday, 6=Sunday
            day_stats[dow]['count'] += 1
            day_stats[dow]['total_revenue'] += entry.get('revenue', 0)
            day_stats[dow]['total_sales'] += entry.get('sales_count', 0)

    # Calculate weekday vs weekend averages
    weekday_rev = sum(day_stats[i]['total_revenue'] for i in range(5))
    weekday_days = sum(day_stats[i]['count'] for i in range(5))
    weekend_rev = sum(day_stats[i]['total_revenue'] for i in [5, 6])
    weekend_days = sum(day_stats[i]['count'] for i in [5, 6])

    weekday_avg = weekday_rev / weekday_days if weekday_days > 0 else 0
    weekend_avg = weekend_rev / weekend_days if weekend_days > 0 else 0
    overall_avg = (weekday_rev + weekend_rev) / (weekday_days + weekend_days) if (weekday_days + weekend_days) > 0 else 0

    # Build day-of-week breakdown
    dow_breakdown = []
    for i, name in enumerate(day_names):
        stats = day_stats[i]
        if stats['count'] > 0:
            avg_rev = stats['total_revenue'] / stats['count']
            avg_sales = stats['total_sales'] / stats['count']
            index = (avg_rev / overall_avg * 100) if overall_avg > 0 else 0
            dow_breakdown.append({
                'day': name,
                'day_num': i,
                'sample_days': stats['count'],
                'avg_revenue': round(avg_rev),
                'avg_sales': round(avg_sales, 1),
                'index': round(index),
                'is_weekend': i >= 5
            })

    dashboard_data['day_of_week'] = {
        'breakdown': dow_breakdown,
        'weekday_avg_revenue': round(weekday_avg),
        'weekend_avg_revenue': round(weekend_avg),
        'weekday_avg_sales': round(sum(day_stats[i]['total_sales'] for i in range(5)) / weekday_days, 1) if weekday_days > 0 else 0,
        'weekend_avg_sales': round(sum(day_stats[i]['total_sales'] for i in [5, 6]) / weekend_days, 1) if weekend_days > 0 else 0,
        'weekend_index': round(weekend_avg / weekday_avg * 100) if weekday_avg > 0 else 0,
        'normalization_factor': round(weekday_avg / weekend_avg, 2) if weekend_avg > 0 else 1.0,
    }

    # ==== INVENTORY METRICS (from Parcl Listings) ====
    if listings_data is not None and len(listings_data) > 0:
        # Parse numeric columns
        def parse_price(val):
            if pd.isna(val) or val == '':
                return np.nan
            return float(str(val).replace('$', '').replace(',', ''))

        listings_data['days_on_market'] = pd.to_numeric(listings_data['Days on Market'], errors='coerce')
        listings_data['price_cuts'] = pd.to_numeric(listings_data['Price Cuts'], errors='coerce')
        listings_data['latest_price'] = listings_data['Latest Listing Price'].apply(parse_price)
        listings_data['original_price'] = listings_data['Original Purchase Price'].apply(parse_price)

        # Calculate unrealized P&L where we have both prices
        listings_with_cost = listings_data[listings_data['original_price'].notna() & listings_data['latest_price'].notna()]

        dashboard_data['inventory'] = {
            'total_listings': len(listings_data),
            'avg_days_on_market': round(listings_data['days_on_market'].mean(), 1) if listings_data['days_on_market'].notna().any() else 0,
            'max_days_on_market': int(listings_data['days_on_market'].max()) if listings_data['days_on_market'].notna().any() else 0,
            'avg_price_cuts': round(listings_data['price_cuts'].mean(), 1) if listings_data['price_cuts'].notna().any() else 0,
            'homes_with_3plus_cuts': int((listings_data['price_cuts'] >= 3).sum()),
            'avg_listing_price': round(listings_data['latest_price'].mean()) if listings_data['latest_price'].notna().any() else 0,
            'total_inventory_value': round(listings_data['latest_price'].sum()) if listings_data['latest_price'].notna().any() else 0,
        }

        # State breakdown
        if 'State' in listings_data.columns:
            state_counts = listings_data['State'].value_counts().head(5).to_dict()
            dashboard_data['inventory']['by_state'] = state_counts

        # ==== KAZ-ERA VS LEGACY LISTED INVENTORY ====
        if 'Original Purchase Date' in listings_data.columns:
            # Parse purchase dates
            listings_data['purchase_date'] = pd.to_datetime(listings_data['Original Purchase Date'], errors='coerce')

            # Filter to listings with purchase date
            listings_with_date = listings_data[listings_data['purchase_date'].notna()].copy()

            if len(listings_with_date) > 0:
                # Split into Kaz-era and Legacy
                kaz_listed = listings_with_date[listings_with_date['purchase_date'] >= KAZ_ERA_START]
                legacy_listed = listings_with_date[listings_with_date['purchase_date'] < KAZ_ERA_START]

                # Calculate underwater status
                def calc_listed_metrics(df, name):
                    if len(df) == 0:
                        return {'count': 0, 'above_water': 0, 'underwater': 0, 'with_cuts': 0, 'uw_exposure': 0}

                    df = df.copy()
                    df['unrealized'] = df['latest_price'] - df['original_price']
                    above_water = (df['unrealized'] >= 0).sum()
                    underwater = (df['unrealized'] < 0).sum()
                    with_cuts = (df['price_cuts'] > 0).sum()
                    uw_exposure = df[df['unrealized'] < 0]['unrealized'].sum() if underwater > 0 else 0

                    return {
                        'count': len(df),
                        'above_water': int(above_water),
                        'underwater': int(underwater),
                        'with_cuts': int(with_cuts),
                        'uw_exposure': round(uw_exposure),
                        'avg_dom': round(df['days_on_market'].mean(), 1) if df['days_on_market'].notna().any() else 0,
                    }

                kaz_listed_metrics = calc_listed_metrics(kaz_listed, 'kaz')
                legacy_listed_metrics = calc_listed_metrics(legacy_listed, 'legacy')

                # Store for later - will be added to kaz_era/legacy sections after they're created
                dashboard_data['_kaz_listed_metrics'] = kaz_listed_metrics
                dashboard_data['_legacy_listed_metrics'] = legacy_listed_metrics

                print(f"  Kaz-era listed: {kaz_listed_metrics['count']} ({kaz_listed_metrics['underwater']} underwater)")
                print(f"  Legacy listed: {legacy_listed_metrics['count']} ({legacy_listed_metrics['underwater']} underwater)")

    # ==== GEOGRAPHIC DATA ====
    # Use Singularity map data for top markets (sorted by listing count)
    if map_data:
        # Sort by listing count descending
        sorted_markets = sorted(map_data, key=lambda x: x.get('listing_count', 0), reverse=True)

        # Top 10 markets with relevant data
        top_markets = []
        for market in sorted_markets[:10]:
            top_markets.append({
                'city': market.get('city', 'Unknown'),
                'listings': market.get('listing_count', 0),
                'avg_price': round(market.get('avg_price', 0)),
            })

        dashboard_data['top_markets'] = top_markets

        # Calculate market concentration (top 5 vs total)
        total_listings = sum(m.get('listing_count', 0) for m in map_data)
        top5_listings = sum(m.get('listing_count', 0) for m in sorted_markets[:5])
        dashboard_data['market_concentration'] = {
            'top5_pct': round(top5_listings / total_listings * 100, 1) if total_listings > 0 else 0,
            'total_markets': len(map_data),
            'total_listings': total_listings,
        }

    # Also keep city counts from sales data
    q1_sales_with_city = q1_sales[q1_sales['city'].notna() & (q1_sales['city'] != '')]
    if len(q1_sales_with_city) > 0:
        city_counts = q1_sales_with_city['city'].value_counts().head(15)
        dashboard_data['top_cities'] = city_counts.to_dict()

    # ==== COHORT ANALYSIS ====
    # Calculate from sales with days_held data
    sales_with_days = q1_sales[q1_sales['days_held'].notna()].copy()

    if len(sales_with_days) > 0:
        # Define cohorts
        def get_cohort(days):
            if days < 90:
                return 'new'
            elif days < 180:
                return 'mid'
            elif days < 365:
                return 'old'
            else:
                return 'toxic'

        sales_with_days['cohort'] = sales_with_days['days_held'].apply(get_cohort)

        cohorts = {}
        for cohort_name in ['new', 'mid', 'old', 'toxic']:
            cohort_sales = sales_with_days[sales_with_days['cohort'] == cohort_name]
            cohort_with_pnl = cohort_sales[cohort_sales['realized_net'].notna()]

            if len(cohort_with_pnl) > 0:
                wins = len(cohort_with_pnl[cohort_with_pnl['realized_net'] > 0])
                total = len(cohort_with_pnl)
                win_rate = wins / total * 100 if total > 0 else 0
                avg_profit = cohort_with_pnl['realized_net'].mean()
            else:
                win_rate = 0
                avg_profit = 0

            cohorts[cohort_name] = {
                'count': len(cohort_sales),
                'win_rate': round(win_rate, 1),
                'avg_profit': round(avg_profit) if not np.isnan(avg_profit) else 0,
                'total_realized': round(cohort_with_pnl['realized_net'].sum()) if len(cohort_with_pnl) > 0 else 0,
            }

        dashboard_data['cohorts'] = cohorts

    # ==== KAZ-ERA vs LEGACY SPLIT ====
    # We need purchase_date to determine era - only available for matched/parcl records
    sales_with_purchase = q1_sales[q1_sales['purchase_date'].notna()].copy()
    sales_with_purchase['purchase_date'] = pd.to_datetime(sales_with_purchase['purchase_date'])

    if len(sales_with_purchase) > 0:
        kaz_era_sales = sales_with_purchase[sales_with_purchase['purchase_date'] >= KAZ_ERA_START]
        legacy_sales = sales_with_purchase[sales_with_purchase['purchase_date'] < KAZ_ERA_START]

        def calc_era_metrics(era_sales, era_name):
            """Calculate metrics for Kaz-era or Legacy portfolio."""
            era_with_pnl = era_sales[era_sales['realized_net'].notna()]

            if len(era_with_pnl) > 0:
                wins = len(era_with_pnl[era_with_pnl['realized_net'] > 0])
                losses = len(era_with_pnl[era_with_pnl['realized_net'] < 0])
                total = wins + losses
                win_rate = wins / total * 100 if total > 0 else 0
                avg_profit = era_with_pnl[era_with_pnl['realized_net'] > 0]['realized_net'].mean() if wins > 0 else 0
                total_realized = era_with_pnl['realized_net'].sum()
            else:
                win_rate = 0
                avg_profit = 0
                total_realized = 0
                wins = 0
                losses = 0

            return {
                'sold_count': len(era_sales),
                'sold_win_rate': round(win_rate, 1),
                'sold_avg_profit': round(avg_profit) if not np.isnan(avg_profit) else 0,
                'sold_total_realized': round(total_realized),
                'wins': wins,
                'losses': losses,
            }

        dashboard_data['kaz_era'] = calc_era_metrics(kaz_era_sales, 'kaz_era')
        dashboard_data['legacy'] = calc_era_metrics(legacy_sales, 'legacy')

        print(f"  Kaz-era: {dashboard_data['kaz_era']['sold_count']} sales, {dashboard_data['kaz_era']['sold_win_rate']}% WR")
        print(f"  Legacy: {dashboard_data['legacy']['sold_count']} sales, {dashboard_data['legacy']['sold_win_rate']}% WR")
    else:
        # Fallback if no purchase date data
        dashboard_data['kaz_era'] = {'sold_count': 0, 'sold_win_rate': 0, 'sold_avg_profit': 0, 'sold_total_realized': 0}
        dashboard_data['legacy'] = {'sold_count': 0, 'sold_win_rate': 0, 'sold_avg_profit': 0, 'sold_total_realized': 0}

    # Merge in listed inventory metrics (calculated earlier from Parcl listings)
    if '_kaz_listed_metrics' in dashboard_data:
        kaz_listed = dashboard_data.pop('_kaz_listed_metrics')
        dashboard_data['kaz_era']['listed_count'] = kaz_listed['count']
        dashboard_data['kaz_era']['listed_above_water'] = kaz_listed['above_water']
        dashboard_data['kaz_era']['listed_underwater'] = kaz_listed['underwater']
        dashboard_data['kaz_era']['listed_with_cuts'] = kaz_listed['with_cuts']
        dashboard_data['kaz_era']['listed_uw_exposure'] = kaz_listed['uw_exposure']

    if '_legacy_listed_metrics' in dashboard_data:
        legacy_listed = dashboard_data.pop('_legacy_listed_metrics')
        dashboard_data['legacy']['listed_count'] = legacy_listed['count']
        dashboard_data['legacy']['listed_above_water'] = legacy_listed['above_water']
        dashboard_data['legacy']['listed_underwater'] = legacy_listed['underwater']
        dashboard_data['legacy']['listed_with_cuts'] = legacy_listed['with_cuts']
        dashboard_data['legacy']['listed_uw_exposure'] = legacy_listed['uw_exposure']

    # ==== WEEKLY SUMMARY ====
    # This week's data
    latest_date = q1_daily['date'].max()
    week_start = latest_date - timedelta(days=6)

    this_week = q1_daily[q1_daily['date'] >= week_start]
    last_week_end = week_start - timedelta(days=1)
    last_week_start = last_week_end - timedelta(days=6)
    last_week = q1_daily[(q1_daily['date'] >= last_week_start) & (q1_daily['date'] <= last_week_end)]

    dashboard_data['this_week'] = {
        'sales': int(this_week['sales_count'].sum()),
        'revenue': this_week['revenue_millions'].sum() * 1_000_000,
        'daily_avg': round(this_week['sales_count'].mean(), 1),
    }

    if len(last_week) > 0:
        dashboard_data['last_week'] = {
            'sales': int(last_week['sales_count'].sum()),
            'revenue': last_week['revenue_millions'].sum() * 1_000_000,
            'daily_avg': round(last_week['sales_count'].mean(), 1),
        }

        # Week-over-week change
        wow_sales = (dashboard_data['this_week']['sales'] / dashboard_data['last_week']['sales'] - 1) * 100 if dashboard_data['last_week']['sales'] > 0 else 0
        wow_revenue = (dashboard_data['this_week']['revenue'] / dashboard_data['last_week']['revenue'] - 1) * 100 if dashboard_data['last_week']['revenue'] > 0 else 0

        dashboard_data['wow_change'] = {
            'sales_pct': round(wow_sales, 1),
            'revenue_pct': round(wow_revenue, 1),
        }

    # ==== Q1 GUIDANCE TRACKING ====
    # Opendoor Q1 guidance: ~$1B revenue
    q1_target = 1_000_000_000
    q1_days_total = 90  # Jan 1 - Mar 31
    days_elapsed = len(q1_daily)
    days_remaining = q1_days_total - days_elapsed

    current_revenue = q1_metrics['revenue']['total']
    daily_target = q1_target / q1_days_total
    daily_actual = current_revenue / days_elapsed if days_elapsed > 0 else 0

    projected_revenue = daily_actual * q1_days_total
    pacing_pct = (projected_revenue / q1_target) * 100

    dashboard_data['guidance'] = {
        'q1_target': q1_target,
        'current_revenue': current_revenue,
        'days_elapsed': days_elapsed,
        'days_remaining': days_remaining,
        'daily_target': round(daily_target),
        'daily_actual': round(daily_actual),
        'projected_revenue': round(projected_revenue),
        'pacing_pct': round(pacing_pct, 1),
        'on_track': pacing_pct >= 95,
    }

    # ==== TOXIC WEEKLY TREND ====
    # Load dashboard_data.json for historical toxic counts
    dashboard_data_file = output_dir / "dashboard_data.json"
    toxic_weekly = []
    if dashboard_data_file.exists():
        try:
            with open(dashboard_data_file) as f:
                db_data = json.load(f)

            history = db_data.get('history', [])
            if history:
                # Group by week (Sunday start)
                weekly_toxic = {}
                for entry in history:
                    date_str = entry.get('date', '')
                    if not date_str:
                        continue
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    # Get week number (ISO week)
                    week_num = date_obj.isocalendar()[1]
                    toxic_count = entry.get('inventory', {}).get('toxic_count', 0)
                    # Keep the latest toxic count for each week
                    weekly_toxic[week_num] = {
                        'week': week_num,
                        'date': date_str,
                        'toxic_count': toxic_count
                    }

                # Sort by week number and take last 4 weeks
                sorted_weeks = sorted(weekly_toxic.items(), key=lambda x: x[0])
                for week_num, data in sorted_weeks[-4:]:
                    toxic_weekly.append(data)
        except Exception as e:
            print(f"Warning: Could not load toxic history: {e}")

    dashboard_data['toxic_weekly_trend'] = toxic_weekly

    # ==== TOXIC CLEARANCE SUMMARY ====
    # Calculate toxic clearance metrics for the signal card
    toxic_remaining = 84  # default
    toxic_cleared = 43  # default
    toxic_rate = 5  # default rate per week

    if toxic_weekly and len(toxic_weekly) >= 2:
        # Get current toxic count
        toxic_remaining = toxic_weekly[-1].get('toxic_count', 84)
        # Calculate rate from recent trend (change over last few weeks)
        first_count = toxic_weekly[0].get('toxic_count', toxic_remaining)
        weeks_elapsed = len(toxic_weekly)
        if weeks_elapsed > 0 and first_count > toxic_remaining:
            toxic_rate = round((first_count - toxic_remaining) / weeks_elapsed, 1)
        toxic_cleared = 127 - toxic_remaining  # Assuming 127 was initial toxic count
    elif 'cohorts' in dashboard_data and 'toxic' in dashboard_data['cohorts']:
        # Fallback to cohorts data
        toxic_remaining = dashboard_data['cohorts']['toxic'].get('count', 84)
        toxic_cleared = 127 - toxic_remaining

    dashboard_data['toxic_clearance'] = {
        'remaining': toxic_remaining,
        'cleared': max(0, toxic_cleared),
        'rate_per_week': max(1, toxic_rate),  # At least 1/week
        'initial': 127
    }

    # ==== THIS WEEK NARRATIVE (AI-powered) ====
    this_week_narrative = generate_this_week_narrative(dashboard_data, dashboard_data_file)
    dashboard_data['this_week_narrative'] = this_week_narrative

    # ==== UNIT ECONOMICS (True margins after all costs) ====
    if HAS_ANALYTICS and len(q1_sales) > 0:
        try:
            print("Calculating true unit economics...")
            calc = UnitEconomicsCalculator()
            unit_econ = calc.analyze_sales(q1_sales, listings_data)

            if unit_econ:
                dashboard_data['unit_economics'] = {
                    'total_sales': unit_econ.get('total_sales', 0),
                    'gross_spread_total': round(unit_econ.get('gross_spread_total', 0)),
                    'gross_spread_avg': round(unit_econ.get('gross_spread_avg', 0)),
                    'total_costs': round(unit_econ.get('total_costs', 0)),
                    'true_net_total': round(unit_econ.get('true_net_total', 0)),
                    'true_net_avg': round(unit_econ.get('true_net_avg', 0)),
                    'true_margin_avg': round(unit_econ.get('true_margin_avg', 0), 1),
                    'profitable_count': unit_econ.get('profitable_count', 0),
                    'profitable_pct': round(unit_econ.get('profitable_pct', 0), 1),
                    'tier_breakdown': unit_econ.get('tier_breakdown', {}),
                    'cost_breakdown': {
                        'renovation_avg': round(unit_econ.get('cost_breakdown', {}).get('renovation_avg', 0)),
                        'holding_avg': round(unit_econ.get('cost_breakdown', {}).get('holding_avg', 0)),
                    },
                    'reported_vs_true': {
                        'hidden_costs_pct': round(
                            (unit_econ.get('gross_spread_total', 0) - unit_econ.get('true_net_total', 0)) /
                            unit_econ.get('gross_spread_total', 1) * 100, 1
                        ) if unit_econ.get('gross_spread_total', 0) > 0 else 0,
                    }
                }
                print(f"  True margin: {dashboard_data['unit_economics']['true_margin_avg']}% "
                      f"(vs {dashboard_data['pnl']['win_rate']}% reported win rate)")
        except Exception as e:
            print(f"  Warning: Could not calculate unit economics: {e}")

    # ==== MARKET P&L MATRIX ====
    if HAS_ANALYTICS and listings_data is not None and len(q1_sales) > 0:
        try:
            print("Analyzing market-level P&L...")
            analyzer = MarketPnLAnalyzer(q1_sales, listings_data)
            market_summary = analyzer.get_summary()

            if market_summary and market_summary.get('markets'):
                dashboard_data['market_pnl'] = {
                    'markets': market_summary['markets'][:12],  # Top 12 markets
                    'actions': market_summary.get('actions', {}),
                    'summary': {
                        'grow_count': len(market_summary.get('actions', {}).get('grow', [])),
                        'hold_count': len(market_summary.get('actions', {}).get('hold', [])),
                        'pause_count': len(market_summary.get('actions', {}).get('pause', [])),
                        'exit_count': len(market_summary.get('actions', {}).get('exit', [])),
                    }
                }
                print(f"  Analyzed {len(market_summary['markets'])} markets")
                print(f"  GROW: {dashboard_data['market_pnl']['summary']['grow_count']}, "
                      f"PAUSE/EXIT: {dashboard_data['market_pnl']['summary']['pause_count'] + dashboard_data['market_pnl']['summary']['exit_count']}")
        except Exception as e:
            print(f"  Warning: Could not analyze market P&L: {e}")

    # ==== SALES FUNNEL DATA ====
    # Combines: Opendoor direct inventory + MLS pending/sold data
    try:
        print("Loading sales funnel data...")

        # Load Opendoor direct inventory (active listings)
        opendoor_inventory_file = find_latest_file(output_dir, "opendoor_listings_*.csv")
        opendoor_inventory = None
        if opendoor_inventory_file:
            opendoor_inventory = pd.read_csv(opendoor_inventory_file)
            print(f"  Loaded {len(opendoor_inventory)} active Opendoor listings")

        # Load pending/sold listings (from MLS scraper)
        pending_csv_file = find_latest_file(output_dir, "pending_listings_*.csv")
        pending_listings = None
        if pending_csv_file:
            pending_listings = pd.read_csv(pending_csv_file)
            print(f"  Loaded {len(pending_listings)} pending/sold Opendoor listings")

        # Load pending JSON for metrics
        pending_json_file = find_latest_file(output_dir, "pending_*.json")
        pending_metrics = {}
        if pending_json_file:
            with open(pending_json_file) as f:
                pending_data = json.load(f)
                pending_metrics = pending_data.get('metrics', {})

        # Build comprehensive sales funnel data
        sales_funnel = {
            'scraped_at': pending_data.get('scraped_at', '') if pending_json_file else datetime.now().isoformat(),
        }

        # Active inventory stats
        if opendoor_inventory is not None and len(opendoor_inventory) > 0:
            sales_funnel['active_inventory'] = {
                'total': len(opendoor_inventory),
                'total_value': int(opendoor_inventory['price'].sum()) if 'price' in opendoor_inventory.columns else 0,
                'avg_price': int(opendoor_inventory['price'].mean()) if 'price' in opendoor_inventory.columns else 0,
                'by_market': opendoor_inventory['market'].value_counts().head(10).to_dict() if 'market' in opendoor_inventory.columns else {},
            }

        # Sales/pending stats
        if pending_listings is not None and len(pending_listings) > 0:
            sales_funnel['recent_sales'] = {
                'total': len(pending_listings),
                'total_value': int(pending_listings['list_price'].sum()) if 'list_price' in pending_listings.columns else 0,
                'avg_price': int(pending_listings['list_price'].mean()) if 'list_price' in pending_listings.columns else 0,
                'by_market': pending_listings['search_market'].value_counts().to_dict() if 'search_market' in pending_listings.columns else {},
                'by_agent': pending_listings['agent_name'].value_counts().to_dict() if 'agent_name' in pending_listings.columns else {},
            }

            # Calculate days-to-pending for each sale
            if 'list_date' in pending_listings.columns and 'pending_date' in pending_listings.columns:
                pending_listings['list_date_dt'] = pd.to_datetime(pending_listings['list_date'], errors='coerce')
                pending_listings['pending_date_dt'] = pd.to_datetime(pending_listings['pending_date'], errors='coerce')
                pending_listings['days_to_pending'] = (pending_listings['pending_date_dt'] - pending_listings['list_date_dt']).dt.days

                valid_days = pending_listings['days_to_pending'].dropna()
                if len(valid_days) > 0:
                    sales_funnel['recent_sales']['avg_days_to_pending'] = int(valid_days.mean())
                    sales_funnel['recent_sales']['min_days_to_pending'] = int(valid_days.min())
                    sales_funnel['recent_sales']['max_days_to_pending'] = int(valid_days.max())

                    # Cohort breakdown by days to pending
                    def categorize_dom(days):
                        if pd.isna(days):
                            return 'unknown'
                        if days < 30:
                            return 'fast (<30d)'
                        elif days < 90:
                            return 'normal (30-90d)'
                        elif days < 180:
                            return 'slow (90-180d)'
                        else:
                            return 'stale (>180d)'

                    pending_listings['speed_cohort'] = pending_listings['days_to_pending'].apply(categorize_dom)
                    sales_funnel['recent_sales']['by_speed'] = pending_listings['speed_cohort'].value_counts().to_dict()

            # Top sales (highest prices)
            if 'list_price' in pending_listings.columns:
                top_sales = pending_listings.nlargest(5, 'list_price')[['scraped_address', 'city', 'state', 'list_price', 'search_market']].to_dict('records')
                sales_funnel['recent_sales']['top_sales'] = top_sales

        # Calculate turnover metrics
        if sales_funnel.get('active_inventory') and sales_funnel.get('recent_sales'):
            active = sales_funnel['active_inventory']['total']
            sold = sales_funnel['recent_sales']['total']
            sales_funnel['turnover'] = {
                'sold_90d': sold,
                'active_inventory': active,
                'turnover_rate_90d': round(sold / active * 100, 1) if active > 0 else 0,
                'monthly_velocity': round(sold / 3, 1),  # 90 days = 3 months
                'months_of_inventory': round(active / (sold / 3), 1) if sold > 0 else 0,
            }

        # Include original pending metrics
        if pending_metrics:
            sales_funnel['funnel_metrics'] = {
                'total_pending': pending_metrics.get('total_pending', 0),
                'kaz_era_pending': pending_metrics.get('kaz_era_pending', 0),
                'legacy_pending': pending_metrics.get('legacy_pending', 0),
                'toxic_pending_count': pending_metrics.get('toxic_pending_count', 0),
                'toxic_pending_pct': pending_metrics.get('toxic_pending_pct', 0),
                'cohort_breakdown': pending_metrics.get('cohort_breakdown', {}),
                'funnel_changes': pending_metrics.get('funnel_changes', {}),
            }

        dashboard_data['sales_funnel'] = sales_funnel

        # Print summary
        if 'turnover' in sales_funnel:
            t = sales_funnel['turnover']
            print(f"  Turnover: {t['sold_90d']} sold / {t['active_inventory']} active = {t['turnover_rate_90d']}%")
            print(f"  Monthly velocity: {t['monthly_velocity']} sales/month")
            print(f"  Months of inventory: {t['months_of_inventory']}")

    except Exception as e:
        print(f"  Warning: Could not load sales funnel data: {e}")
        import traceback
        traceback.print_exc()

    # ==== MARKET INTEL (Rates, Fed, Polymarket) ====
    if market_intel_data:
        dashboard_data['market_intel'] = {
            'mortgage_rates': market_intel_data.get('mortgage_rates', {}),
            'fed_data': market_intel_data.get('fed_data', {}),
            'earnings': market_intel_data.get('earnings', {}),
            'news': market_intel_data.get('news', {}),
        }
        print(f"  Added market intel: rates={market_intel_data.get('mortgage_rates', {}).get('rate_30yr', 'N/A')}%, "
              f"fed={market_intel_data.get('fed_data', {}).get('current_rate', 'N/A')}")

    # ==== ACQUISITIONS DATA ====
    if accountability_data:
        weekly_contracts = accountability_data.get('weekly_contracts', [])
        latest = accountability_data.get('latest', {})
        product_updates = accountability_data.get('product_updates', [])

        dashboard_data['acquisitions'] = {
            'weekly_contracts': weekly_contracts,
            'latest_week': latest.get('contracts', 0),
            'wow_change': latest.get('wow_change', 0),
            'avg_4w': latest.get('avg_4w', 0),
            'q1_total': latest.get('q1_total', 0),
            'q1_weeks': latest.get('q1_weeks', 0),
            'product_updates': product_updates,
        }
        print(f"  Added acquisitions: {latest.get('contracts', 0)} contracts this week, {len(product_updates)} product updates")

    # ==== CAREERS DATA ====
    if careers_data:
        jobs = careers_data.get('jobs', [])
        total_jobs = careers_data.get('total_jobs', len(jobs))

        # Count by department
        dept_counts = {}
        location_counts = {}
        for job in jobs:
            dept = job.get('department', 'Other')
            loc = job.get('location', 'Unknown')
            dept_counts[dept] = dept_counts.get(dept, 0) + 1
            # Extract city from location
            city = loc.split(',')[0].replace('Office - ', '').strip()
            location_counts[city] = location_counts.get(city, 0) + 1

        # Get top location
        top_location = max(location_counts.items(), key=lambda x: x[1])[0] if location_counts else 'Unknown'

        # Count specific categories
        eng_count = dept_counts.get('Engineering & Technology', 0)
        sales_count = dept_counts.get('Sales & Customer Experience', 0)
        finance_count = dept_counts.get('Finance & Accounting', 0) + dept_counts.get('Legal', 0)
        growth_count = dept_counts.get('Marketing & Growth', 0)

        # Count AI/ML roles
        ai_count = sum(1 for j in jobs if 'AI' in j.get('title', '') or 'ML' in j.get('title', '') or
                       'Data Scientist' in j.get('title', '') or 'Machine Learning' in j.get('title', ''))

        # Count senior roles
        senior_count = sum(1 for j in jobs if 'Senior' in j.get('title', '') or 'Director' in j.get('title', '') or
                          'Manager' in j.get('title', '') or 'Lead' in j.get('title', '') or 'VP' in j.get('title', ''))

        dashboard_data['careers'] = {
            'total_jobs': total_jobs,
            'engineering': eng_count,
            'sales': sales_count,
            'finance': finance_count,
            'growth': growth_count,
            'ai_ml': ai_count,
            'senior': senior_count,
            'top_location': top_location,
            'department_breakdown': dept_counts,
            'location_breakdown': dict(sorted(location_counts.items(), key=lambda x: -x[1])[:10]),
        }
        print(f"  Added careers: {total_jobs} jobs, {eng_count} engineering, {ai_count} AI/ML")

    # ==== Q4 2025 EARNINGS ESTIMATES ====
    if len(q4_sales) > 0:
        q4_revenue = q4_sales['sale_price'].sum()
        q4_homes_sold = len(q4_sales)

        # Calculate Q4 acquisitions from accountability data (Oct-Dec 2025 weeks)
        q4_acq = 0
        if accountability_data:
            weekly = accountability_data.get('weekly_contracts', [])
            for w in weekly:
                week_date = w.get('week', '')
                if week_date >= '2025-10-01' and week_date <= '2025-12-31':
                    q4_acq += w.get('actual', 0)

        # Days until earnings (Feb 19, 2026)
        earnings_date = datetime(2026, 2, 19)
        days_to_earnings = (earnings_date - datetime.now()).days

        dashboard_data['q4_estimates'] = {
            'revenue': round(q4_revenue),
            'homes_sold': q4_homes_sold,
            'acquisitions': q4_acq,
            'days_to_earnings': max(0, days_to_earnings),
        }
        print(f"  Added Q4 estimates: {q4_homes_sold} homes, ${q4_revenue/1e6:.1f}M revenue, {q4_acq} acquisitions")

    # ==== SAVE OUTPUT ====
    output_file = output_dir / "unified_dashboard_data.json"
    with open(output_file, 'w') as f:
        json.dump(dashboard_data, f, indent=2, default=str)

    print(f"\nSaved: {output_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("  DASHBOARD DATA SUMMARY")
    print("=" * 60)
    print(f"\n  Q1 2026 ({dashboard_data['as_of']}):")
    print(f"    Sales: {dashboard_data['velocity']['q1_sales']}")
    print(f"    Revenue: ${dashboard_data['velocity']['q1_revenue']:,.0f}")
    print(f"    Daily Avg: {dashboard_data['velocity']['daily_avg_sales']} sales/day")
    print(f"    Win Rate: {dashboard_data['pnl']['win_rate']}%")
    print(f"    Guidance Pacing: {dashboard_data['guidance']['pacing_pct']}%")

    if 'unit_economics' in dashboard_data:
        print(f"\n  True Unit Economics:")
        print(f"    True Margin: {dashboard_data['unit_economics']['true_margin_avg']}%")
        print(f"    Profitable: {dashboard_data['unit_economics']['profitable_pct']}%")
        print(f"    Hidden Costs: {dashboard_data['unit_economics']['reported_vs_true']['hidden_costs_pct']}% of gross")

    if 'market_pnl' in dashboard_data:
        print(f"\n  Market Actions:")
        print(f"    GROW: {dashboard_data['market_pnl']['summary']['grow_count']} markets")
        print(f"    HOLD: {dashboard_data['market_pnl']['summary']['hold_count']} markets")
        print(f"    PAUSE/EXIT: {dashboard_data['market_pnl']['summary']['pause_count'] + dashboard_data['market_pnl']['summary']['exit_count']} markets")

    if 'sales_funnel' in dashboard_data:
        sf = dashboard_data['sales_funnel']
        print(f"\n  Sales Funnel:")
        if 'active_inventory' in sf:
            ai = sf['active_inventory']
            print(f"    Active Inventory: {ai['total']} listings (${ai['total_value']/1e6:.1f}M)")
        if 'recent_sales' in sf:
            rs = sf['recent_sales']
            print(f"    Recent Sales (90d): {rs['total']} (${rs['total_value']/1e6:.1f}M)")
            if 'avg_days_to_pending' in rs:
                print(f"    Avg Days to Pending: {rs['avg_days_to_pending']} days")
        if 'turnover' in sf:
            t = sf['turnover']
            print(f"    Turnover Rate: {t['turnover_rate_90d']}% (90d)")
            print(f"    Months of Inventory: {t['months_of_inventory']}")

    print("=" * 60 + "\n")

    return 0


def generate_this_week_narrative(dashboard_data, dashboard_data_file):
    """Generate 'This Week' narrative using Claude API or fallback to rules."""

    # Prepare data context
    context_data = {
        'velocity': dashboard_data.get('velocity', {}),
        'pnl': dashboard_data.get('pnl', {}),
        'guidance': dashboard_data.get('guidance', {}),
        'cohorts': dashboard_data.get('cohorts', {}),
        'wow_change': dashboard_data.get('wow_change', {}),
        'this_week': dashboard_data.get('this_week', {}),
        'last_week': dashboard_data.get('last_week', {}),
    }

    # Try to get historical data for comparisons
    curr_toxic = 84
    prev_toxic = 89
    curr_underwater = 190
    prev_underwater = 198
    kaz_win_rate = 95.3
    kaz_underwater = 3

    if dashboard_data_file.exists():
        try:
            with open(dashboard_data_file) as f:
                db_data = json.load(f)
            current = db_data.get('current', {})
            history = db_data.get('history', [])
            prev = history[-8] if len(history) >= 8 else (history[0] if history else {})

            curr_toxic = current.get('inventory', {}).get('toxic_count', curr_toxic)
            prev_toxic = prev.get('inventory', {}).get('toxic_count', prev_toxic)
            v3 = current.get('v3', {}).get('portfolio', {})
            curr_underwater = v3.get('legacy', {}).get('underwater', curr_underwater)
            kaz_win_rate = v3.get('kaz_era', {}).get('win_rate', kaz_win_rate)
            kaz_underwater = v3.get('kaz_era', {}).get('underwater', kaz_underwater)
        except:
            pass

    context_data['toxic'] = {'current': curr_toxic, 'previous': prev_toxic}
    context_data['underwater'] = {'current': curr_underwater, 'previous': prev_underwater}
    context_data['kaz_era'] = {'win_rate': kaz_win_rate, 'underwater': kaz_underwater}

    # Try Claude API for intelligent narrative
    if HAS_ANTHROPIC and os.environ.get('ANTHROPIC_API_KEY'):
        try:
            narrative = generate_narrative_with_claude(context_data)
            if narrative:
                print("  Generated narrative with Claude API")
                return narrative
        except Exception as e:
            print(f"  Claude API error: {e}, falling back to rules")

    # Fallback to rule-based narrative
    return generate_narrative_rules(context_data)


def generate_narrative_with_claude(context):
    """Use Claude to generate intelligent weekly narrative."""
    client = anthropic.Anthropic()

    prompt = f"""You are analyzing weekly operational data for Opendoor ($OPEN).

DATA THIS WEEK:
- Daily Sales: {context['velocity'].get('daily_avg_sales', 22)} homes/day (need 29 for guidance)
- Q1 Revenue: ${context['velocity'].get('q1_revenue', 0)/1e6:.1f}M of $1B target
- Win Rate: {context['pnl'].get('win_rate', 79)}%
- Guidance Pacing: {context['guidance'].get('pacing_pct', 96)}%
- WoW Sales Change: {context['wow_change'].get('sales_pct', 0):+.1f}%

PORTFOLIO:
- Toxic Inventory: {context['toxic']['current']} (was {context['toxic']['previous']} last week)
- Legacy Underwater: {context['underwater']['current']} (was {context['underwater']['previous']})
- Kaz-Era Win Rate: {context['kaz_era']['win_rate']}%
- Kaz-Era Underwater: {context['kaz_era']['underwater']}

COHORT PERFORMANCE:
- New (<90d): {context['cohorts'].get('new', {}).get('win_rate', 97)}% win rate
- Mid (90-180d): {context['cohorts'].get('mid', {}).get('win_rate', 95)}% win rate
- Old (180-365d): {context['cohorts'].get('old', {}).get('win_rate', 64)}% win rate
- Toxic (>365d): {context['cohorts'].get('toxic', {}).get('win_rate', 37)}% win rate

Generate a weekly narrative with EXACTLY this JSON format:
{{
    "improving": ["2-3 short bullet points about positive trends"],
    "stable": ["1-2 bullet points about metrics holding steady"],
    "watching": ["1-2 bullet points about concerning trends to monitor"]
}}

Rules:
- Each bullet should be 8-15 words, specific with numbers
- Include arrows for changes (→ or ↑ or ↓)
- Be direct and analytical, no fluff
- Only include categories that have genuine items (can be empty)

Return ONLY the JSON, no other text."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}]
    )

    text = response.content[0].text.strip()
    # Handle markdown code blocks
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]

    return json.loads(text)


def generate_narrative_rules(context):
    """Fallback rule-based narrative generation."""
    narrative = {'improving': [], 'stable': [], 'watching': []}

    # IMPROVING
    toxic_change = context['toxic']['previous'] - context['toxic']['current']
    if toxic_change > 0:
        narrative['improving'].append(
            f"Toxic inventory down {toxic_change} homes ({context['toxic']['previous']} → {context['toxic']['current']})"
        )

    underwater_change = context['underwater']['previous'] - context['underwater']['current']
    if underwater_change > 0:
        narrative['improving'].append(
            f"Legacy underwater down {underwater_change} homes"
        )
        exposure = underwater_change * 15000
        if exposure > 0:
            narrative['improving'].append(f"Underwater exposure improved ${exposure // 1000}K")

    # STABLE
    if context['kaz_era']['win_rate'] >= 94:
        narrative['stable'].append(f"Kaz-era win rate holding ({context['kaz_era']['win_rate']}%)")

    new_cohort = context['cohorts'].get('new', {})
    if new_cohort.get('win_rate', 0) >= 95:
        narrative['stable'].append(f"New cohort performance strong ({new_cohort['win_rate']}% WR)")

    # WATCHING
    daily_vel = context['velocity'].get('daily_avg_sales', 22)
    if daily_vel < 27:
        narrative['watching'].append(f"Daily velocity below target ({round(daily_vel)} vs 29 needed)")

    if context['kaz_era']['underwater'] > 2:
        narrative['watching'].append(f"Kaz-era underwater at {context['kaz_era']['underwater']} homes")

    return narrative


if __name__ == "__main__":
    sys.exit(main())
