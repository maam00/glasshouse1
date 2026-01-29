"""
Generate Dashboard Data
=======================
Creates JSON files for dashboard to display real data.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.database import Database
from src.signals.signal_engine import get_ops_score_for_dashboard


def generate_portfolio_summary(db: Database) -> dict:
    """Generate portfolio summary data (new book vs legacy)."""
    conn = db._get_conn()
    cursor = conn.cursor()

    # Latest snapshot totals
    cursor.execute('''
        SELECT
            COUNT(*) as total_active,
            COUNT(CASE WHEN cohort = 'new' THEN 1 END) as new_count,
            COUNT(CASE WHEN cohort = 'mid' THEN 1 END) as mid_count,
            COUNT(CASE WHEN cohort = 'old' THEN 1 END) as old_count,
            COUNT(CASE WHEN cohort = 'toxic' THEN 1 END) as toxic_count,
            COUNT(CASE WHEN unrealized_pnl < 0 THEN 1 END) as underwater_count,
            COUNT(CASE WHEN price_cuts_count > 0 THEN 1 END) as with_cuts,
            SUM(CASE WHEN unrealized_pnl < 0 THEN unrealized_pnl ELSE 0 END) as total_underwater_value,
            AVG(days_on_market) as avg_dom,
            AVG(price_cuts_count) as avg_cuts
        FROM property_daily_snapshot
        WHERE status = 'FOR_SALE'
          AND snapshot_date = (SELECT MAX(snapshot_date) FROM property_daily_snapshot)
    ''')
    inv = cursor.fetchone()

    # Sales by cohort with P&L
    cursor.execute('''
        SELECT
            cohort,
            COUNT(*) as total_sold,
            COUNT(CASE WHEN realized_net IS NOT NULL THEN 1 END) as with_pnl,
            SUM(CASE WHEN realized_net > 0 THEN 1 ELSE 0 END) as profitable,
            AVG(realized_net) as avg_pnl,
            AVG(days_held) as avg_days_held
        FROM sales_log
        WHERE cohort IS NOT NULL
        GROUP BY cohort
    ''')
    sales_by_cohort = {row['cohort']: dict(row) for row in cursor.fetchall()}

    conn.close()

    total = inv['total_active'] or 1
    new_book = (inv['new_count'] or 0) + (inv['mid_count'] or 0)
    legacy_book = (inv['old_count'] or 0) + (inv['toxic_count'] or 0)

    # Calculate win rates
    new_sales = sales_by_cohort.get('new', {})
    new_with_pnl = new_sales.get('with_pnl', 0) or 1
    new_profitable = new_sales.get('profitable', 0) or 0
    new_win_rate = (new_profitable / new_with_pnl * 100) if new_with_pnl else 0

    legacy_sales_count = 0
    legacy_profitable = 0
    for cohort in ['old', 'toxic']:
        s = sales_by_cohort.get(cohort, {})
        legacy_sales_count += s.get('with_pnl', 0) or 0
        legacy_profitable += s.get('profitable', 0) or 0
    legacy_win_rate = (legacy_profitable / legacy_sales_count * 100) if legacy_sales_count else 0

    return {
        'generated_at': datetime.now().isoformat(),
        'total_active': total,
        'new_book': {
            'count': new_book,
            'pct': round(new_book / total * 100, 1),
            'win_rate': round(new_win_rate, 1),
            'sales_with_pnl': new_with_pnl,
            'avg_pnl': round(new_sales.get('avg_pnl', 0) or 0, 0),
        },
        'legacy_book': {
            'count': legacy_book,
            'pct': round(legacy_book / total * 100, 1),
            'win_rate': round(legacy_win_rate, 1),
            'sales_with_pnl': legacy_sales_count,
            'avg_pnl': round(sum(sales_by_cohort.get(c, {}).get('avg_pnl', 0) or 0 for c in ['old', 'toxic']) / 2, 0),
        },
        'underwater': {
            'count': inv['underwater_count'] or 0,
            'pct': round((inv['underwater_count'] or 0) / total * 100, 1),
            'total_value': round((inv['total_underwater_value'] or 0) / 1e6, 1),
        },
        'price_cuts': {
            'count': inv['with_cuts'] or 0,
            'pct': round((inv['with_cuts'] or 0) / total * 100, 1),
            'avg_cuts': round(inv['avg_cuts'] or 0, 1),
        },
        'avg_dom': round(inv['avg_dom'] or 0, 1),
    }


def generate_cohort_performance(db: Database) -> dict:
    """Generate cohort performance table data."""
    conn = db._get_conn()
    cursor = conn.cursor()

    cohorts = []
    for cohort_name, label, days_range in [
        ('new', 'New (<90d)', '< 90 days'),
        ('mid', 'Mid (90-180d)', '90-180 days'),
        ('old', 'Old (180-365d)', '180-365 days'),
        ('toxic', 'Toxic (>365d)', '> 365 days'),
    ]:
        # Get inventory count
        cursor.execute('''
            SELECT COUNT(*) as count
            FROM property_daily_snapshot
            WHERE status = 'FOR_SALE'
              AND cohort = ?
              AND snapshot_date = (SELECT MAX(snapshot_date) FROM property_daily_snapshot)
        ''', (cohort_name,))
        inv_count = cursor.fetchone()['count'] or 0

        # Get sales performance
        cursor.execute('''
            SELECT
                COUNT(*) as sold,
                COUNT(CASE WHEN realized_net IS NOT NULL THEN 1 END) as with_pnl,
                SUM(CASE WHEN realized_net > 0 THEN 1 ELSE 0 END) as profitable,
                AVG(realized_net) as avg_pnl,
                AVG(days_held) as avg_days
            FROM sales_log
            WHERE cohort = ?
        ''', (cohort_name,))
        sales = cursor.fetchone()

        with_pnl = sales['with_pnl'] or 0
        profitable = sales['profitable'] or 0
        win_rate = (profitable / with_pnl * 100) if with_pnl > 0 else 0
        avg_pnl = sales['avg_pnl'] or 0

        cohorts.append({
            'cohort': cohort_name,
            'label': label,
            'days_range': days_range,
            'inventory': inv_count,
            'sold': sales['sold'] or 0,
            'win_rate': round(win_rate, 1),
            'avg_pnl': round(avg_pnl, 0),
            'avg_days': round(sales['avg_days'] or 0, 0),
        })

    conn.close()
    return {
        'generated_at': datetime.now().isoformat(),
        'cohorts': cohorts,
    }


def generate_risk_data(db: Database) -> dict:
    """Generate risk metrics data."""
    conn = db._get_conn()
    cursor = conn.cursor()

    # Geographic risk - underwater by market
    cursor.execute('''
        SELECT
            city,
            state,
            COUNT(*) as total,
            COUNT(CASE WHEN unrealized_pnl < 0 THEN 1 END) as underwater,
            SUM(CASE WHEN unrealized_pnl < 0 THEN unrealized_pnl ELSE 0 END) as underwater_value
        FROM property_daily_snapshot
        WHERE status = 'FOR_SALE'
          AND snapshot_date = (SELECT MAX(snapshot_date) FROM property_daily_snapshot)
        GROUP BY city, state
        HAVING COUNT(CASE WHEN unrealized_pnl < 0 THEN 1 END) > 0
        ORDER BY COUNT(CASE WHEN unrealized_pnl < 0 THEN 1 END) DESC
        LIMIT 10
    ''')

    geo_risk = []
    for row in cursor.fetchall():
        total = row['total'] or 1
        underwater = row['underwater'] or 0
        geo_risk.append({
            'city': row['city'],
            'state': row['state'],
            'total': total,
            'underwater': underwater,
            'underwater_pct': round(underwater / total * 100, 1),
            'underwater_value': round((row['underwater_value'] or 0) / 1000, 1),
        })

    conn.close()
    return {
        'generated_at': datetime.now().isoformat(),
        'geographic_risk': geo_risk,
    }


def generate_market_velocity(db: Database) -> dict:
    """Generate market velocity data."""
    conn = db._get_conn()
    cursor = conn.cursor()

    # Get active listings by market from latest snapshot
    cursor.execute('''
        SELECT
            market,
            city,
            state,
            COUNT(*) as active,
            AVG(days_on_market) as avg_dom,
            AVG(price_cuts_count) as avg_cuts,
            COUNT(CASE WHEN unrealized_pnl < 0 THEN 1 END) as underwater
        FROM property_daily_snapshot
        WHERE status = 'FOR_SALE'
          AND snapshot_date = (SELECT MAX(snapshot_date) FROM property_daily_snapshot)
        GROUP BY market
        ORDER BY COUNT(*) DESC
    ''')

    market_data = {}
    for row in cursor.fetchall():
        market = row['market']
        market_data[market] = {
            'city': row['city'],
            'state': row['state'],
            'active': row['active'],
            'avg_dom': round(row['avg_dom'] or 0, 1),
            'avg_cuts': round(row['avg_cuts'] or 0, 1),
            'underwater': row['underwater'] or 0,
        }

    # Get sales by city (last 90 days)
    cursor.execute('''
        SELECT
            city,
            COUNT(*) as sold_90d
        FROM sales_log
        WHERE sale_date >= date('now', '-90 days')
        GROUP BY city
    ''')

    sales_by_city = {row['city']: row['sold_90d'] for row in cursor.fetchall()}

    # Match sales to markets
    for market, data in market_data.items():
        city = data['city']
        sold = sales_by_city.get(city, 0)
        data['sold_90d'] = sold
        data['turnover'] = round((sold / data['active'] * 100), 1) if data['active'] > 0 else 0

        # Determine action
        if data['turnover'] >= 15:
            data['action'] = 'GROW'
            data['action_class'] = 'grow'
        elif data['turnover'] >= 5:
            data['action'] = 'HOLD'
            data['action_class'] = 'hold'
        else:
            data['action'] = 'PAUSE'
            data['action_class'] = 'pause'

    conn.close()

    # Sort by active count
    sorted_markets = sorted(market_data.items(), key=lambda x: -x[1]['active'])

    return {
        'generated_at': datetime.now().isoformat(),
        'markets': [{'market': k, **v} for k, v in sorted_markets],
    }


def generate_all_dashboard_data():
    """Generate all dashboard data files."""
    db = Database()
    output_dir = Path('outputs')
    output_dir.mkdir(exist_ok=True)

    print("Generating dashboard data...")

    # Combine all data into one file for simpler loading
    dashboard_data = {
        'generated_at': datetime.now().isoformat(),
    }

    # OPS Score
    print("  - OPS Score...")
    ops_data = get_ops_score_for_dashboard()
    dashboard_data['ops_score'] = ops_data
    print(f"    Score: {ops_data['score']} ({ops_data['grade']})")

    # Portfolio Summary
    print("  - Portfolio Summary...")
    portfolio_data = generate_portfolio_summary(db)
    dashboard_data['portfolio'] = portfolio_data
    print(f"    Active: {portfolio_data['total_active']}, Underwater: {portfolio_data['underwater']['count']}")

    # Cohort Performance
    print("  - Cohort Performance...")
    cohort_data = generate_cohort_performance(db)
    dashboard_data['cohorts'] = cohort_data
    print(f"    Cohorts: {len(cohort_data['cohorts'])}")

    # Market Velocity
    print("  - Market Velocity...")
    velocity_data = generate_market_velocity(db)
    dashboard_data['market_velocity'] = velocity_data
    print(f"    Markets: {len(velocity_data['markets'])}")

    # Risk Data
    print("  - Risk Data...")
    risk_data = generate_risk_data(db)
    dashboard_data['risk'] = risk_data
    print(f"    Markets with underwater: {len(risk_data['geographic_risk'])}")

    # Load tape data if exists
    tape_file = output_dir / 'tape' / 'tape.json'
    if tape_file.exists():
        with open(tape_file) as f:
            dashboard_data['tape'] = json.load(f)
        print(f"    Tape items: {len(dashboard_data['tape'].get('tapes', []))}")

    # Save combined data
    with open(output_dir / 'dashboard_data.json', 'w') as f:
        json.dump(dashboard_data, f, indent=2)

    # Also save individual files for backwards compatibility
    with open(output_dir / 'ops_score.json', 'w') as f:
        json.dump(ops_data, f, indent=2)
    with open(output_dir / 'market_velocity.json', 'w') as f:
        json.dump(velocity_data, f, indent=2)

    print(f"\nSaved to {output_dir}/dashboard_data.json")


if __name__ == "__main__":
    generate_all_dashboard_data()
