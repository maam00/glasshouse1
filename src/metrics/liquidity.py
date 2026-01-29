"""
Liquidity Metrics from Property Snapshots
==========================================
Calculates A-grade liquidity metrics from property_daily_snapshot data.

Replaces MLS-based months_of_inventory with:
- Median days to pending (from actual status transitions)
- Hazard rate / survival curves by market
- True active count (deduped from snapshots)

All metrics include sample sizes for confidence grading.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.db.database import Database
from src.config import (
    TURNOVER_GREEN_MIN, TURNOVER_YELLOW_MIN,
    MONTHS_INV_GREEN_MAX, MONTHS_INV_YELLOW_MAX,
    get_confidence_grade, get_signal_status
)

logger = logging.getLogger(__name__)


@dataclass
class LiquidityMetrics:
    """A-grade liquidity metrics from property snapshots."""

    # Core metrics
    active_inventory: int
    total_value: float
    avg_price: float

    # Days to pending (from actual transitions)
    median_days_to_pending: Optional[float]
    mean_days_to_pending: Optional[float]
    days_to_pending_sample_size: int

    # Velocity
    exits_last_30d: int
    exits_last_90d: int
    monthly_velocity: float
    turnover_rate_90d: float

    # Calculated liquidity
    months_of_inventory: Optional[float]

    # Survival curve summary
    survival_rate_30d: Optional[float]  # % still for sale after 30 days
    survival_rate_60d: Optional[float]
    survival_rate_90d: Optional[float]
    hazard_rate_weekly: Optional[float]  # Weekly exit probability

    # Price stress
    pct_with_price_cuts: float
    avg_price_cuts: float
    total_price_cuts: int

    # Quality indicators
    confidence_grade: str  # A, B, or C
    data_coverage: float  # % of days with data in period
    source: str

    # Market breakdown (top 5)
    by_market: Dict[str, Dict]


@dataclass
class MarketLiquidity:
    """Liquidity metrics for a single market."""
    market: str
    active_count: int
    exits_90d: int
    turnover_rate: float
    median_days_to_pending: Optional[float]
    survival_rate_30d: Optional[float]
    sample_size: int


class LiquidityCalculator:
    """Calculate liquidity metrics from property snapshots."""

    def __init__(self, db: Database = None):
        self.db = db or Database()

    def calculate_metrics(self, lookback_days: int = 90) -> LiquidityMetrics:
        """
        Calculate comprehensive liquidity metrics.

        Args:
            lookback_days: Number of days to look back for transitions

        Returns:
            LiquidityMetrics with all A-grade metrics
        """
        # Get current inventory stats
        inventory_stats = self.db.get_inventory_snapshot_stats()

        # Get days-to-pending stats
        pending_stats = self.db.get_days_to_pending_stats(days=lookback_days)

        # Get survival curve data
        survival_data = self.db.get_survival_curve_data(days=lookback_days)

        # Get transition counts for velocity
        pending_30d = len(self.db.get_status_transitions(
            from_status='FOR_SALE', to_status='PENDING', days=30
        ))
        sold_30d = len(self.db.get_status_transitions(
            from_status='FOR_SALE', to_status='SOLD', days=30
        ))
        pending_90d = len(self.db.get_status_transitions(
            from_status='FOR_SALE', to_status='PENDING', days=90
        ))
        sold_90d = len(self.db.get_status_transitions(
            from_status='FOR_SALE', to_status='SOLD', days=90
        ))

        exits_30d = pending_30d + sold_30d
        exits_90d = pending_90d + sold_90d

        # Calculate velocity and turnover
        active_count = inventory_stats.get('active_count', 0)
        monthly_velocity = exits_30d  # 30-day exits = monthly
        turnover_90d = (exits_90d / active_count * 100) if active_count > 0 else 0

        # Calculate months of inventory
        if monthly_velocity > 0:
            months_of_inventory = active_count / monthly_velocity
        else:
            months_of_inventory = None

        # Extract survival rates
        survival_30d = None
        survival_60d = None
        survival_90d = None
        hazard_rate = None

        if survival_data.get('survival_rates'):
            rates = survival_data['survival_rates']
            for rate in rates:
                if rate['day'] == 28:  # ~30 days
                    survival_30d = rate['survival_rate']
                elif rate['day'] == 56:  # ~60 days
                    survival_60d = rate['survival_rate']
                elif rate['day'] == 84:  # ~90 days
                    survival_90d = rate['survival_rate']

            # Average weekly hazard rate
            hazard_rates = [r['hazard_rate'] for r in rates if r['hazard_rate'] > 0]
            if hazard_rates:
                hazard_rate = sum(hazard_rates) / len(hazard_rates)

        # Calculate confidence grade
        sample_size = pending_stats.get('count', 0)
        # Coverage: % of expected data we have (assuming 1 snapshot per day)
        data_coverage = min(100, (sample_size / max(lookback_days / 3, 1)) * 100)
        confidence = get_confidence_grade(data_coverage, sample_size)

        # Get market breakdown
        by_market = self._calculate_market_breakdown(lookback_days)

        return LiquidityMetrics(
            active_inventory=active_count,
            total_value=inventory_stats.get('total_value', 0),
            avg_price=inventory_stats.get('avg_price', 0),
            median_days_to_pending=pending_stats.get('median'),
            mean_days_to_pending=pending_stats.get('mean'),
            days_to_pending_sample_size=sample_size,
            exits_last_30d=exits_30d,
            exits_last_90d=exits_90d,
            monthly_velocity=monthly_velocity,
            turnover_rate_90d=round(turnover_90d, 1),
            months_of_inventory=round(months_of_inventory, 1) if months_of_inventory else None,
            survival_rate_30d=survival_30d,
            survival_rate_60d=survival_60d,
            survival_rate_90d=survival_90d,
            hazard_rate_weekly=round(hazard_rate, 4) if hazard_rate else None,
            pct_with_price_cuts=inventory_stats.get('pct_with_cuts', 0),
            avg_price_cuts=0,  # TODO: calculate from snapshots
            total_price_cuts=inventory_stats.get('total_price_cuts', 0),
            confidence_grade=confidence,
            data_coverage=round(data_coverage, 1),
            source='property_daily_snapshot',
            by_market=by_market,
        )

    def _calculate_market_breakdown(self, lookback_days: int = 90) -> Dict[str, Dict]:
        """Calculate liquidity metrics per market."""
        markets = {}

        # Get inventory by market
        inventory_stats = self.db.get_inventory_snapshot_stats()
        by_market = inventory_stats.get('by_market', {})

        for market, active_count in by_market.items():
            # Get market-specific stats
            pending_stats = self.db.get_days_to_pending_stats(
                days=lookback_days, market=market
            )
            survival_data = self.db.get_survival_curve_data(
                market=market, days=lookback_days
            )

            # Get exits for this market
            exits_90d = len(self.db.get_status_transitions(
                from_status='FOR_SALE', to_status='PENDING',
                days=lookback_days, market=market
            )) + len(self.db.get_status_transitions(
                from_status='FOR_SALE', to_status='SOLD',
                days=lookback_days, market=market
            ))

            turnover = (exits_90d / active_count * 100) if active_count > 0 else 0

            # Extract 30d survival rate
            survival_30d = None
            if survival_data.get('survival_rates'):
                for rate in survival_data['survival_rates']:
                    if rate['day'] == 28:
                        survival_30d = rate['survival_rate']
                        break

            markets[market] = {
                'active_count': active_count,
                'exits_90d': exits_90d,
                'turnover_rate': round(turnover, 1),
                'median_days_to_pending': pending_stats.get('median'),
                'survival_rate_30d': survival_30d,
                'sample_size': pending_stats.get('count', 0),
            }

        # Sort by turnover rate (highest first)
        return dict(sorted(markets.items(), key=lambda x: -x[1]['turnover_rate']))

    def get_signal_pack_data(self) -> Dict[str, Any]:
        """
        Get liquidity data formatted for the Signal Pack dashboard.

        Returns dict suitable for dashboard consumption with confidence badges.
        """
        metrics = self.calculate_metrics()

        # Determine signal status
        if metrics.months_of_inventory:
            liquidity_status = get_signal_status('months_inv', metrics.months_of_inventory)
        else:
            liquidity_status = 'unknown'

        turnover_status = get_signal_status('turnover', metrics.turnover_rate_90d)

        return {
            'signal_name': 'Inventory Liquidity',
            'primary_value': metrics.months_of_inventory,
            'primary_label': 'Months of Inventory',
            'primary_status': liquidity_status,
            'secondary_metrics': [
                {
                    'label': 'Median Days to Pending',
                    'value': metrics.median_days_to_pending,
                    'sample_size': metrics.days_to_pending_sample_size,
                },
                {
                    'label': '90-Day Turnover Rate',
                    'value': f"{metrics.turnover_rate_90d}%",
                    'status': turnover_status,
                },
                {
                    'label': 'Monthly Exits',
                    'value': metrics.monthly_velocity,
                },
                {
                    'label': '30-Day Survival Rate',
                    'value': f"{metrics.survival_rate_30d*100:.1f}%" if metrics.survival_rate_30d else 'N/A',
                },
            ],
            'confidence': {
                'grade': metrics.confidence_grade,
                'coverage': metrics.data_coverage,
                'sample_size': metrics.days_to_pending_sample_size,
                'source': metrics.source,
            },
            'market_breakdown': [
                {
                    'market': market,
                    **data
                }
                for market, data in list(metrics.by_market.items())[:5]
            ],
        }

    def generate_report(self) -> str:
        """Generate ASCII report of liquidity metrics."""
        metrics = self.calculate_metrics()

        lines = []
        lines.append("\n" + "=" * 70)
        lines.append("  LIQUIDITY METRICS (from Property Snapshots)")
        lines.append("  Grade: {} | Coverage: {:.1f}% | Source: {}".format(
            metrics.confidence_grade,
            metrics.data_coverage,
            metrics.source
        ))
        lines.append("=" * 70)

        lines.append(f"\n  INVENTORY")
        lines.append(f"  {'-'*50}")
        lines.append(f"  Active Listings:     {metrics.active_inventory:>10,}")
        lines.append(f"  Total Value:         ${metrics.total_value:>12,.0f}")
        lines.append(f"  Avg Price:           ${metrics.avg_price:>12,.0f}")

        lines.append(f"\n  VELOCITY (from actual transitions)")
        lines.append(f"  {'-'*50}")
        lines.append(f"  Exits (30d):         {metrics.exits_last_30d:>10}")
        lines.append(f"  Exits (90d):         {metrics.exits_last_90d:>10}")
        lines.append(f"  Monthly Velocity:    {metrics.monthly_velocity:>10}")
        lines.append(f"  90-Day Turnover:     {metrics.turnover_rate_90d:>9.1f}%")

        lines.append(f"\n  DAYS TO PENDING (n={metrics.days_to_pending_sample_size})")
        lines.append(f"  {'-'*50}")
        if metrics.median_days_to_pending:
            lines.append(f"  Median:              {metrics.median_days_to_pending:>10.1f} days")
            lines.append(f"  Mean:                {metrics.mean_days_to_pending:>10.1f} days")
        else:
            lines.append(f"  (Insufficient data)")

        lines.append(f"\n  MONTHS OF INVENTORY")
        lines.append(f"  {'-'*50}")
        if metrics.months_of_inventory:
            status = get_signal_status('months_inv', metrics.months_of_inventory)
            icon = "✓" if status == 'green' else "!" if status == 'yellow' else "✗"
            lines.append(f"  {icon} {metrics.months_of_inventory:.1f} months")
        else:
            lines.append(f"  (Cannot calculate - no exits)")

        lines.append(f"\n  SURVIVAL RATES")
        lines.append(f"  {'-'*50}")
        if metrics.survival_rate_30d:
            lines.append(f"  After 30 days:       {metrics.survival_rate_30d*100:>9.1f}% still for sale")
        if metrics.survival_rate_60d:
            lines.append(f"  After 60 days:       {metrics.survival_rate_60d*100:>9.1f}% still for sale")
        if metrics.survival_rate_90d:
            lines.append(f"  After 90 days:       {metrics.survival_rate_90d*100:>9.1f}% still for sale")
        if metrics.hazard_rate_weekly:
            lines.append(f"  Weekly Exit Rate:    {metrics.hazard_rate_weekly*100:>9.2f}%")

        lines.append(f"\n  PRICE STRESS")
        lines.append(f"  {'-'*50}")
        lines.append(f"  With Price Cuts:     {metrics.pct_with_price_cuts:>9.1f}%")
        lines.append(f"  Total Cuts:          {metrics.total_price_cuts:>10}")

        lines.append(f"\n  TOP MARKETS BY TURNOVER")
        lines.append(f"  {'-'*50}")
        for market, data in list(metrics.by_market.items())[:5]:
            lines.append(f"  {market:<20} {data['turnover_rate']:>5.1f}% turnover  (n={data['sample_size']})")

        lines.append("\n" + "=" * 70)

        return "\n".join(lines)


def calculate_liquidity_metrics() -> LiquidityMetrics:
    """Convenience function to calculate liquidity metrics."""
    calculator = LiquidityCalculator()
    return calculator.calculate_metrics()


def get_liquidity_signal_pack() -> Dict[str, Any]:
    """Convenience function to get signal pack data."""
    calculator = LiquidityCalculator()
    return calculator.get_signal_pack_data()


if __name__ == "__main__":
    # Test the calculator
    calc = LiquidityCalculator()
    print(calc.generate_report())
