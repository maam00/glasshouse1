"""
Pending/Under Contract Tracker
==============================
Tracks the sales funnel from Active → Pending → Sold.

Key insights:
- Conversion rate: What % of pending actually close?
- Fall-through rate: What % go back to active?
- Time in pending: How long from pending to close?
- Toxic clearance: Are >365 day homes getting under contract?
- Kaz vs Legacy: Performance comparison
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import json
from pathlib import Path

import pandas as pd
import numpy as np

from src.config import KAZ_ERA_START, is_kaz_era as config_is_kaz_era

logger = logging.getLogger(__name__)


@dataclass
class PendingHome:
    """Individual pending home data."""
    address: str
    city: str
    state: str
    list_price: float
    pending_date: str
    days_on_market: int
    days_held: int
    cohort: str  # new, mid, old, toxic
    is_kaz_era: bool
    purchase_date: str
    purchase_price: float
    expected_profit: float
    property_id: str = ""


@dataclass
class PendingMetrics:
    """Aggregated pending metrics."""
    total_pending: int
    total_pending_value: float

    # Era breakdown
    kaz_era_count: int
    legacy_count: int

    # Cohort breakdown
    new_cohort_pending: int
    mid_cohort_pending: int
    old_cohort_pending: int
    toxic_cohort_pending: int

    # Key metrics
    avg_days_on_market: float
    avg_days_held: float
    avg_expected_profit: float

    # Funnel metrics (if historical data available)
    conversion_rate: float  # % that close
    fall_through_rate: float  # % that go back to active
    avg_days_in_pending: float

    # Clearance progress
    toxic_pending_pct: float  # What % of pending are toxic homes


@dataclass
class FunnelSnapshot:
    """Point-in-time funnel snapshot for tracking changes."""
    date: str
    active_count: int
    pending_count: int
    sold_count: int

    # Transitions
    new_to_pending: int
    pending_to_sold: int
    pending_to_active: int  # Fall-throughs


class PendingTracker:
    """Track pending listings and funnel metrics."""

    def __init__(self, data_dir: Path = None):
        self.data_dir = data_dir or Path(__file__).parent.parent.parent / "outputs"
        self.history: List[FunnelSnapshot] = []
        self._load_history()

    def _load_history(self):
        """Load historical funnel snapshots."""
        history_file = self.data_dir / "pending_history.json"
        if history_file.exists():
            try:
                with open(history_file) as f:
                    data = json.load(f)
                self.history = [FunnelSnapshot(**s) for s in data]
            except Exception as e:
                logger.warning(f"Could not load pending history: {e}")

    def _save_history(self):
        """Save funnel history."""
        history_file = self.data_dir / "pending_history.json"
        with open(history_file, 'w') as f:
            json.dump([asdict(s) for s in self.history], f, indent=2)

    def classify_cohort(self, days_held: int) -> str:
        """Classify home into cohort based on days held."""
        if days_held < 90:
            return "new"
        elif days_held < 180:
            return "mid"
        elif days_held < 365:
            return "old"
        else:
            return "toxic"

    def is_kaz_era(self, purchase_date: str) -> bool:
        """Check if purchase was in Kaz era.

        Uses the canonical KAZ_ERA_START from src/config.py
        """
        return config_is_kaz_era(purchase_date)

    def analyze_pending_listings(
        self,
        pending_df: pd.DataFrame,
        sales_df: pd.DataFrame = None,
        listings_df: pd.DataFrame = None,
    ) -> PendingMetrics:
        """
        Analyze pending listings and calculate metrics.

        Args:
            pending_df: DataFrame of pending/under-contract homes
            sales_df: Historical sales for conversion tracking
            listings_df: Current active listings for funnel context
        """
        if pending_df.empty:
            return self._empty_metrics()

        # Ensure numeric columns
        pending_df = pending_df.copy()

        # Parse days on market
        if 'days_on_market' in pending_df.columns:
            pending_df['dom'] = pd.to_numeric(pending_df['days_on_market'], errors='coerce').fillna(0)
        elif 'od_days_on_market' in pending_df.columns:
            pending_df['dom'] = pd.to_numeric(pending_df['od_days_on_market'], errors='coerce').fillna(0)
        else:
            pending_df['dom'] = 0

        # Parse days held (may need to calculate from purchase date)
        if 'days_held' in pending_df.columns:
            pending_df['held'] = pd.to_numeric(pending_df['days_held'], errors='coerce').fillna(0)
        elif 'od_purchase_date' in pending_df.columns:
            pending_df['purchase_dt'] = pd.to_datetime(pending_df['od_purchase_date'], errors='coerce')
            pending_df['held'] = (datetime.now() - pending_df['purchase_dt']).dt.days.fillna(0)
        else:
            pending_df['held'] = pending_df['dom']  # Fallback to DOM

        # Classify cohorts
        pending_df['cohort'] = pending_df['held'].apply(self.classify_cohort)

        # Kaz era classification
        if 'od_purchase_date' in pending_df.columns:
            pending_df['is_kaz'] = pending_df['od_purchase_date'].apply(self.is_kaz_era)
        else:
            pending_df['is_kaz'] = False

        # Calculate expected profit
        if 'list_price' in pending_df.columns and 'od_purchase_price' in pending_df.columns:
            list_p = pd.to_numeric(pending_df['list_price'], errors='coerce').fillna(0)
            purch_p = pd.to_numeric(pending_df['od_purchase_price'], errors='coerce').fillna(0)
            pending_df['expected_profit'] = list_p - purch_p
        else:
            pending_df['expected_profit'] = 0

        # Aggregate metrics
        total = len(pending_df)

        # Era breakdown
        kaz_count = pending_df['is_kaz'].sum()
        legacy_count = total - kaz_count

        # Cohort breakdown
        cohort_counts = pending_df['cohort'].value_counts()
        new_pending = cohort_counts.get('new', 0)
        mid_pending = cohort_counts.get('mid', 0)
        old_pending = cohort_counts.get('old', 0)
        toxic_pending = cohort_counts.get('toxic', 0)

        # Averages
        avg_dom = pending_df['dom'].mean() if total > 0 else 0
        avg_held = pending_df['held'].mean() if total > 0 else 0
        avg_profit = pending_df['expected_profit'].mean() if total > 0 else 0

        # Total value
        if 'list_price' in pending_df.columns:
            total_value = pd.to_numeric(pending_df['list_price'], errors='coerce').sum()
        else:
            total_value = 0

        # Funnel metrics (from history if available)
        conversion_rate = self._calculate_conversion_rate()
        fall_through_rate = self._calculate_fall_through_rate()
        avg_days_pending = self._calculate_avg_days_pending()

        # Toxic percentage
        toxic_pct = (toxic_pending / total * 100) if total > 0 else 0

        return PendingMetrics(
            total_pending=total,
            total_pending_value=round(total_value, 0),
            kaz_era_count=int(kaz_count),
            legacy_count=int(legacy_count),
            new_cohort_pending=int(new_pending),
            mid_cohort_pending=int(mid_pending),
            old_cohort_pending=int(old_pending),
            toxic_cohort_pending=int(toxic_pending),
            avg_days_on_market=round(avg_dom, 1),
            avg_days_held=round(avg_held, 1),
            avg_expected_profit=round(avg_profit, 0),
            conversion_rate=round(conversion_rate, 1),
            fall_through_rate=round(fall_through_rate, 1),
            avg_days_in_pending=round(avg_days_pending, 1),
            toxic_pending_pct=round(toxic_pct, 1),
        )

    def _empty_metrics(self) -> PendingMetrics:
        """Return empty metrics."""
        return PendingMetrics(
            total_pending=0,
            total_pending_value=0,
            kaz_era_count=0,
            legacy_count=0,
            new_cohort_pending=0,
            mid_cohort_pending=0,
            old_cohort_pending=0,
            toxic_cohort_pending=0,
            avg_days_on_market=0,
            avg_days_held=0,
            avg_expected_profit=0,
            conversion_rate=0,
            fall_through_rate=0,
            avg_days_in_pending=0,
            toxic_pending_pct=0,
        )

    def _calculate_conversion_rate(self) -> float:
        """Calculate pending → sold conversion rate from history."""
        if len(self.history) < 2:
            return 95.0  # Default assumption

        # Look at recent history
        recent = self.history[-30:] if len(self.history) >= 30 else self.history

        total_pending_to_sold = sum(s.pending_to_sold for s in recent)
        total_pending_to_active = sum(s.pending_to_active for s in recent)
        total_transitions = total_pending_to_sold + total_pending_to_active

        if total_transitions == 0:
            return 95.0

        return (total_pending_to_sold / total_transitions) * 100

    def _calculate_fall_through_rate(self) -> float:
        """Calculate fall-through rate (pending → active)."""
        conversion = self._calculate_conversion_rate()
        return 100 - conversion

    def _calculate_avg_days_pending(self) -> float:
        """Calculate average days in pending status."""
        # Would need to track individual homes through the funnel
        # Default to industry average
        return 30.0

    def record_snapshot(
        self,
        active_count: int,
        pending_count: int,
        sold_count: int,
        previous_snapshot: FunnelSnapshot = None,
    ) -> FunnelSnapshot:
        """Record a new funnel snapshot."""
        today = datetime.now().strftime('%Y-%m-%d')

        # Calculate transitions if we have previous data
        if previous_snapshot:
            # Simplified: assume changes are transitions
            new_to_pending = max(0, pending_count - previous_snapshot.pending_count + sold_count - previous_snapshot.sold_count)
            pending_to_sold = sold_count - previous_snapshot.sold_count
            pending_to_active = max(0, active_count - previous_snapshot.active_count)
        else:
            new_to_pending = 0
            pending_to_sold = 0
            pending_to_active = 0

        snapshot = FunnelSnapshot(
            date=today,
            active_count=active_count,
            pending_count=pending_count,
            sold_count=sold_count,
            new_to_pending=new_to_pending,
            pending_to_sold=pending_to_sold,
            pending_to_active=pending_to_active,
        )

        self.history.append(snapshot)
        self._save_history()

        return snapshot

    def get_funnel_summary(self) -> Dict[str, Any]:
        """Get funnel summary for dashboard."""
        if not self.history:
            return {}

        latest = self.history[-1]
        week_ago = None
        if len(self.history) >= 7:
            week_ago = self.history[-7]

        summary = {
            'current': {
                'date': latest.date,
                'active': latest.active_count,
                'pending': latest.pending_count,
                'sold': latest.sold_count,
            },
            'conversion_rate': self._calculate_conversion_rate(),
            'fall_through_rate': self._calculate_fall_through_rate(),
        }

        if week_ago:
            summary['wow_change'] = {
                'active': latest.active_count - week_ago.active_count,
                'pending': latest.pending_count - week_ago.pending_count,
                'sold': latest.sold_count - week_ago.sold_count,
            }

        return summary

    def generate_report(self, metrics: PendingMetrics) -> str:
        """Generate ASCII report for terminal output."""
        lines = []
        lines.append("\n" + "=" * 70)
        lines.append("  PENDING FUNNEL ANALYSIS")
        lines.append("=" * 70)

        lines.append(f"\n  OVERVIEW")
        lines.append(f"  {'─' * 50}")
        lines.append(f"  Total Pending:        {metrics.total_pending:>6}")
        lines.append(f"  Total Value:          ${metrics.total_pending_value:>12,.0f}")
        lines.append(f"  Avg Expected Profit:  ${metrics.avg_expected_profit:>12,.0f}")

        lines.append(f"\n  ERA BREAKDOWN")
        lines.append(f"  {'─' * 50}")
        lines.append(f"  Kaz-Era Pending:      {metrics.kaz_era_count:>6}  (acquired after Oct 2023)")
        lines.append(f"  Legacy Pending:       {metrics.legacy_count:>6}  (pre-Kaz acquisitions)")

        lines.append(f"\n  COHORT BREAKDOWN (by days held)")
        lines.append(f"  {'─' * 50}")
        lines.append(f"  New (<90d):           {metrics.new_cohort_pending:>6}")
        lines.append(f"  Mid (90-180d):        {metrics.mid_cohort_pending:>6}")
        lines.append(f"  Old (180-365d):       {metrics.old_cohort_pending:>6}")
        lines.append(f"  Toxic (>365d):        {metrics.toxic_cohort_pending:>6}  ← {metrics.toxic_pending_pct:.1f}% of pending")

        lines.append(f"\n  VELOCITY")
        lines.append(f"  {'─' * 50}")
        lines.append(f"  Avg Days on Market:   {metrics.avg_days_on_market:>6.1f}")
        lines.append(f"  Avg Days Held:        {metrics.avg_days_held:>6.1f}")

        lines.append(f"\n  FUNNEL HEALTH")
        lines.append(f"  {'─' * 50}")
        lines.append(f"  Conversion Rate:      {metrics.conversion_rate:>5.1f}%  (pending → sold)")
        lines.append(f"  Fall-Through Rate:    {metrics.fall_through_rate:>5.1f}%  (pending → back to active)")

        # Key insight
        if metrics.toxic_cohort_pending > 0:
            lines.append(f"\n  ✓ POSITIVE: {metrics.toxic_cohort_pending} toxic homes are pending")
            lines.append(f"    This represents {metrics.toxic_pending_pct:.1f}% of the pending pipeline")

        lines.append("\n" + "=" * 70)

        return "\n".join(lines)
