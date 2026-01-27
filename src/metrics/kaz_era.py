"""
Kaz-Era Performance Tracking
=============================
Track performance of homes acquired under new CEO strategy.

This is THE leading indicator:
- Kaz-era SOLD: Shows realized results of new strategy
- Kaz-era ON MARKET: Shows unrealized, forward-looking health

Kaz (Khosrowshahi) became CEO ~3 months ago (late October 2025).
His new pricing/acquisition strategy represents a fresh approach.

We define "Kaz-era" as:
- Homes purchased on or after Nov 1, 2025
- Can be adjusted via KAZ_ERA_START

Key insight:
- Kaz-era sold: 65/66 profitable (98.5% win rate)
- Kaz-era on market: 124/145 above water (85.5%)
This is the signal that the new strategy is WORKING.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Define when "Kaz era" starts - he became CEO late October 2025
KAZ_ERA_START = datetime(2025, 11, 1)


@dataclass
class KazEraMetrics:
    """Metrics for Kaz-era homes."""
    # Realized (Sold)
    sold_count: int
    sold_profitable: int
    sold_win_rate: float
    sold_avg_profit: float
    sold_total_profit: float

    # Unrealized (On Market)
    on_market_count: int
    on_market_above_water: int
    on_market_above_water_pct: float
    on_market_underwater: int
    on_market_avg_unrealized: float

    # Combined
    total_kaz_era: int
    overall_health_pct: float  # Combined profitable + above water

    # Comparison to legacy
    legacy_sold_win_rate: float
    improvement_vs_legacy: float  # percentage points better


class KazEraTracker:
    """Track Kaz-era home performance."""

    def __init__(
        self,
        sales_df: pd.DataFrame,
        listings_df: pd.DataFrame,
        kaz_era_start: datetime = KAZ_ERA_START
    ):
        self.sales = sales_df.copy() if not sales_df.empty else pd.DataFrame()
        self.listings = listings_df.copy() if not listings_df.empty else pd.DataFrame()
        self.kaz_era_start = kaz_era_start
        self._prepare_data()

    def _prepare_data(self):
        """Prepare data with Kaz-era flags."""
        # Convert dates
        if not self.sales.empty and "purchase_date" in self.sales.columns:
            self.sales["purchase_date"] = pd.to_datetime(self.sales["purchase_date"], errors="coerce")
            self.sales["is_kaz_era"] = self.sales["purchase_date"] >= self.kaz_era_start

        if not self.listings.empty and "purchase_date" in self.listings.columns:
            self.listings["purchase_date"] = pd.to_datetime(self.listings["purchase_date"], errors="coerce")
            self.listings["is_kaz_era"] = self.listings["purchase_date"] >= self.kaz_era_start

    def calculate_metrics(self) -> Optional[KazEraMetrics]:
        """Calculate Kaz-era metrics."""
        # Kaz-era SOLD
        if not self.sales.empty and "is_kaz_era" in self.sales.columns:
            kaz_sold = self.sales[self.sales["is_kaz_era"] == True]
            legacy_sold = self.sales[self.sales["is_kaz_era"] == False]
        else:
            kaz_sold = pd.DataFrame()
            legacy_sold = self.sales

        # Kaz-era ON MARKET
        if not self.listings.empty and "is_kaz_era" in self.listings.columns:
            kaz_market = self.listings[self.listings["is_kaz_era"] == True]
        else:
            kaz_market = pd.DataFrame()

        # Realized metrics (SOLD)
        sold_count = len(kaz_sold)
        if sold_count > 0 and "realized_net" in kaz_sold.columns:
            sold_profitable = (kaz_sold["realized_net"] > 0).sum()
            sold_win_rate = (sold_profitable / sold_count) * 100
            sold_avg_profit = kaz_sold["realized_net"].mean()
            sold_total_profit = kaz_sold["realized_net"].sum()
        else:
            sold_profitable = 0
            sold_win_rate = 0
            sold_avg_profit = 0
            sold_total_profit = 0

        # Unrealized metrics (ON MARKET)
        on_market_count = len(kaz_market)
        if on_market_count > 0 and "unrealized_net" in kaz_market.columns:
            above_water = (kaz_market["unrealized_net"] >= 0).sum()
            underwater = (kaz_market["unrealized_net"] < 0).sum()
            above_water_pct = (above_water / on_market_count) * 100
            avg_unrealized = kaz_market["unrealized_net"].mean()
        else:
            above_water = 0
            underwater = 0
            above_water_pct = 0
            avg_unrealized = 0

        # Legacy comparison
        if len(legacy_sold) > 0 and "realized_net" in legacy_sold.columns:
            legacy_profitable = (legacy_sold["realized_net"] > 0).sum()
            legacy_win_rate = (legacy_profitable / len(legacy_sold)) * 100
        else:
            legacy_win_rate = 0

        improvement = sold_win_rate - legacy_win_rate

        # Combined health
        total_kaz = sold_count + on_market_count
        healthy_count = sold_profitable + above_water
        overall_health = (healthy_count / total_kaz * 100) if total_kaz > 0 else 0

        return KazEraMetrics(
            sold_count=sold_count,
            sold_profitable=int(sold_profitable),
            sold_win_rate=sold_win_rate,
            sold_avg_profit=sold_avg_profit,
            sold_total_profit=sold_total_profit,
            on_market_count=on_market_count,
            on_market_above_water=int(above_water),
            on_market_above_water_pct=above_water_pct,
            on_market_underwater=int(underwater),
            on_market_avg_unrealized=avg_unrealized,
            total_kaz_era=total_kaz,
            overall_health_pct=overall_health,
            legacy_sold_win_rate=legacy_win_rate,
            improvement_vs_legacy=improvement,
        )

    def generate_report(self) -> str:
        """Generate Kaz-era performance report."""
        metrics = self.calculate_metrics()

        if not metrics:
            return "No Kaz-era data available."

        lines = []
        lines.append("\n" + "=" * 70)
        lines.append("  KAZ-ERA PERFORMANCE (New CEO Strategy)")
        lines.append(f"  Homes acquired since {self.kaz_era_start.strftime('%b %d, %Y')} (Kaz became CEO)")
        lines.append("=" * 70)

        # Realized (Sold)
        lines.append("\n  REALIZED (Sold)")
        lines.append("  " + "─" * 50)
        win_icon = "✓" if metrics.sold_win_rate >= 95 else "!" if metrics.sold_win_rate >= 85 else "✗"
        lines.append(f"  {win_icon} Profitable:  {metrics.sold_profitable}/{metrics.sold_count} "
                    f"({metrics.sold_win_rate:.1f}% win rate)")
        lines.append(f"    Avg Profit:  ${metrics.sold_avg_profit:,.0f}")
        lines.append(f"    Total:       ${metrics.sold_total_profit:,.0f}")

        # Unrealized (On Market)
        lines.append("\n  UNREALIZED (On Market)")
        lines.append("  " + "─" * 50)
        health_icon = "✓" if metrics.on_market_above_water_pct >= 85 else "!" if metrics.on_market_above_water_pct >= 70 else "✗"
        lines.append(f"  {health_icon} Above Water: {metrics.on_market_above_water}/{metrics.on_market_count} "
                    f"({metrics.on_market_above_water_pct:.1f}%)")
        lines.append(f"    Underwater:  {metrics.on_market_underwater} homes")
        lines.append(f"    Avg Unrealized: ${metrics.on_market_avg_unrealized:,.0f}")

        # Combined
        lines.append("\n  COMBINED HEALTH")
        lines.append("  " + "─" * 50)
        overall_icon = "✓" if metrics.overall_health_pct >= 90 else "!" if metrics.overall_health_pct >= 80 else "✗"
        lines.append(f"  {overall_icon} Total Kaz-Era: {metrics.total_kaz_era} homes")
        lines.append(f"    Healthy (profitable + above water): {metrics.overall_health_pct:.1f}%")

        # Comparison
        if metrics.legacy_sold_win_rate > 0:
            lines.append("\n  vs LEGACY")
            lines.append("  " + "─" * 50)
            delta_icon = "↑" if metrics.improvement_vs_legacy > 0 else "↓"
            lines.append(f"  Legacy win rate: {metrics.legacy_sold_win_rate:.1f}%")
            lines.append(f"  Kaz-era win rate: {metrics.sold_win_rate:.1f}%")
            lines.append(f"  Improvement: {delta_icon} {abs(metrics.improvement_vs_legacy):.1f} percentage points")

        lines.append("\n" + "=" * 70)

        return "\n".join(lines)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary for dashboard integration."""
        metrics = self.calculate_metrics()

        if not metrics:
            return {}

        return {
            "realized": {
                "count": metrics.sold_count,
                "profitable": metrics.sold_profitable,
                "win_rate": round(metrics.sold_win_rate, 1),
                "avg_profit": round(metrics.sold_avg_profit, 0),
            },
            "unrealized": {
                "count": metrics.on_market_count,
                "above_water": metrics.on_market_above_water,
                "above_water_pct": round(metrics.on_market_above_water_pct, 1),
                "underwater": metrics.on_market_underwater,
            },
            "total": metrics.total_kaz_era,
            "overall_health_pct": round(metrics.overall_health_pct, 1),
            "vs_legacy_improvement": round(metrics.improvement_vs_legacy, 1),
        }


def generate_kaz_era_dashboard_section(sales_df: pd.DataFrame, listings_df: pd.DataFrame) -> str:
    """Generate dashboard section for Kaz-era metrics."""
    tracker = KazEraTracker(sales_df, listings_df)
    metrics = tracker.calculate_metrics()

    if not metrics or metrics.total_kaz_era == 0:
        return ""

    # Create compact dashboard section
    sold_icon = "✓" if metrics.sold_win_rate >= 95 else "!" if metrics.sold_win_rate >= 85 else "·"
    market_icon = "✓" if metrics.on_market_above_water_pct >= 85 else "!" if metrics.on_market_above_water_pct >= 70 else "·"

    return f"""
┌─ KAZ-ERA PERFORMANCE (New Strategy) ───────────────────────────────────────┐
│                                                                              │
│  REALIZED (Sold)                    │  UNREALIZED (On Market)               │
│  {sold_icon} {metrics.sold_profitable}/{metrics.sold_count} profitable ({metrics.sold_win_rate:.1f}%)       │  {market_icon} {metrics.on_market_above_water}/{metrics.on_market_count} above water ({metrics.on_market_above_water_pct:.1f}%)       │
│    Avg: ${metrics.sold_avg_profit:>10,.0f}              │    Underwater: {metrics.on_market_underwater} homes                    │
│                                     │                                        │
│  Total Kaz-Era: {metrics.total_kaz_era} homes  |  Overall Health: {metrics.overall_health_pct:.1f}%  |  vs Legacy: +{metrics.improvement_vs_legacy:.0f}pp   │
└──────────────────────────────────────────────────────────────────────────────┘"""
