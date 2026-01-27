"""
Pricing Analysis by Cohort
===========================
Are we pricing new homes better than old ones?

Key questions:
1. What % of each cohort has had price cuts?
2. How deep are the cuts by cohort?
3. Is the new cohort showing better pricing discipline?
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class CohortPricingMetrics:
    """Pricing metrics for a cohort."""
    cohort_name: str
    count: int

    # Price cuts
    homes_with_cuts: int
    pct_with_cuts: float
    avg_cuts_per_home: float
    avg_cut_pct: float  # Average % reduction

    # Spread analysis
    avg_purchase_price: float
    avg_list_price: float
    avg_spread: float
    avg_spread_pct: float

    # Underwater
    underwater_count: int
    underwater_pct: float


class PricingAnalyzer:
    """Analyze pricing strategy by cohort."""

    # Cohort definitions (days on market)
    COHORTS = {
        "Fresh": (0, 30),
        "Normal": (31, 90),
        "Stale": (91, 180),
        "VeryStale": (181, 365),
        "Toxic": (366, 9999),
    }

    def __init__(self, listings_df: pd.DataFrame):
        self.listings = listings_df.copy() if not listings_df.empty else pd.DataFrame()

    def analyze_cohort(self, cohort_name: str, min_dom: int, max_dom: int) -> Optional[CohortPricingMetrics]:
        """Analyze pricing for a specific cohort."""
        if self.listings.empty:
            return None

        # Filter to cohort
        df = self.listings.copy()
        if "days_on_market" not in df.columns:
            return None

        cohort = df[(df["days_on_market"] >= min_dom) & (df["days_on_market"] <= max_dom)]

        if cohort.empty:
            return None

        count = len(cohort)

        # Price cuts analysis
        if "price_cuts" in cohort.columns:
            homes_with_cuts = (cohort["price_cuts"] > 0).sum()
            pct_with_cuts = (homes_with_cuts / count) * 100
            avg_cuts = cohort["price_cuts"].mean()
        else:
            homes_with_cuts = 0
            pct_with_cuts = 0
            avg_cuts = 0

        # Calculate average cut percentage
        if "initial_list_price" in cohort.columns and "list_price" in cohort.columns:
            price_diff = cohort["initial_list_price"] - cohort["list_price"]
            cut_pct = (price_diff / cohort["initial_list_price"]) * 100
            avg_cut_pct = cut_pct[cut_pct > 0].mean() if (cut_pct > 0).any() else 0
        else:
            avg_cut_pct = 0

        # Spread analysis
        if "purchase_price" in cohort.columns and "list_price" in cohort.columns:
            avg_purchase = cohort["purchase_price"].mean()
            avg_list = cohort["list_price"].mean()
            avg_spread = avg_list - avg_purchase
            avg_spread_pct = (avg_spread / avg_purchase * 100) if avg_purchase > 0 else 0
        else:
            avg_purchase = 0
            avg_list = 0
            avg_spread = 0
            avg_spread_pct = 0

        # Underwater analysis
        if "unrealized_net" in cohort.columns:
            underwater = (cohort["unrealized_net"] < 0).sum()
            underwater_pct = (underwater / count) * 100
        else:
            underwater = 0
            underwater_pct = 0

        return CohortPricingMetrics(
            cohort_name=cohort_name,
            count=count,
            homes_with_cuts=homes_with_cuts,
            pct_with_cuts=pct_with_cuts,
            avg_cuts_per_home=avg_cuts,
            avg_cut_pct=avg_cut_pct,
            avg_purchase_price=avg_purchase,
            avg_list_price=avg_list,
            avg_spread=avg_spread,
            avg_spread_pct=avg_spread_pct,
            underwater_count=underwater,
            underwater_pct=underwater_pct,
        )

    def analyze_all_cohorts(self) -> Dict[str, CohortPricingMetrics]:
        """Analyze pricing for all cohorts."""
        results = {}
        for name, (min_dom, max_dom) in self.COHORTS.items():
            metrics = self.analyze_cohort(name, min_dom, max_dom)
            if metrics:
                results[name] = metrics
        return results

    def generate_report(self) -> str:
        """Generate ASCII pricing analysis report."""
        cohorts = self.analyze_all_cohorts()

        if not cohorts:
            return "No pricing data available."

        lines = []
        lines.append("\n" + "=" * 78)
        lines.append("  PRICING ANALYSIS BY COHORT")
        lines.append("=" * 78)

        # Header
        lines.append("")
        lines.append(f"  {'Cohort':<10} {'Count':>6} {'Cut%':>7} {'AvgCuts':>8} {'CutDepth':>9} {'UW%':>6}")
        lines.append("  " + "─" * 58)

        for name, m in cohorts.items():
            cut_depth_str = f"{m.avg_cut_pct:.1f}%" if m.avg_cut_pct > 0 else "0%"
            lines.append(
                f"  {name:<10} {m.count:>6} {m.pct_with_cuts:>6.1f}% {m.avg_cuts_per_home:>7.1f} "
                f"{cut_depth_str:>9} {m.underwater_pct:>5.1f}%"
            )

        # Insights
        lines.append("")
        lines.append("  " + "─" * 58)
        lines.append("  INSIGHTS")
        lines.append("  " + "─" * 58)

        # Compare fresh vs toxic
        fresh = cohorts.get("Fresh")
        toxic = cohorts.get("Toxic")

        if fresh and toxic:
            lines.append(f"  Fresh cohort cut rate:    {fresh.pct_with_cuts:.1f}%")
            lines.append(f"  Toxic cohort cut rate:    {toxic.pct_with_cuts:.1f}%")

            if fresh.pct_with_cuts < toxic.pct_with_cuts * 0.5:
                lines.append("  ✓ New inventory being priced better")
            else:
                lines.append("  ! New inventory still seeing significant cuts")

        # Underwater trend
        if fresh and toxic:
            if fresh.underwater_pct < toxic.underwater_pct * 0.5:
                lines.append("  ✓ Underwater exposure concentrated in legacy inventory")
            else:
                lines.append("  ! Underwater risk present even in fresh inventory")

        lines.append("\n" + "=" * 78)

        return "\n".join(lines)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary for dashboard integration."""
        cohorts = self.analyze_all_cohorts()

        return {
            name: {
                "count": m.count,
                "pct_with_cuts": round(m.pct_with_cuts, 1),
                "avg_cuts": round(m.avg_cuts_per_home, 1),
                "avg_cut_pct": round(m.avg_cut_pct, 1),
                "underwater_pct": round(m.underwater_pct, 1),
                "avg_spread": round(m.avg_spread, 0),
            }
            for name, m in cohorts.items()
        }


def analyze_pricing_discipline(listings_df: pd.DataFrame) -> str:
    """Quick pricing discipline check."""
    analyzer = PricingAnalyzer(listings_df)
    return analyzer.generate_report()
