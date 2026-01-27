"""
Velocity Decomposition Analysis
================================
Breaking down WHERE time is lost in the iBuying cycle.

Full cycle:
  Offer → Close (acquisition) → Renovation → List → Sale

We have:
  - purchase_date (when we closed on acquisition)
  - initial_list_date (when we first listed)
  - sale_date (when we sold)
  - days_held (total days owned)
  - days_on_market (days listed)

We can calculate:
  - Days from purchase to list (renovation + prep time)
  - Days on market to sale
  - Compare new vs old cohort velocity
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class VelocityBreakdown:
    """Breakdown of time in each stage."""
    # Stages
    days_to_list: float  # purchase → first listing
    days_on_market: float  # listing → sale
    total_days: float  # purchase → sale

    # Counts
    sample_size: int

    # Percentiles
    p25_total: float
    p50_total: float
    p75_total: float

    # By cohort (if available)
    new_cohort_velocity: float
    old_cohort_velocity: float


@dataclass
class VelocityTrend:
    """Velocity trend over time."""
    period: str
    avg_days_to_list: float
    avg_days_on_market: float
    avg_total_days: float
    sample_size: int


class VelocityAnalyzer:
    """Analyze velocity at each stage of the iBuying cycle."""

    def __init__(self, sales_df: pd.DataFrame, listings_df: pd.DataFrame):
        self.sales = sales_df.copy() if not sales_df.empty else pd.DataFrame()
        self.listings = listings_df.copy() if not listings_df.empty else pd.DataFrame()
        self._prepare_data()

    def _prepare_data(self):
        """Prepare and merge data for velocity analysis."""
        if self.sales.empty:
            return

        # Ensure date columns are datetime
        date_cols = ["purchase_date", "sale_date"]
        for col in date_cols:
            if col in self.sales.columns:
                self.sales[col] = pd.to_datetime(self.sales[col], errors="coerce")

        if not self.listings.empty:
            listing_date_cols = ["purchase_date", "initial_list_date", "latest_list_date"]
            for col in listing_date_cols:
                if col in self.listings.columns:
                    self.listings[col] = pd.to_datetime(self.listings[col], errors="coerce")

            # Try to enrich sales with listing dates via property_id
            if "property_id" in self.sales.columns and "property_id" in self.listings.columns:
                listing_dates = self.listings[["property_id", "initial_list_date", "days_on_market"]].drop_duplicates()
                listing_dates = listing_dates.set_index("property_id")

                for col in ["initial_list_date", "days_on_market"]:
                    if col not in self.sales.columns:
                        self.sales[col] = self.sales["property_id"].map(
                            listing_dates[col].to_dict() if col in listing_dates.columns else {}
                        )

    def calculate_velocity_breakdown(self) -> Optional[VelocityBreakdown]:
        """Calculate velocity breakdown for all sales."""
        if self.sales.empty:
            return None

        df = self.sales.copy()

        # Calculate days to list (purchase → initial list)
        if "purchase_date" in df.columns and "initial_list_date" in df.columns:
            df["days_to_list"] = (df["initial_list_date"] - df["purchase_date"]).dt.days
            df["days_to_list"] = df["days_to_list"].clip(lower=0)  # Can't be negative
        else:
            df["days_to_list"] = np.nan

        # Get days on market
        if "days_on_market" not in df.columns and "days_held" in df.columns and "days_to_list" in df.columns:
            df["days_on_market"] = df["days_held"] - df["days_to_list"]
        elif "days_on_market" not in df.columns:
            df["days_on_market"] = np.nan

        # Total days
        if "days_held" not in df.columns:
            if "purchase_date" in df.columns and "sale_date" in df.columns:
                df["days_held"] = (df["sale_date"] - df["purchase_date"]).dt.days
            else:
                df["days_held"] = np.nan

        # Calculate averages
        valid = df.dropna(subset=["days_held"])

        if valid.empty:
            return None

        avg_days_to_list = valid["days_to_list"].mean() if "days_to_list" in valid.columns else 0
        avg_dom = valid["days_on_market"].mean() if "days_on_market" in valid.columns else 0
        avg_total = valid["days_held"].mean()

        # Percentiles
        p25 = valid["days_held"].quantile(0.25)
        p50 = valid["days_held"].quantile(0.50)
        p75 = valid["days_held"].quantile(0.75)

        # By cohort
        new_cohort = valid[valid["days_held"] < 90] if "days_held" in valid.columns else pd.DataFrame()
        old_cohort = valid[valid["days_held"] >= 180] if "days_held" in valid.columns else pd.DataFrame()

        return VelocityBreakdown(
            days_to_list=avg_days_to_list if not np.isnan(avg_days_to_list) else 0,
            days_on_market=avg_dom if not np.isnan(avg_dom) else 0,
            total_days=avg_total,
            sample_size=len(valid),
            p25_total=p25,
            p50_total=p50,
            p75_total=p75,
            new_cohort_velocity=new_cohort["days_held"].mean() if not new_cohort.empty else 0,
            old_cohort_velocity=old_cohort["days_held"].mean() if not old_cohort.empty else 0,
        )

    def analyze_listing_velocity(self) -> Dict[str, Any]:
        """Analyze velocity for current listings (unsold inventory)."""
        if self.listings.empty:
            return {}

        df = self.listings.copy()

        # Calculate time to list from purchase
        if "purchase_date" in df.columns and "initial_list_date" in df.columns:
            df["days_to_list"] = (df["initial_list_date"] - df["purchase_date"]).dt.days
        else:
            df["days_to_list"] = np.nan

        # Days since listing
        if "days_on_market" in df.columns:
            dom = df["days_on_market"]
        else:
            dom = pd.Series([np.nan] * len(df))

        # Bucket by days on market
        buckets = {
            "0-30": len(df[dom <= 30]),
            "31-60": len(df[(dom > 30) & (dom <= 60)]),
            "61-90": len(df[(dom > 60) & (dom <= 90)]),
            "91-180": len(df[(dom > 90) & (dom <= 180)]),
            "181-365": len(df[(dom > 180) & (dom <= 365)]),
            "365+": len(df[dom > 365]),
        }

        return {
            "total_listings": len(df),
            "avg_dom": dom.mean() if not dom.isna().all() else 0,
            "median_dom": dom.median() if not dom.isna().all() else 0,
            "avg_days_to_list": df["days_to_list"].mean() if "days_to_list" in df.columns else 0,
            "dom_buckets": buckets,
            "stale_pct": (buckets.get("181-365", 0) + buckets.get("365+", 0)) / len(df) * 100 if len(df) > 0 else 0,
        }

    def generate_velocity_report(self) -> str:
        """Generate ASCII velocity report."""
        breakdown = self.calculate_velocity_breakdown()
        listing_analysis = self.analyze_listing_velocity()

        lines = []
        lines.append("\n" + "=" * 70)
        lines.append("  VELOCITY ANALYSIS")
        lines.append("=" * 70)

        if breakdown:
            lines.append("\n  SALES CYCLE BREAKDOWN")
            lines.append("  " + "─" * 50)

            # ASCII funnel
            lines.append("")
            lines.append("  Purchase ──→ Renovation ──→ Listed ──→ Sold")
            lines.append(f"          {breakdown.days_to_list:>5.0f}d          {breakdown.days_on_market:>5.0f}d")
            lines.append(f"          └───────── {breakdown.total_days:.0f} days total ─────────┘")
            lines.append("")

            lines.append(f"  Avg Days to List (prep/reno):  {breakdown.days_to_list:>6.0f} days")
            lines.append(f"  Avg Days on Market:            {breakdown.days_on_market:>6.0f} days")
            lines.append(f"  Avg Total Cycle:               {breakdown.total_days:>6.0f} days")
            lines.append("")
            lines.append(f"  Distribution (25th/50th/75th): {breakdown.p25_total:.0f} / {breakdown.p50_total:.0f} / {breakdown.p75_total:.0f} days")

            if breakdown.new_cohort_velocity > 0:
                lines.append("")
                lines.append(f"  New Cohort (<90d) Velocity:    {breakdown.new_cohort_velocity:>6.0f} days")
                lines.append(f"  Old Cohort (>180d) Velocity:   {breakdown.old_cohort_velocity:>6.0f} days")

        if listing_analysis:
            lines.append("\n  CURRENT INVENTORY AGING")
            lines.append("  " + "─" * 50)

            buckets = listing_analysis.get("dom_buckets", {})
            total = listing_analysis.get("total_listings", 1)

            for bucket, count in buckets.items():
                pct = count / total * 100 if total > 0 else 0
                bar_len = int(pct / 3)  # Scale to fit
                bar = "█" * bar_len
                lines.append(f"  {bucket:>8}: {count:>4} ({pct:>5.1f}%) {bar}")

            lines.append("")
            lines.append(f"  Stale (>180d): {listing_analysis.get('stale_pct', 0):.1f}% of inventory")

        lines.append("\n" + "=" * 70)

        return "\n".join(lines)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary for dashboard integration."""
        breakdown = self.calculate_velocity_breakdown()
        listing_analysis = self.analyze_listing_velocity()

        return {
            "sales": {
                "days_to_list": round(breakdown.days_to_list, 0) if breakdown else 0,
                "days_on_market": round(breakdown.days_on_market, 0) if breakdown else 0,
                "total_days": round(breakdown.total_days, 0) if breakdown else 0,
                "p50_total": round(breakdown.p50_total, 0) if breakdown else 0,
            },
            "listings": listing_analysis,
        }
