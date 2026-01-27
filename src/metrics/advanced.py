"""
Glass House Advanced Metrics
=============================
CEO-level analytics: velocity, pricing intelligence, market matrix, guidance tracking.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class VelocityMetrics:
    """How fast are we moving inventory?"""
    avg_days_to_sale: float
    median_days_to_sale: float
    sales_per_day_avg: float
    sales_last_7_days: int
    sales_last_30_days: int
    inventory_turnover_days: float  # At current pace, how long to clear inventory?


@dataclass
class PricingMetrics:
    """Are we pricing right?"""
    avg_list_to_sale_ratio: float  # Sale price / List price
    avg_price_cut_pct: float
    homes_with_price_cuts: int
    homes_with_price_cuts_pct: float
    avg_cuts_per_home: float
    avg_initial_vs_final_list: float  # How much did we drop from first list?
    avg_spread: float  # Sale price - Purchase price (before costs)
    spread_trend: str  # "expanding", "stable", "compressing"


@dataclass
class MarketPerformance:
    """Per-market breakdown."""
    state: str
    sales_count: int
    win_rate: float
    avg_profit: float
    avg_days_held: float
    inventory_count: int
    toxic_count: int
    avg_dom: float
    concentration_pct: float  # % of total inventory


@dataclass
class GuidanceTracking:
    """Are we on track for guidance?"""
    q1_target: float
    revenue_to_date: float
    pct_to_target: float
    days_elapsed: int
    days_remaining: int
    required_daily_revenue: float
    current_daily_revenue: float
    pace_vs_required: str  # "ahead", "on_track", "behind"
    projected_quarter_revenue: float


@dataclass
class RiskMetrics:
    """What's at risk?"""
    underwater_count: int  # List price < Purchase price
    underwater_total_exposure: float
    underwater_pct: float
    aged_underwater_count: int  # >180 days AND underwater
    top_concentration_market: str
    top_concentration_pct: float
    markets_above_10pct: int


class AdvancedAnalytics:
    """Calculate advanced CEO-level metrics."""

    Q1_2026_TARGET = 595_000_000  # $595M guidance
    Q1_START = datetime(2026, 1, 1)
    Q1_END = datetime(2026, 3, 31)

    def __init__(self, sales_df: pd.DataFrame, listings_df: pd.DataFrame):
        self.sales = sales_df
        self.listings = listings_df

    def calculate_velocity(self) -> VelocityMetrics:
        """Calculate inventory velocity metrics."""
        if self.sales.empty:
            return VelocityMetrics(0, 0, 0, 0, 0, 999)

        # Days held stats
        days_held = self.sales.get("days_held", pd.Series([0]))
        avg_days = float(days_held.mean()) if len(days_held) > 0 else 0
        median_days = float(days_held.median()) if len(days_held) > 0 else 0

        # Sales velocity
        if "sale_date" in self.sales.columns:
            sale_dates = pd.to_datetime(self.sales["sale_date"], errors="coerce")
            valid_dates = sale_dates.dropna()

            if len(valid_dates) > 0:
                date_range = (valid_dates.max() - valid_dates.min()).days or 1
                sales_per_day = len(self.sales) / date_range

                # Last 7/30 days
                now = datetime.now()
                last_7 = (sale_dates >= (now - timedelta(days=7))).sum()
                last_30 = (sale_dates >= (now - timedelta(days=30))).sum()
            else:
                sales_per_day = 0
                last_7 = 0
                last_30 = 0
        else:
            # Estimate from total sales over quarter
            days_in_quarter = (datetime.now() - self.Q1_START).days or 1
            sales_per_day = len(self.sales) / days_in_quarter
            last_7 = 0
            last_30 = 0

        # Turnover
        inventory_count = len(self.listings) if not self.listings.empty else 1
        turnover_days = inventory_count / sales_per_day if sales_per_day > 0 else 999

        return VelocityMetrics(
            avg_days_to_sale=round(avg_days, 1),
            median_days_to_sale=round(median_days, 1),
            sales_per_day_avg=round(sales_per_day, 1),
            sales_last_7_days=int(last_7),
            sales_last_30_days=int(last_30),
            inventory_turnover_days=round(turnover_days, 0),
        )

    def calculate_pricing(self) -> PricingMetrics:
        """Calculate pricing intelligence metrics."""
        result = PricingMetrics(
            avg_list_to_sale_ratio=0,
            avg_price_cut_pct=0,
            homes_with_price_cuts=0,
            homes_with_price_cuts_pct=0,
            avg_cuts_per_home=0,
            avg_initial_vs_final_list=0,
            avg_spread=0,
            spread_trend="unknown",
        )

        # Spread calculation - prefer realized_net if available (more accurate)
        if not self.sales.empty:
            if "realized_net" in self.sales.columns:
                # Use realized_net directly - it's the true spread after costs
                realized = self.sales["realized_net"]
                valid = realized != 0  # Include negative values too
                if valid.sum() > 0:
                    result.avg_spread = float(realized[valid].mean())
            elif "sale_price" in self.sales.columns and "purchase_price" in self.sales.columns:
                # Fallback to raw price difference
                sale_p = self.sales["sale_price"]
                purch_p = self.sales["purchase_price"]
                valid = (purch_p > 0) & (sale_p > 0)
                if valid.sum() > 0:
                    spreads = sale_p[valid] - purch_p[valid]
                    result.avg_spread = float(spreads.mean())

        # Price cuts from listings
        if not self.listings.empty:
            if "price_cuts" in self.listings.columns:
                cuts = self.listings["price_cuts"].fillna(0)
                has_cuts = cuts > 0
                result.homes_with_price_cuts = int(has_cuts.sum())
                result.homes_with_price_cuts_pct = round(has_cuts.mean() * 100, 1)
                result.avg_cuts_per_home = round(cuts.mean(), 1)

            # Initial vs final list price
            if "initial_list_price" in self.listings.columns and "list_price" in self.listings.columns:
                initial = self.listings["initial_list_price"]
                final = self.listings["list_price"]
                valid = (initial > 0) & (final > 0)
                if valid.sum() > 0:
                    pct_change = ((final[valid] - initial[valid]) / initial[valid] * 100)
                    result.avg_initial_vs_final_list = round(pct_change.mean(), 1)
                    result.avg_price_cut_pct = round(abs(pct_change[pct_change < 0].mean()), 1) if (pct_change < 0).any() else 0

        return result

    def calculate_market_performance(self) -> List[MarketPerformance]:
        """Calculate per-market performance matrix."""
        markets = []

        # Get all states from listings
        if self.listings.empty or "state" not in self.listings.columns:
            return markets

        states = self.listings["state"].dropna().unique()
        total_inventory = len(self.listings)

        for state in states:
            # Inventory metrics
            state_listings = self.listings[self.listings["state"] == state]
            inv_count = len(state_listings)

            dom = state_listings.get("days_on_market", pd.Series([0]))
            avg_dom = float(dom.mean()) if len(dom) > 0 else 0
            toxic_count = int((dom >= 365).sum())

            # Sales metrics (if we have state in sales)
            if not self.sales.empty and "state" in self.sales.columns:
                state_sales = self.sales[self.sales["state"] == state]
                sales_count = len(state_sales)

                if sales_count > 0:
                    realized = state_sales.get("realized_net", pd.Series([0]))
                    win_rate = float((realized > 0).mean() * 100)
                    avg_profit = float(realized.mean())
                    days_held = state_sales.get("days_held", pd.Series([0]))
                    avg_days = float(days_held.mean())
                else:
                    win_rate = 0
                    avg_profit = 0
                    avg_days = 0
                    sales_count = 0
            else:
                # No state in sales data
                sales_count = 0
                win_rate = 0
                avg_profit = 0
                avg_days = 0

            markets.append(MarketPerformance(
                state=state,
                sales_count=sales_count,
                win_rate=round(win_rate, 1),
                avg_profit=round(avg_profit, 0),
                avg_days_held=round(avg_days, 0),
                inventory_count=inv_count,
                toxic_count=toxic_count,
                avg_dom=round(avg_dom, 0),
                concentration_pct=round(inv_count / total_inventory * 100, 1),
            ))

        # Sort by inventory concentration
        markets.sort(key=lambda x: -x.inventory_count)
        return markets

    def calculate_guidance_tracking(self) -> GuidanceTracking:
        """Track progress against quarterly guidance."""
        now = datetime.now()

        # Revenue to date
        if not self.sales.empty and "sale_price" in self.sales.columns:
            revenue = float(self.sales["sale_price"].sum())
        else:
            revenue = 0

        # Days elapsed/remaining
        days_elapsed = (now - self.Q1_START).days
        days_remaining = (self.Q1_END - now).days
        total_days = (self.Q1_END - self.Q1_START).days

        # Required pace
        pct_to_target = (revenue / self.Q1_2026_TARGET * 100) if self.Q1_2026_TARGET > 0 else 0
        required_daily = (self.Q1_2026_TARGET - revenue) / days_remaining if days_remaining > 0 else 0
        current_daily = revenue / days_elapsed if days_elapsed > 0 else 0

        # Projected
        projected = current_daily * total_days

        # Status
        expected_pct = (days_elapsed / total_days * 100)
        if pct_to_target >= expected_pct + 5:
            pace = "ahead"
        elif pct_to_target >= expected_pct - 5:
            pace = "on_track"
        else:
            pace = "behind"

        return GuidanceTracking(
            q1_target=self.Q1_2026_TARGET,
            revenue_to_date=revenue,
            pct_to_target=round(pct_to_target, 1),
            days_elapsed=days_elapsed,
            days_remaining=days_remaining,
            required_daily_revenue=round(required_daily, 0),
            current_daily_revenue=round(current_daily, 0),
            pace_vs_required=pace,
            projected_quarter_revenue=round(projected, 0),
        )

    def calculate_risk(self) -> RiskMetrics:
        """Calculate risk exposure metrics."""
        result = RiskMetrics(
            underwater_count=0,
            underwater_total_exposure=0,
            underwater_pct=0,
            aged_underwater_count=0,
            top_concentration_market="",
            top_concentration_pct=0,
            markets_above_10pct=0,
        )

        if self.listings.empty:
            return result

        # Underwater analysis
        if "list_price" in self.listings.columns and "purchase_price" in self.listings.columns:
            list_p = self.listings["list_price"]
            purch_p = self.listings["purchase_price"]

            # Only count where we have valid purchase price
            valid = purch_p > 0
            if valid.sum() > 0:
                underwater = (list_p < purch_p) & valid
                result.underwater_count = int(underwater.sum())
                result.underwater_pct = round(underwater.sum() / valid.sum() * 100, 1)

                # Total exposure
                exposure = (purch_p[underwater] - list_p[underwater]).sum()
                result.underwater_total_exposure = float(exposure)

                # Aged + underwater
                if "days_on_market" in self.listings.columns:
                    aged = self.listings["days_on_market"] >= 180
                    aged_underwater = underwater & aged
                    result.aged_underwater_count = int(aged_underwater.sum())

        # Concentration risk
        if "state" in self.listings.columns:
            state_counts = self.listings["state"].value_counts()
            total = len(self.listings)

            if len(state_counts) > 0:
                top_state = state_counts.index[0]
                top_pct = state_counts.iloc[0] / total * 100

                result.top_concentration_market = top_state
                result.top_concentration_pct = round(top_pct, 1)
                result.markets_above_10pct = int((state_counts / total * 100 >= 10).sum())

        return result

    def calculate_kaz_era(self) -> Dict[str, Any]:
        """Calculate Kaz-era metrics (new strategy performance)."""
        try:
            from .kaz_era import KazEraTracker
            tracker = KazEraTracker(self.sales, self.listings)
            return tracker.get_summary()
        except Exception as e:
            logger.warning(f"Could not calculate Kaz-era metrics: {e}")
            return {}

    def generate_summary(self) -> Dict[str, Any]:
        """Generate complete advanced analytics summary."""
        return {
            "velocity": asdict(self.calculate_velocity()),
            "pricing": asdict(self.calculate_pricing()),
            "markets": [asdict(m) for m in self.calculate_market_performance()],
            "guidance": asdict(self.calculate_guidance_tracking()),
            "risk": asdict(self.calculate_risk()),
            "kaz_era": self.calculate_kaz_era(),
        }
