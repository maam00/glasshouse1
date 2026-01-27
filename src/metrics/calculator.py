"""
Glass House Metrics Calculator
===============================
Calculates all operational metrics from raw Parcl Labs data.
"""

from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import logging

import pandas as pd

from ..db.database import (
    CohortData, ToxicData, InventoryData, PerformanceData, DailyMetrics
)

logger = logging.getLogger(__name__)


class MetricsCalculator:
    """Calculate all Glass House metrics from raw data."""

    # Cohort thresholds (days held)
    COHORT_NEW_MAX = 90
    COHORT_MID_MIN = 90
    COHORT_MID_MAX = 180
    COHORT_OLD_MIN = 180
    COHORT_OLD_MAX = 365
    COHORT_TOXIC_MIN = 365

    # Inventory age buckets (days on market)
    INV_FRESH_MAX = 30
    INV_NORMAL_MAX = 90
    INV_STALE_MAX = 180
    INV_VERY_STALE_MAX = 365

    def __init__(
        self,
        sales_df: pd.DataFrame,
        listings_df: pd.DataFrame,
        purchases_df: pd.DataFrame = None,
        previous_metrics: Dict = None,
    ):
        self.sales = sales_df
        self.listings = listings_df
        self.purchases = purchases_df if purchases_df is not None else pd.DataFrame()
        self.previous = previous_metrics

        # Normalize column names (Parcl Labs may use different conventions)
        self._normalize_columns()

    def _normalize_columns(self):
        """Standardize column names from Parcl Labs data."""
        # Common column mappings - adjust based on actual API response
        column_maps = {
            # Sales columns
            "sale_amount": "sale_price",
            "sold_price": "sale_price",
            "acquisition_price": "purchase_price",
            "buy_price": "purchase_price",
            "holding_period": "days_held",
            "hold_days": "days_held",
            "net_profit": "realized_net",
            "profit": "realized_net",
            "sale_event_date": "sale_date",
            "sold_date": "sale_date",
            "state_abbr": "state",
            "state_abbreviation": "state",

            # Listings columns
            "listing_price": "list_price",
            "current_price": "list_price",
            "dom": "days_on_market",
            "days_listed": "days_on_market",
            "list_event_date": "list_date",
        }

        for df in [self.sales, self.listings, self.purchases]:
            if df.empty:
                continue
            for old_name, new_name in column_maps.items():
                if old_name in df.columns and new_name not in df.columns:
                    df.rename(columns={old_name: new_name}, inplace=True)

    def _get_days_held(self, df: pd.DataFrame) -> pd.Series:
        """Extract or calculate days held."""
        if df.empty:
            return pd.Series(dtype=float)

        if "days_held" in df.columns:
            return pd.to_numeric(df["days_held"], errors="coerce").fillna(0)

        # Calculate from dates if available
        if "purchase_date" in df.columns and "sale_date" in df.columns:
            try:
                purchase = pd.to_datetime(df["purchase_date"], errors="coerce")
                sale = pd.to_datetime(df["sale_date"], errors="coerce")
                return (sale - purchase).dt.days.fillna(0)
            except Exception:
                pass

        return pd.Series([0] * len(df))

    def _get_realized_net(self, df: pd.DataFrame) -> pd.Series:
        """Extract or calculate realized net profit."""
        if df.empty:
            return pd.Series(dtype=float)

        if "realized_net" in df.columns:
            return pd.to_numeric(df["realized_net"], errors="coerce").fillna(0)

        # Calculate from prices (simplified - doesn't include all costs)
        if "sale_price" in df.columns and "purchase_price" in df.columns:
            sale = pd.to_numeric(df["sale_price"], errors="coerce").fillna(0)
            purchase = pd.to_numeric(df["purchase_price"], errors="coerce").fillna(0)
            return sale - purchase

        return pd.Series([0] * len(df))

    def _get_sale_prices(self, df: pd.DataFrame) -> pd.Series:
        """Extract sale prices."""
        if df.empty:
            return pd.Series(dtype=float)

        if "sale_price" in df.columns:
            return pd.to_numeric(df["sale_price"], errors="coerce").fillna(0)

        return pd.Series([0] * len(df))

    def _get_days_on_market(self, df: pd.DataFrame) -> pd.Series:
        """Extract or calculate days on market."""
        if df.empty:
            return pd.Series(dtype=float)

        if "days_on_market" in df.columns:
            return pd.to_numeric(df["days_on_market"], errors="coerce").fillna(0)

        # Calculate from list date
        if "list_date" in df.columns:
            try:
                list_date = pd.to_datetime(df["list_date"], errors="coerce")
                return (pd.Timestamp.now() - list_date).dt.days.fillna(0)
            except Exception:
                pass

        return pd.Series([0] * len(df))

    def _calculate_cohort(
        self,
        name: str,
        mask: pd.Series,
        days_held: pd.Series,
        realized_net: pd.Series,
        sale_prices: pd.Series,
    ) -> CohortData:
        """Calculate metrics for a single cohort."""
        if mask.sum() == 0:
            return CohortData(
                name=name,
                count=0,
                win_rate=0.0,
                avg_profit=0.0,
                total_profit=0.0,
                contribution_margin=0.0,
            )

        cohort_net = realized_net[mask]
        cohort_prices = sale_prices[mask]

        count = int(mask.sum())
        wins = (cohort_net > 0).sum()
        win_rate = (wins / count * 100) if count > 0 else 0

        avg_profit = float(cohort_net.mean()) if count > 0 else 0
        total_profit = float(cohort_net.sum())
        total_revenue = float(cohort_prices.sum())
        contribution_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0

        return CohortData(
            name=name,
            count=count,
            win_rate=round(win_rate, 1),
            avg_profit=round(avg_profit, 2),
            total_profit=round(total_profit, 2),
            contribution_margin=round(contribution_margin, 2),
        )

    def calculate_cohorts(self) -> Tuple[CohortData, CohortData, CohortData, CohortData]:
        """Calculate metrics for all four cohorts."""
        days_held = self._get_days_held(self.sales)
        realized_net = self._get_realized_net(self.sales)
        sale_prices = self._get_sale_prices(self.sales)

        # New cohort: <90 days (Kaz-era discipline)
        new_mask = days_held < self.COHORT_NEW_MAX
        cohort_new = self._calculate_cohort(
            "New (<90d)", new_mask, days_held, realized_net, sale_prices
        )

        # Mid cohort: 90-180 days
        mid_mask = (days_held >= self.COHORT_MID_MIN) & (days_held < self.COHORT_MID_MAX)
        cohort_mid = self._calculate_cohort(
            "Mid (90-180d)", mid_mask, days_held, realized_net, sale_prices
        )

        # Old cohort: 180-365 days
        old_mask = (days_held >= self.COHORT_OLD_MIN) & (days_held < self.COHORT_OLD_MAX)
        cohort_old = self._calculate_cohort(
            "Old (180-365d)", old_mask, days_held, realized_net, sale_prices
        )

        # Toxic cohort: >365 days
        toxic_mask = days_held >= self.COHORT_TOXIC_MIN
        cohort_toxic = self._calculate_cohort(
            "Toxic (>365d)", toxic_mask, days_held, realized_net, sale_prices
        )

        return cohort_new, cohort_mid, cohort_old, cohort_toxic

    def calculate_toxic_inventory(self) -> ToxicData:
        """Calculate toxic inventory metrics and clearance progress."""
        # Toxic sold (from sales)
        days_held = self._get_days_held(self.sales)
        realized_net = self._get_realized_net(self.sales)
        toxic_sold_mask = days_held >= self.COHORT_TOXIC_MIN

        sold_count = int(toxic_sold_mask.sum())
        toxic_nets = realized_net[toxic_sold_mask]
        sold_avg_loss = float(toxic_nets.mean()) if sold_count > 0 else 0

        # Toxic remaining (from listings)
        dom = self._get_days_on_market(self.listings)
        # For toxic in inventory, we need to estimate days held
        # DOM + some estimate of how long they were held before listing
        # Simplified: use DOM as proxy
        toxic_remaining_mask = dom >= self.COHORT_TOXIC_MIN
        remaining_count = int(toxic_remaining_mask.sum())

        # Clearance progress
        total_toxic = sold_count + remaining_count
        clearance_pct = (sold_count / total_toxic * 100) if total_toxic > 0 else 0

        # Weeks to clear at current pace
        # Use average daily toxic sold rate
        if sold_count > 0 and remaining_count > 0:
            # Assume sales data covers ~30 days
            daily_rate = sold_count / 30
            weeks_to_clear = (remaining_count / daily_rate / 7) if daily_rate > 0 else 999
        else:
            weeks_to_clear = 0 if remaining_count == 0 else 999

        return ToxicData(
            sold_count=sold_count,
            sold_avg_loss=round(sold_avg_loss, 2),
            remaining_count=remaining_count,
            clearance_pct=round(clearance_pct, 1),
            weeks_to_clear=round(weeks_to_clear, 1),
        )

    def calculate_inventory_health(self) -> InventoryData:
        """Calculate inventory age distribution and health metrics."""
        if self.listings.empty:
            return InventoryData(
                total=0,
                fresh_count=0,
                normal_count=0,
                stale_count=0,
                very_stale_count=0,
                toxic_count=0,
                legacy_pct=0,
                avg_dom=0,
                avg_list_price=0,
                total_unrealized_pnl=0,
            )

        dom = self._get_days_on_market(self.listings)
        total = len(self.listings)

        # Age buckets
        fresh = int((dom < self.INV_FRESH_MAX).sum())
        normal = int(((dom >= self.INV_FRESH_MAX) & (dom < self.INV_NORMAL_MAX)).sum())
        stale = int(((dom >= self.INV_NORMAL_MAX) & (dom < self.INV_STALE_MAX)).sum())
        very_stale = int(((dom >= self.INV_STALE_MAX) & (dom < self.INV_VERY_STALE_MAX)).sum())
        toxic = int((dom >= self.INV_VERY_STALE_MAX).sum())

        legacy_pct = ((very_stale + toxic) / total * 100) if total > 0 else 0
        avg_dom = float(dom.mean()) if total > 0 else 0

        # List price
        if "list_price" in self.listings.columns:
            list_prices = pd.to_numeric(self.listings["list_price"], errors="coerce").fillna(0)
            avg_list_price = float(list_prices.mean())
        else:
            avg_list_price = 0

        # Unrealized P&L
        if "list_price" in self.listings.columns and "purchase_price" in self.listings.columns:
            list_p = pd.to_numeric(self.listings["list_price"], errors="coerce").fillna(0)
            purch_p = pd.to_numeric(self.listings["purchase_price"], errors="coerce").fillna(0)
            unrealized = (list_p - purch_p).sum()
        else:
            unrealized = 0

        return InventoryData(
            total=total,
            fresh_count=fresh,
            normal_count=normal,
            stale_count=stale,
            very_stale_count=very_stale,
            toxic_count=toxic,
            legacy_pct=round(legacy_pct, 1),
            avg_dom=round(avg_dom, 1),
            avg_list_price=round(avg_list_price, 2),
            total_unrealized_pnl=round(unrealized, 2),
        )

    def calculate_performance(self, today: str = None) -> PerformanceData:
        """Calculate overall performance metrics."""
        today = today or datetime.now().strftime("%Y-%m-%d")

        if self.sales.empty:
            return PerformanceData(
                win_rate=0,
                contribution_margin=0,
                avg_profit=0,
                homes_sold_total=0,
                homes_sold_today=0,
                revenue_total=0,
                revenue_today=0,
                homes_listed_today=0,
                net_inventory_change=0,
            )

        realized_net = self._get_realized_net(self.sales)
        sale_prices = self._get_sale_prices(self.sales)

        # Overall metrics
        total_homes = len(self.sales)
        wins = (realized_net > 0).sum()
        win_rate = (wins / total_homes * 100) if total_homes > 0 else 0

        total_profit = realized_net.sum()
        total_revenue = sale_prices.sum()
        contribution_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
        avg_profit = realized_net.mean() if total_homes > 0 else 0

        # Today's metrics
        if "sale_date" in self.sales.columns:
            today_mask = self.sales["sale_date"].astype(str).str.startswith(today)
            today_sales = self.sales[today_mask]
            homes_sold_today = len(today_sales)
            revenue_today = float(sale_prices[today_mask].sum())
        else:
            homes_sold_today = 0
            revenue_today = 0

        # Purchases/listings today
        if not self.purchases.empty and "purchase_date" in self.purchases.columns:
            today_purch_mask = self.purchases["purchase_date"].astype(str).str.startswith(today)
            homes_listed_today = int(today_purch_mask.sum())
        else:
            homes_listed_today = 0

        net_change = homes_listed_today - homes_sold_today

        return PerformanceData(
            win_rate=round(win_rate, 1),
            contribution_margin=round(contribution_margin, 2),
            avg_profit=round(avg_profit, 2),
            homes_sold_total=total_homes,
            homes_sold_today=homes_sold_today,
            revenue_total=round(total_revenue, 2),
            revenue_today=round(revenue_today, 2),
            homes_listed_today=homes_listed_today,
            net_inventory_change=net_change,
        )

    def calculate_geographic(self) -> Dict[str, Any]:
        """Calculate geographic breakdown."""
        geo = {
            "sales_by_state": {},
            "inventory_by_state": {},
            "win_rate_by_state": {},
        }

        # Sales by state
        if not self.sales.empty and "state" in self.sales.columns:
            geo["sales_by_state"] = self.sales["state"].value_counts().to_dict()

            # Win rate by state
            realized_net = self._get_realized_net(self.sales)
            for state in self.sales["state"].unique():
                state_mask = self.sales["state"] == state
                state_nets = realized_net[state_mask]
                if len(state_nets) > 0:
                    win_rate = (state_nets > 0).mean() * 100
                    geo["win_rate_by_state"][state] = round(win_rate, 1)

        # Inventory by state
        if not self.listings.empty and "state" in self.listings.columns:
            geo["inventory_by_state"] = self.listings["state"].value_counts().to_dict()

        return geo

    def calculate_all(self, date: str = None) -> DailyMetrics:
        """Calculate all metrics and return DailyMetrics object."""
        date = date or datetime.now().strftime("%Y-%m-%d")

        cohort_new, cohort_mid, cohort_old, cohort_toxic = self.calculate_cohorts()
        toxic = self.calculate_toxic_inventory()
        inventory = self.calculate_inventory_health()
        performance = self.calculate_performance(date)
        geographic = self.calculate_geographic()

        # Generate alerts
        alerts = self._generate_alerts(
            cohort_new, toxic, performance, inventory
        )

        return DailyMetrics(
            date=date,
            cohort_new=cohort_new,
            cohort_mid=cohort_mid,
            cohort_old=cohort_old,
            cohort_toxic=cohort_toxic,
            toxic=toxic,
            inventory=inventory,
            performance=performance,
            geographic=geographic,
            alerts=alerts,
        )

    def _generate_alerts(
        self,
        cohort_new: CohortData,
        toxic: ToxicData,
        performance: PerformanceData,
        inventory: InventoryData,
    ) -> list:
        """Generate alerts for concerning metrics."""
        alerts = []

        # New cohort win rate check
        if cohort_new.count > 0 and cohort_new.win_rate < 95:
            alerts.append(
                f"New cohort win rate dropped to {cohort_new.win_rate}% (target: >95%)"
            )

        # Contribution margin check
        if performance.contribution_margin < 5:
            alerts.append(
                f"Contribution margin at {performance.contribution_margin}% (target: 5-7%)"
            )

        # Toxic inventory increasing (if we have previous data)
        if self.previous:
            prev_toxic = self.previous.get("toxic", {}).get("remaining_count", 0)
            if toxic.remaining_count > prev_toxic:
                alerts.append(
                    f"Toxic inventory increased: {prev_toxic} -> {toxic.remaining_count}"
                )

        # WoW comparison alerts
        if self.previous:
            prev_perf = self.previous.get("performance", {})
            for metric, name in [
                ("win_rate", "Win rate"),
                ("contribution_margin", "Contribution margin"),
            ]:
                prev_val = prev_perf.get(metric, 0)
                curr_val = getattr(performance, metric, 0)
                if prev_val > 0:
                    change_pct = abs((curr_val - prev_val) / prev_val * 100)
                    if change_pct > 10:
                        direction = "+" if curr_val > prev_val else ""
                        alerts.append(
                            f"{name} moved {direction}{curr_val - prev_val:.1f}% WoW"
                        )

        return alerts
