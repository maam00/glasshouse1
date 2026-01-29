"""
Glass House V3 Metrics
======================
Phase 1: No survivorship bias, complete picture.

New metrics:
1. Days to Sale by Cohort
2. Price Cut Severity
3. Kaz-Era Underwater Watchlist
4. Contribution Margin by Cohort
5. Complete Portfolio View (Sold + Listed)
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import pandas as pd
import numpy as np

from src.config import KAZ_ERA_START

logger = logging.getLogger(__name__)


@dataclass
class DaysToSaleByCohort:
    """How fast do homes sell by cohort?"""
    cohort: str
    sold_count: int
    avg_days_to_sale: float
    median_days_to_sale: float


@dataclass
class PriceCutSeverity:
    """Price cut breakdown - leading indicator."""
    portfolio: str  # "kaz_era" or "legacy"
    listed_count: int
    with_1_cut: int
    with_2_cuts: int
    with_3plus_cuts: int
    total_with_cuts: int
    pct_with_cuts: float
    avg_cut_amount: float
    avg_pct_below_list: float


@dataclass
class UnderwaterHome:
    """Detail on underwater home."""
    city: str
    state: str
    days_on_market: int
    underwater_amount: float
    price_cuts: int
    pct_below_purchase: float
    status: str  # "Watching", "Concern", "Distressed"


@dataclass
class CohortMargin:
    """Contribution margin by cohort."""
    cohort: str
    sold_count: int
    win_rate: float
    total_revenue: float
    total_profit: float
    contribution_margin: float
    avg_profit: float


@dataclass
class PortfolioView:
    """Complete portfolio view - sold + listed."""
    era: str  # "kaz_era" or "legacy"

    # Sold
    sold_count: int
    sold_win_rate: float
    sold_avg_profit: float
    sold_total_realized: float

    # Listed
    listed_count: int
    listed_above_water: int
    listed_above_water_pct: float
    listed_underwater: int
    listed_underwater_pct: float
    listed_underwater_exposure: float

    # Price cuts
    listed_with_cuts: int
    listed_with_cuts_pct: float
    avg_cut_amount: float

    # Combined
    total_homes: int
    net_position: float  # realized + unrealized above water - underwater exposure


class V3Metrics:
    """Calculate V3 honest metrics - no survivorship bias."""

    def __init__(self, sales_df: pd.DataFrame, listings_df: pd.DataFrame):
        self.sales = sales_df.copy() if not sales_df.empty else pd.DataFrame()
        self.listings = listings_df.copy() if not listings_df.empty else pd.DataFrame()
        self._prepare_data()

    def _prepare_data(self):
        """Prepare data with era flags and cohort assignments."""
        # Sales
        if not self.sales.empty:
            if "purchase_date" in self.sales.columns:
                self.sales["purchase_date"] = pd.to_datetime(self.sales["purchase_date"], errors="coerce")
                self.sales["is_kaz_era"] = self.sales["purchase_date"] >= KAZ_ERA_START

            if "days_held" in self.sales.columns:
                days = self.sales["days_held"].fillna(0)
                self.sales["cohort"] = pd.cut(
                    days,
                    bins=[0, 90, 180, 365, 9999],
                    labels=["New (<90d)", "Mid (90-180d)", "Old (180-365d)", "Toxic (>365d)"]
                )

        # Listings
        if not self.listings.empty:
            if "purchase_date" in self.listings.columns:
                self.listings["purchase_date"] = pd.to_datetime(self.listings["purchase_date"], errors="coerce")
                self.listings["is_kaz_era"] = self.listings["purchase_date"] >= KAZ_ERA_START

            # Calculate underwater status
            if "list_price" in self.listings.columns and "purchase_price" in self.listings.columns:
                self.listings["underwater_amount"] = self.listings["purchase_price"] - self.listings["list_price"]
                self.listings["is_underwater"] = self.listings["underwater_amount"] > 0

    def calculate_days_to_sale_by_cohort(self) -> List[DaysToSaleByCohort]:
        """Calculate avg days to sale by cohort - proves good homes sell fast."""
        results = []

        if self.sales.empty or "days_held" not in self.sales.columns:
            return results

        cohorts = ["New (<90d)", "Mid (90-180d)", "Old (180-365d)", "Toxic (>365d)"]

        for cohort in cohorts:
            if "cohort" in self.sales.columns:
                cohort_sales = self.sales[self.sales["cohort"] == cohort]
            else:
                # Fallback: calculate cohort on the fly
                days = self.sales["days_held"].fillna(0)
                if cohort == "New (<90d)":
                    cohort_sales = self.sales[days < 90]
                elif cohort == "Mid (90-180d)":
                    cohort_sales = self.sales[(days >= 90) & (days < 180)]
                elif cohort == "Old (180-365d)":
                    cohort_sales = self.sales[(days >= 180) & (days < 365)]
                else:
                    cohort_sales = self.sales[days >= 365]

            if len(cohort_sales) > 0:
                days_held = cohort_sales["days_held"]
                results.append(DaysToSaleByCohort(
                    cohort=cohort,
                    sold_count=len(cohort_sales),
                    avg_days_to_sale=round(days_held.mean(), 0),
                    median_days_to_sale=round(days_held.median(), 0),
                ))

        return results

    def calculate_price_cut_severity(self) -> List[PriceCutSeverity]:
        """Calculate price cut severity - leading indicator of distress."""
        results = []

        if self.listings.empty:
            return results

        for era, is_kaz in [("kaz_era", True), ("legacy", False)]:
            if "is_kaz_era" in self.listings.columns:
                era_listings = self.listings[self.listings["is_kaz_era"] == is_kaz]
            else:
                era_listings = self.listings if era == "legacy" else pd.DataFrame()

            if len(era_listings) == 0:
                continue

            listed_count = len(era_listings)

            # Price cuts
            if "price_cuts" in era_listings.columns:
                cuts = era_listings["price_cuts"].fillna(0)
                with_1 = int(((cuts >= 1) & (cuts < 2)).sum())
                with_2 = int(((cuts >= 2) & (cuts < 3)).sum())
                with_3plus = int((cuts >= 3).sum())
                total_with = int((cuts > 0).sum())
                pct_with = round(total_with / listed_count * 100, 1)
            else:
                with_1 = with_2 = with_3plus = total_with = 0
                pct_with = 0

            # Average cut amount and % below list
            avg_cut = 0
            avg_pct_below = 0
            if "initial_list_price" in era_listings.columns and "list_price" in era_listings.columns:
                initial = era_listings["initial_list_price"]
                final = era_listings["list_price"]
                valid = (initial > 0) & (final > 0) & (initial > final)
                if valid.sum() > 0:
                    cut_amounts = initial[valid] - final[valid]
                    avg_cut = float(cut_amounts.mean())
                    pct_below = (initial[valid] - final[valid]) / initial[valid] * 100
                    avg_pct_below = float(pct_below.mean())

            results.append(PriceCutSeverity(
                portfolio=era,
                listed_count=listed_count,
                with_1_cut=with_1,
                with_2_cuts=with_2,
                with_3plus_cuts=with_3plus,
                total_with_cuts=total_with,
                pct_with_cuts=pct_with,
                avg_cut_amount=round(avg_cut, 0),
                avg_pct_below_list=round(avg_pct_below, 1),
            ))

        return results

    def get_kaz_era_underwater_watchlist(self) -> List[UnderwaterHome]:
        """Get detail on Kaz-era underwater homes - the canary."""
        watchlist = []

        if self.listings.empty:
            return watchlist

        # Filter to Kaz-era underwater
        if "is_kaz_era" not in self.listings.columns or "is_underwater" not in self.listings.columns:
            return watchlist

        kaz_underwater = self.listings[
            (self.listings["is_kaz_era"] == True) &
            (self.listings["is_underwater"] == True)
        ]

        for _, row in kaz_underwater.iterrows():
            city = row.get("city", "Unknown")
            state = row.get("state", "XX")
            dom = int(row.get("days_on_market", 0))
            uw_amount = float(row.get("underwater_amount", 0))
            cuts = int(row.get("price_cuts", 0))

            # Calculate % below purchase
            purchase = row.get("purchase_price", 0)
            list_price = row.get("list_price", 0)
            pct_below = 0
            if purchase > 0 and list_price > 0:
                pct_below = (purchase - list_price) / purchase * 100

            # Determine status
            if uw_amount > 30000 or cuts >= 3 or dom > 90:
                status = "Distressed"
            elif uw_amount > 15000 or cuts >= 2 or dom > 60:
                status = "Concern"
            else:
                status = "Watching"

            watchlist.append(UnderwaterHome(
                city=city,
                state=state,
                days_on_market=dom,
                underwater_amount=round(uw_amount, 0),
                price_cuts=cuts,
                pct_below_purchase=round(pct_below, 1),
                status=status,
            ))

        # Sort by underwater amount (worst first)
        watchlist.sort(key=lambda x: -x.underwater_amount)

        return watchlist

    def calculate_cohort_margins(self) -> List[CohortMargin]:
        """Calculate contribution margin by cohort."""
        results = []

        if self.sales.empty:
            return results

        cohorts = ["New (<90d)", "Mid (90-180d)", "Old (180-365d)", "Toxic (>365d)"]

        for cohort in cohorts:
            if "cohort" in self.sales.columns:
                cohort_sales = self.sales[self.sales["cohort"] == cohort]
            else:
                days = self.sales.get("days_held", pd.Series([0])).fillna(0)
                if cohort == "New (<90d)":
                    cohort_sales = self.sales[days < 90]
                elif cohort == "Mid (90-180d)":
                    cohort_sales = self.sales[(days >= 90) & (days < 180)]
                elif cohort == "Old (180-365d)":
                    cohort_sales = self.sales[(days >= 180) & (days < 365)]
                else:
                    cohort_sales = self.sales[days >= 365]

            if len(cohort_sales) == 0:
                continue

            sold_count = len(cohort_sales)

            # Win rate
            if "realized_net" in cohort_sales.columns:
                realized = cohort_sales["realized_net"]
                win_rate = float((realized > 0).sum() / sold_count * 100)
                total_profit = float(realized.sum())
                avg_profit = float(realized.mean())
            else:
                win_rate = 0
                total_profit = 0
                avg_profit = 0

            # Revenue
            if "sale_price" in cohort_sales.columns:
                total_revenue = float(cohort_sales["sale_price"].sum())
            else:
                total_revenue = 0

            # Contribution margin
            margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0

            results.append(CohortMargin(
                cohort=cohort,
                sold_count=sold_count,
                win_rate=round(win_rate, 1),
                total_revenue=round(total_revenue, 0),
                total_profit=round(total_profit, 0),
                contribution_margin=round(margin, 1),
                avg_profit=round(avg_profit, 0),
            ))

        return results

    def calculate_portfolio_view(self) -> List[PortfolioView]:
        """Calculate complete portfolio view - sold + listed, no survivorship bias."""
        results = []

        for era, is_kaz in [("kaz_era", True), ("legacy", False)]:
            # SOLD
            if not self.sales.empty and "is_kaz_era" in self.sales.columns:
                era_sold = self.sales[self.sales["is_kaz_era"] == is_kaz]
            else:
                era_sold = pd.DataFrame() if is_kaz else self.sales

            sold_count = len(era_sold)
            if sold_count > 0 and "realized_net" in era_sold.columns:
                realized = era_sold["realized_net"]
                sold_win_rate = float((realized > 0).sum() / sold_count * 100)
                sold_avg_profit = float(realized.mean())
                sold_total = float(realized.sum())
            else:
                sold_win_rate = 0
                sold_avg_profit = 0
                sold_total = 0

            # LISTED
            if not self.listings.empty and "is_kaz_era" in self.listings.columns:
                era_listed = self.listings[self.listings["is_kaz_era"] == is_kaz]
            else:
                era_listed = pd.DataFrame() if is_kaz else self.listings

            listed_count = len(era_listed)

            if listed_count > 0:
                # Above water / underwater
                if "is_underwater" in era_listed.columns:
                    underwater = era_listed["is_underwater"]
                    listed_underwater = int(underwater.sum())
                    listed_above = listed_count - listed_underwater
                    listed_above_pct = round(listed_above / listed_count * 100, 1)
                    listed_uw_pct = round(listed_underwater / listed_count * 100, 1)
                else:
                    listed_underwater = 0
                    listed_above = listed_count
                    listed_above_pct = 100
                    listed_uw_pct = 0

                # Underwater exposure
                if "underwater_amount" in era_listed.columns:
                    uw_exposure = float(era_listed[era_listed["underwater_amount"] > 0]["underwater_amount"].sum())
                else:
                    uw_exposure = 0

                # Unrealized gains (above water)
                if "unrealized_net" in era_listed.columns:
                    above_water_gains = float(era_listed[era_listed["unrealized_net"] >= 0]["unrealized_net"].sum())
                else:
                    above_water_gains = 0

                # Price cuts
                if "price_cuts" in era_listed.columns:
                    cuts = era_listed["price_cuts"].fillna(0)
                    with_cuts = int((cuts > 0).sum())
                    with_cuts_pct = round(with_cuts / listed_count * 100, 1)
                else:
                    with_cuts = 0
                    with_cuts_pct = 0

                # Avg cut amount
                avg_cut = 0
                if "initial_list_price" in era_listed.columns and "list_price" in era_listed.columns:
                    initial = era_listed["initial_list_price"]
                    final = era_listed["list_price"]
                    valid = (initial > 0) & (final > 0) & (initial > final)
                    if valid.sum() > 0:
                        avg_cut = float((initial[valid] - final[valid]).mean())
            else:
                listed_above = 0
                listed_above_pct = 0
                listed_underwater = 0
                listed_uw_pct = 0
                uw_exposure = 0
                above_water_gains = 0
                with_cuts = 0
                with_cuts_pct = 0
                avg_cut = 0

            # Net position
            net_position = sold_total + above_water_gains - uw_exposure

            results.append(PortfolioView(
                era=era,
                sold_count=sold_count,
                sold_win_rate=round(sold_win_rate, 1),
                sold_avg_profit=round(sold_avg_profit, 0),
                sold_total_realized=round(sold_total, 0),
                listed_count=listed_count,
                listed_above_water=listed_above,
                listed_above_water_pct=listed_above_pct,
                listed_underwater=listed_underwater,
                listed_underwater_pct=listed_uw_pct,
                listed_underwater_exposure=round(uw_exposure, 0),
                listed_with_cuts=with_cuts,
                listed_with_cuts_pct=with_cuts_pct,
                avg_cut_amount=round(avg_cut, 0),
                total_homes=sold_count + listed_count,
                net_position=round(net_position, 0),
            ))

        return results

    def generate_summary(self) -> Dict[str, Any]:
        """Generate complete V3 metrics summary."""
        portfolio_views = self.calculate_portfolio_view()

        return {
            "portfolio": {p.era: asdict(p) for p in portfolio_views},
            "days_to_sale": [asdict(d) for d in self.calculate_days_to_sale_by_cohort()],
            "price_cut_severity": [asdict(p) for p in self.calculate_price_cut_severity()],
            "underwater_watchlist": [asdict(u) for u in self.get_kaz_era_underwater_watchlist()],
            "cohort_margins": [asdict(c) for c in self.calculate_cohort_margins()],
        }
