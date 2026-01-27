"""
True Unit Economics Calculator
==============================
Because realized_net doesn't tell the whole story.

A home showing $30K "profit" might actually be:
  Sale Price:        $400,000
  Purchase Price:   -$370,000
  Gross Spread:       $30,000

  Renovation:        -$18,000  (avg $15-25K)
  Holding Costs:      -$8,000  (160 days × $50/day)
  Transaction:        -$6,000  (title, commissions, etc.)

  TRUE NET:           -$2,000  ← Break-even, not $30K profit

This module estimates true unit economics.
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class UnitEconomicsConfig:
    """Configurable cost assumptions."""
    # Renovation costs (based on SEC filings, typically 4-6% of purchase price)
    renovation_pct_of_purchase: float = 0.05  # 5% default
    renovation_min: float = 8000
    renovation_max: float = 35000

    # Holding costs per day
    holding_cost_per_day: float = 55  # Property tax, insurance, utilities, maintenance

    # Transaction costs
    buy_side_closing_pct: float = 0.01  # 1% closing costs on purchase
    sell_side_closing_pct: float = 0.02  # 2% closing costs on sale
    agent_commission_pct: float = 0.025  # 2.5% if using agent (not all sales)

    # Financing costs (if applicable)
    cost_of_capital_annual: float = 0.08  # 8% annual cost of capital


@dataclass
class HomeUnitEconomics:
    """Unit economics for a single home."""
    property_id: str
    state: str

    # Prices
    purchase_price: float
    sale_price: float
    gross_spread: float

    # Costs
    estimated_renovation: float
    holding_costs: float
    buy_closing_costs: float
    sell_closing_costs: float
    total_costs: float

    # True economics
    true_net: float
    true_margin_pct: float
    days_held: int

    # Classification
    is_profitable: bool
    profitability_tier: str  # "strong", "marginal", "loss"


class UnitEconomicsCalculator:
    """Calculate true unit economics for Opendoor homes."""

    def __init__(self, config: UnitEconomicsConfig = None):
        self.config = config or UnitEconomicsConfig()

    def estimate_renovation_cost(self, purchase_price: float) -> float:
        """Estimate renovation cost based on purchase price."""
        estimated = purchase_price * self.config.renovation_pct_of_purchase
        return max(self.config.renovation_min, min(estimated, self.config.renovation_max))

    def calculate_holding_costs(self, days_held: int) -> float:
        """Calculate holding costs."""
        return days_held * self.config.holding_cost_per_day

    def calculate_home_economics(
        self,
        property_id: str,
        purchase_price: float,
        sale_price: float,
        days_held: int,
        state: str = "Unknown"
    ) -> HomeUnitEconomics:
        """Calculate true unit economics for a single home."""

        gross_spread = sale_price - purchase_price

        # Estimate costs
        renovation = self.estimate_renovation_cost(purchase_price)
        holding = self.calculate_holding_costs(days_held)
        buy_closing = purchase_price * self.config.buy_side_closing_pct
        sell_closing = sale_price * self.config.sell_side_closing_pct

        total_costs = renovation + holding + buy_closing + sell_closing
        true_net = gross_spread - total_costs
        true_margin_pct = (true_net / sale_price * 100) if sale_price > 0 else 0

        # Classify profitability
        if true_margin_pct >= 5:
            tier = "strong"
        elif true_margin_pct >= 0:
            tier = "marginal"
        else:
            tier = "loss"

        return HomeUnitEconomics(
            property_id=property_id,
            state=state,
            purchase_price=purchase_price,
            sale_price=sale_price,
            gross_spread=gross_spread,
            estimated_renovation=renovation,
            holding_costs=holding,
            buy_closing_costs=buy_closing,
            sell_closing_costs=sell_closing,
            total_costs=total_costs,
            true_net=true_net,
            true_margin_pct=true_margin_pct,
            days_held=days_held,
            is_profitable=true_net > 0,
            profitability_tier=tier,
        )

    def analyze_sales(self, sales_df: pd.DataFrame, listings_df: pd.DataFrame = None) -> Dict[str, Any]:
        """
        Analyze unit economics for all sales.
        Optionally join with listings to get state data.
        """
        if sales_df.empty:
            return {}

        # Try to enrich with state from listings
        if listings_df is not None and not listings_df.empty:
            sales_df = self._enrich_with_state(sales_df, listings_df)

        results = []
        for _, row in sales_df.iterrows():
            econ = self.calculate_home_economics(
                property_id=str(row.get("property_id", "")),
                purchase_price=float(row.get("purchase_price", 0)),
                sale_price=float(row.get("sale_price", 0)),
                days_held=int(row.get("days_held", 0)),
                state=str(row.get("state", "Unknown")),
            )
            results.append(econ)

        return self._aggregate_results(results)

    def _enrich_with_state(self, sales_df: pd.DataFrame, listings_df: pd.DataFrame) -> pd.DataFrame:
        """Join sales with listings to get state, or estimate from price patterns."""
        # If sales already has state column with real values, don't overwrite
        if "state" in sales_df.columns:
            non_unknown = sales_df["state"].notna() & (sales_df["state"] != "Unknown")
            if non_unknown.sum() > 0:
                logger.debug(f"Sales already has {non_unknown.sum()} state values, skipping enrichment")
                return sales_df

        if "property_id" not in sales_df.columns or "property_id" not in listings_df.columns:
            return sales_df

        # First try direct property_id lookup
        if "state" in listings_df.columns:
            state_lookup = listings_df[["property_id", "state"]].drop_duplicates()
            state_lookup = state_lookup.set_index("property_id")["state"].to_dict()

            sales_df["state"] = sales_df["property_id"].map(state_lookup).fillna("Unknown")
            direct_matches = (sales_df["state"] != "Unknown").sum()

            if direct_matches > 0:
                logger.info(f"Enriched {direct_matches} sales with state data (direct match)")
                return sales_df

        # If no direct matches, use price-based estimation
        try:
            from src.api.property_enrichment import enrich_sales_with_state_estimate
            sales_df = enrich_sales_with_state_estimate(sales_df, listings_df)
        except ImportError:
            logger.warning("Could not import property enrichment module")

        return sales_df

    def _aggregate_results(self, results: List[HomeUnitEconomics]) -> Dict[str, Any]:
        """Aggregate unit economics results."""
        if not results:
            return {}

        df = pd.DataFrame([{
            "property_id": r.property_id,
            "state": r.state,
            "purchase_price": r.purchase_price,
            "sale_price": r.sale_price,
            "gross_spread": r.gross_spread,
            "estimated_renovation": r.estimated_renovation,
            "holding_costs": r.holding_costs,
            "total_costs": r.total_costs,
            "true_net": r.true_net,
            "true_margin_pct": r.true_margin_pct,
            "days_held": r.days_held,
            "is_profitable": r.is_profitable,
            "profitability_tier": r.profitability_tier,
        } for r in results])

        # Overall metrics
        total_sales = len(df)
        profitable_count = df["is_profitable"].sum()

        summary = {
            "total_sales": total_sales,
            "gross_spread_total": df["gross_spread"].sum(),
            "gross_spread_avg": df["gross_spread"].mean(),
            "total_costs": df["total_costs"].sum(),
            "true_net_total": df["true_net"].sum(),
            "true_net_avg": df["true_net"].mean(),
            "true_margin_avg": df["true_margin_pct"].mean(),
            "profitable_count": int(profitable_count),
            "profitable_pct": (profitable_count / total_sales * 100) if total_sales > 0 else 0,
            "tier_breakdown": df["profitability_tier"].value_counts().to_dict(),
            "cost_breakdown": {
                "renovation_total": df["estimated_renovation"].sum(),
                "holding_total": df["holding_costs"].sum(),
                "renovation_avg": df["estimated_renovation"].mean(),
                "holding_avg": df["holding_costs"].mean(),
            },
        }

        # By state (if available)
        if "state" in df.columns and df["state"].nunique() > 1:
            by_state = df.groupby("state").agg({
                "property_id": "count",
                "gross_spread": "mean",
                "true_net": ["mean", "sum"],
                "true_margin_pct": "mean",
                "is_profitable": "mean",
                "days_held": "mean",
            }).round(2)

            by_state.columns = ["count", "avg_gross_spread", "avg_true_net", "total_true_net",
                               "avg_margin", "win_rate", "avg_days"]
            by_state["win_rate"] = (by_state["win_rate"] * 100).round(1)

            summary["by_state"] = by_state.sort_values("count", ascending=False).to_dict("index")

        # Reported vs True comparison
        if "realized_net" in df.columns:
            summary["reported_vs_true"] = {
                "reported_total": df["realized_net"].sum() if "realized_net" in df.columns else df["gross_spread"].sum(),
                "true_total": df["true_net"].sum(),
                "difference": df["gross_spread"].sum() - df["true_net"].sum(),
                "hidden_costs_pct": ((df["gross_spread"].sum() - df["true_net"].sum()) / df["gross_spread"].sum() * 100)
                                   if df["gross_spread"].sum() > 0 else 0,
            }

        return summary


def compare_reported_vs_true(sales_df: pd.DataFrame, listings_df: pd.DataFrame = None) -> str:
    """Generate a comparison report of reported vs true economics."""
    calc = UnitEconomicsCalculator()
    analysis = calc.analyze_sales(sales_df, listings_df)

    if not analysis:
        return "No data to analyze."

    lines = []
    lines.append("\n" + "=" * 70)
    lines.append("  TRUE UNIT ECONOMICS ANALYSIS")
    lines.append("=" * 70)

    # Headline comparison
    gross_total = analysis["gross_spread_total"]
    true_total = analysis["true_net_total"]
    hidden = gross_total - true_total

    lines.append(f"\n  REPORTED vs TRUE")
    lines.append(f"  {'─' * 50}")
    lines.append(f"  Gross Spread (buy→sell):    ${gross_total:>12,.0f}")
    lines.append(f"  Estimated Costs:            ${hidden:>12,.0f}")
    lines.append(f"  TRUE Net:                   ${true_total:>12,.0f}")
    lines.append(f"  {'─' * 50}")
    lines.append(f"  Hidden costs eat {hidden/gross_total*100:.1f}% of gross spread" if gross_total > 0 else "")

    # Profitability breakdown
    tiers = analysis.get("tier_breakdown", {})
    lines.append(f"\n  PROFITABILITY TIERS")
    lines.append(f"  {'─' * 50}")
    lines.append(f"  Strong (>5% margin):        {tiers.get('strong', 0):>5} homes")
    lines.append(f"  Marginal (0-5% margin):     {tiers.get('marginal', 0):>5} homes")
    lines.append(f"  Loss (<0% margin):          {tiers.get('loss', 0):>5} homes")

    # Cost breakdown
    costs = analysis.get("cost_breakdown", {})
    lines.append(f"\n  COST DRIVERS (per home avg)")
    lines.append(f"  {'─' * 50}")
    lines.append(f"  Renovation (estimated):     ${costs.get('renovation_avg', 0):>10,.0f}")
    lines.append(f"  Holding costs:              ${costs.get('holding_avg', 0):>10,.0f}")

    # By state if available
    by_state = analysis.get("by_state", {})
    if by_state and len(by_state) > 1:
        lines.append(f"\n  BY STATE (True Economics)")
        lines.append(f"  {'─' * 50}")
        lines.append(f"  {'State':<6} {'Count':>6} {'Win%':>7} {'Margin':>8} {'Avg Net':>10}")

        for state, data in sorted(by_state.items(), key=lambda x: x[1].get("count", 0), reverse=True)[:10]:
            if state == "Unknown":
                continue
            lines.append(
                f"  {state:<6} {data.get('count', 0):>6} {data.get('win_rate', 0):>6.1f}% "
                f"{data.get('avg_margin', 0):>7.1f}% ${data.get('avg_true_net', 0):>9,.0f}"
            )

    lines.append("\n" + "=" * 70)

    return "\n".join(lines)
