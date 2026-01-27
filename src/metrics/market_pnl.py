"""
Market-Level P&L Analysis
==========================
Per-market performance to answer: Which markets are working?

This joins sales data with listings to get state, then calculates
per-market metrics that matter for market allocation decisions.
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class MarketPnL:
    """P&L for a single market."""
    state: str

    # Volume
    inventory_count: int
    sales_count: int
    toxic_count: int

    # Performance
    win_rate: float
    contribution_margin: float
    avg_profit: float
    total_profit: float

    # Velocity
    avg_dom: float
    avg_days_held: float

    # Risk
    underwater_count: int
    underwater_pct: float

    # Trend (requires historical data)
    trend: str  # "improving", "stable", "declining"

    # Recommendation
    action: str  # "GROW", "HOLD", "PAUSE", "EXIT"


class MarketPnLAnalyzer:
    """Analyze per-market P&L."""

    def __init__(self, sales_df: pd.DataFrame, listings_df: pd.DataFrame):
        self.sales = sales_df.copy() if not sales_df.empty else pd.DataFrame()
        self.listings = listings_df.copy() if not listings_df.empty else pd.DataFrame()
        self._enrich_sales_with_state()

    def _enrich_sales_with_state(self):
        """Join sales with listings to get state, or estimate from price patterns."""
        if self.sales.empty or self.listings.empty:
            return

        if "property_id" not in self.sales.columns:
            return

        if "state" not in self.sales.columns or self.sales["state"].isna().all():
            # Try direct property_id lookup first
            if "property_id" in self.listings.columns and "state" in self.listings.columns:
                state_lookup = self.listings.set_index("property_id")["state"].to_dict()
                self.sales["state"] = self.sales["property_id"].map(state_lookup)
                direct_matches = self.sales["state"].notna().sum()

                if direct_matches > 0:
                    logger.info(f"Enriched sales with state (direct): {direct_matches}/{len(self.sales)}")
                    return

            # Fall back to price-based estimation
            try:
                from src.api.property_enrichment import enrich_sales_with_state_estimate
                self.sales = enrich_sales_with_state_estimate(self.sales, self.listings)
                estimated = (self.sales["state"] != "Unknown").sum() if "state" in self.sales.columns else 0
                logger.info(f"Enriched sales with state (estimated): {estimated}/{len(self.sales)}")
            except ImportError:
                logger.warning("Could not import property enrichment module")

    def calculate_market_pnl(self, state: str) -> Optional[MarketPnL]:
        """Calculate P&L for a single market."""
        # Filter data for this state
        if "state" in self.listings.columns:
            inv = self.listings[self.listings["state"] == state]
        else:
            inv = pd.DataFrame()

        if "state" in self.sales.columns:
            sales = self.sales[self.sales["state"] == state]
        else:
            sales = pd.DataFrame()

        if inv.empty and sales.empty:
            return None

        # Inventory metrics
        inventory_count = len(inv)
        toxic_count = len(inv[inv["days_on_market"] > 365]) if "days_on_market" in inv.columns else 0

        # Sales metrics
        sales_count = len(sales)

        if sales_count > 0 and "realized_net" in sales.columns:
            wins = (sales["realized_net"] > 0).sum()
            win_rate = (wins / sales_count) * 100

            total_profit = sales["realized_net"].sum()
            avg_profit = sales["realized_net"].mean()

            if "sale_price" in sales.columns:
                total_revenue = sales["sale_price"].sum()
                contribution_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
            else:
                contribution_margin = 0

            avg_days_held = sales["days_held"].mean() if "days_held" in sales.columns else 0
        else:
            win_rate = 0
            total_profit = 0
            avg_profit = 0
            contribution_margin = 0
            avg_days_held = 0

        # Inventory metrics
        avg_dom = inv["days_on_market"].mean() if "days_on_market" in inv.columns and not inv.empty else 0

        # Underwater (negative unrealized)
        if "unrealized_net" in inv.columns and not inv.empty:
            underwater = inv[inv["unrealized_net"] < 0]
            underwater_count = len(underwater)
            underwater_pct = (underwater_count / inventory_count * 100) if inventory_count > 0 else 0
        else:
            underwater_count = 0
            underwater_pct = 0

        # Determine trend (would need historical data for real trend)
        # For now, use margin as proxy
        if contribution_margin >= 7:
            trend = "strong"
        elif contribution_margin >= 3:
            trend = "stable"
        else:
            trend = "weak"

        # Determine action recommendation
        action = self._recommend_action(
            win_rate=win_rate,
            contribution_margin=contribution_margin,
            avg_dom=avg_dom,
            underwater_pct=underwater_pct,
            inventory_count=inventory_count,
        )

        return MarketPnL(
            state=state,
            inventory_count=inventory_count,
            sales_count=sales_count,
            toxic_count=toxic_count,
            win_rate=win_rate,
            contribution_margin=contribution_margin,
            avg_profit=avg_profit,
            total_profit=total_profit,
            avg_dom=avg_dom,
            avg_days_held=avg_days_held,
            underwater_count=underwater_count,
            underwater_pct=underwater_pct,
            trend=trend,
            action=action,
        )

    def _recommend_action(
        self,
        win_rate: float,
        contribution_margin: float,
        avg_dom: float,
        underwater_pct: float,
        inventory_count: int,
    ) -> str:
        """Recommend market action based on metrics."""
        # Exit: Very poor performance
        if contribution_margin < -5 or (win_rate < 50 and inventory_count > 20):
            return "EXIT"

        # Pause: Poor performance, high risk
        if contribution_margin < 0 or win_rate < 60 or underwater_pct > 50:
            return "PAUSE"

        # Hold: Acceptable but not great
        if contribution_margin < 5 or win_rate < 80 or avg_dom > 200:
            return "HOLD"

        # Grow: Strong performance
        return "GROW"

    def analyze_all_markets(self) -> Dict[str, MarketPnL]:
        """Analyze P&L for all markets."""
        markets = {}

        # Get all states from both sources
        states = set()
        if "state" in self.listings.columns:
            states.update(self.listings["state"].dropna().unique())
        if "state" in self.sales.columns:
            states.update(self.sales["state"].dropna().unique())

        for state in states:
            if not state or state == "Unknown":
                continue
            pnl = self.calculate_market_pnl(state)
            if pnl:
                markets[state] = pnl

        return markets

    def generate_market_matrix(self) -> str:
        """Generate ASCII market matrix."""
        markets = self.analyze_all_markets()

        if not markets:
            return "No market data available."

        # Sort by inventory
        sorted_markets = sorted(markets.values(), key=lambda x: x.inventory_count, reverse=True)

        lines = []
        lines.append("\n" + "=" * 78)
        lines.append("  MARKET P&L MATRIX")
        lines.append("=" * 78)
        lines.append("")
        lines.append(f"  {'State':<6} {'Inv':>5} {'Sales':>6} {'Win%':>6} {'Margin':>7} {'DOM':>5} "
                    f"{'UW%':>5} {'Trend':<8} {'Action':<6}")
        lines.append("  " + "─" * 72)

        for m in sorted_markets[:12]:
            trend_icon = {"strong": "↑", "stable": "→", "weak": "↓"}.get(m.trend, "?")
            action_color = {"GROW": "✓", "HOLD": "·", "PAUSE": "!", "EXIT": "✗"}.get(m.action, "?")

            lines.append(
                f"  {m.state:<6} {m.inventory_count:>5} {m.sales_count:>6} "
                f"{m.win_rate:>5.1f}% {m.contribution_margin:>6.1f}% {m.avg_dom:>5.0f} "
                f"{m.underwater_pct:>4.0f}% {trend_icon} {m.trend:<6} {action_color} {m.action:<5}"
            )

        # Summary
        total_inv = sum(m.inventory_count for m in sorted_markets)
        total_sales = sum(m.sales_count for m in sorted_markets)
        total_profit = sum(m.total_profit for m in sorted_markets)

        grow_markets = [m for m in sorted_markets if m.action == "GROW"]
        pause_markets = [m for m in sorted_markets if m.action in ("PAUSE", "EXIT")]

        lines.append("  " + "─" * 72)
        lines.append(f"  TOTAL: {total_inv} inventory, {total_sales} sales, ${total_profit:,.0f} profit")
        lines.append(f"  GROW: {len(grow_markets)} markets | PAUSE/EXIT: {len(pause_markets)} markets")
        lines.append("=" * 78)

        return "\n".join(lines)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary for dashboard integration."""
        markets = self.analyze_all_markets()

        return {
            "markets": [
                {
                    "state": m.state,
                    "inventory_count": m.inventory_count,
                    "sales_count": m.sales_count,
                    "toxic_count": m.toxic_count,
                    "win_rate": round(m.win_rate, 1),
                    "contribution_margin": round(m.contribution_margin, 1),
                    "avg_profit": round(m.avg_profit, 0),
                    "avg_dom": round(m.avg_dom, 0),
                    "underwater_pct": round(m.underwater_pct, 1),
                    "action": m.action,
                }
                for m in sorted(markets.values(), key=lambda x: x.inventory_count, reverse=True)
            ],
            "actions": {
                "grow": [m.state for m in markets.values() if m.action == "GROW"],
                "hold": [m.state for m in markets.values() if m.action == "HOLD"],
                "pause": [m.state for m in markets.values() if m.action == "PAUSE"],
                "exit": [m.state for m in markets.values() if m.action == "EXIT"],
            }
        }
