"""
Strategic Parcl Labs API Usage
===============================
Use 1,000 monthly credits wisely for data CSVs don't have.

Strategy:
1. Market-level metrics (low credits) - context for Opendoor performance
2. Investor activity (low credits) - what are other iBuyers doing?
3. Enrich sales with geo (high credits, do sparingly) - per-market win rate

Credit Budget:
- Market metrics: ~5-10 credits per market, ~50 total for key markets
- Investor metrics: ~10 credits per query
- Property enrichment: ~1 credit per property (expensive at scale)

Target: Use ~200-300 credits/month, save buffer for deep dives.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

try:
    from parcllabs import ParclLabsClient
    PARCL_AVAILABLE = True
except ImportError:
    PARCL_AVAILABLE = False
    logger.warning("parcllabs not installed")

import pandas as pd


# Opendoor's key markets (by inventory concentration from CSVs)
OPENDOOR_MARKETS = {
    # State: (Name, Parcl Market ID for major metro)
    "NC": ("Charlotte", 2900078),
    "GA": ("Atlanta", 2900012),
    "TX": ("Dallas", 2900098),  # Also Houston, Austin, San Antonio
    "FL": ("Tampa", 2900464),   # Also Orlando, Jacksonville
    "AZ": ("Phoenix", 2900038),
    "CA": ("Los Angeles", 2900278),
    "CO": ("Denver", 2900116),
    "TN": ("Nashville", 2900350),
    "SC": ("Charlotte-adjacent", 2900078),  # Uses Charlotte metro
}

# Additional Texas metros
TX_METROS = {
    "Houston": 2900206,
    "Austin": 2900014,
    "San Antonio": 2900416,
    "Dallas": 2900098,
}

# Florida metros
FL_METROS = {
    "Tampa": 2900464,
    "Orlando": 2900372,
    "Jacksonville": 2900222,
}


@dataclass
class MarketMetrics:
    """Market-level performance metrics."""
    market_name: str
    parcl_id: int

    # Inventory
    active_listings: int
    median_list_price: float
    avg_days_on_market: float
    months_supply: float

    # Sales
    closed_sales_count: int
    median_sale_price: float
    avg_sale_to_list: float

    # Trends
    price_change_yoy: float
    inventory_change_yoy: float

    # Investor activity
    investor_purchase_share: float
    investor_sale_share: float


@dataclass
class InvestorActivity:
    """Investor/iBuyer activity in market."""
    market_name: str
    parcl_id: int

    # Large portfolio (1000+ homes) activity
    large_portfolio_purchases: int
    large_portfolio_sales: int
    large_portfolio_net: int

    # Medium portfolio (100-999 homes)
    medium_portfolio_purchases: int
    medium_portfolio_sales: int


class StrategicParclAPI:
    """Strategic use of Parcl API credits."""

    def __init__(self, api_key: str = None):
        if not PARCL_AVAILABLE:
            raise ImportError("parcllabs package required")

        self.api_key = api_key or os.getenv("PARCLLABS_API_KEY")
        if not self.api_key:
            raise ValueError("PARCLLABS_API_KEY required")

        self.client = ParclLabsClient(self.api_key, num_workers=3)
        self._credit_usage = 0

    def _log_credits(self, operation: str, estimated_credits: int):
        """Track credit usage."""
        self._credit_usage += estimated_credits
        logger.info(f"  [{operation}] ~{estimated_credits} credits (total: {self._credit_usage})")

    def get_market_metrics(self, parcl_id: int, market_name: str = "") -> Optional[MarketMetrics]:
        """
        Get market-level metrics for context.
        Estimated credits: ~15 per market (3 API calls)
        """
        try:
            # For-sale inventory count
            inventory = self.client.for_sale_market_metrics.for_sale_inventory.retrieve(
                parcl_ids=[parcl_id],
                limit=1,
            )

            # Sales counts
            sales = self.client.market_metrics.housing_event_counts.retrieve(
                parcl_ids=[parcl_id],
                limit=1,
            )

            # Prices (median sale price, median list price)
            prices = self.client.market_metrics.housing_event_prices.retrieve(
                parcl_ids=[parcl_id],
                limit=1,
            )

            self._log_credits(f"market_metrics:{market_name}", 15)

            # Parse results
            inv_row = inventory.iloc[0].to_dict() if not inventory.empty else {}
            sales_row = sales.iloc[0].to_dict() if not sales.empty else {}
            price_row = prices.iloc[0].to_dict() if not prices.empty else {}

            # Calculate months of supply: inventory / (sales per month)
            inventory_count = int(inv_row.get("for_sale_inventory", 0))
            sales_count = int(sales_row.get("sales", 0))
            months_supply = inventory_count / sales_count if sales_count > 0 else 0

            return MarketMetrics(
                market_name=market_name,
                parcl_id=parcl_id,
                active_listings=inventory_count,
                median_list_price=float(price_row.get("price_median_new_listings_for_sale", 0)),
                avg_days_on_market=0,  # Not directly available from these endpoints
                months_supply=months_supply,
                closed_sales_count=sales_count,
                median_sale_price=float(price_row.get("price_median_sales", 0)),
                avg_sale_to_list=0,
                price_change_yoy=0,
                inventory_change_yoy=0,
                investor_purchase_share=0,
                investor_sale_share=0,
            )

        except Exception as e:
            logger.error(f"Failed to get market metrics for {market_name}: {e}")
            return None

    def get_investor_activity(self, parcl_id: int, market_name: str = "") -> Optional[InvestorActivity]:
        """
        Get investor/iBuyer activity in market.
        Estimated credits: ~10 per market
        """
        try:
            # Get investor activity (all investors, not portfolio-specific)
            investor_data = self.client.investor_metrics.housing_event_counts.retrieve(
                parcl_ids=[parcl_id],
                limit=1,
            )

            self._log_credits(f"investor_activity:{market_name}", 10)

            if not investor_data.empty:
                row = investor_data.iloc[0].to_dict()
                logger.debug(f"Investor data for {market_name}: {row}")

                # API returns: acquisitions, dispositions, new_listings_for_sale, new_rental_listings, transfers
                acquisitions = int(row.get("acquisitions", 0))
                dispositions = int(row.get("dispositions", 0))

                return InvestorActivity(
                    market_name=market_name,
                    parcl_id=parcl_id,
                    large_portfolio_purchases=acquisitions,
                    large_portfolio_sales=dispositions,
                    large_portfolio_net=acquisitions - dispositions,
                    medium_portfolio_purchases=0,  # Not available from this endpoint
                    medium_portfolio_sales=0,
                )
            else:
                return InvestorActivity(
                    market_name=market_name,
                    parcl_id=parcl_id,
                    large_portfolio_purchases=0,
                    large_portfolio_sales=0,
                    large_portfolio_net=0,
                    medium_portfolio_purchases=0,
                    medium_portfolio_sales=0,
                )

        except Exception as e:
            logger.error(f"Failed to get investor activity for {market_name}: {e}")
            return None

    def get_opendoor_market_context(self) -> Dict[str, Any]:
        """
        Get market context for all Opendoor markets.
        Estimated credits: ~200 total
        """
        logger.info("Fetching Opendoor market context...")

        results = {
            "markets": {},
            "investor_activity": {},
            "credit_usage": 0,
            "timestamp": datetime.now().isoformat(),
        }

        # Get metrics for key markets
        for state, (name, parcl_id) in OPENDOOR_MARKETS.items():
            logger.info(f"  Fetching {name} ({state})...")

            metrics = self.get_market_metrics(parcl_id, name)
            if metrics:
                results["markets"][state] = {
                    "name": name,
                    "parcl_id": parcl_id,
                    "active_listings": metrics.active_listings,
                    "median_list_price": metrics.median_list_price,
                    "median_sale_price": metrics.median_sale_price,
                    "sales_count": metrics.closed_sales_count,
                    "months_supply": metrics.months_supply,
                }

            activity = self.get_investor_activity(parcl_id, name)
            if activity:
                results["investor_activity"][state] = {
                    "large_purchases": activity.large_portfolio_purchases,
                    "large_sales": activity.large_portfolio_sales,
                    "large_net": activity.large_portfolio_net,
                }

        results["credit_usage"] = self._credit_usage
        logger.info(f"Market context complete. Credits used: {self._credit_usage}")

        return results

    def enrich_sales_with_geo(self, sales_df: pd.DataFrame, sample_size: int = 50) -> pd.DataFrame:
        """
        Enrich sales data with geographic info by looking up property IDs.
        EXPENSIVE: ~1 credit per property. Use sparingly.

        Args:
            sales_df: Sales DataFrame with property_id column
            sample_size: Max properties to look up (to limit credits)
        """
        if "property_id" not in sales_df.columns:
            logger.warning("No property_id column in sales data")
            return sales_df

        # Only enrich sales missing state
        if "state" in sales_df.columns:
            missing_state = sales_df[sales_df["state"].isna() | (sales_df["state"] == "")]
        else:
            missing_state = sales_df
            sales_df["state"] = None

        if missing_state.empty:
            logger.info("All sales already have state data")
            return sales_df

        # Sample to limit credit usage
        to_lookup = missing_state.head(sample_size)
        logger.info(f"Looking up {len(to_lookup)} properties (of {len(missing_state)} missing)")

        # This would use property lookup API
        # For now, placeholder - actual implementation depends on Parcl's property API
        logger.warning("Property geo enrichment not yet implemented - need to test Parcl property lookup")

        self._log_credits("property_geo_enrichment", len(to_lookup))

        return sales_df

    def get_credit_usage(self) -> int:
        """Get estimated credits used this session."""
        return self._credit_usage

    def get_monthly_budget_status(self, monthly_limit: int = 1000) -> Dict[str, Any]:
        """Check budget status."""
        return {
            "session_usage": self._credit_usage,
            "monthly_limit": monthly_limit,
            "remaining_estimate": monthly_limit - self._credit_usage,
            "usage_pct": (self._credit_usage / monthly_limit) * 100,
        }


def get_market_context_if_affordable(monthly_credits_used: int = 0, monthly_limit: int = 1000) -> Optional[Dict]:
    """
    Get market context only if we have budget.
    Estimated cost: ~200 credits
    """
    estimated_cost = 200
    remaining = monthly_limit - monthly_credits_used

    if remaining < estimated_cost:
        logger.warning(f"Insufficient credits for market context ({remaining} < {estimated_cost})")
        return None

    try:
        api = StrategicParclAPI()
        return api.get_opendoor_market_context()
    except Exception as e:
        logger.error(f"Failed to get market context: {e}")
        return None
