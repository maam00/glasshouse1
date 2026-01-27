"""
Property Enrichment
====================
Enrich sales data with geographic info.

The sales CSV from Parcl doesn't include state, but we can:
1. Try to look up properties via Parcl API (expensive - 1 credit each)
2. Use Zillow/other free APIs
3. Estimate based on historical patterns

For now, we'll try free approaches first.
"""

import logging
import urllib.request
import json
import re
from typing import Dict, Optional, List
import pandas as pd

logger = logging.getLogger(__name__)


class PropertyEnricher:
    """Enrich property data with geographic info."""

    def __init__(self):
        pass

    def estimate_state_from_price_patterns(
        self,
        sales_df: pd.DataFrame,
        listings_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Estimate state for sales based on price patterns from listings.

        This is an approximation - we match sales to the most likely state
        based on purchase price similarity to listing purchase prices.

        Not perfect, but better than nothing for market-level analysis.
        """
        if sales_df.empty or listings_df.empty:
            return sales_df

        if "state" not in listings_df.columns or "purchase_price" not in listings_df.columns:
            logger.warning("Cannot estimate state - listings missing required columns")
            return sales_df

        # Build state price distributions from listings
        state_stats = listings_df.groupby("state")["purchase_price"].agg(["mean", "std", "count"])
        state_stats = state_stats[state_stats["count"] >= 5]  # Need enough data

        if state_stats.empty:
            return sales_df

        def find_likely_state(purchase_price: float) -> str:
            """Find most likely state based on purchase price."""
            if pd.isna(purchase_price) or purchase_price <= 0:
                return "Unknown"

            best_match = "Unknown"
            best_score = float("inf")

            for state, row in state_stats.iterrows():
                # Z-score distance
                if row["std"] > 0:
                    z_score = abs((purchase_price - row["mean"]) / row["std"])
                else:
                    z_score = abs(purchase_price - row["mean"]) / row["mean"]

                # Weight by count (more data = more confidence)
                weighted_score = z_score / (row["count"] ** 0.5)

                if weighted_score < best_score:
                    best_score = weighted_score
                    best_match = state

            return best_match if best_score < 3 else "Unknown"  # 3 std devs threshold

        # Apply estimation
        sales_df = sales_df.copy()
        if "state" not in sales_df.columns:
            sales_df["state"] = "Unknown"

        mask = sales_df["state"].isna() | (sales_df["state"] == "Unknown")
        sales_df.loc[mask, "state"] = sales_df.loc[mask, "purchase_price"].apply(find_likely_state)

        # Log results
        estimated = (sales_df["state"] != "Unknown").sum()
        logger.info(f"Estimated state for {estimated}/{len(sales_df)} sales based on price patterns")

        return sales_df

    def use_state_distribution(
        self,
        sales_df: pd.DataFrame,
        listings_df: pd.DataFrame
    ) -> Dict[str, float]:
        """
        Instead of per-sale state, estimate sales distribution by state
        based on inventory distribution.

        Returns: dict of state -> estimated sales count
        """
        if listings_df.empty or "state" not in listings_df.columns:
            return {}

        # Get inventory distribution
        inv_dist = listings_df["state"].value_counts(normalize=True)

        # Apply to total sales
        total_sales = len(sales_df)

        return {
            state: int(pct * total_sales)
            for state, pct in inv_dist.items()
        }


def enrich_sales_with_state_estimate(
    sales_df: pd.DataFrame,
    listings_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Convenience function to enrich sales with estimated state.
    """
    enricher = PropertyEnricher()
    return enricher.estimate_state_from_price_patterns(sales_df, listings_df)


def get_state_distribution_for_sales(
    sales_df: pd.DataFrame,
    listings_df: pd.DataFrame
) -> Dict[str, int]:
    """
    Get estimated state distribution for sales based on inventory.
    """
    enricher = PropertyEnricher()
    return enricher.use_state_distribution(sales_df, listings_df)
