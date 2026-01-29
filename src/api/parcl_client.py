"""
Parcl Labs API Client for Opendoor Data
========================================
Handles all API interactions with rate limiting and error handling.
"""

import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import logging

from .retry import retry_with_backoff, RetryConfig, calculate_backoff

logger = logging.getLogger(__name__)

try:
    from parcllabs import ParclLabsClient
    PARCL_AVAILABLE = True
except ImportError:
    PARCL_AVAILABLE = False
    logger.warning("parcllabs not installed. Run: pip install parcllabs")

try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas required. Run: pip install pandas")


class ParclClient:
    """Low-level Parcl Labs API wrapper with rate limiting."""

    # Known Parcl IDs
    US_NATIONAL_ID = 2900187

    # Opendoor entity name variations (they may appear differently in records)
    OPENDOOR_ENTITIES = [
        "OPENDOOR",
        "OPENDOOR LABS",
        "OPENDOOR PROPERTY",
        "OPENDOOR PROPERTY TRUST",
        "OPENDOOR PROPERTY J LLC",
        "OPENDOOR PROPERTY C LLC",
        "OPENDOOR PROPERTY TRUST I LLC",
        "OPENDOOR TECHNOLOGIES",
        "OD HOMES",
    ]

    def __init__(self, api_key: Optional[str] = None, num_workers: int = 5):
        if not PARCL_AVAILABLE:
            raise ImportError("parcllabs package not installed")

        self.api_key = api_key or os.getenv("PARCLLABS_API_KEY")
        if not self.api_key:
            raise ValueError(
                "API key required. Set PARCLLABS_API_KEY env var or pass api_key parameter"
            )

        self.client = ParclLabsClient(self.api_key, num_workers=num_workers)
        self._last_request_time = 0
        self._min_request_interval = 0.2  # 200ms between requests (safer rate limit)
        self._retry_config = RetryConfig(
            max_retries=3,
            base_delay=1.0,
            max_delay=30.0,
            retryable_exceptions=(Exception,),  # Parcl client raises generic exceptions
        )

    def _rate_limit(self):
        """Rate limiting with jitter to prevent thundering herd."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            # Add small jitter (0-50ms) to prevent synchronized requests
            import random
            jitter = random.uniform(0, 0.05)
            time.sleep(self._min_request_interval - elapsed + jitter)
        self._last_request_time = time.time()

    def search_properties(
        self,
        parcl_ids: List[int] = None,
        entity_seller_name: List[str] = None,
        current_entity_owner_name: List[str] = None,
        current_on_market_flag: bool = None,
        event_names: List[str] = None,
        min_event_date: str = None,
        max_event_date: str = None,
        include_property_details: bool = True,
        include_full_event_history: bool = True,
        owner_name: List[str] = None,
        limit: int = 10000,
    ) -> pd.DataFrame:
        """
        Search properties via Property Search V2 endpoint.

        This is the core endpoint for Opendoor tracking.
        """
        parcl_ids = parcl_ids or [self.US_NATIONAL_ID]

        # Build kwargs dynamically to avoid passing None values
        kwargs = {
            "parcl_ids": parcl_ids,
            "include_property_details": include_property_details,
            "limit": limit,
        }

        if entity_seller_name:
            kwargs["entity_seller_name"] = entity_seller_name
        if current_entity_owner_name:
            kwargs["current_entity_owner_name"] = current_entity_owner_name
        if current_on_market_flag is not None:
            kwargs["current_on_market_flag"] = current_on_market_flag
        if event_names:
            kwargs["event_names"] = event_names
        if min_event_date:
            kwargs["min_event_date"] = min_event_date
        if max_event_date:
            kwargs["max_event_date"] = max_event_date
        if include_full_event_history:
            kwargs["include_full_event_history"] = include_full_event_history
        if owner_name:
            kwargs["owner_name"] = owner_name

        def make_request():
            self._rate_limit()
            df = self.client.property_v2.search.retrieve(**kwargs)
            return df if df is not None else pd.DataFrame()

        try:
            return retry_with_backoff(
                make_request,
                self._retry_config,
                on_retry=lambda attempt, e: logger.warning(
                    f"Parcl API retry {attempt + 1}/{self._retry_config.max_retries + 1}: {e}"
                )
            )
        except Exception as e:
            logger.error(f"Property search failed after retries: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()


class OpendoorDataFetcher:
    """High-level data fetcher specifically for Opendoor metrics."""

    def __init__(self, api_key: Optional[str] = None):
        self.client = ParclClient(api_key)
        self.entities = ParclClient.OPENDOOR_ENTITIES

    def get_sales(
        self,
        start_date: str,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        Get all Opendoor home sales (dispositions) in date range.

        Returns DataFrame with columns like:
        - sale_price, purchase_price, days_held, realized_net
        - sale_date, state, city, address
        """
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Fetching Opendoor sales from {start_date} to {end_date}")

        df = self.client.search_properties(
            entity_seller_name=self.entities,
            event_names=["ALL_SOLD"],
            min_event_date=start_date,
            max_event_date=end_date,
            include_full_event_history=True,
        )

        logger.info(f"Retrieved {len(df)} sales records")
        return df

    def get_current_listings(self) -> pd.DataFrame:
        """
        Get all current Opendoor listings (active inventory).

        Returns DataFrame with columns like:
        - list_price, purchase_price, days_on_market, unrealized_pnl
        - state, city, address
        """
        logger.info("Fetching current Opendoor listings")

        df = self.client.search_properties(
            current_entity_owner_name=self.entities,
            current_on_market_flag=True,
        )

        logger.info(f"Retrieved {len(df)} active listings")
        return df

    def get_purchases(
        self,
        start_date: str,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        Get all Opendoor home purchases (acquisitions) in date range.

        Note: Uses owner_name filter to find properties where Opendoor
        became the owner during the date range.
        """
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Fetching Opendoor purchases from {start_date} to {end_date}")

        # Get properties where Opendoor is/was owner with transaction in date range
        df = self.client.search_properties(
            owner_name=self.entities,
            event_names=["ALL_SOLD"],
            min_event_date=start_date,
            max_event_date=end_date,
        )

        logger.info(f"Retrieved {len(df)} purchase records")
        return df

    def get_all_inventory(self) -> pd.DataFrame:
        """
        Get all Opendoor-owned properties (listed or not).
        """
        logger.info("Fetching all Opendoor inventory")

        df = self.client.search_properties(
            current_entity_owner_name=self.entities,
            current_on_market_flag=None,  # All, not just listed
        )

        logger.info(f"Retrieved {len(df)} total inventory records")
        return df

    def get_historical_sales(self, lookback_days: int = 365) -> pd.DataFrame:
        """Get sales history for trend analysis."""
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        return self.get_sales(start_date)


# Utility function to check available fields
def inspect_api_response(df: pd.DataFrame) -> Dict[str, Any]:
    """Inspect DataFrame to understand available fields."""
    if df.empty:
        return {"empty": True, "columns": []}

    return {
        "columns": list(df.columns),
        "dtypes": df.dtypes.to_dict(),
        "sample_row": df.iloc[0].to_dict() if len(df) > 0 else {},
        "row_count": len(df),
    }
