"""
FRED (Federal Reserve Economic Data) API Client
=================================================
Free API for macro economic data.

Get API key at: https://fred.stlouisfed.org/docs/api/api_key.html
(Optional - some data available without key)
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)


# Key FRED series for housing/real estate
FRED_SERIES = {
    # Mortgage rates
    "MORTGAGE30US": "30-Year Fixed Mortgage Rate",
    "MORTGAGE15US": "15-Year Fixed Mortgage Rate",

    # Housing market
    "HOUST": "Housing Starts (Thousands)",
    "PERMIT": "Building Permits (Thousands)",
    "HSN1F": "New Home Sales (Thousands)",
    "EXHOSLUSM495S": "Existing Home Sales (Millions)",

    # Prices
    "CSUSHPINSA": "Case-Shiller Home Price Index",
    "MSPUS": "Median Sales Price of Houses Sold",

    # Inventory
    "MSACSR": "Monthly Supply of Houses",
    "ACTLISCOUUS": "Active Listing Count",

    # Economic indicators
    "UNRATE": "Unemployment Rate",
    "CPIAUCSL": "Consumer Price Index",
    "FEDFUNDS": "Federal Funds Rate",
}


@dataclass
class FREDSeries:
    series_id: str
    title: str
    value: float
    date: str
    units: str
    change_from_year_ago: float


class FREDClient:
    """Fetch economic data from FRED."""

    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        # Note: Some endpoints work without API key with limited access

    def _fetch(self, endpoint: str, params: Dict) -> Optional[Dict]:
        """Make HTTP request to FRED API."""
        if self.api_key:
            params["api_key"] = self.api_key
        params["file_type"] = "json"

        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{self.BASE_URL}/{endpoint}?{query}"

        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=10) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            logger.error(f"FRED request failed: {e}")
            return None

    def get_series(self, series_id: str, limit: int = 10) -> List[Dict]:
        """Get observations for a FRED series."""
        data = self._fetch("series/observations", {
            "series_id": series_id,
            "sort_order": "desc",
            "limit": limit,
        })

        if not data:
            return []

        observations = data.get("observations", [])
        return [
            {
                "date": obs.get("date"),
                "value": float(obs.get("value", 0)) if obs.get("value", ".") != "." else None,
            }
            for obs in observations
        ]

    def get_latest(self, series_id: str) -> Optional[FREDSeries]:
        """Get latest value for a series."""
        observations = self.get_series(series_id, limit=2)

        if not observations or observations[0]["value"] is None:
            return None

        current = observations[0]

        # Try to get year-ago value for comparison
        year_ago_obs = self.get_series(series_id, limit=52)  # ~1 year of weekly data
        year_ago_value = None
        if len(year_ago_obs) > 50:
            year_ago_value = year_ago_obs[-1]["value"]

        change = 0
        if year_ago_value and year_ago_value != 0:
            change = ((current["value"] - year_ago_value) / year_ago_value) * 100

        return FREDSeries(
            series_id=series_id,
            title=FRED_SERIES.get(series_id, series_id),
            value=current["value"],
            date=current["date"],
            units="",  # Would need series/info call
            change_from_year_ago=round(change, 1),
        )

    def get_mortgage_rates(self) -> Dict[str, Any]:
        """Get current mortgage rates."""
        rate_30 = self.get_latest("MORTGAGE30US")
        rate_15 = self.get_latest("MORTGAGE15US")

        return {
            "rate_30yr": rate_30.value if rate_30 else None,
            "rate_30yr_change_yoy": rate_30.change_from_year_ago if rate_30 else None,
            "rate_15yr": rate_15.value if rate_15 else None,
            "as_of": rate_30.date if rate_30 else None,
        }

    def get_housing_indicators(self) -> Dict[str, Any]:
        """Get key housing market indicators."""
        indicators = {}

        series_to_fetch = [
            "HOUST",      # Housing starts
            "PERMIT",     # Building permits
            "HSN1F",      # New home sales
            "MSACSR",     # Months supply
            "MSPUS",      # Median price
        ]

        for series_id in series_to_fetch:
            data = self.get_latest(series_id)
            if data:
                indicators[series_id.lower()] = {
                    "value": data.value,
                    "title": data.title,
                    "date": data.date,
                    "yoy_change": data.change_from_year_ago,
                }

        return indicators

    def get_macro_snapshot(self) -> Dict[str, Any]:
        """Get broad macro economic snapshot."""
        snapshot = {}

        series_to_fetch = [
            "FEDFUNDS",   # Fed funds rate
            "UNRATE",     # Unemployment
            "CPIAUCSL",   # CPI
        ]

        for series_id in series_to_fetch:
            data = self.get_latest(series_id)
            if data:
                snapshot[series_id.lower()] = {
                    "value": data.value,
                    "title": data.title,
                    "yoy_change": data.change_from_year_ago,
                }

        return snapshot
