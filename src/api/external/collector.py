"""
External Data Collector
========================
Unified collector for all external data sources.
"""

import logging
from datetime import datetime
from typing import Dict, Any
from dataclasses import dataclass, asdict

from .yahoo_finance import YahooFinanceClient
from .fred import FREDClient
from .sec_edgar import SECEdgarClient
from .mortgage_rates import MortgageRateClient

logger = logging.getLogger(__name__)


@dataclass
class MarketContext:
    """Complete market context from external sources."""

    # Stock data
    stock_price: float
    stock_change_pct: float
    market_cap: float
    pe_ratio: float
    week_52_high: float
    week_52_low: float
    pct_from_52_high: float
    short_interest_pct: float

    # Earnings
    next_earnings_date: str
    days_to_earnings: int

    # Mortgage rates
    mortgage_rate_30yr: float
    mortgage_rate_change_month: float
    mortgage_rate_change_year: float

    # Housing market
    housing_starts: float
    building_permits: float
    months_supply: float

    # Macro
    fed_funds_rate: float
    unemployment_rate: float

    # SEC filings
    last_10q_date: str
    last_8k_date: str

    timestamp: str


class ExternalDataCollector:
    """Collect data from all external sources."""

    def __init__(self, fred_api_key: str = None):
        self.yahoo = YahooFinanceClient()
        self.fred = FREDClient(api_key=fred_api_key)
        self.sec = SECEdgarClient()
        self.mortgage = MortgageRateClient()

    def collect_all(self) -> Dict[str, Any]:
        """Collect data from all sources."""
        logger.info("Collecting external data...")

        data = {
            "stock": self._collect_stock(),
            "earnings": self._collect_earnings(),
            "mortgage": self._collect_mortgage(),
            "housing": self._collect_housing(),
            "macro": self._collect_macro(),
            "sec": self._collect_sec(),
            "timestamp": datetime.now().isoformat(),
        }

        logger.info("External data collection complete")
        return data

    def _collect_stock(self) -> Dict[str, Any]:
        """Collect stock data."""
        logger.info("  Fetching stock data...")
        try:
            quote = self.yahoo.get_quote("OPEN")
            stats = self.yahoo.get_key_stats("OPEN")

            if not quote:
                return {"error": "Failed to fetch stock data"}

            pct_from_high = 0
            if quote.week_52_high > 0:
                pct_from_high = ((quote.price - quote.week_52_high) / quote.week_52_high) * 100

            return {
                "price": quote.price,
                "change": quote.change,
                "change_pct": quote.change_pct,
                "volume": quote.volume,
                "market_cap": quote.market_cap,
                "pe_ratio": quote.pe_ratio,
                "week_52_high": quote.week_52_high,
                "week_52_low": quote.week_52_low,
                "pct_from_52_high": round(pct_from_high, 1),
                "short_pct_float": stats.get("short_pct_float", 0),
                "total_cash": stats.get("total_cash", 0),
                "total_debt": stats.get("total_debt", 0),
                "revenue_growth": stats.get("revenue_growth", 0),
            }
        except Exception as e:
            logger.error(f"Stock data error: {e}")
            return {"error": str(e)}

    def _collect_earnings(self) -> Dict[str, Any]:
        """Collect earnings info."""
        logger.info("  Fetching earnings data...")
        try:
            info = self.yahoo.get_earnings_info("OPEN")
            if not info:
                return {"error": "Failed to fetch earnings data"}

            return {
                "next_date": info.next_earnings_date,
                "days_until": info.days_until_earnings,
                "last_eps_actual": info.last_eps_actual,
                "last_eps_estimate": info.last_eps_estimate,
                "last_surprise_pct": info.last_eps_surprise_pct,
            }
        except Exception as e:
            logger.error(f"Earnings data error: {e}")
            return {"error": str(e)}

    def _collect_mortgage(self) -> Dict[str, Any]:
        """Collect mortgage rate data."""
        logger.info("  Fetching mortgage rates...")
        try:
            rates = self.mortgage.get_current_rates()
            changes = self.mortgage.get_rate_change()
            impact = self.mortgage.get_affordability_impact()

            if not rates:
                return {"error": "Failed to fetch mortgage data"}

            return {
                "rate_30yr": rates.rate_30yr,
                "rate_15yr": rates.rate_15yr,
                "as_of": rates.date,
                "week_change": changes.get("week_change"),
                "month_change": changes.get("month_change"),
                "year_change": changes.get("year_change"),
                "monthly_payment_400k": impact.get("monthly_payment"),
            }
        except Exception as e:
            logger.error(f"Mortgage data error: {e}")
            return {"error": str(e)}

    def _collect_housing(self) -> Dict[str, Any]:
        """Collect housing market indicators."""
        logger.info("  Fetching housing indicators...")
        try:
            indicators = self.fred.get_housing_indicators()
            return indicators
        except Exception as e:
            logger.error(f"Housing data error: {e}")
            return {"error": str(e)}

    def _collect_macro(self) -> Dict[str, Any]:
        """Collect macro economic data."""
        logger.info("  Fetching macro data...")
        try:
            return self.fred.get_macro_snapshot()
        except Exception as e:
            logger.error(f"Macro data error: {e}")
            return {"error": str(e)}

    def _collect_sec(self) -> Dict[str, Any]:
        """Collect SEC filing info."""
        logger.info("  Fetching SEC filings...")
        try:
            company = self.sec.get_company_info()
            dates = self.sec.get_filing_dates()
            metrics = self.sec.extract_key_metrics()
            recent_8k = self.sec.get_8k_filings(limit=3)

            return {
                "company_name": company.get("name", "Opendoor Technologies Inc"),
                "last_10q": dates.get("last_10q"),
                "last_10k": dates.get("last_10k"),
                "last_8k": dates.get("last_8k"),
                "recent_8k_filings": [
                    {"date": f.filed_date, "description": f.description[:50]}
                    for f in recent_8k
                ],
                "revenue_from_filing": metrics.get("revenue"),
                "inventory_from_filing": metrics.get("inventory"),
                "cash_from_filing": metrics.get("cash"),
            }
        except Exception as e:
            logger.error(f"SEC data error: {e}")
            return {"error": str(e)}

    def get_market_context(self) -> MarketContext:
        """Get structured market context."""
        data = self.collect_all()

        stock = data.get("stock", {})
        earnings = data.get("earnings", {})
        mortgage = data.get("mortgage", {})
        housing = data.get("housing", {})
        macro = data.get("macro", {})
        sec = data.get("sec", {})

        return MarketContext(
            stock_price=stock.get("price", 0),
            stock_change_pct=stock.get("change_pct", 0),
            market_cap=stock.get("market_cap", 0),
            pe_ratio=stock.get("pe_ratio", 0),
            week_52_high=stock.get("week_52_high", 0),
            week_52_low=stock.get("week_52_low", 0),
            pct_from_52_high=stock.get("pct_from_52_high", 0),
            short_interest_pct=stock.get("short_pct_float", 0),
            next_earnings_date=earnings.get("next_date", "Unknown"),
            days_to_earnings=earnings.get("days_until", 0),
            mortgage_rate_30yr=mortgage.get("rate_30yr", 0),
            mortgage_rate_change_month=mortgage.get("month_change", 0),
            mortgage_rate_change_year=mortgage.get("year_change", 0),
            housing_starts=housing.get("houst", {}).get("value", 0),
            building_permits=housing.get("permit", {}).get("value", 0),
            months_supply=housing.get("msacsr", {}).get("value", 0),
            fed_funds_rate=macro.get("fedfunds", {}).get("value", 0),
            unemployment_rate=macro.get("unrate", {}).get("value", 0),
            last_10q_date=sec.get("last_10q", ""),
            last_8k_date=sec.get("last_8k", ""),
            timestamp=data.get("timestamp", ""),
        )
