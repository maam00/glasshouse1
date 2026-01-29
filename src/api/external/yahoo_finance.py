"""
Yahoo Finance API Client
=========================
Free stock data, earnings dates, and company financials.
No API key required.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import urllib.request
import urllib.error

from ..retry import retry_with_backoff, RetryConfig

logger = logging.getLogger(__name__)

# Retry config for Yahoo Finance API
YAHOO_RETRY_CONFIG = RetryConfig(
    max_retries=3,
    base_delay=1.0,
    max_delay=10.0,
    retryable_exceptions=(urllib.error.URLError, urllib.error.HTTPError, TimeoutError),
)


@dataclass
class StockQuote:
    symbol: str
    price: float
    change: float
    change_pct: float
    volume: int
    market_cap: float
    pe_ratio: float
    week_52_high: float
    week_52_low: float
    avg_volume: int
    timestamp: str


@dataclass
class EarningsInfo:
    next_earnings_date: str
    days_until_earnings: int
    last_earnings_date: str
    last_eps_actual: float
    last_eps_estimate: float
    last_eps_surprise_pct: float


class YahooFinanceClient:
    """Fetch stock data from Yahoo Finance."""

    BASE_URL = "https://query1.finance.yahoo.com/v8/finance"

    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        }

    def _fetch(self, url: str) -> Optional[Dict]:
        """Make HTTP request with retry logic."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        }

        def make_request():
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as response:
                return json.loads(response.read().decode())

        try:
            return retry_with_backoff(make_request, YAHOO_RETRY_CONFIG)
        except urllib.error.HTTPError as e:
            # Try alternative endpoint for chart data
            if "quoteSummary" in url and e.code in (403, 404):
                return self._fetch_chart_fallback(url)
            logger.error(f"Yahoo Finance request failed after retries: {e}")
            return None
        except Exception as e:
            logger.error(f"Yahoo Finance request failed after retries: {e}")
            return None

    def _fetch_chart_fallback(self, original_url: str) -> Optional[Dict]:
        """Fallback to chart API if quoteSummary fails."""
        try:
            # Extract symbol from URL
            symbol = original_url.split("/")[-1].split("?")[0]
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"

            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            }
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())

                # Convert chart data to quoteSummary-like format
                result = data.get("chart", {}).get("result", [{}])[0]
                meta = result.get("meta", {})

                return {
                    "quoteSummary": {
                        "result": [{
                            "price": {
                                "regularMarketPrice": {"raw": meta.get("regularMarketPrice", 0)},
                                "regularMarketChange": {"raw": 0},
                                "regularMarketChangePercent": {"raw": 0},
                                "regularMarketVolume": {"raw": meta.get("regularMarketVolume", 0)},
                                "marketCap": {"raw": 0},
                            },
                            "summaryDetail": {
                                "fiftyTwoWeekHigh": {"raw": meta.get("fiftyTwoWeekHigh", 0)},
                                "fiftyTwoWeekLow": {"raw": meta.get("fiftyTwoWeekLow", 0)},
                            }
                        }]
                    }
                }
        except Exception as e:
            logger.error(f"Yahoo Finance fallback failed: {e}")
            return None

    def get_quote(self, symbol: str = "OPEN") -> Optional[StockQuote]:
        """Get current stock quote."""
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=price,summaryDetail"

        data = self._fetch(url)
        if not data:
            return None

        try:
            result = data.get("quoteSummary", {}).get("result", [{}])[0]
            price_data = result.get("price", {})
            summary = result.get("summaryDetail", {})

            return StockQuote(
                symbol=symbol,
                price=price_data.get("regularMarketPrice", {}).get("raw", 0),
                change=price_data.get("regularMarketChange", {}).get("raw", 0),
                change_pct=price_data.get("regularMarketChangePercent", {}).get("raw", 0) * 100,
                volume=price_data.get("regularMarketVolume", {}).get("raw", 0),
                market_cap=price_data.get("marketCap", {}).get("raw", 0),
                pe_ratio=summary.get("trailingPE", {}).get("raw", 0),
                week_52_high=summary.get("fiftyTwoWeekHigh", {}).get("raw", 0),
                week_52_low=summary.get("fiftyTwoWeekLow", {}).get("raw", 0),
                avg_volume=summary.get("averageVolume", {}).get("raw", 0),
                timestamp=datetime.now().isoformat(),
            )
        except Exception as e:
            logger.error(f"Error parsing quote data: {e}")
            return None

    def get_earnings_info(self, symbol: str = "OPEN") -> Optional[EarningsInfo]:
        """Get earnings calendar and history."""
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=calendarEvents,earnings"

        data = self._fetch(url)
        if not data:
            return None

        try:
            result = data.get("quoteSummary", {}).get("result", [{}])[0]
            calendar = result.get("calendarEvents", {})
            earnings = result.get("earnings", {})

            # Next earnings date
            earnings_dates = calendar.get("earnings", {}).get("earningsDate", [])
            next_earnings = None
            if earnings_dates:
                next_earnings = datetime.fromtimestamp(earnings_dates[0].get("raw", 0))

            days_until = (next_earnings - datetime.now()).days if next_earnings else 0

            # Last earnings
            history = earnings.get("earningsChart", {}).get("quarterly", [])
            last_eps_actual = 0
            last_eps_estimate = 0
            if history:
                last = history[-1]
                last_eps_actual = last.get("actual", {}).get("raw", 0)
                last_eps_estimate = last.get("estimate", {}).get("raw", 0)

            surprise_pct = 0
            if last_eps_estimate != 0:
                surprise_pct = ((last_eps_actual - last_eps_estimate) / abs(last_eps_estimate)) * 100

            return EarningsInfo(
                next_earnings_date=next_earnings.strftime("%Y-%m-%d") if next_earnings else "Unknown",
                days_until_earnings=days_until,
                last_earnings_date="",  # Would need separate query
                last_eps_actual=last_eps_actual,
                last_eps_estimate=last_eps_estimate,
                last_eps_surprise_pct=round(surprise_pct, 1),
            )
        except Exception as e:
            logger.error(f"Error parsing earnings data: {e}")
            return None

    def get_historical_prices(self, symbol: str = "OPEN", days: int = 30) -> List[Dict]:
        """Get historical daily prices."""
        end = int(datetime.now().timestamp())
        start = int((datetime.now() - timedelta(days=days)).timestamp())

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={start}&period2={end}&interval=1d"

        data = self._fetch(url)
        if not data:
            return []

        try:
            result = data.get("chart", {}).get("result", [{}])[0]
            timestamps = result.get("timestamp", [])
            quotes = result.get("indicators", {}).get("quote", [{}])[0]

            prices = []
            for i, ts in enumerate(timestamps):
                prices.append({
                    "date": datetime.fromtimestamp(ts).strftime("%Y-%m-%d"),
                    "open": quotes.get("open", [])[i],
                    "high": quotes.get("high", [])[i],
                    "low": quotes.get("low", [])[i],
                    "close": quotes.get("close", [])[i],
                    "volume": quotes.get("volume", [])[i],
                })

            return prices
        except Exception as e:
            logger.error(f"Error parsing historical data: {e}")
            return []

    def get_key_stats(self, symbol: str = "OPEN") -> Dict[str, Any]:
        """Get key financial statistics."""
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=defaultKeyStatistics,financialData"

        data = self._fetch(url)
        if not data:
            return {}

        try:
            result = data.get("quoteSummary", {}).get("result", [{}])[0]
            stats = result.get("defaultKeyStatistics", {})
            financials = result.get("financialData", {})

            return {
                "enterprise_value": stats.get("enterpriseValue", {}).get("raw", 0),
                "book_value": stats.get("bookValue", {}).get("raw", 0),
                "price_to_book": stats.get("priceToBook", {}).get("raw", 0),
                "short_ratio": stats.get("shortRatio", {}).get("raw", 0),
                "short_pct_float": stats.get("shortPercentOfFloat", {}).get("raw", 0) * 100,
                "revenue": financials.get("totalRevenue", {}).get("raw", 0),
                "gross_profit": financials.get("grossProfits", {}).get("raw", 0),
                "operating_cashflow": financials.get("operatingCashflow", {}).get("raw", 0),
                "free_cashflow": financials.get("freeCashflow", {}).get("raw", 0),
                "total_cash": financials.get("totalCash", {}).get("raw", 0),
                "total_debt": financials.get("totalDebt", {}).get("raw", 0),
                "revenue_growth": financials.get("revenueGrowth", {}).get("raw", 0) * 100,
            }
        except Exception as e:
            logger.error(f"Error parsing key stats: {e}")
            return {}
