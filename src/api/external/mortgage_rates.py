"""
Mortgage Rate Data
===================
Fetch current and historical mortgage rates.
Uses Freddie Mac PMMS data (free, no API key).
"""

import csv
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import urllib.request
import urllib.error
from io import StringIO

logger = logging.getLogger(__name__)


@dataclass
class MortgageRate:
    date: str
    rate_30yr: float
    rate_15yr: float
    points_30yr: float
    points_15yr: float


class MortgageRateClient:
    """Fetch mortgage rate data from Freddie Mac."""

    # Freddie Mac Primary Mortgage Market Survey
    PMMS_URL = "https://www.freddiemac.com/pmms/docs/PMMS_history.csv"

    # Alternative: FRED API (more reliable)
    FRED_30YR_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=MORTGAGE30US"
    FRED_15YR_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=MORTGAGE15US"

    def __init__(self):
        self._cache = {}
        self._cache_time = None

    def _fetch_csv(self, url: str) -> Optional[str]:
        """Fetch CSV data from URL."""
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0"
            })
            with urllib.request.urlopen(req, timeout=15) as response:
                return response.read().decode("utf-8")
        except Exception as e:
            logger.error(f"Failed to fetch mortgage data: {e}")
            return None

    def get_current_rates(self) -> Optional[MortgageRate]:
        """Get the most recent mortgage rates."""
        # Try FRED first (more reliable)
        csv_data = self._fetch_csv(self.FRED_30YR_URL)
        if csv_data:
            lines = csv_data.strip().split("\n")
            if len(lines) > 1:
                last_line = lines[-1]
                parts = last_line.split(",")
                if len(parts) >= 2:
                    date = parts[0]
                    rate_30 = float(parts[1]) if parts[1] != "." else 0

                    # Get 15yr rate
                    csv_15 = self._fetch_csv(self.FRED_15YR_URL)
                    rate_15 = 0
                    if csv_15:
                        lines_15 = csv_15.strip().split("\n")
                        if len(lines_15) > 1:
                            parts_15 = lines_15[-1].split(",")
                            if len(parts_15) >= 2 and parts_15[1] != ".":
                                rate_15 = float(parts_15[1])

                    return MortgageRate(
                        date=date,
                        rate_30yr=rate_30,
                        rate_15yr=rate_15,
                        points_30yr=0,  # Not available from FRED
                        points_15yr=0,
                    )

        return None

    def get_rate_history(self, days: int = 365) -> List[MortgageRate]:
        """Get historical mortgage rates."""
        csv_data = self._fetch_csv(self.FRED_30YR_URL)
        if not csv_data:
            return []

        rates = []
        lines = csv_data.strip().split("\n")[1:]  # Skip header

        cutoff = datetime.now() - timedelta(days=days)

        for line in lines:
            parts = line.split(",")
            if len(parts) >= 2:
                try:
                    date = datetime.strptime(parts[0], "%Y-%m-%d")
                    if date >= cutoff and parts[1] != ".":
                        rates.append(MortgageRate(
                            date=parts[0],
                            rate_30yr=float(parts[1]),
                            rate_15yr=0,  # Would need separate query
                            points_30yr=0,
                            points_15yr=0,
                        ))
                except ValueError:
                    continue

        return rates

    def get_rate_change(self) -> Dict[str, float]:
        """Calculate rate changes over various periods."""
        rates = self.get_rate_history(days=365)

        if len(rates) < 2:
            return {}

        current = rates[-1].rate_30yr

        # Find rates at different points
        week_ago = None
        month_ago = None
        year_ago = None

        now = datetime.now()

        for r in reversed(rates):
            rate_date = datetime.strptime(r.date, "%Y-%m-%d")
            days_diff = (now - rate_date).days

            if week_ago is None and days_diff >= 7:
                week_ago = r.rate_30yr
            if month_ago is None and days_diff >= 30:
                month_ago = r.rate_30yr
            if year_ago is None and days_diff >= 365:
                year_ago = r.rate_30yr
                break

        return {
            "current": current,
            "week_change": round(current - week_ago, 2) if week_ago else None,
            "month_change": round(current - month_ago, 2) if month_ago else None,
            "year_change": round(current - year_ago, 2) if year_ago else None,
        }

    def get_affordability_impact(self, home_price: float = 400000, down_pct: float = 0.20) -> Dict[str, Any]:
        """
        Calculate how rate changes impact monthly payment.
        Useful for understanding buyer demand.
        """
        rates = self.get_current_rates()
        if not rates:
            return {}

        loan_amount = home_price * (1 - down_pct)

        def monthly_payment(principal: float, annual_rate: float, years: int = 30) -> float:
            if annual_rate == 0:
                return principal / (years * 12)
            monthly_rate = annual_rate / 100 / 12
            n_payments = years * 12
            return principal * (monthly_rate * (1 + monthly_rate)**n_payments) / ((1 + monthly_rate)**n_payments - 1)

        current_payment = monthly_payment(loan_amount, rates.rate_30yr)

        # Compare to 1% lower rate
        payment_at_minus_1 = monthly_payment(loan_amount, rates.rate_30yr - 1)

        return {
            "home_price": home_price,
            "down_payment": home_price * down_pct,
            "loan_amount": loan_amount,
            "current_rate": rates.rate_30yr,
            "monthly_payment": round(current_payment, 2),
            "payment_if_rate_minus_1pct": round(payment_at_minus_1, 2),
            "monthly_savings_per_1pct": round(current_payment - payment_at_minus_1, 2),
        }
