"""
SEC EDGAR API Client
=====================
Free access to company filings - 10-Q, 10-K, 8-K.
No API key required.
"""

import json
import logging
import re
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)


# Opendoor CIK (Central Index Key)
OPENDOOR_CIK = "0001801169"


@dataclass
class SECFiling:
    form_type: str
    filed_date: str
    accession_number: str
    description: str
    url: str


@dataclass
class QuarterlyData:
    period: str
    revenue: float
    gross_profit: float
    net_income: float
    homes_sold: int
    homes_purchased: int
    inventory_value: float
    cash: float


class SECEdgarClient:
    """Fetch SEC filings for Opendoor."""

    BASE_URL = "https://data.sec.gov"
    FILINGS_URL = "https://www.sec.gov/cgi-bin/browse-edgar"

    def __init__(self, cik: str = OPENDOOR_CIK):
        self.cik = cik.zfill(10)  # Pad to 10 digits
        self.headers = {
            "User-Agent": "GlassHouse Research contact@example.com",
            "Accept-Encoding": "gzip, deflate",
        }

    def _fetch(self, url: str) -> Optional[bytes]:
        """Make HTTP request."""
        try:
            req = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(req, timeout=15) as response:
                data = response.read()
                # Handle gzip encoding
                if response.info().get('Content-Encoding') == 'gzip':
                    import gzip
                    data = gzip.decompress(data)
                return data
        except Exception as e:
            logger.error(f"SEC EDGAR request failed: {e}")
            return None

    def _fetch_json(self, url: str) -> Optional[Dict]:
        """Fetch JSON data."""
        data = self._fetch(url)
        if data:
            return json.loads(data.decode())
        return None

    def get_company_info(self) -> Dict[str, Any]:
        """Get basic company information."""
        url = f"{self.BASE_URL}/submissions/CIK{self.cik}.json"
        data = self._fetch_json(url)

        if not data:
            return {}

        return {
            "name": data.get("name", ""),
            "cik": data.get("cik", ""),
            "sic": data.get("sic", ""),
            "sic_description": data.get("sicDescription", ""),
            "ticker": data.get("tickers", [""])[0] if data.get("tickers") else "",
            "exchange": data.get("exchanges", [""])[0] if data.get("exchanges") else "",
            "fiscal_year_end": data.get("fiscalYearEnd", ""),
        }

    def get_recent_filings(self, form_types: List[str] = None, limit: int = 20) -> List[SECFiling]:
        """Get recent SEC filings."""
        form_types = form_types or ["10-Q", "10-K", "8-K"]

        url = f"{self.BASE_URL}/submissions/CIK{self.cik}.json"
        data = self._fetch_json(url)

        if not data:
            return []

        filings = []
        recent = data.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        descriptions = recent.get("primaryDocument", [])

        for i in range(min(len(forms), limit * 3)):  # Fetch extra to filter
            if forms[i] in form_types:
                accession = accessions[i].replace("-", "")
                filings.append(SECFiling(
                    form_type=forms[i],
                    filed_date=dates[i],
                    accession_number=accessions[i],
                    description=descriptions[i],
                    url=f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{accession}/{descriptions[i]}",
                ))

                if len(filings) >= limit:
                    break

        return filings

    def get_latest_10q(self) -> Optional[SECFiling]:
        """Get the most recent 10-Q filing."""
        filings = self.get_recent_filings(form_types=["10-Q"], limit=1)
        return filings[0] if filings else None

    def get_latest_10k(self) -> Optional[SECFiling]:
        """Get the most recent 10-K filing."""
        filings = self.get_recent_filings(form_types=["10-K"], limit=1)
        return filings[0] if filings else None

    def get_8k_filings(self, limit: int = 5) -> List[SECFiling]:
        """Get recent 8-K filings (material events)."""
        return self.get_recent_filings(form_types=["8-K"], limit=limit)

    def get_facts(self) -> Dict[str, Any]:
        """
        Get company facts (XBRL data).
        This contains structured financial data from filings.
        """
        url = f"{self.BASE_URL}/api/xbrl/companyfacts/CIK{self.cik}.json"
        data = self._fetch_json(url)

        if not data:
            return {}

        return data

    def extract_key_metrics(self) -> Dict[str, Any]:
        """
        Extract key financial metrics from XBRL facts.
        """
        facts = self.get_facts()
        if not facts:
            return {}

        us_gaap = facts.get("facts", {}).get("us-gaap", {})

        def get_latest_value(concept: str) -> Optional[float]:
            """Get most recent value for a concept."""
            concept_data = us_gaap.get(concept, {})
            units = concept_data.get("units", {})

            # Try USD first, then shares
            values = units.get("USD", []) or units.get("shares", []) or units.get("pure", [])

            if not values:
                return None

            # Sort by end date and get most recent
            sorted_values = sorted(values, key=lambda x: x.get("end", ""), reverse=True)

            # Prefer 10-Q/10-K over other forms
            for v in sorted_values:
                if v.get("form") in ["10-Q", "10-K"]:
                    return v.get("val")

            return sorted_values[0].get("val") if sorted_values else None

        return {
            "revenue": get_latest_value("Revenues") or get_latest_value("RevenueFromContractWithCustomerExcludingAssessedTax"),
            "gross_profit": get_latest_value("GrossProfit"),
            "operating_income": get_latest_value("OperatingIncomeLoss"),
            "net_income": get_latest_value("NetIncomeLoss"),
            "total_assets": get_latest_value("Assets"),
            "total_liabilities": get_latest_value("Liabilities"),
            "stockholders_equity": get_latest_value("StockholdersEquity"),
            "cash": get_latest_value("CashAndCashEquivalentsAtCarryingValue"),
            "inventory": get_latest_value("InventoryRealEstate") or get_latest_value("RealEstateInventory"),
            "homes_sold": get_latest_value("NumberOfHomesSold"),  # Custom metric if reported
        }

    def get_filing_dates(self) -> Dict[str, str]:
        """Get important filing dates."""
        filings = self.get_recent_filings(form_types=["10-Q", "10-K", "8-K"], limit=10)

        dates = {
            "last_10q": None,
            "last_10k": None,
            "last_8k": None,
        }

        for f in filings:
            if f.form_type == "10-Q" and not dates["last_10q"]:
                dates["last_10q"] = f.filed_date
            elif f.form_type == "10-K" and not dates["last_10k"]:
                dates["last_10k"] = f.filed_date
            elif f.form_type == "8-K" and not dates["last_8k"]:
                dates["last_8k"] = f.filed_date

        return dates
