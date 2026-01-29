"""
Filings Watcher - SEC EDGAR Monitoring
======================================
Monitors SEC EDGAR for new Opendoor filings (8-K, 10-Q, 10-K).
Summarizes key changes and flags material items.
"""

import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import requests

logger = logging.getLogger(__name__)


@dataclass
class Filing:
    """SEC filing metadata."""
    accession_number: str
    form_type: str
    filing_date: str
    description: str
    document_url: str
    company: str
    cik: str


@dataclass
class FilingAlert:
    """Alert for a new/updated filing."""
    filing: Filing
    summary: str
    materiality: str  # high, medium, low
    key_items: List[str]


# Opendoor SEC info
OPENDOOR_CIK = "0001801169"
EDGAR_BASE = "https://www.sec.gov"
EDGAR_SUBMISSIONS = f"https://data.sec.gov/submissions/CIK{OPENDOOR_CIK}.json"

# Filing types to monitor
MONITORED_FORMS = ['8-K', '10-K', '10-Q', '4', 'SC 13G', 'SC 13D', 'DEF 14A']

# 8-K item descriptions for context
FORM_8K_ITEMS = {
    '1.01': 'Entry into Material Agreement',
    '1.02': 'Termination of Material Agreement',
    '1.03': 'Bankruptcy/Receivership',
    '2.01': 'Completion of Acquisition/Disposition',
    '2.02': 'Results of Operations and Financial Condition',
    '2.03': 'Creation of Obligation',
    '2.04': 'Triggering Events',
    '2.05': 'Exit Activities/Restructuring',
    '2.06': 'Material Impairments',
    '3.01': 'Securities Act Registration',
    '3.02': 'Unregistered Sales of Equity',
    '3.03': 'Material Modification of Rights',
    '4.01': 'Changes in Registrant\'s Certifying Accountant',
    '4.02': 'Non-Reliance on Financial Statements',
    '5.01': 'Changes in Control',
    '5.02': 'Departure/Election of Directors/Officers',
    '5.03': 'Amendments to Articles/Bylaws',
    '5.04': 'Temporary Suspension of Trading',
    '5.05': 'Amendments to Code of Ethics',
    '5.06': 'Change in Shell Company Status',
    '5.07': 'Submission of Matters to Vote',
    '5.08': 'Shareholder Director Nominations',
    '7.01': 'Regulation FD Disclosure',
    '8.01': 'Other Events',
    '9.01': 'Financial Statements and Exhibits',
}

# High materiality items
HIGH_MATERIALITY_ITEMS = ['1.01', '2.05', '2.06', '4.01', '4.02', '5.01', '5.02']


class FilingsWatcher:
    """Watch SEC EDGAR for new Opendoor filings."""

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or Path(__file__).parent.parent.parent / "outputs" / "tape"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.headers = {
            'User-Agent': 'Glass House Research admin@glasshouse.io',
            'Accept-Encoding': 'gzip, deflate',
        }

    def fetch_recent_filings(self, days: int = 7) -> List[Filing]:
        """Fetch recent filings from SEC EDGAR."""
        logger.info(f"Fetching Opendoor SEC filings (last {days} days)...")

        try:
            response = requests.get(EDGAR_SUBMISSIONS, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            filings = []
            cutoff = datetime.now() - timedelta(days=days)

            recent = data.get('filings', {}).get('recent', {})
            forms = recent.get('form', [])
            dates = recent.get('filingDate', [])
            accessions = recent.get('accessionNumber', [])
            documents = recent.get('primaryDocument', [])
            descriptions = recent.get('primaryDocDescription', [])

            for i in range(min(len(forms), 50)):  # Check last 50 filings
                form = forms[i]
                date_str = dates[i]

                # Parse date
                try:
                    filing_date = datetime.strptime(date_str, '%Y-%m-%d')
                except ValueError:
                    continue

                if filing_date < cutoff:
                    continue

                if form not in MONITORED_FORMS:
                    continue

                accession = accessions[i].replace('-', '')
                doc_url = f"{EDGAR_BASE}/Archives/edgar/data/{OPENDOOR_CIK}/{accession}/{documents[i]}"

                filing = Filing(
                    accession_number=accessions[i],
                    form_type=form,
                    filing_date=date_str,
                    description=descriptions[i] if i < len(descriptions) else '',
                    document_url=doc_url,
                    company='Opendoor Technologies Inc.',
                    cik=OPENDOOR_CIK,
                )
                filings.append(filing)

            logger.info(f"Found {len(filings)} recent filings")
            return filings

        except requests.RequestException as e:
            logger.error(f"Failed to fetch filings: {e}")
            return []

    def load_seen_filings(self) -> set:
        """Load previously seen filing accession numbers."""
        seen_file = self.cache_dir / "seen_filings.json"

        if not seen_file.exists():
            return set()

        try:
            with open(seen_file) as f:
                data = json.load(f)
            return set(data.get('seen', []))
        except Exception:
            return set()

    def save_seen_filings(self, seen: set):
        """Save seen filings."""
        seen_file = self.cache_dir / "seen_filings.json"

        data = {
            'timestamp': datetime.now().isoformat(),
            'seen': list(seen),
        }

        with open(seen_file, 'w') as f:
            json.dump(data, f, indent=2)

    def analyze_8k(self, filing: Filing) -> FilingAlert:
        """Analyze an 8-K filing for material items."""
        # Try to fetch and parse the 8-K
        items_found = []
        materiality = 'low'

        try:
            response = requests.get(filing.document_url, headers=self.headers, timeout=30)
            if response.ok:
                text = response.text.lower()

                # Look for item numbers
                for item_num, item_desc in FORM_8K_ITEMS.items():
                    if f"item {item_num}" in text:
                        items_found.append(f"{item_num}: {item_desc}")
                        if item_num in HIGH_MATERIALITY_ITEMS:
                            materiality = 'high'
                        elif materiality != 'high':
                            materiality = 'medium'

        except Exception as e:
            logger.debug(f"Could not fetch 8-K content: {e}")

        # Generate summary
        if items_found:
            summary = f"8-K filed with items: {', '.join(items_found[:3])}"
        else:
            summary = f"8-K filing: {filing.description or 'No description'}"

        return FilingAlert(
            filing=filing,
            summary=summary,
            materiality=materiality,
            key_items=items_found,
        )

    def analyze_filing(self, filing: Filing) -> FilingAlert:
        """Analyze a filing and generate alert."""
        if filing.form_type == '8-K':
            return self.analyze_8k(filing)

        # For other forms, use simple analysis
        materiality = 'high' if filing.form_type in ['10-K', '10-Q'] else 'medium'

        summary_map = {
            '10-K': 'Annual Report filed',
            '10-Q': 'Quarterly Report filed',
            '4': f"Insider transaction: {filing.description}",
            'SC 13G': f"Institutional ownership filing",
            'SC 13D': f"Activist ownership filing (material)",
            'DEF 14A': 'Proxy statement filed',
        }

        return FilingAlert(
            filing=filing,
            summary=summary_map.get(filing.form_type, f"{filing.form_type} filed"),
            materiality=materiality,
            key_items=[],
        )

    def check_for_new_filings(self, days: int = 7) -> List[FilingAlert]:
        """Check for new filings and return alerts."""
        filings = self.fetch_recent_filings(days)
        seen = self.load_seen_filings()

        alerts = []
        for filing in filings:
            if filing.accession_number not in seen:
                alert = self.analyze_filing(filing)
                alerts.append(alert)
                seen.add(filing.accession_number)

        self.save_seen_filings(seen)

        # Sort by materiality
        order = {'high': 0, 'medium': 1, 'low': 2}
        alerts.sort(key=lambda a: order.get(a.materiality, 2))

        return alerts

    def get_tape_items(self, alerts: List[FilingAlert], max_items: int = 2) -> List[Dict[str, Any]]:
        """Generate tape items from filing alerts."""
        items = []

        for alert in alerts[:max_items]:
            items.append({
                'type': 'filing',
                'category': alert.filing.form_type,
                'materiality': alert.materiality,
                'headline': f"SEC {alert.filing.form_type}: {alert.summary}",
                'detail': f"Filed {alert.filing.filing_date}. {', '.join(alert.key_items[:2]) if alert.key_items else ''}",
                'url': alert.filing.document_url,
                'timestamp': alert.filing.filing_date,
            })

        return items


def check_opendoor_filings(days: int = 7) -> List[FilingAlert]:
    """Convenience function to check for new filings."""
    watcher = FilingsWatcher()
    return watcher.check_for_new_filings(days)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    watcher = FilingsWatcher()
    alerts = watcher.check_for_new_filings(days=30)

    print(f"\n{'='*60}")
    print(f"  SEC FILINGS UPDATE")
    print(f"{'='*60}")
    for alert in alerts:
        mat_icon = {'high': '!!!', 'medium': '!!', 'low': '!'}.get(alert.materiality, '')
        print(f"\n  {mat_icon} [{alert.filing.form_type}] {alert.summary}")
        print(f"     Filed: {alert.filing.filing_date}")
        if alert.key_items:
            print(f"     Items: {', '.join(alert.key_items[:3])}")
    print(f"\n{'='*60}\n")
