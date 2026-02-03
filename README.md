# Glass House

**Real-time operational intelligence for Opendoor ($OPEN)**

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://opendoor-tracker.com)
[![Data Refresh](https://img.shields.io/badge/data-daily%20refresh-blue)]()
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

Glass House tracks Opendoor's home-flipping operations in real-time, providing investors with transparency into sales velocity, inventory health, and profitability metrics that aren't available in quarterly filings.

**Live Dashboard:** [opendoor-tracker.com](https://opendoor-tracker.com)

---

## Why This Exists

Opendoor's stock moves on operational execution, but investors only get visibility quarterly. Glass House provides:

- **Daily sales velocity** - Are they hitting guidance pace?
- **Win/loss rates** - Are they actually making money on each home?
- **Inventory health** - How much "toxic" inventory (>365 days) remains?
- **Kaz-era vs Legacy** - Is the new CEO's strategy working?

---

## Key Metrics

| Metric | What It Measures |
|--------|-----------------|
| **Guidance Pace** | Daily sales vs. required pace to hit quarterly guidance |
| **Win Rate** | % of sales with positive realized profit |
| **Kaz-Era Performance** | Homes acquired after Sep 10, 2025 (new CEO) |
| **Toxic Inventory** | Homes held >365 days (legacy problem assets) |
| **Days to Pending** | Speed from listing to under contract |

---

## Data Sources

| Source | Data | Update Frequency |
|--------|------|-----------------|
| [Singularity Research](https://singularityresearchfund.com/opendoor-tracker) | Sales velocity, geographic distribution | Real-time |
| [Parcl Labs](https://parcllabs.com) | Transaction P&L (purchase price, sale price, profit) | Weekly |
| Opendoor.com | Active inventory across 20 markets | Daily |
| MLS/Realtor.com | Recently sold homes, pending sales | Daily |

---

## Quick Start

```bash
# Clone
git clone https://github.com/maam00/glasshouse1.git
cd glasshouse1

# Setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure (optional - for API access)
cp .env.example .env
# Edit .env with your API keys

# Run daily data pull
python scripts/daily_pull.py

# Generate dashboard data
python scripts/generate_unified_dashboard.py

# Validate data integrity
python scripts/validate_data.py
```

---

## Project Structure

```
glasshouse1/
├── src/                    # Core library
│   ├── config.py           # Centralized config (dates, thresholds)
│   ├── api/                # Data source integrations
│   ├── db/                 # SQLite storage
│   ├── metrics/            # Calculations (win rate, velocity, P&L)
│   └── reports/            # Report generators
├── scripts/                # Automation scripts
│   ├── daily_pull.py       # Main data refresh
│   ├── merge_datasets.py   # Combine Singularity + Parcl data
│   ├── validate_data.py    # Data integrity checks
│   └── scrape_*.py         # Individual scrapers
├── tests/                  # Test suite
├── index.html              # Production dashboard (GitHub Pages)
└── outputs/                # Generated data files
```

---

## Key Concepts

### Cohort Classification

| Cohort | Days Held | Risk Level |
|--------|-----------|------------|
| New | <90 days | Low - Fresh inventory |
| Mid | 90-180 days | Medium - Normal cycle |
| Old | 180-365 days | High - Getting stale |
| Toxic | >365 days | Critical - Legacy problems |

### Kaz-Era vs Legacy

- **Kaz-Era**: Homes acquired on/after September 10, 2025 (new CEO)
- **Legacy**: Pre-Kaz acquisitions with higher underwater rates

All date thresholds are centralized in `src/config.py`.

---

## Validation

The system includes automated data validation:

```bash
python scripts/validate_data.py --strict
```

Checks include:
- Kaz-era sales count sanity
- Revenue calculation validation
- Win rate verification
- Cross-source data reconciliation
- Data freshness monitoring

---

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed technical documentation.

---

## Contributing

Contributions welcome! Please:

1. Fork the repo
2. Create a feature branch
3. Run tests: `pytest`
4. Submit a PR

---

## Disclaimer

This project is for informational purposes only. It is not investment advice. The data is scraped from public sources and may contain errors. Always do your own research before making investment decisions.

---

## License

MIT License - see [LICENSE](LICENSE)
