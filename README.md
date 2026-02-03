# Glass House

**Real-time operational intelligence for Opendoor ($OPEN)**

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://opendoor-tracker.com)
[![Data Refresh](https://img.shields.io/badge/data-daily%20refresh-blue)]()
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

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

### Environment Variables

Copy `.env.example` to `.env` and configure:

| Variable | Required | Description |
|----------|----------|-------------|
| `PARCLLABS_API_KEY` | Optional | For Parcl Labs API access ([get one here](https://parcllabs.com)) |
| `ANTHROPIC_API_KEY` | Optional | For AI-powered analysis features |
| `FRED_API_KEY` | Optional | For Federal Reserve economic data |

> **Security Note:** Never commit your `.env` file. It's already in `.gitignore`.

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
│   ├── generate_unified_dashboard.py  # Creates unified JSON for dashboard
│   ├── merge_datasets.py   # Combine Singularity + Parcl data
│   ├── validate_data.py    # Data integrity checks
│   └── scrape_*.py         # Individual scrapers
├── tests/                  # Test suite
├── docs/                   # Documentation
│   └── ARCHITECTURE.md     # Technical architecture
├── index.html              # Production dashboard (GitHub Pages)
└── outputs/                # Generated data files
    └── unified_dashboard_data.json  # Single source of truth for dashboard
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

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test file
pytest tests/test_metrics.py
```

### Code Style

This project uses standard Python conventions:
- PEP 8 for code style
- Type hints where practical
- Docstrings for public functions

### Local Dashboard Development

The dashboard is a single `index.html` file that reads from `outputs/unified_dashboard_data.json`:

```bash
# Generate fresh data
python scripts/generate_unified_dashboard.py

# Serve locally (Python 3)
python -m http.server 8000

# Open http://localhost:8000 in browser
```

---

## Contributing

Contributions are welcome! Here's how to get started:

### Good First Issues

Looking for something to work on? Check out issues labeled [`good first issue`](https://github.com/maam00/glasshouse1/labels/good%20first%20issue).

Ideas for contributions:
- **New data sources** - Add scrapers for additional real estate data
- **New metrics** - Calculate additional insights from existing data
- **Dashboard improvements** - UI/UX enhancements, new visualizations
- **Documentation** - Improve docs, add examples, fix typos
- **Tests** - Increase test coverage

### How to Contribute

1. **Fork** the repository
2. **Clone** your fork locally
3. **Create a branch** for your feature: `git checkout -b feature/amazing-feature`
4. **Make your changes** and add tests if applicable
5. **Run tests**: `pytest`
6. **Commit** with a clear message: `git commit -m "Add amazing feature"`
7. **Push** to your fork: `git push origin feature/amazing-feature`
8. **Open a Pull Request** against `master`

### Pull Request Guidelines

- Keep PRs focused on a single change
- Update documentation if needed
- Add tests for new functionality
- Ensure all tests pass before submitting

### Reporting Issues

Found a bug or have a feature request? [Open an issue](https://github.com/maam00/glasshouse1/issues/new) with:
- Clear description of the problem or feature
- Steps to reproduce (for bugs)
- Expected vs actual behavior
- Screenshots if applicable

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

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed technical documentation including:
- Data pipeline flow
- API integrations
- Database schema
- Metrics calculations

---

## Disclaimer

This project is for **informational purposes only**. It is **not investment advice**.

- Data is scraped from public sources and may contain errors
- Calculations are estimates based on available data
- Always do your own research before making investment decisions
- The authors are not responsible for any financial decisions made based on this data

---

## License

MIT License - see [LICENSE](LICENSE)

---

## Acknowledgments

- [Singularity Research](https://singularityresearchfund.com) for sales velocity data
- [Parcl Labs](https://parcllabs.com) for transaction P&L data
- The open source community for inspiration and tools
