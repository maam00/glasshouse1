# Glass House - Opendoor ($OPEN) Operational Intelligence Platform

## Executive Summary

Glass House is a comprehensive operational intelligence platform for tracking Opendoor Technologies ($OPEN) real estate operations. It scrapes, processes, and visualizes data about Opendoor's home inventory, sales velocity, profit/loss metrics, and market performance to provide investors with transparency into the company's operational health.

**Core Thesis:** Opendoor's stock performance depends on their ability to:
1. Clear legacy "toxic" inventory (homes held >365 days)
2. Maintain profitability on newer acquisitions ("Kaz-era" - post Oct 2023)
3. Hit quarterly revenue guidance (~$1B/quarter)
4. Improve sales velocity across markets

---

## Data Sources

### 1. Singularity Research Fund
- **URL:** https://singularityresearchfund.com/opendoor-tracker
- **Data:** Real-time sales data, daily velocity, geographic distribution
- **Scraper:** `scripts/scrape_singularity.py`
- **Output:** `outputs/singularity_map_*.json`, `outputs/singularity_charts_*.json`

### 2. Parcl Labs Research
- **Source:** CSV exports from Parcl's Opendoor tracking
- **Data:** Individual property transactions with P&L (purchase price, sale price, realized net)
- **Importer:** `src/api/csv_importer.py`
- **Contains:** Days held, renovation costs, holding costs, true profit/loss per home

### 3. Opendoor.com Direct Scraping
- **URL:** https://www.opendoor.com/homes/{market}
- **Scraper:** `scripts/scrape_opendoor.py` (Playwright-based)
- **Data:** Live active inventory across 20 markets
- **Output:** `outputs/opendoor_listings_*.csv`
- **Fields:** Address, price, beds/baths/sqft, status, market

### 4. MLS Data via Realtor.com
- **Scraper:** `scripts/scrape_pending.py` (HomeHarvest library)
- **Data:** Recently sold homes, matched to Opendoor by agent name
- **Opendoor Agents:** Karen Albright, Tara Jones, Amber Broadway, Thomas Shoupe, etc.
- **Output:** `outputs/pending_listings_*.csv`, `outputs/pending_*.json`

### 5. External APIs
- **Yahoo Finance:** Stock price data (`src/api/external/yahoo_finance.py`)
- **FRED:** Federal Reserve economic data (`src/api/external/fred.py`)
- **SEC EDGAR:** Company filings (`src/api/external/sec_edgar.py`)
- **Mortgage Rates:** Interest rate tracking (`src/api/external/mortgage_rates.py`)

---

## Key Concepts & Terminology

### Cohort Classification (by Days on Market)
| Cohort | Days Held | Description |
|--------|-----------|-------------|
| New | <90 days | Fresh inventory, highest win rates |
| Mid | 90-180 days | Normal cycle |
| Old | 180-365 days | Getting stale, lower margins |
| Toxic | >365 days | Legacy problem inventory |

### Kaz-Era vs Legacy
- **Kaz-Era:** Homes acquired after October 2023 (when CEO Kaz changed strategy)
- **Legacy:** Pre-Kaz acquisitions, higher underwater rates, more price cuts

### Key Metrics
- **Win Rate:** % of sales with positive realized net profit
- **Underwater:** Listing price < purchase price (losing money)
- **Days to Pending:** Time from listing to under contract
- **Turnover Rate:** Sales / Active Inventory (velocity)
- **Months of Inventory:** Active / (Monthly Sales) - lower is better

---

## Project Structure

```
glasshouse1/
├── src/                          # Core library code
│   ├── config.py                 # Centralized configuration
│   ├── api/
│   │   ├── parcl_client.py       # Parcl API client
│   │   ├── csv_importer.py       # CSV data import
│   │   ├── retry.py              # Exponential backoff retry logic
│   │   └── external/             # External API integrations
│   ├── db/
│   │   └── database.py           # SQLite database operations
│   ├── metrics/
│   │   ├── calculator.py         # Core metrics calculations
│   │   ├── unit_economics.py     # True profit after all costs
│   │   ├── market_pnl.py         # Market-level P&L analysis
│   │   ├── pending_tracker.py    # Sales funnel tracking
│   │   ├── kaz_era.py            # Kaz-era portfolio analysis
│   │   ├── velocity.py           # Sales velocity metrics
│   │   └── trends.py             # Trend analysis
│   ├── reports/
│   │   ├── terminal.py           # CLI reports
│   │   └── ceo_dashboard.py      # Executive summary
│   └── alerts/
│       └── monitor.py            # Alert monitoring
│
├── scripts/                      # Executable scripts
│   ├── scrape_opendoor.py        # Direct Opendoor.com scraper
│   ├── scrape_pending.py         # MLS pending/sold scraper
│   ├── scrape_singularity.py     # Singularity Research scraper
│   ├── merge_datasets.py         # Combine all data sources
│   ├── generate_unified_dashboard.py  # Build dashboard JSON
│   ├── update_dashboard_funnel.py     # Update sales funnel
│   ├── daily_pull.py             # Daily data refresh
│   └── debug_opendoor.py         # Debugging tool
│
├── outputs/                      # Generated data files
│   ├── dashboard_data.json       # Main dashboard data
│   ├── unified_dashboard_data.json
│   ├── sales_funnel_*.json       # Sales funnel metrics
│   ├── opendoor_listings_*.csv   # Active inventory
│   ├── pending_listings_*.csv    # Recent sales
│   └── singularity_*.json        # Singularity data
│
├── tests/                        # Unit tests
│   ├── test_unit_economics.py
│   ├── test_pending_tracker.py
│   ├── test_config.py
│   └── test_database.py
│
├── dashboard-v6.html             # Main HTML dashboard (latest)
├── dashboard-v5.html             # Previous versions
├── dashboard-v3.html
└── glasshouse.py                 # Main CLI entry point
```

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA COLLECTION                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Opendoor.com ──► scrape_opendoor.py ──► opendoor_listings_*.csv    │
│  (Playwright)      (Active Inventory)     (600 listings, 20 mkts)   │
│                                                                      │
│  Realtor.com ───► scrape_pending.py ──► pending_listings_*.csv      │
│  (HomeHarvest)    (Sold via agents)      (42 Opendoor sales)        │
│                                                                      │
│  Singularity ───► scrape_singularity.py ──► singularity_*.json      │
│  (Web scrape)     (Daily velocity)          (Sales, revenue, map)   │
│                                                                      │
│  Parcl CSVs ────► csv_importer.py ──► SQLite Database               │
│  (Manual export)  (P&L data)          (Full transaction history)    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA PROCESSING                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  merge_datasets.py ──► Combines Singularity + Parcl data            │
│                        Creates unified_sales_*.csv                   │
│                        Creates unified_daily_*.csv                   │
│                                                                      │
│  update_dashboard_funnel.py ──► Calculates:                         │
│                                 - Turnover rate                      │
│                                 - Months of inventory                │
│                                 - Market velocity                    │
│                                 - Sales by speed cohort              │
│                                                                      │
│  src/metrics/*.py ──► Unit Economics (true profit after costs)      │
│                       Market P&L (GROW/HOLD/PAUSE/EXIT)             │
│                       Cohort Analysis (new/mid/old/toxic)           │
│                       Kaz-Era vs Legacy performance                  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA OUTPUT                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  dashboard_data.json ──► Main dashboard metrics                     │
│  unified_dashboard_data.json ──► Extended metrics                   │
│  sales_funnel_*.json ──► Inventory vs sales analysis                │
│                                                                      │
│  dashboard-v6.html ──► Web visualization                            │
│                        - Loads JSON via fetch()                      │
│                        - Plotly.js charts                           │
│                        - Auto-updates from data                      │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Dashboard Sections (dashboard-v6.html)

### 1. The Verdict (Hero Section)
- Q1 revenue tracking vs $1B guidance
- Pacing percentage
- Win rate and total realized P&L

### 2. Kaz-Era Portfolio
- Sold: Count, win rate, avg profit, total realized
- Listed: Above water %, underwater %, price cuts
- Net position: Realized gains, unrealized, underwater exposure
- Watchlist: Individual underwater homes

### 3. Legacy Portfolio
- Same structure as Kaz-Era but for pre-Oct 2023 acquisitions
- Typically shows worse metrics (lower win rate, more underwater)

### 4. Toxic Inventory Clearance
- Progress bar showing cleared vs remaining
- Weekly trend (89 → 85 → 84 → 84)
- Projected zero-out date

### 5. Sales Funnel (NEW)
- **Active Inventory:** 600 listings, $260M value, $433K avg
- **Recent Sales (90d):** 42 sold, $15.7M value, 129 avg days to pending
- **Turnover Metrics:** 7% turnover, 14 sales/month, 42.9 months of inventory
- **Sales by Speed:** Fast/Normal/Slow/Stale breakdown
- **Top Agents:** Karen Albright (14), Tara Jones (10), etc.
- **Market Velocity:** Charlotte 20%, Phoenix 16.7%, Austin 0%

### 6. Daily Velocity
- Homes/day, revenue/day
- Q1 projected vs guidance
- Revenue chart (Plotly.js bar chart)

### 7. Cohort Performance
- Table: New/Mid/Old/Toxic
- Columns: Sold, Win Rate, Margin, Avg Profit

### 8. Risk Monitoring
- Geographic risk by state (FL, GA highest)
- Price cut severity (Kaz-era 35% vs Legacy 96%)

### 9. This Week
- Improving: Toxic down, underwater down
- Stable: Win rate holding
- Watching: Velocity below target

---

## Key Scripts & Usage

### Daily Data Refresh
```bash
# Activate virtual environment
source venv/bin/activate

# Scrape active Opendoor inventory (20 markets)
python scripts/scrape_opendoor.py --all-markets

# Scrape recent sales via MLS (matches by agent name)
python scripts/scrape_pending.py --all-markets

# Update dashboard with sales funnel data
python scripts/update_dashboard_funnel.py

# Open dashboard
open dashboard-v6.html
```

### Scraper Details

#### scrape_opendoor.py
- Uses Playwright (headless Chrome)
- Navigates to opendoor.com/homes/{market}
- Scrolls to load lazy content
- Extracts via JavaScript: price, beds, baths, sqft, address, status
- Outputs CSV with 600 listings across 20 markets

#### scrape_pending.py
- Uses HomeHarvest library (MLS data)
- Searches for recently sold homes in each market
- Filters by known Opendoor agent names
- Calculates days to pending (list date → pending date)
- Outputs 42 recent Opendoor sales

---

## Configuration (src/config.py)

```python
# Cohort thresholds (days)
NEW_COHORT_MAX = 90
MID_COHORT_MAX = 180
OLD_COHORT_MAX = 365
# >365 = toxic

# Kaz-era start date
KAZ_ERA_START = datetime(2023, 10, 1)

# Unit economics assumptions
RENOVATION_COST_PCT = 0.03      # 3% of purchase price
MONTHLY_HOLDING_COST = 1500    # Insurance, taxes, maintenance
TRANSACTION_COST_PCT = 0.06    # 6% (commissions, fees)

# Market action thresholds
GROW_WIN_RATE = 80             # >80% = GROW
HOLD_WIN_RATE = 60             # 60-80% = HOLD
PAUSE_WIN_RATE = 40            # 40-60% = PAUSE
# <40% = EXIT
```

---

## Database Schema (SQLite)

```sql
-- Main sales table
CREATE TABLE sales (
    id INTEGER PRIMARY KEY,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    purchase_date DATE,
    purchase_price REAL,
    sale_date DATE,
    sale_price REAL,
    realized_net REAL,
    days_held INTEGER,
    beds INTEGER,
    baths REAL,
    sqft INTEGER,
    year_built INTEGER,
    -- Calculated fields
    cohort TEXT,           -- new/mid/old/toxic
    is_kaz_era BOOLEAN,    -- post Oct 2023
    is_win BOOLEAN         -- realized_net > 0
);

-- Inventory table
CREATE TABLE inventory (
    id INTEGER PRIMARY KEY,
    address TEXT,
    city TEXT,
    state TEXT,
    list_price REAL,
    purchase_price REAL,
    days_on_market INTEGER,
    price_cuts INTEGER,
    is_underwater BOOLEAN,
    cohort TEXT,
    is_kaz_era BOOLEAN
);
```

---

## Output JSON Structures

### dashboard_data.json
```json
{
  "generated_at": "2026-01-29T...",
  "current": {
    "inventory": {
      "total": 764,
      "toxic_count": 84,
      "underwater_count": 193
    },
    "velocity": {
      "daily_sales": 22.0,
      "daily_revenue": 8500000
    },
    "v3": {
      "kaz_era": { "win_rate": 95.3, "underwater": 3 },
      "legacy": { "win_rate": 64.5, "underwater": 190 }
    }
  },
  "sales_funnel": { ... }  // See below
}
```

### sales_funnel_*.json
```json
{
  "scraped_at": "2026-01-29T13:15:53",
  "active_inventory": {
    "total": 600,
    "total_value": 260020000,
    "avg_price": 433366,
    "by_market": { "phoenix-az": 30, "dallas-tx": 30, ... }
  },
  "recent_sales": {
    "total": 42,
    "total_value": 15720000,
    "avg_price": 374285,
    "by_market": { "Jacksonville, FL": 7, "Charlotte, NC": 6, ... },
    "by_agent": { "Karen Albright": 14, "Tara Jones": 10, ... },
    "days_to_pending": { "avg": 129, "min": 1, "max": 471 },
    "by_speed": {
      "fast_under_30d": 10,
      "normal_30_90d": 13,
      "slow_90_180d": 6,
      "stale_over_180d": 13
    }
  },
  "turnover": {
    "sold_90d": 42,
    "active_inventory": 600,
    "turnover_rate_90d_pct": 7.0,
    "monthly_velocity": 14.0,
    "months_of_inventory": 42.9
  },
  "market_velocity": [
    { "market": "charlotte-nc", "active": 30, "sold_90d": 6, "turnover_pct": 20.0 },
    { "market": "phoenix-az", "active": 30, "sold_90d": 5, "turnover_pct": 16.7 },
    ...
  ]
}
```

---

## Opendoor Markets Tracked

| Market | Slug | State |
|--------|------|-------|
| Phoenix | phoenix-az | AZ |
| Dallas | dallas-tx | TX |
| Houston | houston-tx | TX |
| Austin | austin-tx | TX |
| San Antonio | san-antonio-tx | TX |
| Atlanta | atlanta-ga | GA |
| Charlotte | charlotte-nc | NC |
| Raleigh | raleigh-nc | NC |
| Tampa | tampa-fl | FL |
| Orlando | orlando-fl | FL |
| Jacksonville | jacksonville-fl | FL |
| Denver | denver-co | CO |
| Las Vegas | las-vegas-nv | NV |
| Nashville | nashville-tn | TN |
| Sacramento | sacramento-ca | CA |
| Riverside | riverside-ca | CA |
| Minneapolis | minneapolis-mn | MN |
| Portland | portland-or | OR |
| Salt Lake City | salt-lake-city-ut | UT |
| Tucson | tucson-az | AZ |

---

## Known Opendoor Agents (for MLS matching)

- Karen Albright (FL, TN)
- Tara Jones (AZ, CO)
- Amber Broadway (TX)
- Thomas L Shoupe / Thomas Shoupe (NC)
- Whitney Hunt-Sailors (NC)
- Tanya Pickens (GA)
- Kristopher Furrow (GA)
- Lisa McGill (NC)
- Sae Kim (TX)

These agents are used to identify Opendoor sales in MLS data since Opendoor lists through their brokerage (Opendoor Brokerage Inc).

---

## Investment Thesis Tracking

### Bullish Signals
- Kaz-era win rate >90%
- Toxic inventory declining weekly
- New cohort margins >10%
- Daily velocity approaching 29 homes/day

### Bearish Signals
- Months of inventory >24 (currently 42.9)
- Turnover rate <10% (currently 7%)
- Legacy underwater >25% (currently 28.7%)
- Velocity below guidance pace

### Current Status (Jan 2026)
- **Q1 Pacing:** 94.9% of $1B guidance
- **Win Rate:** 79.1% overall
- **Toxic Remaining:** 84 homes
- **Kaz-Era Win Rate:** 95.3%
- **Months of Inventory:** 42.9 (very high)

---

## Dependencies

```
# requirements.txt
pandas>=2.0.0
numpy>=1.24.0
playwright>=1.40.0
homeharvest>=0.3.0
plotly>=5.18.0
anthropic>=0.18.0  # For AI insights
python-dotenv>=1.0.0
requests>=2.31.0
beautifulsoup4>=4.12.0
```

---

## Environment Variables

```bash
# .env file
ANTHROPIC_API_KEY=sk-ant-...   # For AI-generated insights
PARCL_API_KEY=...              # If using Parcl API directly
```

---

## Running Tests

```bash
source venv/bin/activate
pytest tests/ -v
```

---

## Future Enhancements

1. **Automated daily scraping** via cron/scheduler
2. **Historical tracking** of inventory changes over time
3. **Price cut velocity** analysis (how fast are they cutting?)
4. **Geographic heat maps** of profitability
5. **Earnings call transcript analysis**
6. **Competitor tracking** (Offerpad, Zillow Offers history)
7. **Mobile-responsive dashboard**
8. **Email/Slack alerts** for significant changes

---

## Summary

Glass House provides comprehensive operational intelligence on Opendoor by:

1. **Scraping multiple sources** (Opendoor.com, MLS, Singularity)
2. **Processing & enriching** with P&L calculations, cohort classification
3. **Visualizing** via interactive HTML dashboard
4. **Tracking key metrics** like win rate, turnover, toxic clearance

The goal is to give investors transparency into Opendoor's real operational performance beyond what's disclosed in earnings reports.
