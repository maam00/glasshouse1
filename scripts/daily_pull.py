#!/usr/bin/env python3
"""
Glass House Daily Intelligence
===============================
Complete $OPEN operational + market intelligence.

Usage:
    python glasshouse.py                          # Full dashboard
    python glasshouse.py --quick                  # Ops only (no external APIs)
    python glasshouse.py --csv sales.csv listings.csv
    python glasshouse.py --history                # Show historical charts
    python glasshouse.py --market-context         # Fetch Parcl market context (~200 credits)
    python glasshouse.py --deep                   # CEO deep analysis (unit economics, market P&L)
"""

import sys
import os
import argparse
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path

# Load .env file
def load_dotenv():
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip().strip('"\'')

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

from src.api import CSVImporter
from src.db import Database
from src.metrics import MetricsCalculator, AdvancedAnalytics
from src.reports import TerminalReport, CEODashboard
from src.reports.full_dashboard import FullDashboard
from src.reports.charts import HistoricalCharts

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


def collect_market_data() -> dict:
    """Collect all external market data."""
    try:
        from src.api.external.collector import ExternalDataCollector
        collector = ExternalDataCollector()
        return collector.collect_all()
    except Exception as e:
        logger.warning(f"Could not collect market data: {e}")
        return {}


def collect_parcl_market_context() -> dict:
    """
    Fetch market context from Parcl API.
    Uses ~200 credits - call sparingly (weekly recommended).
    """
    try:
        from src.api.parcl_strategic import StrategicParclAPI
        api = StrategicParclAPI()
        return api.get_opendoor_market_context()
    except ImportError:
        logger.warning("parcllabs package not installed")
        return {}
    except Exception as e:
        logger.warning(f"Could not fetch Parcl market context: {e}")
        return {}


def run_deep_analysis(sales_path: str = None, listings_path: str = None):
    """Run deep analysis: unit economics, market P&L, velocity, pricing."""
    from src.metrics.unit_economics import UnitEconomicsCalculator, compare_reported_vs_true
    from src.metrics.market_pnl import MarketPnLAnalyzer
    from src.metrics.velocity import VelocityAnalyzer
    from src.metrics.pricing_analysis import PricingAnalyzer
    from src.api.property_enrichment import enrich_sales_with_state_estimate

    print("\n" + "═" * 78)
    print("  GLASS HOUSE — Deep Analysis (CEO View)")
    print("═" * 78)

    importer = CSVImporter()

    # Find CSV files
    if sales_path:
        sales_file = Path(sales_path)
    else:
        sales_file, _ = find_csv_files()

    if listings_path:
        listings_file = Path(listings_path)
    else:
        _, listings_file = find_csv_files()

    # Load data
    print("\n[1] Loading data...")
    if sales_file:
        sales = importer.import_sales_csv(str(sales_file))
        print(f"    Sales: {len(sales)} records")
    else:
        sales = pd.DataFrame()

    if listings_file:
        listings = importer.import_listings_csv(str(listings_file))
        print(f"    Listings: {len(listings)} records")
    else:
        listings = pd.DataFrame()

    if sales.empty and listings.empty:
        print("    No data found!")
        return 1

    # Enrich sales with state (shared across all analyses)
    print("\n[1.5] Enriching sales with state data...")
    sales = enrich_sales_with_state_estimate(sales, listings)
    state_count = (sales["state"] != "Unknown").sum() if "state" in sales.columns else 0
    print(f"      Estimated state for {state_count}/{len(sales)} sales")

    # 1. Unit Economics
    print("\n[2] True Unit Economics...")
    unit_report = compare_reported_vs_true(sales, listings)
    print(unit_report)

    # 2. Market P&L (now with enriched sales)
    print("\n[3] Market P&L Analysis...")
    # Debug: check state before passing
    if "state" in sales.columns:
        logger.debug(f"Sales state before MarketPnL: {sales['state'].value_counts().head().to_dict()}")
    market_analyzer = MarketPnLAnalyzer(sales, listings)
    # Debug: check state after analyzer init
    if "state" in market_analyzer.sales.columns:
        logger.debug(f"Analyzer sales state: {market_analyzer.sales['state'].value_counts().head().to_dict()}")
    market_report = market_analyzer.generate_market_matrix()
    print(market_report)

    # 3. Velocity Decomposition
    print("\n[4] Velocity Analysis...")
    velocity_analyzer = VelocityAnalyzer(sales, listings)
    velocity_report = velocity_analyzer.generate_velocity_report()
    print(velocity_report)

    # 4. Pricing Analysis
    print("\n[5] Pricing by Cohort...")
    pricing_analyzer = PricingAnalyzer(listings)
    pricing_report = pricing_analyzer.generate_report()
    print(pricing_report)

    print("\n" + "═" * 78)
    print("  Analysis complete. Run with --market-context for competitive intel.")
    print("═" * 78 + "\n")

    return 0


def show_historical_charts(days: int = 30):
    """Display historical trend charts."""
    db = Database()
    charts = HistoricalCharts(db)

    print("\n" + "═" * 78)
    print("  GLASS HOUSE — Historical Analysis")
    print("═" * 78)

    # Generate dashboard charts (sparklines)
    dashboard_output = charts.generate_dashboard_charts(days)
    print(dashboard_output)

    # Show period-over-period changes
    changes = charts.calculate_changes()
    if changes:
        print("\n┌─ PERIOD CHANGES ────────────────────────────────────────────────────────────┐")
        print("│  Metric                    Current     DoD       WoW       MoM              │")
        print("│  ─────────────────────────────────────────────────────────────────────────  │")

        for name, data in changes.items():
            curr = data.get("current", 0)
            dod = data.get("dod")
            wow = data.get("wow")
            mom = data.get("mom")

            dod_str = f"{dod:+.1f}%" if dod is not None else "N/A"
            wow_str = f"{wow:+.1f}%" if wow is not None else "N/A"
            mom_str = f"{mom:+.1f}%" if mom is not None else "N/A"

            display_name = name.replace("_", " ").title()
            if "rate" in name or "margin" in name or "pct" in name:
                curr_str = f"{curr:.1f}%"
            elif curr > 1000:
                curr_str = f"{curr:,.0f}"
            else:
                curr_str = f"{curr:.1f}"

            print(f"│  {display_name:24} {curr_str:>10} {dod_str:>8} {wow_str:>8} {mom_str:>8}      │")

        print("└──────────────────────────────────────────────────────────────────────────────┘")
    else:
        print("\n  Run daily to accumulate historical data for trend analysis.")

    print("\n" + "═" * 78 + "\n")


def find_csv_files() -> tuple:
    """Find CSV files in common locations."""
    locations = [
        Path.home() / "Desktop" / "glasshouse",
        Path.home() / "Downloads",
        Path(__file__).parent.parent / "data" / "imports",
    ]

    sales_file = None
    listings_file = None

    for loc in locations:
        if not loc.exists():
            continue

        for f in loc.glob("*.csv"):
            name_lower = f.name.lower()
            if "sales" in name_lower or "sold" in name_lower:
                if sales_file is None or f.stat().st_mtime > sales_file.stat().st_mtime:
                    sales_file = f
            elif "listing" in name_lower or "for-sale" in name_lower:
                if listings_file is None or f.stat().st_mtime > listings_file.stat().st_mtime:
                    listings_file = f

    return sales_file, listings_file


def run_full_dashboard(
    sales_path: str = None,
    listings_path: str = None,
    quick: bool = False,
    parcl_context: dict = None,
    show_charts: bool = True
):
    """Run the full intelligence dashboard."""
    print("\n" + "═" * 78)
    print("  GLASS HOUSE — Loading Intelligence")
    print("═" * 78)

    db = Database()
    importer = CSVImporter()
    today = datetime.now().strftime("%Y-%m-%d")

    # Find CSV files
    if sales_path:
        sales_file = Path(sales_path)
    else:
        sales_file, _ = find_csv_files()

    if listings_path:
        listings_file = Path(listings_path)
    else:
        _, listings_file = find_csv_files()

    if not sales_file and not listings_file:
        sales_file, listings_file = find_csv_files()

    # Load operational data
    print("\n[1] Loading operational data...")
    if sales_file:
        sales = importer.import_sales_csv(str(sales_file))
        print(f"    Sales: {len(sales)} records from {sales_file.name}")
    else:
        sales = pd.DataFrame()
        print("    Sales: No CSV found")

    if listings_file:
        listings = importer.import_listings_csv(str(listings_file))
        print(f"    Listings: {len(listings)} records from {listings_file.name}")
    else:
        listings = pd.DataFrame()
        print("    Listings: No CSV found")

    if sales.empty and listings.empty:
        print("\n  ERROR: No data found!")
        print("  Download CSVs from Parcl Research and specify paths:")
        print("  python glasshouse.py --csv sales.csv listings.csv")
        return 1

    # Calculate metrics
    print("\n[2] Calculating operational metrics...")
    previous = db.get_previous_metrics(days_ago=1)

    calculator = MetricsCalculator(
        sales_df=sales,
        listings_df=listings,
        purchases_df=pd.DataFrame(),
        previous_metrics=previous,
    )
    metrics = calculator.calculate_all(today)

    advanced = AdvancedAnalytics(sales, listings)
    advanced_metrics = advanced.generate_summary()

    # Collect market data (unless quick mode)
    if quick:
        print("\n[3] Skipping market data (quick mode)")
        market_data = {}
    else:
        print("\n[3] Collecting market intelligence...")
        market_data = collect_market_data()

    # Merge Parcl context if provided
    if parcl_context:
        market_data["parcl_context"] = parcl_context

    # Save to database
    print("\n[4] Saving to database...")
    db.save_daily_metrics(metrics)

    # Generate dashboard
    print("\n[5] Generating dashboard...\n")

    if market_data:
        dashboard = FullDashboard(metrics, advanced_metrics, market_data, previous)
    else:
        dashboard = CEODashboard(metrics, advanced_metrics, previous)

    dashboard.print()

    # Historical charts
    charts = HistoricalCharts(db)

    # Show historical charts
    if show_charts:
        charts_output = charts.generate_dashboard_charts(days=30)
        if "No historical data" not in charts_output:
            print(charts_output)

    # Save outputs
    output_dir = Path(__file__).parent.parent / "outputs"
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f"intelligence_{today}.json"
    with open(output_file, "w") as f:
        json.dump({
            "date": today,
            "operational": metrics.to_dict(),
            "advanced": advanced_metrics,
            "market": market_data,
        }, f, indent=2, default=str)

    # Save chart data
    try:
        chart_file = charts.save_chart_data(output_dir)
        logger.info(f"Chart data saved: {chart_file}")
    except Exception:
        pass

    print(f"\nData saved: {output_file}")
    print("═" * 78 + "\n")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Glass House — $OPEN Intelligence"
    )
    parser.add_argument(
        "--csv", "-c",
        nargs="*",
        metavar="FILE",
        help="CSV files: sales.csv listings.csv"
    )
    parser.add_argument(
        "--quick", "-q",
        action="store_true",
        help="Skip external API calls"
    )
    parser.add_argument(
        "--history", "-H",
        action="store_true",
        help="Show historical trend charts"
    )
    parser.add_argument(
        "--market-context", "-m",
        action="store_true",
        help="Fetch Parcl market context (~200 credits)"
    )
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=30,
        help="Days of history to show (default: 30)"
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Deep analysis: unit economics, market P&L, velocity, pricing"
    )
    args = parser.parse_args()

    # History-only mode
    if args.history:
        show_historical_charts(days=args.days)
        return 0

    # Deep analysis mode
    if args.deep:
        sales_path = args.csv[0] if args.csv and len(args.csv) >= 1 else None
        listings_path = args.csv[1] if args.csv and len(args.csv) >= 2 else None
        return run_deep_analysis(sales_path, listings_path)

    # Market context fetch
    parcl_context = None
    if args.market_context:
        print("\n[*] Fetching Parcl market context (~200 credits)...")
        parcl_context = collect_parcl_market_context()
        if parcl_context:
            print(f"    Credits used: {parcl_context.get('credit_usage', 0)}")
            # Save to file
            output_dir = Path(__file__).parent.parent / "outputs"
            output_dir.mkdir(exist_ok=True)
            ctx_file = output_dir / f"parcl_context_{datetime.now().strftime('%Y%m%d')}.json"
            with open(ctx_file, "w") as f:
                json.dump(parcl_context, f, indent=2, default=str)
            print(f"    Saved: {ctx_file}")

    sales_path = None
    listings_path = None

    if args.csv:
        if len(args.csv) >= 1:
            sales_path = args.csv[0]
        if len(args.csv) >= 2:
            listings_path = args.csv[1]

    return run_full_dashboard(
        sales_path=sales_path,
        listings_path=listings_path,
        quick=args.quick,
        parcl_context=parcl_context,
        show_charts=not args.market_context  # Show charts unless just fetching context
    )


if __name__ == "__main__":
    sys.exit(main())
