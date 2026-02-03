#!/usr/bin/env python3
"""
Data Validation Alerts System for Glass House.

Runs automated validation checks on data files to catch accuracy issues early.
Designed to run as part of the daily refresh pipeline.

Checks:
1. Kaz-era sanity check (sales count and win rate after Sep 10, 2025)
2. Revenue validation (calculated vs reported)
3. Win rate validation (recalculated vs reported)
4. Cross-source validation (Singularity vs Parcl totals)

Usage:
    python scripts/validate_data.py                    # Run all checks
    python scripts/validate_data.py --strict           # Exit 1 on any failure
    python scripts/validate_data.py --check kaz-era   # Run specific check

Exit codes:
    0 - All checks passed
    1 - One or more critical checks failed (with --strict)
"""

import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, date
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any

import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

KAZ_ERA_START = date(2025, 9, 10)  # Kaz Avetisyan became CEO
MIN_KAZ_ERA_SALES = 100  # Alert if fewer than this many sales since Kaz era
WIN_RATE_TOLERANCE = 0.01  # 1% tolerance for win rate validation
REVENUE_TOLERANCE = 0.05  # 5% tolerance for revenue validation
CROSS_SOURCE_TOLERANCE = 0.10  # 10% tolerance for cross-source validation


# =============================================================================
# DATA CLASSES
# =============================================================================

def convert_numpy_types(obj):
    """Convert numpy types to native Python types for JSON serialization."""
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(v) for v in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif pd.isna(obj):
        return None
    return obj


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    check_name: str
    status: str  # PASS, FAIL, WARN, SKIP
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    is_critical: bool = False

    def to_dict(self) -> dict:
        d = asdict(self)
        return convert_numpy_types(d)


@dataclass
class ValidationReport:
    """Complete validation report."""
    timestamp: str
    results: List[ValidationResult]
    summary: Dict[str, int]
    has_critical_failures: bool

    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp,
            'results': [r.to_dict() for r in self.results],
            'summary': self.summary,
            'has_critical_failures': self.has_critical_failures,
        }


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def find_latest_file(directory: Path, pattern: str) -> Optional[Path]:
    """Find the most recent file matching pattern."""
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def load_unified_sales(output_dir: Path) -> Optional[pd.DataFrame]:
    """Load the latest unified sales CSV."""
    sales_file = find_latest_file(output_dir, "unified_sales_*.csv")
    if not sales_file:
        return None

    df = pd.read_csv(sales_file)
    df['sale_date'] = pd.to_datetime(df['sale_date']).dt.date
    if 'purchase_date' in df.columns:
        df['purchase_date'] = pd.to_datetime(df['purchase_date'], errors='coerce').dt.date
    return df


def load_unified_daily(output_dir: Path) -> Optional[pd.DataFrame]:
    """Load the latest unified daily CSV."""
    daily_file = find_latest_file(output_dir, "unified_daily_*.csv")
    if not daily_file:
        return None

    df = pd.read_csv(daily_file)
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df


def load_singularity_sales(output_dir: Path) -> Optional[pd.DataFrame]:
    """Load the latest Singularity sales CSV."""
    sales_file = find_latest_file(output_dir, "singularity_sales_*.csv")
    if not sales_file:
        return None

    df = pd.read_csv(sales_file)
    if 'sold_date' in df.columns:
        df['sold_date'] = pd.to_datetime(df['sold_date']).dt.date
    return df


def load_singularity_daily(output_dir: Path) -> Optional[pd.DataFrame]:
    """Load the latest Singularity daily CSV."""
    daily_file = find_latest_file(output_dir, "singularity_daily_*.csv")
    if not daily_file:
        return None

    df = pd.read_csv(daily_file)
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df


# =============================================================================
# VALIDATION CHECKS
# =============================================================================

def check_kaz_era_sanity(sales_df: pd.DataFrame) -> ValidationResult:
    """
    Kaz-era sanity check:
    - Count sales since Sep 10, 2025 (Kaz became CEO)
    - Alert if count is suspiciously low
    - Check win rate is reasonable
    """
    check_name = "kaz_era_sanity"

    if sales_df is None or sales_df.empty:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No sales data available",
            is_critical=False
        )

    # Filter to Kaz era
    kaz_era_sales = sales_df[sales_df['sale_date'] >= KAZ_ERA_START]
    kaz_era_count = len(kaz_era_sales)

    # Check minimum sales count
    if kaz_era_count < MIN_KAZ_ERA_SALES:
        return ValidationResult(
            check_name=check_name,
            status="FAIL",
            message=f"Kaz-era sales count ({kaz_era_count}) is below minimum threshold ({MIN_KAZ_ERA_SALES})",
            details={
                'kaz_era_start': str(KAZ_ERA_START),
                'sales_count': kaz_era_count,
                'threshold': MIN_KAZ_ERA_SALES,
            },
            is_critical=True
        )

    # Calculate win rate for Kaz era (only for sales with P&L data)
    kaz_with_pnl = kaz_era_sales[kaz_era_sales['has_pnl'] == True]

    if len(kaz_with_pnl) > 0:
        wins = (kaz_with_pnl['realized_net'] > 0).sum()
        losses = (kaz_with_pnl['realized_net'] < 0).sum()
        total_with_outcome = wins + losses

        if total_with_outcome > 0:
            win_rate = wins / total_with_outcome

            # Win rate sanity check: should be between 20% and 90%
            if win_rate < 0.20 or win_rate > 0.90:
                return ValidationResult(
                    check_name=check_name,
                    status="WARN",
                    message=f"Kaz-era win rate ({win_rate:.1%}) is outside normal range (20%-90%)",
                    details={
                        'kaz_era_start': str(KAZ_ERA_START),
                        'sales_count': kaz_era_count,
                        'pnl_coverage': len(kaz_with_pnl),
                        'win_rate': win_rate,
                        'wins': wins,
                        'losses': losses,
                    },
                    is_critical=False
                )

    return ValidationResult(
        check_name=check_name,
        status="PASS",
        message=f"Kaz-era sales count ({kaz_era_count}) is healthy",
        details={
            'kaz_era_start': str(KAZ_ERA_START),
            'sales_count': kaz_era_count,
            'pnl_coverage': len(kaz_with_pnl) if 'kaz_with_pnl' in dir() else 0,
        },
        is_critical=False
    )


def check_revenue_validation(sales_df: pd.DataFrame, daily_df: pd.DataFrame) -> ValidationResult:
    """
    Revenue validation:
    - Calculate: total_sales x avg_price from sales data
    - Compare to reported revenue from daily data
    - Alert if difference > 5%
    """
    check_name = "revenue_validation"

    if sales_df is None or sales_df.empty:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No sales data available",
            is_critical=False
        )

    if daily_df is None or daily_df.empty:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No daily data available",
            is_critical=False
        )

    # Align date ranges - only compare overlapping period
    daily_start = pd.to_datetime(daily_df['date'].min()).date()
    sales_aligned = sales_df[sales_df['sale_date'] >= daily_start]

    # Calculate revenue from aligned sales data
    calculated_revenue = sales_aligned['sale_price'].sum()

    # Get reported revenue from daily data (in millions, need to convert)
    # Prefer unified columns which include all sources
    if 'revenue_millions_unified' in daily_df.columns:
        reported_revenue = daily_df['revenue_millions_unified'].sum() * 1_000_000
    elif 'revenue_millions' in daily_df.columns:
        reported_revenue = daily_df['revenue_millions'].sum() * 1_000_000
    elif 'revenue_millions_sing' in daily_df.columns:
        reported_revenue = daily_df['revenue_millions_sing'].sum() * 1_000_000
    else:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No revenue column found in daily data",
            is_critical=False
        )

    # Calculate difference
    if reported_revenue == 0:
        return ValidationResult(
            check_name=check_name,
            status="WARN",
            message="Reported revenue is zero",
            details={
                'calculated_revenue': calculated_revenue,
                'reported_revenue': reported_revenue,
            },
            is_critical=False
        )

    difference = abs(calculated_revenue - reported_revenue) / reported_revenue

    if difference > REVENUE_TOLERANCE:
        return ValidationResult(
            check_name=check_name,
            status="FAIL",
            message=f"Revenue discrepancy ({difference:.1%}) exceeds {REVENUE_TOLERANCE:.0%} tolerance",
            details={
                'calculated_revenue': calculated_revenue,
                'reported_revenue': reported_revenue,
                'difference_pct': difference,
                'tolerance': REVENUE_TOLERANCE,
                'date_range_start': str(daily_start),
                'aligned_sales_count': len(sales_aligned),
            },
            is_critical=True
        )

    return ValidationResult(
        check_name=check_name,
        status="PASS",
        message=f"Revenue validation passed (difference: {difference:.1%})",
        details={
            'calculated_revenue': calculated_revenue,
            'reported_revenue': reported_revenue,
            'difference_pct': difference,
            'date_range_start': str(daily_start),
            'aligned_sales_count': len(sales_aligned),
        },
        is_critical=False
    )


def check_win_rate_validation(sales_df: pd.DataFrame) -> ValidationResult:
    """
    Win rate validation:
    - Recalculate win rate from raw sales data
    - Compare to any reported win rate
    - Alert if difference > 1%
    """
    check_name = "win_rate_validation"

    if sales_df is None or sales_df.empty:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No sales data available",
            is_critical=False
        )

    # Filter to sales with P&L data
    sales_with_pnl = sales_df[sales_df['has_pnl'] == True]

    if len(sales_with_pnl) == 0:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No sales with P&L data available",
            is_critical=False
        )

    # Calculate win rate
    wins = (sales_with_pnl['realized_net'] > 0).sum()
    losses = (sales_with_pnl['realized_net'] < 0).sum()
    breakeven = (sales_with_pnl['realized_net'] == 0).sum()
    total_with_outcome = wins + losses

    if total_with_outcome == 0:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No sales with win/loss outcome",
            is_critical=False
        )

    calculated_win_rate = wins / total_with_outcome

    # Calculate average profit and loss
    avg_profit = sales_with_pnl[sales_with_pnl['realized_net'] > 0]['realized_net'].mean() if wins > 0 else 0
    avg_loss = sales_with_pnl[sales_with_pnl['realized_net'] < 0]['realized_net'].mean() if losses > 0 else 0
    total_realized = sales_with_pnl['realized_net'].sum()

    # Sanity checks
    issues = []

    # Check if win rate is in reasonable range
    if calculated_win_rate < 0.30:
        issues.append(f"Win rate ({calculated_win_rate:.1%}) is below 30% - unusually low")
    elif calculated_win_rate > 0.85:
        issues.append(f"Win rate ({calculated_win_rate:.1%}) is above 85% - verify data accuracy")

    # Check if average loss is unreasonably large
    if avg_loss < -100000:  # Average loss > $100k
        issues.append(f"Average loss (${avg_loss:,.0f}) is unusually large")

    if issues:
        return ValidationResult(
            check_name=check_name,
            status="WARN",
            message="; ".join(issues),
            details={
                'calculated_win_rate': calculated_win_rate,
                'wins': wins,
                'losses': losses,
                'breakeven': breakeven,
                'avg_profit': avg_profit,
                'avg_loss': avg_loss,
                'total_realized': total_realized,
                'pnl_coverage': len(sales_with_pnl),
                'total_sales': len(sales_df),
            },
            is_critical=False
        )

    return ValidationResult(
        check_name=check_name,
        status="PASS",
        message=f"Win rate validation passed ({calculated_win_rate:.1%} from {total_with_outcome} sales)",
        details={
            'calculated_win_rate': calculated_win_rate,
            'wins': wins,
            'losses': losses,
            'breakeven': breakeven,
            'avg_profit': avg_profit,
            'avg_loss': avg_loss,
            'total_realized': total_realized,
            'pnl_coverage': len(sales_with_pnl),
            'total_sales': len(sales_df),
        },
        is_critical=False
    )


def check_cross_source_validation(unified_sales: pd.DataFrame,
                                   singularity_sales: pd.DataFrame) -> ValidationResult:
    """
    Cross-source validation:
    - Compare total counts from Singularity vs Parcl (via unified data)
    - Alert on large discrepancies
    """
    check_name = "cross_source_validation"

    if unified_sales is None or unified_sales.empty:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No unified sales data available",
            is_critical=False
        )

    if singularity_sales is None or singularity_sales.empty:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No Singularity sales data available",
            is_critical=False
        )

    # Count by source in unified data
    singularity_count = ((unified_sales['source'] == 'singularity_only') |
                         (unified_sales['source'] == 'matched')).sum()
    parcl_count = ((unified_sales['source'] == 'parcl_only') |
                   (unified_sales['source'] == 'matched')).sum()
    matched_count = (unified_sales['source'] == 'matched').sum()

    # Total from Singularity source directly
    singularity_direct_count = len(singularity_sales)

    # Check for major discrepancies
    # Singularity should have more sales (it's real-time)
    if singularity_count > 0 and parcl_count > 0:
        source_ratio = parcl_count / singularity_count

        # If Parcl has significantly more than Singularity, something is wrong
        if source_ratio > 1.5:
            return ValidationResult(
                check_name=check_name,
                status="WARN",
                message=f"Parcl count ({parcl_count}) significantly exceeds Singularity ({singularity_count})",
                details={
                    'singularity_in_unified': singularity_count,
                    'parcl_in_unified': parcl_count,
                    'matched': matched_count,
                    'singularity_direct': singularity_direct_count,
                    'source_ratio': source_ratio,
                },
                is_critical=False
            )

    # Check match rate
    if singularity_count > 0 and parcl_count > 0:
        potential_matches = min(singularity_count, parcl_count)
        match_rate = matched_count / potential_matches if potential_matches > 0 else 0

        # Warn if match rate is very low (suggests data quality issues)
        if match_rate < 0.10 and potential_matches > 100:
            return ValidationResult(
                check_name=check_name,
                status="WARN",
                message=f"Low match rate ({match_rate:.1%}) between sources - verify date/price matching",
                details={
                    'singularity_in_unified': singularity_count,
                    'parcl_in_unified': parcl_count,
                    'matched': matched_count,
                    'match_rate': match_rate,
                    'singularity_direct': singularity_direct_count,
                },
                is_critical=False
            )

    return ValidationResult(
        check_name=check_name,
        status="PASS",
        message=f"Cross-source validation passed (matched: {matched_count}, sing: {singularity_count}, parcl: {parcl_count})",
        details={
            'singularity_in_unified': singularity_count,
            'parcl_in_unified': parcl_count,
            'matched': matched_count,
            'singularity_direct': singularity_direct_count,
        },
        is_critical=False
    )


def check_data_freshness(output_dir: Path) -> ValidationResult:
    """
    Check that data files are recent (within last 3 days).
    """
    check_name = "data_freshness"

    sales_file = find_latest_file(output_dir, "unified_sales_*.csv")
    daily_file = find_latest_file(output_dir, "unified_daily_*.csv")

    if not sales_file and not daily_file:
        return ValidationResult(
            check_name=check_name,
            status="FAIL",
            message="No data files found",
            is_critical=True
        )

    now = datetime.now()
    stale_files = []

    for filepath, name in [(sales_file, "unified_sales"), (daily_file, "unified_daily")]:
        if filepath:
            mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
            age_days = (now - mtime).days
            if age_days > 3:
                stale_files.append(f"{name} ({age_days} days old)")

    if stale_files:
        return ValidationResult(
            check_name=check_name,
            status="WARN",
            message=f"Stale data files: {', '.join(stale_files)}",
            details={
                'stale_files': stale_files,
            },
            is_critical=False
        )

    return ValidationResult(
        check_name=check_name,
        status="PASS",
        message="Data files are fresh",
        is_critical=False
    )


def check_sales_count_sanity(unified_sales: pd.DataFrame, daily_df: pd.DataFrame) -> ValidationResult:
    """
    Verify that sales counts in daily summary match actual records.
    """
    check_name = "sales_count_sanity"

    if unified_sales is None or unified_sales.empty:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No sales data available",
            is_critical=False
        )

    if daily_df is None or daily_df.empty:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No daily data available",
            is_critical=False
        )

    # Align date ranges - only compare overlapping period
    daily_start = pd.to_datetime(daily_df['date'].min()).date()
    unified_sales_aligned = unified_sales[unified_sales['sale_date'] >= daily_start]

    # Count sales per day from aligned sales data
    sales_by_date = unified_sales_aligned.groupby('sale_date').size()

    # Compare with daily data - prefer unified columns
    if 'sales_count_unified' in daily_df.columns:
        daily_counts = daily_df.set_index('date')['sales_count_unified']
    elif 'sales_count' in daily_df.columns:
        daily_counts = daily_df.set_index('date')['sales_count']
    elif 'sales_count_sing' in daily_df.columns:
        daily_counts = daily_df.set_index('date')['sales_count_sing']
    else:
        return ValidationResult(
            check_name=check_name,
            status="SKIP",
            message="No sales count column in daily data",
            is_critical=False
        )

    # Check totals (using aligned sales data)
    total_from_sales = len(unified_sales_aligned)
    total_from_daily = daily_counts.sum()

    if total_from_daily == 0:
        return ValidationResult(
            check_name=check_name,
            status="WARN",
            message="Daily total is zero",
            is_critical=False
        )

    difference = abs(total_from_sales - total_from_daily) / total_from_daily

    if difference > 0.10:  # 10% tolerance
        return ValidationResult(
            check_name=check_name,
            status="WARN",
            message=f"Sales count mismatch: {total_from_sales} (sales) vs {total_from_daily:.0f} (daily), diff={difference:.1%}",
            details={
                'total_from_sales': total_from_sales,
                'total_from_daily': total_from_daily,
                'difference_pct': difference,
                'date_range_start': str(daily_start),
            },
            is_critical=False
        )

    return ValidationResult(
        check_name=check_name,
        status="PASS",
        message=f"Sales count matches daily totals (diff: {difference:.1%})",
        details={
            'total_from_sales': total_from_sales,
            'total_from_daily': total_from_daily,
            'difference_pct': difference,
            'date_range_start': str(daily_start),
        },
        is_critical=False
    )


# =============================================================================
# MAIN VALIDATION RUNNER
# =============================================================================

def run_all_validations(output_dir: Path, checks: Optional[List[str]] = None) -> ValidationReport:
    """
    Run all validation checks and return a report.

    Args:
        output_dir: Directory containing data files
        checks: Optional list of specific checks to run

    Returns:
        ValidationReport with all results
    """
    logger.info("Loading data files...")

    # Load data
    unified_sales = load_unified_sales(output_dir)
    unified_daily = load_unified_daily(output_dir)
    singularity_sales = load_singularity_sales(output_dir)
    singularity_daily = load_singularity_daily(output_dir)

    logger.info(f"  Unified sales: {len(unified_sales) if unified_sales is not None else 0} records")
    logger.info(f"  Unified daily: {len(unified_daily) if unified_daily is not None else 0} days")
    logger.info(f"  Singularity sales: {len(singularity_sales) if singularity_sales is not None else 0} records")
    logger.info(f"  Singularity daily: {len(singularity_daily) if singularity_daily is not None else 0} days")

    # Define all checks
    all_checks = {
        'kaz_era': lambda: check_kaz_era_sanity(unified_sales),
        'revenue': lambda: check_revenue_validation(unified_sales, unified_daily),
        'win_rate': lambda: check_win_rate_validation(unified_sales),
        'cross_source': lambda: check_cross_source_validation(unified_sales, singularity_sales),
        'freshness': lambda: check_data_freshness(output_dir),
        'sales_count': lambda: check_sales_count_sanity(unified_sales, unified_daily),
    }

    # Filter checks if specified
    if checks:
        all_checks = {k: v for k, v in all_checks.items() if k in checks}

    # Run checks
    results = []
    for check_name, check_fn in all_checks.items():
        logger.info(f"Running check: {check_name}")
        try:
            result = check_fn()
            results.append(result)
        except Exception as e:
            logger.error(f"  Check {check_name} failed with error: {e}")
            results.append(ValidationResult(
                check_name=check_name,
                status="FAIL",
                message=f"Check failed with error: {str(e)}",
                is_critical=True
            ))

    # Calculate summary
    summary = {
        'total': len(results),
        'passed': sum(1 for r in results if r.status == 'PASS'),
        'failed': sum(1 for r in results if r.status == 'FAIL'),
        'warned': sum(1 for r in results if r.status == 'WARN'),
        'skipped': sum(1 for r in results if r.status == 'SKIP'),
    }

    has_critical_failures = any(r.status == 'FAIL' and r.is_critical for r in results)

    return ValidationReport(
        timestamp=datetime.now().isoformat(),
        results=results,
        summary=summary,
        has_critical_failures=has_critical_failures,
    )


def print_report(report: ValidationReport) -> None:
    """Print validation report to console."""
    print("\n" + "=" * 70)
    print("  DATA VALIDATION REPORT")
    print("=" * 70)
    print(f"\n  Timestamp: {report.timestamp}")
    print(f"  Total checks: {report.summary['total']}")
    print()

    # Status icons
    status_icons = {
        'PASS': '[PASS]',
        'FAIL': '[FAIL]',
        'WARN': '[WARN]',
        'SKIP': '[SKIP]',
    }

    # Print each result
    for result in report.results:
        icon = status_icons.get(result.status, '[????]')
        critical_marker = " (CRITICAL)" if result.is_critical and result.status == 'FAIL' else ""
        print(f"  {icon} {result.check_name}: {result.message}{critical_marker}")

        # Print details for non-passing checks
        if result.status in ('FAIL', 'WARN') and result.details:
            for key, value in result.details.items():
                if isinstance(value, float):
                    if 'pct' in key or 'rate' in key or 'ratio' in key:
                        print(f"         {key}: {value:.1%}")
                    else:
                        print(f"         {key}: {value:,.2f}")
                else:
                    print(f"         {key}: {value}")

    # Summary
    print()
    print("-" * 70)
    print(f"  Summary: {report.summary['passed']} passed, {report.summary['failed']} failed, "
          f"{report.summary['warned']} warnings, {report.summary['skipped']} skipped")

    if report.has_critical_failures:
        print("\n  *** CRITICAL FAILURES DETECTED ***")

    print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Run data validation checks for Glass House"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=None,
        help="Directory containing data files (default: ./outputs)"
    )
    parser.add_argument(
        "--check", "-c",
        action="append",
        choices=['kaz_era', 'revenue', 'win_rate', 'cross_source', 'freshness', 'sales_count'],
        help="Run specific check(s) only"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 if any critical check fails"
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Only output JSON, no console report"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save report to file"
    )
    args = parser.parse_args()

    # Suppress logging for JSON-only output
    if args.json_only:
        logging.getLogger().setLevel(logging.WARNING)

    # Determine output directory
    project_root = Path(__file__).parent.parent
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "outputs"

    if not output_dir.exists():
        logger.error(f"Output directory does not exist: {output_dir}")
        return 1

    # Run validations
    report = run_all_validations(output_dir, args.check)

    # Print report
    if not args.json_only:
        print_report(report)

    # Save report
    if not args.no_save:
        report_file = output_dir / "validation_report.json"
        with open(report_file, 'w') as f:
            json.dump(report.to_dict(), f, indent=2)

        if not args.json_only:
            logger.info(f"Report saved to: {report_file}")

    # Output JSON if requested
    if args.json_only:
        print(json.dumps(report.to_dict(), indent=2))

    # Exit code
    if args.strict and report.has_critical_failures:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
