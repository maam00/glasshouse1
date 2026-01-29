"""
Glass House Configuration
=========================
Centralized configuration for all constants and settings.

IMPORTANT: This file is the SINGLE SOURCE OF TRUTH for:
- KAZ_ERA_START date
- Cohort definitions
- Signal thresholds

DO NOT hardcode these values elsewhere. Import from this module.
"""

from dataclasses import dataclass, field
from typing import Dict, List
from datetime import datetime
import os


# =============================================================================
# KAZ-ERA DEFINITION - SINGLE SOURCE OF TRUTH
# =============================================================================
# Kaz Nejatian became Opendoor CEO on September 10, 2025
# "Kaz-era" = homes acquired ON OR AFTER this date (new strategy)
# "Legacy" = homes acquired BEFORE this date (previous management)

KAZ_ERA_START = datetime(2025, 9, 10)
KAZ_ERA_START_STR = "Sep 2025"  # For display in UI
KAZ_ERA_START_DISPLAY = "September 10, 2025"  # Full display format


def is_kaz_era(purchase_date) -> bool:
    """Check if a purchase date falls in Kaz era.

    Args:
        purchase_date: datetime object or string parseable to datetime

    Returns:
        True if purchase_date >= KAZ_ERA_START, False otherwise
    """
    if purchase_date is None:
        return False
    if isinstance(purchase_date, str):
        try:
            import pandas as pd
            purchase_date = pd.to_datetime(purchase_date)
        except:
            return False
    return purchase_date >= KAZ_ERA_START


# =============================================================================
# COHORT DEFINITIONS (by days held at time of sale)
# =============================================================================
COHORT_NEW_MAX_DAYS = 90       # <90 days = "new" (fresh inventory)
COHORT_MID_MAX_DAYS = 180      # 90-180 days = "mid" (normal cycle)
COHORT_OLD_MAX_DAYS = 365      # 180-365 days = "old" (stale)
# TOXIC = anything > 365 days  # Legacy problem inventory


def get_cohort(days_held: int) -> str:
    """Get cohort name based on days held.

    Args:
        days_held: Number of days property was held before sale

    Returns:
        Cohort name: 'new', 'mid', 'old', or 'toxic'
    """
    if days_held is None:
        return 'unknown'
    if days_held < COHORT_NEW_MAX_DAYS:
        return 'new'
    elif days_held < COHORT_MID_MAX_DAYS:
        return 'mid'
    elif days_held < COHORT_OLD_MAX_DAYS:
        return 'old'
    else:
        return 'toxic'


# =============================================================================
# SIGNAL THRESHOLDS (for OPS Score and Signal Pack)
# =============================================================================

# Guidance Pace (% of target)
PACE_GREEN_MIN = 95.0          # On track
PACE_YELLOW_MIN = 80.0         # Slightly behind
PACE_RED_BELOW = 80.0          # Significantly behind

# Win Rate (%)
WIN_RATE_GREEN_MIN = 85.0      # Healthy profitability
WIN_RATE_YELLOW_MIN = 70.0     # Acceptable
WIN_RATE_RED_BELOW = 70.0      # Concerning

# Kaz-Era Win Rate (%)
KAZ_WIN_RATE_GREEN_MIN = 90.0  # New strategy working
KAZ_WIN_RATE_YELLOW_MIN = 80.0 # Acceptable
KAZ_WIN_RATE_RED_BELOW = 80.0  # Strategy not working

# Inventory Turnover (% per 90 days)
TURNOVER_GREEN_MIN = 15.0      # Healthy velocity
TURNOVER_YELLOW_MIN = 10.0     # Slow but acceptable
TURNOVER_RED_BELOW = 10.0      # Stagnant

# Months of Inventory
MONTHS_INV_GREEN_MAX = 6.0     # Healthy
MONTHS_INV_YELLOW_MAX = 12.0   # Elevated
MONTHS_INV_RED_ABOVE = 12.0    # Severely overstocked

# Toxic Inventory (% of portfolio)
TOXIC_PCT_GREEN_MAX = 5.0      # Minimal legacy drag
TOXIC_PCT_YELLOW_MAX = 10.0    # Moderate concern
TOXIC_PCT_RED_ABOVE = 10.0     # Major drag on profitability

# Price Cut Stress (% of inventory with 3+ cuts)
PRICE_CUT_GREEN_MAX = 30.0     # Normal repricing
PRICE_CUT_YELLOW_MAX = 50.0    # Elevated stress
PRICE_CUT_RED_ABOVE = 50.0     # Severe stress


def get_signal_status(metric: str, value: float) -> str:
    """Get GREEN/YELLOW/RED status for a metric value."""
    thresholds = {
        'pace': (PACE_GREEN_MIN, PACE_YELLOW_MIN, 'higher_better'),
        'win_rate': (WIN_RATE_GREEN_MIN, WIN_RATE_YELLOW_MIN, 'higher_better'),
        'kaz_win_rate': (KAZ_WIN_RATE_GREEN_MIN, KAZ_WIN_RATE_YELLOW_MIN, 'higher_better'),
        'turnover': (TURNOVER_GREEN_MIN, TURNOVER_YELLOW_MIN, 'higher_better'),
        'months_inv': (MONTHS_INV_GREEN_MAX, MONTHS_INV_YELLOW_MAX, 'lower_better'),
        'toxic_pct': (TOXIC_PCT_GREEN_MAX, TOXIC_PCT_YELLOW_MAX, 'lower_better'),
        'price_cut': (PRICE_CUT_GREEN_MAX, PRICE_CUT_YELLOW_MAX, 'lower_better'),
    }

    if metric not in thresholds:
        return 'unknown'

    green_thresh, yellow_thresh, direction = thresholds[metric]

    if direction == 'higher_better':
        if value >= green_thresh:
            return 'green'
        elif value >= yellow_thresh:
            return 'yellow'
        else:
            return 'red'
    else:  # lower_better
        if value <= green_thresh:
            return 'green'
        elif value <= yellow_thresh:
            return 'yellow'
        else:
            return 'red'


# =============================================================================
# CONFIDENCE GRADES
# =============================================================================
CONFIDENCE_A_MIN_COVERAGE = 80.0
CONFIDENCE_A_MIN_N = 100
CONFIDENCE_B_MIN_COVERAGE = 50.0
CONFIDENCE_B_MIN_N = 50


def get_confidence_grade(coverage_pct: float, sample_size: int) -> str:
    """Get confidence grade A/B/C based on coverage and sample size."""
    if coverage_pct >= CONFIDENCE_A_MIN_COVERAGE and sample_size >= CONFIDENCE_A_MIN_N:
        return 'A'
    elif coverage_pct >= CONFIDENCE_B_MIN_COVERAGE and sample_size >= CONFIDENCE_B_MIN_N:
        return 'B'
    else:
        return 'C'


# =============================================================================
# OPS SCORE WEIGHTING
# =============================================================================
OPS_SCORE_WEIGHTS = {
    'guidance_pace': 0.25,
    'kaz_execution': 0.20,
    'legacy_burndown': 0.15,
    'inventory_liquidity': 0.15,
    'market_quality': 0.15,
    'price_cut_stress': 0.10,
}

CONFIDENCE_C_PENALTY = 0.5  # Multiply weight by this for C-grade signals


# =============================================================================
# DATACLASS CONFIGS (legacy support)
# =============================================================================

@dataclass
class CohortConfig:
    """Cohort boundary thresholds (days held)."""
    new_max: int = 90        # <90 days = "new" (Kaz-era)
    mid_min: int = 90        # 90-180 days = "mid"
    mid_max: int = 180
    old_min: int = 180       # 180-365 days = "old"
    old_max: int = 365
    toxic_min: int = 365     # >365 days = "toxic"


@dataclass
class InventoryAgeConfig:
    """Inventory age buckets (days on market)."""
    fresh_max: int = 30      # <30 days
    normal_max: int = 90     # 30-90 days
    stale_max: int = 180     # 90-180 days
    very_stale_max: int = 365  # 180-365 days
    # >365 days = toxic


@dataclass
class UnitEconomicsConfig:
    """Unit economics cost assumptions."""
    # Renovation costs (based on SEC filings, typically 4-6% of purchase price)
    renovation_pct_of_purchase: float = 0.05  # 5% default
    renovation_min: float = 8000
    renovation_max: float = 35000

    # Holding costs per day
    holding_cost_per_day: float = 55  # Property tax, insurance, utilities, maintenance

    # Transaction costs
    buy_side_closing_pct: float = 0.01   # 1% closing costs on purchase
    sell_side_closing_pct: float = 0.02  # 2% closing costs on sale
    agent_commission_pct: float = 0.025  # 2.5% if using agent

    # Financing costs
    cost_of_capital_annual: float = 0.08  # 8% annual cost of capital


@dataclass
class APIConfig:
    """API settings."""
    # Rate limiting
    parcl_rate_limit_ms: int = 200  # Milliseconds between requests
    max_retries: int = 3
    retry_base_delay: float = 1.0  # Base delay in seconds for exponential backoff
    retry_max_delay: float = 30.0  # Maximum delay between retries

    # Timeouts
    request_timeout: int = 30  # Seconds


@dataclass
class GuidanceConfig:
    """Guidance and target settings."""
    q1_revenue_target: float = 1_000_000_000  # $1B Q1 guidance
    q1_days: int = 90  # Q1 has 90 days
    daily_sales_target: int = 29  # Homes/day needed for guidance


@dataclass
class MarketActionThresholds:
    """Thresholds for market action recommendations."""
    # EXIT thresholds
    exit_margin_below: float = -5.0
    exit_win_rate_below: float = 50.0
    exit_min_inventory: int = 20

    # PAUSE thresholds
    pause_margin_below: float = 0.0
    pause_win_rate_below: float = 60.0
    pause_underwater_above: float = 50.0

    # HOLD thresholds (not GROW)
    hold_margin_below: float = 5.0
    hold_win_rate_below: float = 80.0
    hold_dom_above: float = 200.0


@dataclass
class GlassHouseConfig:
    """Main configuration container."""
    cohorts: CohortConfig = field(default_factory=CohortConfig)
    inventory_age: InventoryAgeConfig = field(default_factory=InventoryAgeConfig)
    unit_economics: UnitEconomicsConfig = field(default_factory=UnitEconomicsConfig)
    api: APIConfig = field(default_factory=APIConfig)
    guidance: GuidanceConfig = field(default_factory=GuidanceConfig)
    market_thresholds: MarketActionThresholds = field(default_factory=MarketActionThresholds)


# Global config instance
_config: GlassHouseConfig = None


def get_config() -> GlassHouseConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = GlassHouseConfig()
    return _config


def load_config_from_env() -> GlassHouseConfig:
    """Load configuration with environment variable overrides."""
    config = GlassHouseConfig()

    # Override from environment if set
    if os.environ.get('GLASSHOUSE_HOLDING_COST'):
        config.unit_economics.holding_cost_per_day = float(
            os.environ['GLASSHOUSE_HOLDING_COST']
        )

    if os.environ.get('GLASSHOUSE_RENOVATION_PCT'):
        config.unit_economics.renovation_pct_of_purchase = float(
            os.environ['GLASSHOUSE_RENOVATION_PCT']
        )

    if os.environ.get('GLASSHOUSE_Q1_TARGET'):
        config.guidance.q1_revenue_target = float(
            os.environ['GLASSHOUSE_Q1_TARGET']
        )

    return config
