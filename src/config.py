"""
Glass House Configuration
=========================
Centralized configuration for all constants and settings.
"""

from dataclasses import dataclass, field
from typing import Dict, List
import os


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
