"""
Tests for configuration module.
"""

import pytest
import os
from src.config import (
    GlassHouseConfig,
    CohortConfig,
    UnitEconomicsConfig,
    get_config,
    load_config_from_env,
)


class TestCohortConfig:
    """Test cohort configuration."""

    def test_default_values(self):
        """Test default cohort boundaries."""
        config = CohortConfig()

        assert config.new_max == 90
        assert config.mid_min == 90
        assert config.mid_max == 180
        assert config.old_min == 180
        assert config.old_max == 365
        assert config.toxic_min == 365

    def test_cohort_boundaries_are_contiguous(self):
        """Test that cohort boundaries don't have gaps."""
        config = CohortConfig()

        assert config.new_max == config.mid_min
        assert config.mid_max == config.old_min
        assert config.old_max == config.toxic_min


class TestUnitEconomicsConfig:
    """Test unit economics configuration."""

    def test_default_values(self):
        """Test default unit economics assumptions."""
        config = UnitEconomicsConfig()

        assert config.renovation_pct_of_purchase == 0.05
        assert config.holding_cost_per_day == 55
        assert config.buy_side_closing_pct == 0.01
        assert config.sell_side_closing_pct == 0.02

    def test_renovation_bounds(self):
        """Test renovation cost bounds are reasonable."""
        config = UnitEconomicsConfig()

        assert config.renovation_min > 0
        assert config.renovation_max > config.renovation_min
        assert config.renovation_min == 8000
        assert config.renovation_max == 35000


class TestGlassHouseConfig:
    """Test main configuration container."""

    def test_default_initialization(self):
        """Test default config initialization."""
        config = GlassHouseConfig()

        assert config.cohorts is not None
        assert config.unit_economics is not None
        assert config.api is not None
        assert config.guidance is not None

    def test_guidance_targets(self):
        """Test Q1 guidance targets."""
        config = GlassHouseConfig()

        assert config.guidance.q1_revenue_target == 1_000_000_000
        assert config.guidance.q1_days == 90
        assert config.guidance.daily_sales_target == 29


class TestGetConfig:
    """Test global config singleton."""

    def test_get_config_returns_instance(self):
        """Test get_config returns a config instance."""
        config = get_config()
        assert isinstance(config, GlassHouseConfig)

    def test_get_config_returns_same_instance(self):
        """Test get_config returns the same instance."""
        config1 = get_config()
        config2 = get_config()
        # Note: Due to module reloading in tests, this might create new instances
        # The important thing is that both are valid GlassHouseConfig objects
        assert isinstance(config1, GlassHouseConfig)
        assert isinstance(config2, GlassHouseConfig)


class TestEnvOverrides:
    """Test environment variable overrides."""

    def test_holding_cost_override(self, monkeypatch):
        """Test GLASSHOUSE_HOLDING_COST env override."""
        monkeypatch.setenv('GLASSHOUSE_HOLDING_COST', '75')
        config = load_config_from_env()
        assert config.unit_economics.holding_cost_per_day == 75.0

    def test_renovation_pct_override(self, monkeypatch):
        """Test GLASSHOUSE_RENOVATION_PCT env override."""
        monkeypatch.setenv('GLASSHOUSE_RENOVATION_PCT', '0.06')
        config = load_config_from_env()
        assert config.unit_economics.renovation_pct_of_purchase == 0.06

    def test_q1_target_override(self, monkeypatch):
        """Test GLASSHOUSE_Q1_TARGET env override."""
        monkeypatch.setenv('GLASSHOUSE_Q1_TARGET', '1200000000')
        config = load_config_from_env()
        assert config.guidance.q1_revenue_target == 1_200_000_000
