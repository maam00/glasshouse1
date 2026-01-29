"""
Tests for metrics calculation module.
"""

import pytest
import pandas as pd
from src.metrics.calculator import MetricsCalculator


class TestCohortClassification:
    """Test cohort classification logic."""

    def test_new_cohort_under_90_days(self, sample_sales_with_cohorts):
        """Test that homes <90 days are classified as 'new'."""
        calc = MetricsCalculator(sample_sales_with_cohorts, pd.DataFrame())
        new, mid, old, toxic = calc.calculate_cohorts()

        # Should have 5 homes in new cohort (days_held < 90)
        assert new.count == 5
        assert new.name == "New (<90d)"

    def test_mid_cohort_90_to_180_days(self, sample_sales_with_cohorts):
        """Test that homes 90-180 days are classified as 'mid'."""
        calc = MetricsCalculator(sample_sales_with_cohorts, pd.DataFrame())
        new, mid, old, toxic = calc.calculate_cohorts()

        # Should have 4 homes in mid cohort (90 <= days_held < 180)
        assert mid.count == 4
        assert mid.name == "Mid (90-180d)"

    def test_old_cohort_180_to_365_days(self, sample_sales_with_cohorts):
        """Test that homes 180-365 days are classified as 'old'."""
        calc = MetricsCalculator(sample_sales_with_cohorts, pd.DataFrame())
        new, mid, old, toxic = calc.calculate_cohorts()

        # Should have 4 homes in old cohort (180 <= days_held < 365)
        assert old.count == 4
        assert old.name == "Old (180-365d)"

    def test_toxic_cohort_over_365_days(self, sample_sales_with_cohorts):
        """Test that homes >365 days are classified as 'toxic'."""
        calc = MetricsCalculator(sample_sales_with_cohorts, pd.DataFrame())
        new, mid, old, toxic = calc.calculate_cohorts()

        # Should have 7 homes in toxic cohort (days_held >= 365)
        assert toxic.count == 7
        assert toxic.name == "Toxic (>365d)"


class TestWinRate:
    """Test win rate calculation."""

    def test_win_rate_calculation(self, sample_sales_with_cohorts):
        """Test win rate is correctly calculated."""
        calc = MetricsCalculator(sample_sales_with_cohorts, pd.DataFrame())
        new, mid, old, toxic = calc.calculate_cohorts()

        # New cohort: 5 wins out of 5 = 100%
        assert new.win_rate == 100.0

        # Mid cohort: 2 wins out of 4 = 50%
        assert mid.win_rate == 50.0

        # Old cohort: 0 wins out of 4 = 0%
        assert old.win_rate == 0.0

        # Toxic cohort: 0 wins out of 7 = 0%
        assert toxic.win_rate == 0.0

    def test_overall_performance_win_rate(self, sample_sales_df):
        """Test overall win rate calculation."""
        calc = MetricsCalculator(sample_sales_df, pd.DataFrame())
        performance = calc.calculate_performance()

        # 4 wins out of 5 = 80%
        assert performance.win_rate == 80.0


class TestProfitCalculation:
    """Test profit calculations."""

    def test_average_profit_calculation(self, sample_sales_with_cohorts):
        """Test average profit is correctly calculated per cohort."""
        calc = MetricsCalculator(sample_sales_with_cohorts, pd.DataFrame())
        new, mid, old, toxic = calc.calculate_cohorts()

        # New cohort average: (20000+15000+18000+22000+25000) / 5 = 20000
        assert new.avg_profit == 20000.0

    def test_total_profit_calculation(self, sample_sales_with_cohorts):
        """Test total profit is correctly calculated."""
        calc = MetricsCalculator(sample_sales_with_cohorts, pd.DataFrame())
        new, mid, old, toxic = calc.calculate_cohorts()

        # New cohort total: 20000+15000+18000+22000+25000 = 100000
        assert new.total_profit == 100000.0


class TestInventoryHealth:
    """Test inventory health calculations."""

    def test_inventory_age_buckets(self, sample_listings_df):
        """Test inventory is correctly bucketed by age."""
        calc = MetricsCalculator(pd.DataFrame(), sample_listings_df)
        inventory = calc.calculate_inventory_health()

        assert inventory.total == 4
        # Fresh: 25 days (< 30)
        assert inventory.fresh_count == 1
        # Normal: 30-90 days - none in our sample
        assert inventory.normal_count == 0
        # Stale: 100 days (90-180)
        assert inventory.stale_count == 1
        # Very stale: 250 days (180-365)
        assert inventory.very_stale_count == 1
        # Toxic: 400 days (> 365)
        assert inventory.toxic_count == 1

    def test_legacy_percentage(self, sample_listings_df):
        """Test legacy percentage calculation (>180 days)."""
        calc = MetricsCalculator(pd.DataFrame(), sample_listings_df)
        inventory = calc.calculate_inventory_health()

        # 2 out of 4 are legacy (very_stale + toxic)
        assert inventory.legacy_pct == 50.0


class TestEmptyDataHandling:
    """Test handling of empty DataFrames."""

    def test_empty_sales_returns_zero_cohorts(self):
        """Test that empty sales returns zero-count cohorts."""
        calc = MetricsCalculator(pd.DataFrame(), pd.DataFrame())
        new, mid, old, toxic = calc.calculate_cohorts()

        assert new.count == 0
        assert mid.count == 0
        assert old.count == 0
        assert toxic.count == 0

    def test_empty_listings_returns_zero_inventory(self):
        """Test that empty listings returns zero inventory."""
        calc = MetricsCalculator(pd.DataFrame(), pd.DataFrame())
        inventory = calc.calculate_inventory_health()

        assert inventory.total == 0
        assert inventory.avg_dom == 0


class TestColumnNormalization:
    """Test column name normalization."""

    def test_alternative_column_names(self):
        """Test that alternative column names are normalized."""
        # Use alternative column names
        sales_df = pd.DataFrame({
            'property_id': ['prop_001'],
            'sold_price': [400000],  # Alternative for sale_price
            'buy_price': [380000],   # Alternative for purchase_price
            'hold_days': [60],       # Alternative for days_held
            'profit': [20000],       # Alternative for realized_net
        })

        calc = MetricsCalculator(sales_df, pd.DataFrame())

        # Should normalize columns and calculate correctly
        performance = calc.calculate_performance()
        assert performance.homes_sold_total == 1
        assert performance.win_rate == 100.0
