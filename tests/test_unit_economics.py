"""
Tests for true unit economics calculation.
"""

import pytest
import pandas as pd
from src.metrics.unit_economics import (
    UnitEconomicsCalculator,
    UnitEconomicsConfig,
    HomeUnitEconomics,
)


class TestUnitEconomicsConfig:
    """Test unit economics configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = UnitEconomicsConfig()

        assert config.renovation_pct_of_purchase == 0.05
        assert config.holding_cost_per_day == 55
        assert config.buy_side_closing_pct == 0.01
        assert config.sell_side_closing_pct == 0.02


class TestRenovationCostEstimation:
    """Test renovation cost estimation."""

    def test_renovation_percentage_of_purchase(self):
        """Test renovation cost is calculated as percentage of purchase price."""
        calc = UnitEconomicsCalculator()
        purchase_price = 400000

        renovation = calc.estimate_renovation_cost(purchase_price)

        # 5% of 400000 = 20000
        assert renovation == 20000

    def test_renovation_minimum_floor(self):
        """Test renovation cost doesn't go below minimum."""
        calc = UnitEconomicsCalculator()
        purchase_price = 100000  # 5% = 5000, below min of 8000

        renovation = calc.estimate_renovation_cost(purchase_price)

        assert renovation == 8000  # Should hit minimum

    def test_renovation_maximum_ceiling(self):
        """Test renovation cost doesn't exceed maximum."""
        calc = UnitEconomicsCalculator()
        purchase_price = 1000000  # 5% = 50000, above max of 35000

        renovation = calc.estimate_renovation_cost(purchase_price)

        assert renovation == 35000  # Should hit maximum


class TestHoldingCosts:
    """Test holding cost calculation."""

    def test_holding_cost_calculation(self):
        """Test holding costs are calculated correctly."""
        calc = UnitEconomicsCalculator()

        # 100 days at $55/day = $5500
        holding = calc.calculate_holding_costs(100)
        assert holding == 5500

    def test_zero_days_zero_holding(self):
        """Test zero days results in zero holding costs."""
        calc = UnitEconomicsCalculator()

        holding = calc.calculate_holding_costs(0)
        assert holding == 0


class TestHomeEconomicsCalculation:
    """Test individual home economics calculation."""

    def test_true_net_calculation(self):
        """Test true net profit calculation."""
        calc = UnitEconomicsCalculator()

        econ = calc.calculate_home_economics(
            property_id="test_001",
            purchase_price=370000,
            sale_price=400000,
            days_held=160,
            state="TX",
        )

        # Gross spread: 400000 - 370000 = 30000
        assert econ.gross_spread == 30000

        # Renovation: 5% of 370000 = 18500
        assert econ.estimated_renovation == 18500

        # Holding: 160 * 55 = 8800
        assert econ.holding_costs == 8800

        # Buy closing: 1% of 370000 = 3700
        assert econ.buy_closing_costs == 3700

        # Sell closing: 2% of 400000 = 8000
        assert econ.sell_closing_costs == 8000

        # Total costs: 18500 + 8800 + 3700 + 8000 = 39000
        assert econ.total_costs == 39000

        # True net: 30000 - 39000 = -9000
        assert econ.true_net == -9000

        # This home is NOT profitable despite positive gross spread
        assert econ.is_profitable == False
        assert econ.profitability_tier == "loss"

    def test_profitable_home(self):
        """Test a truly profitable home."""
        calc = UnitEconomicsCalculator()

        econ = calc.calculate_home_economics(
            property_id="test_002",
            purchase_price=300000,
            sale_price=400000,
            days_held=30,  # Quick flip
            state="AZ",
        )

        # Gross spread: 100000
        # Renovation: 15000 (5% of 300000)
        # Holding: 30 * 55 = 1650
        # Buy closing: 3000 (1% of 300000)
        # Sell closing: 8000 (2% of 400000)
        # Total costs: ~27650
        # True net: 100000 - 27650 = ~72350

        assert econ.is_profitable == True
        assert econ.profitability_tier == "strong"


class TestProfitabilityTiers:
    """Test profitability tier classification."""

    def test_strong_tier_above_5_percent(self):
        """Test homes with >5% margin are 'strong'."""
        calc = UnitEconomicsCalculator()

        # Big spread, quick flip = strong margin
        econ = calc.calculate_home_economics(
            property_id="test",
            purchase_price=300000,
            sale_price=400000,
            days_held=30,
        )

        assert econ.profitability_tier == "strong"

    def test_marginal_tier_0_to_5_percent(self):
        """Test homes with 0-5% margin are 'marginal'."""
        calc = UnitEconomicsCalculator()

        # Small margin, longer hold
        econ = calc.calculate_home_economics(
            property_id="test",
            purchase_price=380000,
            sale_price=420000,
            days_held=60,
        )

        # This should be marginal (between 0-5% margin)
        assert econ.profitability_tier in ("marginal", "strong", "loss")

    def test_loss_tier_below_0_percent(self):
        """Test homes with <0% margin are 'loss'."""
        calc = UnitEconomicsCalculator()

        # Negative gross spread
        econ = calc.calculate_home_economics(
            property_id="test",
            purchase_price=400000,
            sale_price=380000,
            days_held=200,
        )

        assert econ.profitability_tier == "loss"
        assert econ.is_profitable == False


class TestSalesAnalysis:
    """Test analysis of multiple sales."""

    def test_analyze_sales_aggregation(self, sample_sales_df):
        """Test that sales analysis correctly aggregates results."""
        calc = UnitEconomicsCalculator()
        analysis = calc.analyze_sales(sample_sales_df)

        assert 'total_sales' in analysis
        assert analysis['total_sales'] == 5

        assert 'profitable_count' in analysis
        assert 'profitable_pct' in analysis
        assert 'tier_breakdown' in analysis

    def test_cost_breakdown_in_analysis(self, sample_sales_df):
        """Test that cost breakdown is included in analysis."""
        calc = UnitEconomicsCalculator()
        analysis = calc.analyze_sales(sample_sales_df)

        assert 'cost_breakdown' in analysis
        assert 'renovation_avg' in analysis['cost_breakdown']
        assert 'holding_avg' in analysis['cost_breakdown']

    def test_empty_sales_returns_empty_analysis(self):
        """Test that empty sales returns empty analysis."""
        calc = UnitEconomicsCalculator()
        analysis = calc.analyze_sales(pd.DataFrame())

        assert analysis == {}
