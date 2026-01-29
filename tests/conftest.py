"""
Pytest configuration and fixtures for Glass House tests.
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def sample_sales_df():
    """Sample sales DataFrame for testing."""
    return pd.DataFrame({
        'property_id': ['prop_001', 'prop_002', 'prop_003', 'prop_004', 'prop_005'],
        'sale_price': [400000, 350000, 500000, 300000, 450000],
        'purchase_price': [370000, 340000, 480000, 320000, 420000],
        'days_held': [60, 120, 200, 400, 45],  # new, mid, old, toxic, new
        'realized_net': [30000, 10000, 20000, -20000, 30000],
        'sale_date': ['2026-01-15', '2026-01-14', '2026-01-13', '2026-01-12', '2026-01-11'],
        'state': ['TX', 'AZ', 'TX', 'GA', 'NC'],
        'city': ['Austin', 'Phoenix', 'Dallas', 'Atlanta', 'Charlotte'],
    })


@pytest.fixture
def sample_listings_df():
    """Sample listings DataFrame for testing."""
    return pd.DataFrame({
        'property_id': ['list_001', 'list_002', 'list_003', 'list_004'],
        'list_price': [450000, 380000, 520000, 290000],
        'purchase_price': [420000, 360000, 500000, 310000],
        'days_on_market': [25, 100, 250, 400],  # fresh, stale, very_stale, toxic
        'state': ['TX', 'AZ', 'GA', 'FL'],
        'city': ['Houston', 'Tucson', 'Savannah', 'Miami'],
    })


@pytest.fixture
def sample_sales_with_cohorts():
    """Sales data with clear cohort distribution for testing."""
    return pd.DataFrame({
        'property_id': [f'prop_{i:03d}' for i in range(20)],
        'sale_price': [400000] * 20,
        'purchase_price': [380000] * 10 + [420000] * 10,  # 10 wins, 10 losses
        'days_held': (
            [30, 45, 60, 75, 85] +  # 5 new (<90)
            [100, 120, 150, 170] +  # 4 mid (90-180)
            [200, 250, 300, 350] +  # 4 old (180-365)
            [400, 450, 500, 550, 600, 650, 700]  # 7 toxic (>365)
        ),
        'realized_net': (
            [20000, 15000, 18000, 22000, 25000] +  # new: all wins
            [10000, 5000, -5000, -10000] +  # mid: 2 wins, 2 losses
            [-15000, -20000, -25000, -30000] +  # old: all losses
            [-35000, -40000, -45000, -50000, -55000, -60000, -65000]  # toxic: all losses
        ),
        'sale_date': ['2026-01-15'] * 20,
        'state': ['TX'] * 20,
    })
