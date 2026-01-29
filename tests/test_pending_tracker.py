"""
Tests for pending tracker module.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.metrics.pending_tracker import PendingTracker, PendingMetrics


class TestCohortClassification:
    """Test cohort classification for pending homes."""

    def test_new_cohort_under_90_days(self):
        """Test homes under 90 days are classified as new."""
        tracker = PendingTracker()
        assert tracker.classify_cohort(30) == "new"
        assert tracker.classify_cohort(89) == "new"

    def test_mid_cohort_90_to_180_days(self):
        """Test homes 90-180 days are classified as mid."""
        tracker = PendingTracker()
        assert tracker.classify_cohort(90) == "mid"
        assert tracker.classify_cohort(179) == "mid"

    def test_old_cohort_180_to_365_days(self):
        """Test homes 180-365 days are classified as old."""
        tracker = PendingTracker()
        assert tracker.classify_cohort(180) == "old"
        assert tracker.classify_cohort(364) == "old"

    def test_toxic_cohort_over_365_days(self):
        """Test homes over 365 days are classified as toxic."""
        tracker = PendingTracker()
        assert tracker.classify_cohort(365) == "toxic"
        assert tracker.classify_cohort(500) == "toxic"


class TestKazEraClassification:
    """Test Kaz era classification."""

    def test_kaz_era_after_oct_2023(self):
        """Test purchases after Oct 2023 are Kaz era."""
        tracker = PendingTracker()
        assert tracker.is_kaz_era("2024-01-15") == True
        assert tracker.is_kaz_era("2023-10-01") == True
        assert tracker.is_kaz_era("2023-12-25") == True

    def test_legacy_before_oct_2023(self):
        """Test purchases before Oct 2023 are legacy."""
        tracker = PendingTracker()
        assert tracker.is_kaz_era("2023-09-30") == False
        assert tracker.is_kaz_era("2022-06-15") == False
        assert tracker.is_kaz_era("2021-01-01") == False

    def test_invalid_date_returns_false(self):
        """Test invalid dates return False."""
        tracker = PendingTracker()
        assert tracker.is_kaz_era("") == False
        assert tracker.is_kaz_era(None) == False
        assert tracker.is_kaz_era("invalid") == False


class TestPendingMetricsAnalysis:
    """Test pending metrics analysis."""

    @pytest.fixture
    def sample_pending_df(self):
        """Create sample pending data."""
        return pd.DataFrame({
            'scraped_address': ['123 Main St', '456 Oak Ave', '789 Pine Rd', '321 Elm Blvd'],
            'city': ['Phoenix', 'Dallas', 'Phoenix', 'Atlanta'],
            'state': ['AZ', 'TX', 'AZ', 'GA'],
            'list_price': [400000, 350000, 500000, 300000],
            'od_purchase_price': [380000, 340000, 480000, 320000],
            'od_days_on_market': [30, 120, 200, 400],
            'od_purchase_date': ['2024-06-01', '2024-01-15', '2023-06-01', '2022-06-01'],
        })

    def test_analyze_pending_total_count(self, sample_pending_df):
        """Test total pending count."""
        tracker = PendingTracker()
        metrics = tracker.analyze_pending_listings(sample_pending_df)

        assert metrics.total_pending == 4

    def test_analyze_pending_cohort_breakdown(self, sample_pending_df):
        """Test cohort breakdown in pending."""
        tracker = PendingTracker()
        metrics = tracker.analyze_pending_listings(sample_pending_df)

        # DOM: 30 (new), 120 (mid), 200 (old), 400 (toxic)
        assert metrics.new_cohort_pending == 1
        assert metrics.mid_cohort_pending == 1
        assert metrics.old_cohort_pending == 1
        assert metrics.toxic_cohort_pending == 1

    def test_analyze_pending_kaz_era_breakdown(self, sample_pending_df):
        """Test Kaz era vs legacy breakdown."""
        tracker = PendingTracker()
        metrics = tracker.analyze_pending_listings(sample_pending_df)

        # Kaz era: 2024-06-01, 2024-01-15 (2 homes)
        # Legacy: 2023-06-01, 2022-06-01 (2 homes)
        assert metrics.kaz_era_count == 2
        assert metrics.legacy_count == 2

    def test_analyze_pending_toxic_percentage(self, sample_pending_df):
        """Test toxic percentage calculation."""
        tracker = PendingTracker()
        metrics = tracker.analyze_pending_listings(sample_pending_df)

        # 1 toxic out of 4 = 25%
        assert metrics.toxic_pending_pct == 25.0

    def test_empty_dataframe_returns_empty_metrics(self):
        """Test empty dataframe returns zero metrics."""
        tracker = PendingTracker()
        metrics = tracker.analyze_pending_listings(pd.DataFrame())

        assert metrics.total_pending == 0
        assert metrics.kaz_era_count == 0
        assert metrics.toxic_cohort_pending == 0


class TestPendingReport:
    """Test report generation."""

    def test_generate_report_includes_key_sections(self):
        """Test that report includes all key sections."""
        tracker = PendingTracker()

        metrics = PendingMetrics(
            total_pending=50,
            total_pending_value=20000000,
            kaz_era_count=30,
            legacy_count=20,
            new_cohort_pending=15,
            mid_cohort_pending=12,
            old_cohort_pending=13,
            toxic_cohort_pending=10,
            avg_days_on_market=120,
            avg_days_held=180,
            avg_expected_profit=15000,
            conversion_rate=95.0,
            fall_through_rate=5.0,
            avg_days_in_pending=30,
            toxic_pending_pct=20.0,
        )

        report = tracker.generate_report(metrics)

        assert "PENDING FUNNEL ANALYSIS" in report
        assert "Total Pending:" in report
        assert "Kaz-Era Pending:" in report
        assert "Toxic (>365d):" in report
        assert "Conversion Rate:" in report
