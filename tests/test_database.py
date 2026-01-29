"""
Tests for database module, including SQL injection prevention.
"""

import pytest
import tempfile
from pathlib import Path
from src.db.database import Database


class TestDatabaseInitialization:
    """Test database initialization."""

    def test_database_creates_file(self):
        """Test that database creates the SQLite file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = Database(str(db_path))

            assert db_path.exists()

    def test_database_creates_tables(self):
        """Test that database creates required tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = Database(str(db_path))

            conn = db._get_conn()
            cursor = conn.cursor()

            # Check tables exist
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = {row['name'] for row in cursor.fetchall()}

            assert 'daily_metrics' in tables
            assert 'sales_log' in tables
            assert 'listings_log' in tables
            assert 'alerts' in tables

            conn.close()


class TestValidMetrics:
    """Test valid metric whitelist."""

    def test_valid_metrics_defined(self):
        """Test that VALID_METRICS is defined and non-empty."""
        assert hasattr(Database, 'VALID_METRICS')
        assert len(Database.VALID_METRICS) > 0

    def test_valid_metrics_includes_key_columns(self):
        """Test that key columns are in VALID_METRICS."""
        assert 'new_cohort_win_rate' in Database.VALID_METRICS
        assert 'toxic_remaining' in Database.VALID_METRICS
        assert 'overall_win_rate' in Database.VALID_METRICS
        assert 'contribution_margin' in Database.VALID_METRICS


class TestSQLInjectionPrevention:
    """Test SQL injection prevention in get_time_series."""

    def test_valid_metric_path_allowed(self):
        """Test that valid metric paths work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = Database(str(db_path))

            # Should not raise
            result = db.get_time_series('new_cohort_win_rate', days=7)
            assert isinstance(result, list)

    def test_invalid_metric_path_raises(self):
        """Test that invalid metric paths raise ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = Database(str(db_path))

            # Should raise ValueError for invalid metric
            with pytest.raises(ValueError) as excinfo:
                db.get_time_series('invalid_metric', days=7)

            assert "Invalid metric" in str(excinfo.value)

    def test_sql_injection_attempt_blocked(self):
        """Test that SQL injection attempts are blocked."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = Database(str(db_path))

            # Attempt SQL injection
            injection_attempts = [
                "toxic_remaining; DROP TABLE daily_metrics; --",
                "1; DELETE FROM daily_metrics",
                "toxic_remaining OR 1=1",
                "toxic_remaining UNION SELECT * FROM alerts",
                "'; DROP TABLE daily_metrics; --",
            ]

            for injection in injection_attempts:
                with pytest.raises(ValueError):
                    db.get_time_series(injection, days=7)

    def test_error_message_lists_valid_metrics(self):
        """Test that error message includes list of valid metrics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = Database(str(db_path))

            with pytest.raises(ValueError) as excinfo:
                db.get_time_series('fake_metric', days=7)

            error_msg = str(excinfo.value)
            assert "Must be one of" in error_msg
            assert "new_cohort_win_rate" in error_msg


class TestGetDailyMetrics:
    """Test daily metrics retrieval."""

    def test_get_nonexistent_date_returns_none(self):
        """Test that querying a nonexistent date returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = Database(str(db_path))

            result = db.get_daily_metrics('2099-01-01')
            assert result is None

    def test_get_metrics_range_empty(self):
        """Test that empty range returns empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = Database(str(db_path))

            result = db.get_metrics_range('2099-01-01', '2099-01-31')
            assert result == []


class TestInitialToxicCount:
    """Test initial toxic count retrieval."""

    def test_initial_toxic_count_empty_db(self):
        """Test that empty DB returns 0 for initial toxic count."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            db = Database(str(db_path))

            result = db.get_initial_toxic_count()
            assert result == 0
