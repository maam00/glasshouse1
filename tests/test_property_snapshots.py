"""
Tests for property snapshot functionality.
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

from src.db.database import (
    Database, generate_property_id, normalize_address
)


class TestAddressNormalization:
    """Test address normalization functions."""

    def test_normalize_basic_address(self):
        """Test basic address normalization."""
        result = normalize_address("123 Main Street")
        assert result == "123 main st"

    def test_normalize_with_directions(self):
        """Test direction word normalization."""
        result = normalize_address("456 North Oak Avenue")
        assert result == "456 n oak ave"

    def test_normalize_removes_punctuation(self):
        """Test punctuation removal."""
        result = normalize_address("789 Pine Rd., Suite 100")
        assert "," not in result
        assert "." not in result

    def test_normalize_with_city_state(self):
        """Test normalization with city and state."""
        result = normalize_address("123 Main St", "Phoenix", "AZ")
        assert "phoenix" in result
        assert "az" in result

    def test_normalize_empty_address(self):
        """Test empty address handling."""
        assert normalize_address("") == ""
        assert normalize_address(None) == ""


class TestPropertyIdGeneration:
    """Test property ID generation."""

    def test_generate_property_id_basic(self):
        """Test basic property ID generation."""
        prop_id = generate_property_id("123 Main St", "Phoenix", "AZ")
        assert len(prop_id) == 16
        assert prop_id.isalnum()

    def test_property_id_deterministic(self):
        """Test that same input produces same ID."""
        id1 = generate_property_id("123 Main St", "Phoenix", "AZ")
        id2 = generate_property_id("123 Main St", "Phoenix", "AZ")
        assert id1 == id2

    def test_property_id_different_for_different_addresses(self):
        """Test different addresses produce different IDs."""
        id1 = generate_property_id("123 Main St", "Phoenix", "AZ")
        id2 = generate_property_id("456 Oak Ave", "Phoenix", "AZ")
        assert id1 != id2

    def test_property_id_with_zip(self):
        """Test property ID with zip code."""
        id1 = generate_property_id("123 Main St", "Phoenix", "AZ", "85001")
        id2 = generate_property_id("123 Main St", "Phoenix", "AZ", "85002")
        # Different zips should produce different IDs
        assert id1 != id2

    def test_property_id_normalized(self):
        """Test that variations of same address produce same ID."""
        id1 = generate_property_id("123 Main Street", "Phoenix", "AZ")
        id2 = generate_property_id("123 MAIN ST", "Phoenix", "AZ")
        assert id1 == id2


class TestPropertySnapshotDatabase:
    """Test property snapshot database operations."""

    @pytest.fixture
    def db(self):
        """Create a temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        db = Database(db_path)
        yield db
        # Cleanup
        Path(db_path).unlink(missing_ok=True)

    def test_upsert_new_property(self, db):
        """Test upserting a new property."""
        snapshot = {
            'address': '123 Main St',
            'city': 'Phoenix',
            'state': 'AZ',
            'market': 'phoenix-az',
            'list_price': 450000,
            'status': 'FOR_SALE',
            'beds': 3,
            'baths': 2,
            'sqft': 1800,
            'url': 'https://opendoor.com/properties/123-main-st',
        }

        property_id, is_new, transition = db.upsert_property_snapshot(snapshot)

        assert property_id is not None
        assert len(property_id) == 16
        assert is_new == True
        assert transition is None  # No transition for new property

    def test_upsert_same_property_twice(self, db):
        """Test upserting same property twice (same day)."""
        snapshot = {
            'address': '123 Main St',
            'city': 'Phoenix',
            'state': 'AZ',
            'market': 'phoenix-az',
            'list_price': 450000,
            'status': 'FOR_SALE',
        }

        property_id1, is_new1, _ = db.upsert_property_snapshot(snapshot)
        property_id2, is_new2, _ = db.upsert_property_snapshot(snapshot)

        assert property_id1 == property_id2
        assert is_new1 == True
        assert is_new2 == False  # Second insert should not be "new"

    def test_detect_status_transition(self, db):
        """Test status transition detection."""
        # First snapshot: FOR_SALE
        snapshot1 = {
            'address': '456 Oak Ave',
            'city': 'Dallas',
            'state': 'TX',
            'market': 'dallas-tx',
            'list_price': 350000,
            'status': 'FOR_SALE',
        }
        property_id, _, _ = db.upsert_property_snapshot(snapshot1)

        # Simulate next day by directly inserting a different date record
        # (In real use, the date would change naturally)
        conn = db._get_conn()
        cursor = conn.cursor()

        # Update the first snapshot to be "yesterday"
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        cursor.execute("""
            UPDATE property_daily_snapshot
            SET snapshot_date = ?
            WHERE property_id = ?
        """, (yesterday, property_id))
        conn.commit()
        conn.close()

        # Second snapshot: PENDING (should detect transition)
        snapshot2 = {
            'address': '456 Oak Ave',
            'city': 'Dallas',
            'state': 'TX',
            'market': 'dallas-tx',
            'list_price': 350000,
            'status': 'PENDING',
        }
        _, _, transition = db.upsert_property_snapshot(snapshot2)

        assert transition is not None
        assert transition['from_status'] == 'FOR_SALE'
        assert transition['to_status'] == 'PENDING'

    def test_get_active_inventory(self, db):
        """Test getting active inventory."""
        # Insert some properties
        for i, status in enumerate(['FOR_SALE', 'FOR_SALE', 'PENDING', 'SOLD']):
            db.upsert_property_snapshot({
                'address': f'{100+i} Test St',
                'city': 'Phoenix',
                'state': 'AZ',
                'market': 'phoenix-az',
                'list_price': 400000 + i * 10000,
                'status': status,
            })

        active = db.get_active_inventory()

        assert len(active) == 2  # Only FOR_SALE
        for prop in active:
            assert prop['status'] == 'FOR_SALE'

    def test_get_inventory_snapshot_stats(self, db):
        """Test inventory statistics calculation."""
        # Insert test properties
        for i in range(5):
            db.upsert_property_snapshot({
                'address': f'{200+i} Test Blvd',
                'city': 'Atlanta',
                'state': 'GA',
                'market': 'atlanta-ga',
                'list_price': 300000 + i * 50000,
                'status': 'FOR_SALE',
            })

        stats = db.get_inventory_snapshot_stats()

        assert stats['total_tracked'] == 5
        assert stats['active_count'] == 5
        assert stats['by_status']['FOR_SALE'] == 5
        assert stats['avg_price'] > 0
        assert stats['total_value'] > 0


class TestStatusTransitions:
    """Test status transition tracking."""

    @pytest.fixture
    def db(self):
        """Create a temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        db = Database(db_path)
        yield db
        Path(db_path).unlink(missing_ok=True)

    def test_get_days_to_pending_stats_empty(self, db):
        """Test days to pending with no data."""
        stats = db.get_days_to_pending_stats()

        assert stats['count'] == 0
        assert stats['median'] is None

    def test_survival_curve_empty(self, db):
        """Test survival curve with no data."""
        data = db.get_survival_curve_data()

        # With no transitions, exits should be 0
        assert data.get('total_exits', 0) == 0 or data.get('exits', 0) == 0
        assert len(data.get('survival_rates', [])) == 0


class TestPropertyIdUniqueness:
    """Test that property_id + snapshot_date is unique."""

    @pytest.fixture
    def db(self):
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        db = Database(db_path)
        yield db
        Path(db_path).unlink(missing_ok=True)

    def test_unique_constraint(self, db):
        """Test UNIQUE(property_id, snapshot_date) constraint."""
        snapshot = {
            'address': '999 Unique St',
            'city': 'Tampa',
            'state': 'FL',
            'market': 'tampa-fl',
            'list_price': 500000,
            'status': 'FOR_SALE',
        }

        # First insert
        db.upsert_property_snapshot(snapshot)

        # Second insert same day - should update, not fail
        snapshot['list_price'] = 490000  # Price change
        db.upsert_property_snapshot(snapshot)

        # Verify only one record for today
        today = datetime.now().strftime('%Y-%m-%d')
        conn = db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM property_daily_snapshot
            WHERE snapshot_date = ?
        """, (today,))
        result = cursor.fetchone()
        conn.close()

        # Should have exactly one record (upserted)
        assert result['cnt'] == 1
