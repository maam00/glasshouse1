"""
Glass House Database Layer
===========================
SQLite storage for daily metrics, historical tracking, and property snapshots.
"""

import sqlite3
import json
import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


def normalize_address(address: str, city: str = None, state: str = None) -> str:
    """Normalize address for consistent matching.

    Converts to lowercase, standardizes abbreviations, removes punctuation.
    """
    if not address:
        return ""

    addr = str(address).lower().strip()

    # Standardize direction prefixes/suffixes
    direction_map = {
        'north': 'n', 'south': 's', 'east': 'e', 'west': 'w',
        'northeast': 'ne', 'northwest': 'nw', 'southeast': 'se', 'southwest': 'sw',
    }
    for full, abbr in direction_map.items():
        addr = re.sub(rf'\b{full}\b', abbr, addr)

    # Standardize street type abbreviations
    street_types = [
        ('street', 'st'), ('avenue', 'ave'), ('boulevard', 'blvd'),
        ('drive', 'dr'), ('lane', 'ln'), ('road', 'rd'),
        ('court', 'ct'), ('place', 'pl'), ('circle', 'cir'),
        ('terrace', 'ter'), ('highway', 'hwy'), ('parkway', 'pkwy'),
    ]
    for full, abbr in street_types:
        addr = re.sub(rf'\b{full}\b', abbr, addr)

    # Remove periods, commas, extra spaces
    addr = addr.replace('.', '').replace(',', '')
    addr = ' '.join(addr.split())

    # Add city/state if provided
    if city:
        addr = f"{addr} {city.lower().strip()}"
    if state:
        addr = f"{addr} {state.lower().strip()}"

    return addr


def generate_property_id(address: str, city: str = None, state: str = None, zip_code: str = None) -> str:
    """Generate stable property ID from normalized address + zip.

    Uses SHA256 hash truncated to 16 characters for uniqueness.
    """
    normalized = normalize_address(address, city, state)

    # Add zip code if available for extra uniqueness
    if zip_code:
        normalized = f"{normalized} {str(zip_code).strip()}"

    # Generate hash
    hash_input = normalized.encode('utf-8')
    hash_digest = hashlib.sha256(hash_input).hexdigest()

    # Return first 16 characters for readability
    return hash_digest[:16]


@dataclass
class CohortData:
    """Metrics for a single cohort."""
    name: str
    count: int
    win_rate: float  # Percentage
    avg_profit: float
    total_profit: float
    contribution_margin: float  # Percentage


@dataclass
class ToxicData:
    """Toxic inventory tracking."""
    sold_count: int
    sold_avg_loss: float
    remaining_count: int
    clearance_pct: float
    weeks_to_clear: float


@dataclass
class InventoryData:
    """Inventory health metrics."""
    total: int
    fresh_count: int       # <30 days
    normal_count: int      # 30-90 days
    stale_count: int       # 90-180 days
    very_stale_count: int  # 180-365 days
    toxic_count: int       # >365 days
    legacy_pct: float      # >180 days as percentage
    avg_dom: float         # Average days on market
    avg_list_price: float
    total_unrealized_pnl: float


@dataclass
class PerformanceData:
    """Overall performance metrics."""
    win_rate: float
    contribution_margin: float
    avg_profit: float
    homes_sold_total: int
    homes_sold_today: int
    revenue_total: float
    revenue_today: float
    homes_listed_today: int
    net_inventory_change: int


@dataclass
class DailyMetrics:
    """Complete daily snapshot."""
    date: str
    cohort_new: CohortData
    cohort_mid: CohortData
    cohort_old: CohortData
    cohort_toxic: CohortData
    toxic: ToxicData
    inventory: InventoryData
    performance: PerformanceData
    geographic: Dict[str, Any]
    alerts: List[str]

    def to_dict(self) -> Dict:
        return {
            "date": self.date,
            "cohort_new": asdict(self.cohort_new),
            "cohort_mid": asdict(self.cohort_mid),
            "cohort_old": asdict(self.cohort_old),
            "cohort_toxic": asdict(self.cohort_toxic),
            "toxic": asdict(self.toxic),
            "inventory": asdict(self.inventory),
            "performance": asdict(self.performance),
            "geographic": self.geographic,
            "alerts": self.alerts,
        }


class Database:
    """SQLite database for Glass House metrics."""

    # Whitelist of valid metric column names (prevents SQL injection)
    VALID_METRICS = frozenset([
        "date", "new_cohort_win_rate", "new_cohort_count",
        "mid_cohort_win_rate", "old_cohort_win_rate",
        "toxic_cohort_win_rate", "toxic_cohort_count",
        "toxic_remaining", "toxic_sold", "clearance_pct",
        "overall_win_rate", "contribution_margin", "avg_profit",
        "homes_sold_total", "homes_sold_today",
        "revenue_total", "revenue_today",
        "total_listings", "legacy_pct", "avg_dom",
    ])

    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = Path(__file__).parent.parent.parent / "data" / "glasshouse.db"

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        """Get database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self):
        """Initialize database schema."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Daily metrics table (stores full snapshot as JSON + key fields for querying)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_metrics (
                date TEXT PRIMARY KEY,

                -- Cohort metrics (for quick queries)
                new_cohort_win_rate REAL,
                new_cohort_count INTEGER,
                mid_cohort_win_rate REAL,
                old_cohort_win_rate REAL,
                toxic_cohort_win_rate REAL,
                toxic_cohort_count INTEGER,

                -- Toxic inventory
                toxic_remaining INTEGER,
                toxic_sold INTEGER,
                clearance_pct REAL,

                -- Performance
                overall_win_rate REAL,
                contribution_margin REAL,
                avg_profit REAL,
                homes_sold_total INTEGER,
                homes_sold_today INTEGER,
                revenue_total REAL,
                revenue_today REAL,

                -- Inventory
                total_listings INTEGER,
                legacy_pct REAL,
                avg_dom REAL,

                -- Full snapshot as JSON
                snapshot_json TEXT,

                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Sales log (individual sales records)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_date TEXT,
                address TEXT,
                state TEXT,
                city TEXT,
                sale_price REAL,
                purchase_price REAL,
                days_held INTEGER,
                realized_net REAL,
                cohort TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sale_date, address)
            )
        """)

        # Listings log (inventory snapshots)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS listings_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT,
                address TEXT,
                state TEXT,
                city TEXT,
                list_price REAL,
                purchase_price REAL,
                days_on_market INTEGER,
                unrealized_pnl REAL,
                age_bucket TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Alerts log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                alert_type TEXT,
                message TEXT,
                metric_name TEXT,
                old_value REAL,
                new_value REAL,
                change_pct REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Property daily snapshots - for tracking status transitions and liquidity
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS property_daily_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id TEXT NOT NULL,
                snapshot_date DATE NOT NULL,

                -- Location
                address_normalized TEXT NOT NULL,
                city TEXT,
                state TEXT,
                market TEXT,

                -- Listing details
                list_price REAL,
                status TEXT,
                beds INTEGER,
                baths REAL,
                sqft INTEGER,

                -- Metadata
                opendoor_url TEXT,
                first_seen_date DATE,
                days_on_market INTEGER,

                -- Price tracking
                previous_price REAL,
                price_change REAL,
                price_cuts_count INTEGER DEFAULT 0,

                -- Scrape metadata
                scrape_timestamp TIMESTAMP,
                source TEXT DEFAULT 'opendoor_scrape',

                UNIQUE(property_id, snapshot_date)
            )
        """)

        # Create indexes for common queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshot_date
            ON property_daily_snapshot(snapshot_date)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_property_id
            ON property_daily_snapshot(property_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_market
            ON property_daily_snapshot(market)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_status
            ON property_daily_snapshot(status)
        """)

        # Status transitions table - tracks when properties change status
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS status_transitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id TEXT NOT NULL,
                transition_date DATE NOT NULL,
                from_status TEXT,
                to_status TEXT,
                days_in_previous_status INTEGER,
                list_price_at_transition REAL,
                market TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transition_date
            ON status_transitions(transition_date)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transition_market
            ON status_transitions(market)
        """)

        conn.commit()
        conn.close()

    def save_daily_metrics(self, metrics: DailyMetrics):
        """Save daily metrics snapshot."""
        conn = self._get_conn()
        cursor = conn.cursor()

        snapshot_json = json.dumps(metrics.to_dict())

        cursor.execute("""
            INSERT OR REPLACE INTO daily_metrics (
                date,
                new_cohort_win_rate, new_cohort_count,
                mid_cohort_win_rate, old_cohort_win_rate,
                toxic_cohort_win_rate, toxic_cohort_count,
                toxic_remaining, toxic_sold, clearance_pct,
                overall_win_rate, contribution_margin, avg_profit,
                homes_sold_total, homes_sold_today,
                revenue_total, revenue_today,
                total_listings, legacy_pct, avg_dom,
                snapshot_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            metrics.date,
            metrics.cohort_new.win_rate, metrics.cohort_new.count,
            metrics.cohort_mid.win_rate, metrics.cohort_old.win_rate,
            metrics.cohort_toxic.win_rate, metrics.cohort_toxic.count,
            metrics.toxic.remaining_count, metrics.toxic.sold_count, metrics.toxic.clearance_pct,
            metrics.performance.win_rate, metrics.performance.contribution_margin, metrics.performance.avg_profit,
            metrics.performance.homes_sold_total, metrics.performance.homes_sold_today,
            metrics.performance.revenue_total, metrics.performance.revenue_today,
            metrics.inventory.total, metrics.inventory.legacy_pct, metrics.inventory.avg_dom,
            snapshot_json,
        ))

        # Save alerts
        for alert in metrics.alerts:
            cursor.execute("""
                INSERT INTO alerts (date, alert_type, message)
                VALUES (?, 'metric_change', ?)
            """, (metrics.date, alert))

        conn.commit()
        conn.close()

        logger.info(f"Saved metrics for {metrics.date}")

    def save_raw_metrics(self, date: str, metrics_dict: Dict):
        """Save raw metrics dict (for backfill)."""
        import numpy as np

        def convert_numpy(obj):
            if isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        conn = self._get_conn()
        cursor = conn.cursor()

        snapshot_json = json.dumps(metrics_dict, default=convert_numpy)

        # Extract key fields with safe defaults
        cohort_new = metrics_dict.get("cohort_new", {})
        cohort_mid = metrics_dict.get("cohort_mid", {})
        cohort_old = metrics_dict.get("cohort_old", {})
        cohort_toxic = metrics_dict.get("cohort_toxic", {})
        toxic = metrics_dict.get("toxic", {})
        perf = metrics_dict.get("performance", {})
        inv = metrics_dict.get("inventory", {})

        cursor.execute("""
            INSERT OR REPLACE INTO daily_metrics (
                date,
                new_cohort_win_rate, new_cohort_count,
                mid_cohort_win_rate, old_cohort_win_rate,
                toxic_cohort_win_rate, toxic_cohort_count,
                toxic_remaining, toxic_sold, clearance_pct,
                overall_win_rate, contribution_margin, avg_profit,
                homes_sold_total, homes_sold_today,
                revenue_total, revenue_today,
                total_listings, legacy_pct, avg_dom,
                snapshot_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            date,
            cohort_new.get("win_rate", 0), cohort_new.get("count", 0),
            cohort_mid.get("win_rate", 0), cohort_old.get("win_rate", 0),
            cohort_toxic.get("win_rate", 0), cohort_toxic.get("count", 0),
            toxic.get("remaining_count", 0), toxic.get("sold_count", 0), toxic.get("clearance_pct", 0),
            perf.get("win_rate", 0), perf.get("contribution_margin", 0), perf.get("avg_profit", 0),
            perf.get("homes_sold_total", 0), perf.get("homes_sold_today", 0),
            perf.get("revenue_total", 0), perf.get("revenue_today", 0),
            inv.get("total", 0), inv.get("legacy_pct", 0), inv.get("avg_dom", 0),
            snapshot_json,
        ))

        conn.commit()
        conn.close()

    def get_daily_metrics(self, date: str) -> Optional[Dict]:
        """Get metrics for a specific date."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT snapshot_json FROM daily_metrics WHERE date = ?",
            (date,)
        )
        row = cursor.fetchone()
        conn.close()

        if row:
            return json.loads(row["snapshot_json"])
        return None

    def get_previous_metrics(self, days_ago: int = 1) -> Optional[Dict]:
        """Get metrics from N days ago."""
        target_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT snapshot_json FROM daily_metrics
            WHERE date <= ?
            ORDER BY date DESC
            LIMIT 1
        """, (target_date,))

        row = cursor.fetchone()
        conn.close()

        if row:
            return json.loads(row["snapshot_json"])
        return None

    def get_metrics_range(self, start_date: str, end_date: str = None) -> List[Dict]:
        """Get metrics for a date range."""
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")

        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT snapshot_json FROM daily_metrics
            WHERE date >= ? AND date <= ?
            ORDER BY date ASC
        """, (start_date, end_date))

        rows = cursor.fetchall()
        conn.close()

        return [json.loads(row["snapshot_json"]) for row in rows]

    def get_time_series(self, metric_path: str, days: int = 30) -> List[tuple]:
        """
        Get time series for a specific metric.

        metric_path examples: "new_cohort_win_rate", "toxic_remaining", etc.
        """
        # Validate metric_path against whitelist to prevent SQL injection
        if metric_path not in self.VALID_METRICS:
            raise ValueError(
                f"Invalid metric '{metric_path}'. "
                f"Must be one of: {', '.join(sorted(self.VALID_METRICS))}"
            )

        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        conn = self._get_conn()
        cursor = conn.cursor()

        # Safe to interpolate metric_path since it's validated against whitelist
        cursor.execute(f"""
            SELECT date, {metric_path} FROM daily_metrics
            WHERE date >= ?
            ORDER BY date ASC
        """, (start_date,))

        rows = cursor.fetchall()
        conn.close()

        return [(row["date"], row[metric_path]) for row in rows]

    def get_initial_toxic_count(self) -> int:
        """Get toxic count from earliest record (for clearance tracking)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT toxic_remaining FROM daily_metrics
            ORDER BY date ASC
            LIMIT 1
        """)

        row = cursor.fetchone()
        conn.close()

        return row["toxic_remaining"] if row else 0

    def get_wow_comparison(self) -> Optional[Dict]:
        """Get week-over-week comparison data."""
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        current = self.get_daily_metrics(today)
        previous = self.get_previous_metrics(days_ago=7)

        if not current or not previous:
            return None

        return {"current": current, "previous": previous}

    # =========================================================================
    # PROPERTY SNAPSHOT METHODS
    # =========================================================================

    def upsert_property_snapshot(self, snapshot: Dict) -> Tuple[str, bool, Optional[Dict]]:
        """
        Upsert a property snapshot.

        Args:
            snapshot: Dict with property data (address, city, state, market,
                     list_price, status, beds, baths, sqft, opendoor_url)

        Returns:
            Tuple of (property_id, is_new_property, transition_info)
            transition_info is None if no status change, else dict with transition details
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        # Generate property ID
        property_id = generate_property_id(
            snapshot.get('address', ''),
            snapshot.get('city', ''),
            snapshot.get('state', ''),
            snapshot.get('zip_code', '')
        )

        today = datetime.now().strftime('%Y-%m-%d')
        now = datetime.now().isoformat()

        # Check for existing snapshot today (update case)
        cursor.execute("""
            SELECT id, status, list_price FROM property_daily_snapshot
            WHERE property_id = ? AND snapshot_date = ?
        """, (property_id, today))
        existing_today = cursor.fetchone()

        # Get most recent previous snapshot (for transition detection)
        cursor.execute("""
            SELECT snapshot_date, status, list_price, first_seen_date, price_cuts_count
            FROM property_daily_snapshot
            WHERE property_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        """, (property_id,))
        previous = cursor.fetchone()

        is_new_property = previous is None
        transition_info = None

        # Determine first_seen_date
        if previous:
            first_seen = previous['first_seen_date']
            days_on_market = (datetime.strptime(today, '%Y-%m-%d') -
                            datetime.strptime(first_seen, '%Y-%m-%d')).days
            prev_price = previous['list_price']
            prev_status = previous['status']
            price_cuts = previous['price_cuts_count'] or 0
        else:
            first_seen = today
            days_on_market = 0
            prev_price = None
            prev_status = None
            price_cuts = 0

        current_price = snapshot.get('list_price') or snapshot.get('price')
        current_status = snapshot.get('status', 'FOR_SALE')

        # Track price changes
        price_change = None
        if prev_price and current_price and prev_price != current_price:
            price_change = current_price - prev_price
            if price_change < 0:
                price_cuts += 1

        # Detect status transition
        if prev_status and prev_status != current_status:
            # Calculate days in previous status
            if previous:
                prev_date = datetime.strptime(previous['snapshot_date'], '%Y-%m-%d')
                days_in_status = (datetime.strptime(today, '%Y-%m-%d') - prev_date).days
            else:
                days_in_status = 0

            transition_info = {
                'property_id': property_id,
                'from_status': prev_status,
                'to_status': current_status,
                'days_in_previous_status': days_in_status,
                'list_price': current_price,
                'market': snapshot.get('market'),
            }

            # Record transition
            cursor.execute("""
                INSERT INTO status_transitions
                (property_id, transition_date, from_status, to_status,
                 days_in_previous_status, list_price_at_transition, market)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                property_id, today, prev_status, current_status,
                days_in_status, current_price, snapshot.get('market')
            ))

        # Normalize address for storage
        address_normalized = normalize_address(
            snapshot.get('address', ''),
            snapshot.get('city', ''),
            snapshot.get('state', '')
        )

        # Upsert the snapshot
        cursor.execute("""
            INSERT INTO property_daily_snapshot
            (property_id, snapshot_date, address_normalized, city, state, market,
             list_price, status, beds, baths, sqft, opendoor_url,
             first_seen_date, days_on_market, previous_price, price_change,
             price_cuts_count, scrape_timestamp, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(property_id, snapshot_date) DO UPDATE SET
                list_price = excluded.list_price,
                status = excluded.status,
                days_on_market = excluded.days_on_market,
                previous_price = excluded.previous_price,
                price_change = excluded.price_change,
                price_cuts_count = excluded.price_cuts_count,
                scrape_timestamp = excluded.scrape_timestamp
        """, (
            property_id, today, address_normalized,
            snapshot.get('city'), snapshot.get('state'), snapshot.get('market'),
            current_price, current_status,
            snapshot.get('beds'), snapshot.get('baths'), snapshot.get('sqft'),
            snapshot.get('opendoor_url') or snapshot.get('url'),
            first_seen, days_on_market,
            prev_price, price_change, price_cuts,
            now, 'opendoor_scrape'
        ))

        conn.commit()
        conn.close()

        return property_id, is_new_property, transition_info

    def get_property_history(self, property_id: str, days: int = 90) -> List[Dict]:
        """Get snapshot history for a property."""
        conn = self._get_conn()
        cursor = conn.cursor()

        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        cursor.execute("""
            SELECT * FROM property_daily_snapshot
            WHERE property_id = ? AND snapshot_date >= ?
            ORDER BY snapshot_date ASC
        """, (property_id, start_date))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_active_inventory(self, snapshot_date: str = None) -> List[Dict]:
        """Get active (FOR_SALE) inventory for a date."""
        snapshot_date = snapshot_date or datetime.now().strftime('%Y-%m-%d')

        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM property_daily_snapshot
            WHERE snapshot_date = ? AND status = 'FOR_SALE'
            ORDER BY market, list_price DESC
        """, (snapshot_date,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_status_transitions(self, from_status: str = None, to_status: str = None,
                               days: int = 90, market: str = None) -> List[Dict]:
        """Get status transitions with optional filters."""
        conn = self._get_conn()
        cursor = conn.cursor()

        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        query = """
            SELECT * FROM status_transitions
            WHERE transition_date >= ?
        """
        params = [start_date]

        if from_status:
            query += " AND from_status = ?"
            params.append(from_status)

        if to_status:
            query += " AND to_status = ?"
            params.append(to_status)

        if market:
            query += " AND market = ?"
            params.append(market)

        query += " ORDER BY transition_date DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_days_to_pending_stats(self, days: int = 90, market: str = None) -> Dict:
        """
        Calculate days-to-pending statistics from actual transitions.

        Returns median, mean, min, max, and distribution.
        """
        transitions = self.get_status_transitions(
            from_status='FOR_SALE',
            to_status='PENDING',
            days=days,
            market=market
        )

        if not transitions:
            return {
                'count': 0,
                'median': None,
                'mean': None,
                'min': None,
                'max': None,
                'distribution': {}
            }

        days_list = [t['days_in_previous_status'] for t in transitions
                    if t['days_in_previous_status'] is not None]

        if not days_list:
            return {
                'count': 0,
                'median': None,
                'mean': None,
                'min': None,
                'max': None,
                'distribution': {}
            }

        days_list.sort()
        n = len(days_list)

        # Calculate median
        if n % 2 == 0:
            median = (days_list[n//2 - 1] + days_list[n//2]) / 2
        else:
            median = days_list[n//2]

        # Calculate distribution buckets
        distribution = {
            'fast_under_30d': len([d for d in days_list if d < 30]),
            'normal_30_90d': len([d for d in days_list if 30 <= d < 90]),
            'slow_90_180d': len([d for d in days_list if 90 <= d < 180]),
            'stale_over_180d': len([d for d in days_list if d >= 180]),
        }

        return {
            'count': n,
            'median': round(median, 1),
            'mean': round(sum(days_list) / n, 1),
            'min': min(days_list),
            'max': max(days_list),
            'distribution': distribution,
            'sample_transitions': transitions[:10]  # First 10 for inspection
        }

    def get_inventory_snapshot_stats(self, snapshot_date: str = None) -> Dict:
        """Get comprehensive inventory stats from snapshots."""
        snapshot_date = snapshot_date or datetime.now().strftime('%Y-%m-%d')

        conn = self._get_conn()
        cursor = conn.cursor()

        # Get all snapshots for this date
        cursor.execute("""
            SELECT market, status, list_price, days_on_market, price_cuts_count
            FROM property_daily_snapshot
            WHERE snapshot_date = ?
        """, (snapshot_date,))

        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return {'total': 0, 'by_status': {}, 'by_market': {}}

        # Calculate stats
        total = len(rows)
        by_status = {}
        by_market = {}
        prices = []
        dom_values = []
        price_cuts_total = 0

        for row in rows:
            # Status breakdown
            status = row['status'] or 'UNKNOWN'
            by_status[status] = by_status.get(status, 0) + 1

            # Market breakdown (only FOR_SALE)
            if status == 'FOR_SALE':
                market = row['market'] or 'unknown'
                by_market[market] = by_market.get(market, 0) + 1

                if row['list_price']:
                    prices.append(row['list_price'])
                if row['days_on_market'] is not None:
                    dom_values.append(row['days_on_market'])
                if row['price_cuts_count']:
                    price_cuts_total += row['price_cuts_count']

        active_count = by_status.get('FOR_SALE', 0)

        return {
            'snapshot_date': snapshot_date,
            'total_tracked': total,
            'active_count': active_count,
            'by_status': by_status,
            'by_market': by_market,
            'avg_price': round(sum(prices) / len(prices), 0) if prices else 0,
            'total_value': round(sum(prices), 0) if prices else 0,
            'avg_dom': round(sum(dom_values) / len(dom_values), 1) if dom_values else 0,
            'median_dom': sorted(dom_values)[len(dom_values)//2] if dom_values else 0,
            'total_price_cuts': price_cuts_total,
            'pct_with_cuts': round(
                len([r for r in rows if r['price_cuts_count'] and r['price_cuts_count'] > 0]) / total * 100, 1
            ) if total > 0 else 0,
        }

    def get_survival_curve_data(self, market: str = None, days: int = 180) -> Dict:
        """
        Calculate survival curve data (hazard rates) by market.

        Returns data for Kaplan-Meier style survival analysis.
        """
        # Get all FOR_SALE -> PENDING/SOLD transitions
        pending_transitions = self.get_status_transitions(
            from_status='FOR_SALE', to_status='PENDING', days=days, market=market
        )
        sold_transitions = self.get_status_transitions(
            from_status='FOR_SALE', to_status='SOLD', days=days, market=market
        )

        # Combine as "exits"
        all_exits = pending_transitions + sold_transitions
        exit_times = [t['days_in_previous_status'] for t in all_exits
                     if t['days_in_previous_status'] is not None]

        if not exit_times:
            return {'market': market, 'exits': 0, 'survival_rates': []}

        # Group by time buckets (weekly)
        max_time = max(exit_times) if exit_times else 0
        buckets = list(range(0, max(max_time + 7, 91), 7))  # Weekly buckets up to max or 90 days

        survival_rates = []
        remaining = len(exit_times)
        initial = remaining

        for i, bucket_start in enumerate(buckets[:-1]):
            bucket_end = buckets[i + 1]
            exits_in_bucket = len([t for t in exit_times if bucket_start <= t < bucket_end])
            hazard_rate = exits_in_bucket / remaining if remaining > 0 else 0
            survival_rate = remaining / initial if initial > 0 else 0

            survival_rates.append({
                'day': bucket_start,
                'day_end': bucket_end,
                'exits': exits_in_bucket,
                'remaining': remaining,
                'hazard_rate': round(hazard_rate, 4),
                'survival_rate': round(survival_rate, 4),
            })

            remaining -= exits_in_bucket

        return {
            'market': market or 'all',
            'total_exits': len(exit_times),
            'median_days_to_exit': sorted(exit_times)[len(exit_times)//2] if exit_times else None,
            'survival_rates': survival_rates,
        }
