"""
Glass House Database Layer
===========================
SQLite storage for daily metrics and historical tracking.
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


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
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        conn = self._get_conn()
        cursor = conn.cursor()

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
