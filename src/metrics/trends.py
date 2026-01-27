"""
Glass House Trends Analysis Module
==================================
Analyzes historical data to calculate WoW deltas and prepare chart data
for the dashboard.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class TrendAnalyzer:
    """
    Analyzes historical metrics data to calculate trends and prepare chart data.

    Expected history entry structure:
    {
        "date": "2026-01-27",
        "kaz_era": {
            "realized": {"count": 43, "win_rate": 95.3, "avg_profit": 43237},
            "unrealized": {"count": 103, "above_water": 100, "underwater": 3}
        },
        "toxic": {"remaining_count": 84, "weeks_to_clear": 8.4},
        "inventory": {"total": 764, "toxic_count": 84},
        "performance": {"revenue_today": 0, "homes_sold_today": 0, "win_rate": 68.7}
    }
    """

    # Target revenue per day (used for chart annotations)
    DAILY_REVENUE_TARGET = 6_500_000  # $6.5M daily target

    def __init__(self, daily_revenue_target: float = None):
        """
        Initialize the TrendAnalyzer.

        Args:
            daily_revenue_target: Optional daily revenue target for chart data
        """
        if daily_revenue_target is not None:
            self.DAILY_REVENUE_TARGET = daily_revenue_target

    def _get_entry_value(self, entry: Dict, *keys, default=0) -> Any:
        """Safely navigate nested dictionary keys."""
        current = entry
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key, default)
            else:
                return default
        return current if current is not None else default

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse a date string into a datetime object."""
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def _get_entries_by_date_range(
        self,
        history: List[Dict],
        days: int = None,
        start_date: str = None,
        end_date: str = None
    ) -> List[Dict]:
        """
        Filter history entries by date range.

        Args:
            history: List of history entries
            days: Number of days from the most recent entry (if specified)
            start_date: Start date string (YYYY-MM-DD)
            end_date: End date string (YYYY-MM-DD)

        Returns:
            Filtered and sorted list of history entries
        """
        if not history:
            return []

        # Sort by date
        sorted_history = sorted(
            history,
            key=lambda x: x.get("date", ""),
            reverse=False
        )

        if days is not None and days > 0:
            # Get last N days from most recent entry
            if sorted_history:
                latest_date = self._parse_date(sorted_history[-1].get("date", ""))
                if latest_date:
                    cutoff = latest_date - timedelta(days=days - 1)
                    sorted_history = [
                        e for e in sorted_history
                        if self._parse_date(e.get("date", "")) and
                           self._parse_date(e.get("date", "")) >= cutoff
                    ]

        if start_date:
            sorted_history = [
                e for e in sorted_history
                if e.get("date", "") >= start_date
            ]

        if end_date:
            sorted_history = [
                e for e in sorted_history
                if e.get("date", "") <= end_date
            ]

        return sorted_history

    def _calculate_delta(
        self,
        current: float,
        previous: float,
        as_percentage: bool = False
    ) -> Dict[str, Any]:
        """
        Calculate the delta between two values.

        Returns:
            Dict with 'value', 'absolute', 'percentage', and 'direction'
        """
        absolute = current - previous

        if previous != 0:
            percentage = ((current - previous) / abs(previous)) * 100
        else:
            percentage = 100.0 if current > 0 else 0.0

        if absolute > 0:
            direction = "up"
        elif absolute < 0:
            direction = "down"
        else:
            direction = "flat"

        return {
            "current": current,
            "previous": previous,
            "absolute": round(absolute, 2),
            "percentage": round(percentage, 2),
            "direction": direction
        }

    def calculate_wow_deltas(self, history: List[Dict]) -> Dict[str, Any]:
        """
        Calculate week-over-week deltas for key metrics.

        Args:
            history: List of historical data entries

        Returns:
            Dict with WoW changes for key metrics including:
            - win_rate: Overall win rate delta
            - kaz_win_rate: Kaz-era win rate delta
            - toxic_remaining: Toxic inventory count delta
            - underwater_count: Underwater homes delta
            - revenue: Total revenue delta
            - homes_sold: Homes sold delta
        """
        if not history or len(history) < 2:
            return {
                "win_rate": self._calculate_delta(0, 0),
                "kaz_win_rate": self._calculate_delta(0, 0),
                "toxic_remaining": self._calculate_delta(0, 0),
                "underwater_count": self._calculate_delta(0, 0),
                "revenue": self._calculate_delta(0, 0),
                "homes_sold": self._calculate_delta(0, 0),
                "has_data": False
            }

        # Sort by date to find current and week-ago entries
        sorted_history = sorted(
            history,
            key=lambda x: x.get("date", ""),
            reverse=True
        )

        current = sorted_history[0]
        current_date = self._parse_date(current.get("date", ""))

        # Find entry from ~7 days ago
        week_ago = None
        for entry in sorted_history[1:]:
            entry_date = self._parse_date(entry.get("date", ""))
            if entry_date and current_date:
                days_diff = (current_date - entry_date).days
                if days_diff >= 7:
                    week_ago = entry
                    break

        # If no week-ago entry, use the oldest available
        if week_ago is None and len(sorted_history) > 1:
            week_ago = sorted_history[-1]

        if week_ago is None:
            week_ago = current  # Fall back to comparing with self

        # Extract metrics - handle both legacy and new format
        # Current values
        curr_win_rate = self._get_entry_value(current, "performance", "win_rate", default=0)
        curr_kaz_win_rate = self._get_entry_value(current, "kaz_era", "realized", "win_rate", default=0)
        curr_toxic = self._get_entry_value(current, "toxic", "remaining_count", default=0)

        # Underwater count: prefer kaz_era unrealized, fall back to risk
        curr_underwater = self._get_entry_value(current, "kaz_era", "unrealized", "underwater", default=0)
        if curr_underwater == 0:
            curr_underwater = self._get_entry_value(current, "risk", "underwater_count", default=0)

        curr_revenue = self._get_entry_value(current, "performance", "revenue_total", default=0)
        curr_homes_sold = self._get_entry_value(current, "performance", "homes_sold_total", default=0)

        # Previous values
        prev_win_rate = self._get_entry_value(week_ago, "performance", "win_rate", default=0)
        prev_kaz_win_rate = self._get_entry_value(week_ago, "kaz_era", "realized", "win_rate", default=0)
        prev_toxic = self._get_entry_value(week_ago, "toxic", "remaining_count", default=0)

        prev_underwater = self._get_entry_value(week_ago, "kaz_era", "unrealized", "underwater", default=0)
        if prev_underwater == 0:
            prev_underwater = self._get_entry_value(week_ago, "risk", "underwater_count", default=0)

        prev_revenue = self._get_entry_value(week_ago, "performance", "revenue_total", default=0)
        prev_homes_sold = self._get_entry_value(week_ago, "performance", "homes_sold_total", default=0)

        return {
            "win_rate": self._calculate_delta(curr_win_rate, prev_win_rate),
            "kaz_win_rate": self._calculate_delta(curr_kaz_win_rate, prev_kaz_win_rate),
            "toxic_remaining": self._calculate_delta(curr_toxic, prev_toxic),
            "underwater_count": self._calculate_delta(curr_underwater, prev_underwater),
            "revenue": self._calculate_delta(curr_revenue, prev_revenue),
            "homes_sold": self._calculate_delta(curr_homes_sold, prev_homes_sold),
            "comparison_date": week_ago.get("date", ""),
            "current_date": current.get("date", ""),
            "has_data": True
        }

    def prepare_revenue_chart(
        self,
        history: List[Dict],
        days: int = 14
    ) -> List[Dict[str, Any]]:
        """
        Prepare bar chart data for daily revenue.

        Args:
            history: List of historical data entries
            days: Number of days to include (default 14)

        Returns:
            List of dicts with: {date, revenue, above_target}
        """
        filtered = self._get_entries_by_date_range(history, days=days)

        chart_data = []
        for entry in filtered:
            date = entry.get("date", "")

            # Get revenue - try revenue_today first, fall back to calculating daily
            revenue = self._get_entry_value(entry, "performance", "revenue_today", default=0)

            # If revenue_today is 0, try to estimate from total revenue change
            if revenue == 0:
                revenue = self._get_entry_value(entry, "performance", "revenue_total", default=0)
                # Note: This gives cumulative, not daily - caller should be aware

            chart_data.append({
                "date": date,
                "revenue": revenue,
                "above_target": revenue >= self.DAILY_REVENUE_TARGET
            })

        return chart_data

    def prepare_toxic_countdown(
        self,
        history: List[Dict]
    ) -> Dict[str, Any]:
        """
        Prepare area chart data for toxic inventory countdown with projection.

        Args:
            history: List of historical data entries

        Returns:
            Dict with:
            - actual: List of {date, count} for actual data
            - projected: List of {date, count} for projection
            - clear_date: Estimated date when toxic inventory reaches 0
        """
        if not history:
            return {
                "actual": [],
                "projected": [],
                "clear_date": None
            }

        sorted_history = self._get_entries_by_date_range(history)

        # Build actual data
        actual_data = []
        for entry in sorted_history:
            date = entry.get("date", "")
            count = self._get_entry_value(entry, "toxic", "remaining_count", default=0)
            actual_data.append({
                "date": date,
                "count": count
            })

        if not actual_data:
            return {
                "actual": [],
                "projected": [],
                "clear_date": None
            }

        # Calculate clearance rate for projection
        latest_entry = sorted_history[-1] if sorted_history else {}
        current_count = self._get_entry_value(latest_entry, "toxic", "remaining_count", default=0)
        weeks_to_clear = self._get_entry_value(latest_entry, "toxic", "weeks_to_clear", default=0)

        # Generate projection data
        projected_data = []
        clear_date = None

        if current_count > 0 and weeks_to_clear > 0:
            # Calculate daily clearance rate
            daily_rate = current_count / (weeks_to_clear * 7)

            latest_date = self._parse_date(actual_data[-1]["date"])
            if latest_date and daily_rate > 0:
                # Project forward until count reaches 0
                remaining = current_count
                projection_days = 0

                while remaining > 0 and projection_days < 365:  # Cap at 1 year
                    projection_days += 1
                    remaining = max(0, current_count - (daily_rate * projection_days))
                    proj_date = latest_date + timedelta(days=projection_days)

                    # Add weekly projection points
                    if projection_days % 7 == 0 or remaining == 0:
                        projected_data.append({
                            "date": proj_date.strftime("%Y-%m-%d"),
                            "count": round(remaining)
                        })

                # Set clear date
                if remaining == 0:
                    clear_date = proj_date.strftime("%Y-%m-%d")

        return {
            "actual": actual_data,
            "projected": projected_data,
            "clear_date": clear_date,
            "current_count": current_count,
            "weeks_to_clear": weeks_to_clear
        }

    def prepare_win_rate_trend(
        self,
        history: List[Dict],
        days: int = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Prepare line chart data for Kaz vs Legacy win rate trends.

        Args:
            history: List of historical data entries
            days: Optional number of days to include

        Returns:
            Dict with:
            - kaz: List of {date, win_rate} for Kaz-era
            - legacy: List of {date, win_rate} for legacy/overall
        """
        filtered = self._get_entries_by_date_range(history, days=days)

        kaz_data = []
        legacy_data = []

        for entry in filtered:
            date = entry.get("date", "")

            # Kaz-era win rate
            kaz_win_rate = self._get_entry_value(
                entry, "kaz_era", "realized", "win_rate", default=None
            )

            # Legacy/overall win rate
            overall_win_rate = self._get_entry_value(
                entry, "performance", "win_rate", default=None
            )

            # Calculate legacy win rate if we have improvement data
            vs_legacy_improvement = self._get_entry_value(
                entry, "kaz_era", "vs_legacy_improvement", default=None
            )

            if kaz_win_rate is not None:
                kaz_data.append({
                    "date": date,
                    "win_rate": kaz_win_rate
                })

            # For legacy, use overall win rate or calculate from kaz - improvement
            if overall_win_rate is not None:
                # The overall includes both kaz and legacy, so we estimate legacy
                # If we have the improvement delta, we can back-calculate
                if kaz_win_rate is not None and vs_legacy_improvement is not None:
                    legacy_win_rate = kaz_win_rate - vs_legacy_improvement
                else:
                    legacy_win_rate = overall_win_rate

                legacy_data.append({
                    "date": date,
                    "win_rate": round(legacy_win_rate, 1)
                })

        return {
            "kaz": kaz_data,
            "legacy": legacy_data
        }

    def prepare_underwater_trend(
        self,
        history: List[Dict],
        days: int = None
    ) -> List[Dict[str, Any]]:
        """
        Prepare chart data for underwater exposure trend.

        Args:
            history: List of historical data entries
            days: Optional number of days to include

        Returns:
            List of dicts with: {date, kaz_exposure, legacy_exposure}
        """
        filtered = self._get_entries_by_date_range(history, days=days)

        chart_data = []
        for entry in filtered:
            date = entry.get("date", "")

            # Kaz-era underwater
            kaz_underwater = self._get_entry_value(
                entry, "kaz_era", "unrealized", "underwater", default=0
            )

            # Total underwater (from risk section if available)
            total_underwater = self._get_entry_value(
                entry, "risk", "underwater_count", default=0
            )

            # If we have risk data, legacy = total - kaz
            # If not, we only have kaz data
            if total_underwater > 0:
                legacy_underwater = total_underwater - kaz_underwater
            else:
                # Fallback: estimate from inventory data
                legacy_underwater = 0

            chart_data.append({
                "date": date,
                "kaz_exposure": kaz_underwater,
                "legacy_exposure": max(0, legacy_underwater),
                "total_exposure": total_underwater if total_underwater > 0 else kaz_underwater
            })

        return chart_data

    def generate_all_trends(
        self,
        history: List[Dict],
        chart_days: int = 14
    ) -> Dict[str, Any]:
        """
        Generate all trend data needed for the dashboard.

        Args:
            history: List of historical data entries
            chart_days: Number of days for chart data (default 14)

        Returns:
            Dict containing all trend data:
            - wow_deltas: Week-over-week changes
            - revenue_chart: Daily revenue bar chart data
            - toxic_countdown: Toxic inventory area chart with projection
            - win_rate_trend: Kaz vs Legacy win rate lines
            - underwater_trend: Underwater exposure by era
            - summary: High-level summary stats
        """
        wow_deltas = self.calculate_wow_deltas(history)
        revenue_chart = self.prepare_revenue_chart(history, days=chart_days)
        toxic_countdown = self.prepare_toxic_countdown(history)
        win_rate_trend = self.prepare_win_rate_trend(history)
        underwater_trend = self.prepare_underwater_trend(history)

        # Generate summary stats
        summary = self._generate_summary(
            history,
            wow_deltas,
            toxic_countdown
        )

        return {
            "wow_deltas": wow_deltas,
            "revenue_chart": revenue_chart,
            "toxic_countdown": toxic_countdown,
            "win_rate_trend": win_rate_trend,
            "underwater_trend": underwater_trend,
            "summary": summary,
            "generated_at": datetime.now().isoformat(),
            "chart_days": chart_days,
            "total_history_days": len(history)
        }

    def _generate_summary(
        self,
        history: List[Dict],
        wow_deltas: Dict[str, Any],
        toxic_countdown: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate high-level summary statistics."""
        if not history:
            return {}

        # Get latest entry
        sorted_history = sorted(
            history,
            key=lambda x: x.get("date", ""),
            reverse=True
        )
        latest = sorted_history[0] if sorted_history else {}

        # Key indicators
        kaz_win_rate = self._get_entry_value(
            latest, "kaz_era", "realized", "win_rate", default=0
        )
        overall_win_rate = self._get_entry_value(
            latest, "performance", "win_rate", default=0
        )
        toxic_remaining = self._get_entry_value(
            latest, "toxic", "remaining_count", default=0
        )

        # Trend directions
        win_rate_trending = wow_deltas.get("win_rate", {}).get("direction", "flat")
        toxic_trending = wow_deltas.get("toxic_remaining", {}).get("direction", "flat")

        # Health assessment
        health_status = "healthy"
        if kaz_win_rate < 90:
            health_status = "warning"
        if kaz_win_rate < 80 or overall_win_rate < 60:
            health_status = "critical"

        return {
            "current_date": latest.get("date", ""),
            "kaz_win_rate": kaz_win_rate,
            "overall_win_rate": overall_win_rate,
            "toxic_remaining": toxic_remaining,
            "toxic_clear_date": toxic_countdown.get("clear_date"),
            "win_rate_trending": win_rate_trending,
            "toxic_trending": toxic_trending,
            "health_status": health_status,
            "days_of_history": len(history)
        }


def load_history_from_dashboard_data(dashboard_data: Dict) -> List[Dict]:
    """
    Extract history array from dashboard_data.json structure.

    Args:
        dashboard_data: The full dashboard_data.json content

    Returns:
        List of history entries
    """
    return dashboard_data.get("history", [])


def analyze_dashboard_trends(dashboard_data: Dict, chart_days: int = 14) -> Dict[str, Any]:
    """
    Convenience function to analyze trends from dashboard_data.json.

    Args:
        dashboard_data: The full dashboard_data.json content
        chart_days: Number of days for chart data

    Returns:
        Complete trend analysis results
    """
    history = load_history_from_dashboard_data(dashboard_data)
    analyzer = TrendAnalyzer()
    return analyzer.generate_all_trends(history, chart_days=chart_days)
