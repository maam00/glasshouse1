"""
Glass House Historical Charts
==============================
Generate chart data and ASCII visualizations from historical data.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from ..db.database import Database


@dataclass
class ChartPoint:
    date: str
    value: float
    label: str = ""


class HistoricalCharts:
    """Generate historical charts and trend analysis."""

    def __init__(self, db: Database = None):
        self.db = db or Database()

    def get_time_series(self, days: int = 30) -> Dict[str, List[ChartPoint]]:
        """Get time series data for key metrics."""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        metrics = self.db.get_metrics_range(start_date, end_date)

        if not metrics:
            return {}

        series = {
            "win_rate": [],
            "new_cohort_win_rate": [],
            "contribution_margin": [],
            "toxic_remaining": [],
            "total_listings": [],
            "homes_sold": [],
            "revenue": [],
        }

        for m in metrics:
            date = m.get("date", "")

            # Overall metrics
            perf = m.get("performance", {})
            series["win_rate"].append(ChartPoint(
                date=date,
                value=perf.get("win_rate", 0),
            ))
            series["contribution_margin"].append(ChartPoint(
                date=date,
                value=perf.get("contribution_margin", 0),
            ))
            series["homes_sold"].append(ChartPoint(
                date=date,
                value=perf.get("homes_sold_total", 0),
            ))
            series["revenue"].append(ChartPoint(
                date=date,
                value=perf.get("revenue_total", 0),
            ))

            # Cohort metrics
            cohort_new = m.get("cohort_new", {})
            series["new_cohort_win_rate"].append(ChartPoint(
                date=date,
                value=cohort_new.get("win_rate", 0),
            ))

            # Toxic
            toxic = m.get("toxic", {})
            series["toxic_remaining"].append(ChartPoint(
                date=date,
                value=toxic.get("remaining_count", 0),
            ))

            # Inventory
            inv = m.get("inventory", {})
            series["total_listings"].append(ChartPoint(
                date=date,
                value=inv.get("total", 0),
            ))

        return series

    def ascii_spark(self, values: List[float], width: int = 40) -> str:
        """Generate ASCII sparkline."""
        if not values:
            return ""

        min_val = min(values)
        max_val = max(values)
        range_val = max_val - min_val or 1

        chars = "▁▂▃▄▅▆▇█"

        spark = ""
        step = max(1, len(values) // width)

        for i in range(0, len(values), step):
            v = values[i]
            normalized = (v - min_val) / range_val
            idx = int(normalized * (len(chars) - 1))
            spark += chars[idx]

        return spark[:width]

    def ascii_chart(
        self,
        values: List[float],
        labels: List[str] = None,
        width: int = 50,
        height: int = 10,
        title: str = "",
    ) -> str:
        """Generate ASCII line chart."""
        if not values:
            return "No data"

        min_val = min(values)
        max_val = max(values)
        range_val = max_val - min_val or 1

        # Normalize values to chart height
        normalized = [
            int((v - min_val) / range_val * (height - 1))
            for v in values
        ]

        # Sample to fit width
        step = max(1, len(normalized) // width)
        sampled = [normalized[i] for i in range(0, len(normalized), step)][:width]

        # Build chart
        lines = []

        if title:
            lines.append(f"  {title}")
            lines.append("")

        for row in range(height - 1, -1, -1):
            if row == height - 1:
                label = f"{max_val:>8.1f} │"
            elif row == 0:
                label = f"{min_val:>8.1f} │"
            else:
                label = "         │"

            line = ""
            for col, val in enumerate(sampled):
                if val == row:
                    line += "●"
                elif val > row:
                    line += "│" if col > 0 and sampled[col-1] >= row else " "
                else:
                    line += " "

            lines.append(label + line)

        # X-axis
        lines.append("         └" + "─" * len(sampled))

        # Date labels
        if labels and len(labels) >= 2:
            first = labels[0][:10] if labels else ""
            last = labels[-1][:10] if labels else ""
            padding = len(sampled) - len(first) - len(last)
            lines.append(f"          {first}" + " " * max(0, padding) + last)

        return "\n".join(lines)

    def trend_indicator(self, values: List[float], periods: int = 7) -> str:
        """Calculate trend direction."""
        if len(values) < periods:
            return "→"

        recent = values[-periods:]
        earlier = values[-periods*2:-periods] if len(values) >= periods*2 else values[:periods]

        recent_avg = sum(recent) / len(recent)
        earlier_avg = sum(earlier) / len(earlier) if earlier else recent_avg

        if recent_avg > earlier_avg * 1.05:
            return "↑"
        elif recent_avg < earlier_avg * 0.95:
            return "↓"
        else:
            return "→"

    def generate_dashboard_charts(self, days: int = 30) -> str:
        """Generate ASCII charts for dashboard."""
        series = self.get_time_series(days)

        if not series or not series.get("win_rate"):
            return "\n  No historical data yet. Run daily to accumulate.\n"

        output = []
        output.append("")
        output.append("=" * 70)
        output.append("  HISTORICAL TRENDS")
        output.append("=" * 70)

        # Win Rate Chart
        win_values = [p.value for p in series["win_rate"]]
        win_dates = [p.date for p in series["win_rate"]]
        if win_values:
            trend = self.trend_indicator(win_values)
            output.append(f"\n  Win Rate {trend}  (last {days} days)")
            output.append(f"  {self.ascii_spark(win_values, 50)}")
            output.append(f"  Range: {min(win_values):.1f}% - {max(win_values):.1f}%")

        # New Cohort Win Rate
        new_values = [p.value for p in series["new_cohort_win_rate"]]
        if new_values:
            trend = self.trend_indicator(new_values)
            output.append(f"\n  New Cohort Win Rate {trend}")
            output.append(f"  {self.ascii_spark(new_values, 50)}")
            output.append(f"  Range: {min(new_values):.1f}% - {max(new_values):.1f}%")

        # Contribution Margin
        margin_values = [p.value for p in series["contribution_margin"]]
        if margin_values:
            trend = self.trend_indicator(margin_values)
            output.append(f"\n  Contribution Margin {trend}")
            output.append(f"  {self.ascii_spark(margin_values, 50)}")
            output.append(f"  Range: {min(margin_values):.1f}% - {max(margin_values):.1f}%")

        # Toxic Remaining
        toxic_values = [p.value for p in series["toxic_remaining"]]
        if toxic_values:
            trend = self.trend_indicator(toxic_values)
            direction = "good" if trend == "↓" else "watch" if trend == "↑" else "flat"
            output.append(f"\n  Toxic Remaining {trend} ({direction})")
            output.append(f"  {self.ascii_spark(toxic_values, 50)}")
            output.append(f"  Range: {int(min(toxic_values))} - {int(max(toxic_values))}")

        # Revenue Accumulation
        rev_values = [p.value / 1_000_000 for p in series["revenue"]]
        if rev_values:
            output.append(f"\n  Revenue (cumulative $M)")
            output.append(f"  {self.ascii_spark(rev_values, 50)}")
            output.append(f"  Range: ${min(rev_values):.1f}M - ${max(rev_values):.1f}M")

        output.append("")
        output.append("=" * 70)

        return "\n".join(output)

    def generate_detailed_chart(self, metric: str, days: int = 30) -> str:
        """Generate detailed ASCII chart for a specific metric."""
        series = self.get_time_series(days)

        if metric not in series or not series[metric]:
            return f"No data for {metric}"

        points = series[metric]
        values = [p.value for p in points]
        dates = [p.date for p in points]

        return self.ascii_chart(
            values=values,
            labels=dates,
            width=50,
            height=12,
            title=metric.replace("_", " ").title(),
        )

    def calculate_changes(self) -> Dict[str, Dict[str, float]]:
        """Calculate period-over-period changes."""
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        month_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        current = self.db.get_daily_metrics(today)
        prev_day = self.db.get_previous_metrics(days_ago=1)
        prev_week = self.db.get_previous_metrics(days_ago=7)
        prev_month = self.db.get_previous_metrics(days_ago=30)

        if not current:
            return {}

        def calc_change(curr_val, prev_val):
            if prev_val is None or prev_val == 0:
                return None
            return ((curr_val - prev_val) / abs(prev_val)) * 100

        metrics_to_track = [
            ("win_rate", ["performance", "win_rate"]),
            ("contribution_margin", ["performance", "contribution_margin"]),
            ("new_cohort_win_rate", ["cohort_new", "win_rate"]),
            ("toxic_remaining", ["toxic", "remaining_count"]),
            ("total_listings", ["inventory", "total"]),
        ]

        changes = {}

        for name, path in metrics_to_track:
            curr_val = current
            for key in path:
                curr_val = curr_val.get(key, {}) if isinstance(curr_val, dict) else 0
            curr_val = curr_val if not isinstance(curr_val, dict) else 0

            day_val = prev_day
            week_val = prev_week
            month_val = prev_month

            for key in path:
                day_val = day_val.get(key, {}) if isinstance(day_val, dict) else 0
                week_val = week_val.get(key, {}) if isinstance(week_val, dict) else 0
                month_val = month_val.get(key, {}) if isinstance(month_val, dict) else 0

            day_val = day_val if not isinstance(day_val, dict) else 0
            week_val = week_val if not isinstance(week_val, dict) else 0
            month_val = month_val if not isinstance(month_val, dict) else 0

            changes[name] = {
                "current": curr_val,
                "dod": calc_change(curr_val, day_val),
                "wow": calc_change(curr_val, week_val),
                "mom": calc_change(curr_val, month_val),
            }

        return changes

    def export_for_plotting(self, days: int = 90) -> Dict[str, Any]:
        """Export data in format suitable for external plotting tools."""
        series = self.get_time_series(days)
        changes = self.calculate_changes()

        return {
            "generated_at": datetime.now().isoformat(),
            "time_series": {
                name: [{"date": p.date, "value": p.value} for p in points]
                for name, points in series.items()
            },
            "changes": changes,
        }

    def save_chart_data(self, output_dir: Path = None) -> Path:
        """Save chart data to JSON."""
        output_dir = output_dir or Path(__file__).parent.parent.parent / "outputs"
        output_dir.mkdir(parents=True, exist_ok=True)

        data = self.export_for_plotting(days=90)

        output_file = output_dir / f"chart_data_{datetime.now().strftime('%Y%m%d')}.json"
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)

        return output_file


class ChartDataGenerator(HistoricalCharts):
    """Alias for backwards compatibility."""

    def x_post_format(self) -> str:
        """Generate formatted text for X post."""
        changes = self.calculate_changes()

        if not changes:
            return "No data available."

        lines = ["$OPEN Weekly Update\n"]

        win_rate = changes.get("win_rate", {})
        if win_rate:
            wow = win_rate.get("wow")
            trend = "↑" if wow and wow > 0 else "↓" if wow and wow < 0 else "→"
            lines.append(f"Win Rate: {win_rate['current']:.1f}% {trend}")

        new_cohort = changes.get("new_cohort_win_rate", {})
        if new_cohort:
            lines.append(f"New Cohort: {new_cohort['current']:.1f}%")

        margin = changes.get("contribution_margin", {})
        if margin:
            lines.append(f"Margin: {margin['current']:.1f}%")

        toxic = changes.get("toxic_remaining", {})
        if toxic:
            lines.append(f"Toxic Remaining: {int(toxic['current'])}")

        lines.append("\n#OPEN #Opendoor")

        return "\n".join(lines)
