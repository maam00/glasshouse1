"""
Glass House Alert Monitor
==========================
Monitors metrics and generates alerts for significant changes.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class Alert:
    """Single alert."""
    level: str  # "warning", "critical"
    metric: str
    message: str
    old_value: float
    new_value: float
    change_pct: float


class AlertMonitor:
    """Monitor metrics and generate alerts."""

    # Thresholds
    THRESHOLDS = {
        "new_cohort_win_rate_min": 95.0,  # Critical if below
        "contribution_margin_min": 5.0,    # Warning if below
        "contribution_margin_max": 7.0,    # Target range
        "wow_change_warning": 10.0,        # % change to trigger alert
        "toxic_increase_any": True,        # Alert on any increase
    }

    def __init__(self, current: Dict, previous: Optional[Dict] = None):
        self.current = current
        self.previous = previous

    def check_all(self) -> List[Alert]:
        """Run all alert checks."""
        alerts = []

        alerts.extend(self._check_new_cohort())
        alerts.extend(self._check_contribution_margin())
        alerts.extend(self._check_toxic_trend())
        alerts.extend(self._check_wow_changes())

        return alerts

    def _check_new_cohort(self) -> List[Alert]:
        """Check new cohort win rate."""
        alerts = []

        win_rate = self.current.get("cohort_new", {}).get("win_rate", 0)
        threshold = self.THRESHOLDS["new_cohort_win_rate_min"]

        if win_rate > 0 and win_rate < threshold:
            alerts.append(Alert(
                level="critical",
                metric="new_cohort_win_rate",
                message=f"New cohort win rate {win_rate:.1f}% below {threshold}% threshold",
                old_value=threshold,
                new_value=win_rate,
                change_pct=((win_rate - threshold) / threshold) * 100,
            ))

        return alerts

    def _check_contribution_margin(self) -> List[Alert]:
        """Check contribution margin."""
        alerts = []

        margin = self.current.get("performance", {}).get("contribution_margin", 0)
        min_target = self.THRESHOLDS["contribution_margin_min"]

        if margin > 0 and margin < min_target:
            alerts.append(Alert(
                level="warning",
                metric="contribution_margin",
                message=f"Contribution margin {margin:.2f}% below {min_target}% target",
                old_value=min_target,
                new_value=margin,
                change_pct=((margin - min_target) / min_target) * 100,
            ))

        return alerts

    def _check_toxic_trend(self) -> List[Alert]:
        """Check if toxic inventory is increasing."""
        alerts = []

        if not self.previous:
            return alerts

        curr_toxic = self.current.get("toxic", {}).get("remaining_count", 0)
        prev_toxic = self.previous.get("toxic", {}).get("remaining_count", 0)

        if curr_toxic > prev_toxic:
            alerts.append(Alert(
                level="warning",
                metric="toxic_remaining",
                message=f"Toxic inventory increased: {prev_toxic} -> {curr_toxic}",
                old_value=prev_toxic,
                new_value=curr_toxic,
                change_pct=((curr_toxic - prev_toxic) / prev_toxic * 100) if prev_toxic > 0 else 100,
            ))

        return alerts

    def _check_wow_changes(self) -> List[Alert]:
        """Check for significant week-over-week changes."""
        alerts = []

        if not self.previous:
            return alerts

        threshold = self.THRESHOLDS["wow_change_warning"]

        metrics_to_check = [
            ("performance", "win_rate", "Overall win rate"),
            ("performance", "contribution_margin", "Contribution margin"),
            ("inventory", "total", "Total listings"),
        ]

        for section, key, name in metrics_to_check:
            curr_val = self.current.get(section, {}).get(key, 0)
            prev_val = self.previous.get(section, {}).get(key, 0)

            if prev_val > 0:
                change_pct = ((curr_val - prev_val) / prev_val) * 100

                if abs(change_pct) > threshold:
                    direction = "increased" if change_pct > 0 else "decreased"
                    alerts.append(Alert(
                        level="warning",
                        metric=f"{section}.{key}",
                        message=f"{name} {direction} {abs(change_pct):.1f}% WoW",
                        old_value=prev_val,
                        new_value=curr_val,
                        change_pct=change_pct,
                    ))

        return alerts

    def format_alerts(self, alerts: List[Alert]) -> List[str]:
        """Format alerts as strings for display."""
        return [
            f"[{a.level.upper()}] {a.message}"
            for a in alerts
        ]
