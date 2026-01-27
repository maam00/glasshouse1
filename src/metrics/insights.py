"""
Glass House AI Insights Module
==============================
Detects anomalies, generates thesis checkpoints, and prepares alert data.

This module provides:
1. Anomaly detection (>10% WoW changes)
2. Thesis checkpoint generation (factual, no promotional language)
3. Weekly summary generation (template-based, not AI-generated)
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

# Constants
KAZ_ERA_WIN_RATE_TARGET = 90.0  # Kaz-era win rate should be >90%
KAZ_ERA_UNDERWATER_TARGET = 10.0  # Kaz-era underwater should be <10%
WOW_CHANGE_THRESHOLD = 10.0  # >10% WoW change triggers anomaly
Q1_2026_TARGET = 595_000_000  # Q1 2026 guidance target


@dataclass
class Anomaly:
    """Detected anomaly in metrics."""
    metric: str
    change_pct: float
    direction: str  # "up" or "down"
    severity: str  # "warning" or "critical"
    message: str
    current_value: float
    previous_value: float


@dataclass
class ThesisCheckpoint:
    """Factual checkpoint for thesis validation."""
    checkpoint: str
    status: str  # "pass", "warn", "fail"
    value: float
    target: float
    description: str


@dataclass
class AlertData:
    """Alert data ready for notification systems."""
    alert_type: str  # "anomaly", "checkpoint_fail", "threshold_breach"
    severity: str  # "info", "warning", "critical"
    title: str
    message: str
    metric: str
    value: float
    timestamp: str


class InsightsGenerator:
    """Generate insights, detect anomalies, and create thesis checkpoints."""

    # Metrics to check for WoW anomalies
    ANOMALY_METRICS = [
        ("win_rate", "v3.portfolio.kaz_era.sold_win_rate", "Kaz-era win rate"),
        ("underwater_count", "v3.portfolio.kaz_era.listed_underwater", "Kaz-era underwater count"),
        ("legacy_win_rate", "v3.portfolio.legacy.sold_win_rate", "Legacy win rate"),
        ("toxic_remaining", "toxic.remaining_count", "Toxic inventory remaining"),
        ("velocity", "velocity.sales_per_day_avg", "Sales velocity"),
        ("total_sold", "v3.portfolio.kaz_era.sold_count", "Kaz-era homes sold"),
    ]

    def __init__(self):
        self.timestamp = datetime.now().isoformat()

    def _safe_get(self, data: dict, path: str, default: float = 0.0) -> float:
        """Safely get a nested value from a dictionary using dot notation."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        try:
            return float(current)
        except (TypeError, ValueError):
            return default

    def _calculate_change_pct(self, current: float, previous: float) -> float:
        """Calculate percentage change between two values."""
        if previous == 0:
            return 100.0 if current > 0 else 0.0
        return ((current - previous) / abs(previous)) * 100

    def _get_severity(self, change_pct: float, metric: str) -> str:
        """Determine severity based on change percentage and metric type."""
        abs_change = abs(change_pct)

        # Critical thresholds for certain metrics
        critical_metrics = ["win_rate", "underwater_count", "toxic_remaining"]

        if abs_change >= 20:
            return "critical"
        elif abs_change >= 15 or (metric in critical_metrics and abs_change >= 10):
            return "critical" if metric == "underwater_count" and change_pct > 0 else "warning"
        else:
            return "warning"

    def detect_anomalies(self, current: dict, previous: dict) -> List[dict]:
        """
        Check for >10% WoW changes in key metrics.

        Checks:
        - Win rate changes
        - Underwater count increases
        - Geographic hotspots (underwater rate spikes)
        - Velocity changes

        Returns:
            List of {metric, change_pct, direction, severity, message}
        """
        anomalies = []

        if not current or not previous:
            logger.warning("Missing current or previous data for anomaly detection")
            return anomalies

        # Check standard metrics
        for metric_name, path, display_name in self.ANOMALY_METRICS:
            current_val = self._safe_get(current, path)
            previous_val = self._safe_get(previous, path)

            if previous_val == 0 and current_val == 0:
                continue

            change_pct = self._calculate_change_pct(current_val, previous_val)

            if abs(change_pct) > WOW_CHANGE_THRESHOLD:
                direction = "up" if change_pct > 0 else "down"
                severity = self._get_severity(change_pct, metric_name)

                # Determine if this change is good or bad
                is_concerning = self._is_concerning_change(metric_name, change_pct)

                if is_concerning:
                    anomaly = Anomaly(
                        metric=metric_name,
                        change_pct=round(change_pct, 1),
                        direction=direction,
                        severity=severity,
                        message=f"{display_name} changed {change_pct:+.1f}% WoW "
                                f"({previous_val:.1f} -> {current_val:.1f})",
                        current_value=current_val,
                        previous_value=previous_val,
                    )
                    anomalies.append(asdict(anomaly))

        # Check geographic hotspots
        geo_anomalies = self._detect_geographic_anomalies(current, previous)
        anomalies.extend(geo_anomalies)

        return anomalies

    def _is_concerning_change(self, metric: str, change_pct: float) -> bool:
        """Determine if a change is concerning (bad) vs positive (good)."""
        # Win rate going down is bad
        if "win_rate" in metric and change_pct < 0:
            return True
        # Underwater/toxic going up is bad
        if "underwater" in metric and change_pct > 0:
            return True
        if "toxic" in metric and change_pct > 0:
            return True
        # Velocity going down significantly is concerning
        if "velocity" in metric and change_pct < -15:
            return True
        # Any large change (>20%) should be flagged for review
        if abs(change_pct) > 20:
            return True
        return False

    def _detect_geographic_anomalies(self, current: dict, previous: dict) -> List[dict]:
        """Detect geographic hotspots with underwater rate spikes."""
        anomalies = []

        # Get market performance data
        current_markets = current.get("markets", [])
        previous_markets = previous.get("markets", [])

        if not current_markets or not previous_markets:
            return anomalies

        # Create lookup for previous data
        prev_by_state = {m.get("state"): m for m in previous_markets if m.get("state")}

        for market in current_markets:
            state = market.get("state")
            if not state or state not in prev_by_state:
                continue

            prev_market = prev_by_state[state]

            # Check win rate changes
            curr_win = market.get("win_rate", 0)
            prev_win = prev_market.get("win_rate", 0)

            if prev_win > 0:
                win_change = self._calculate_change_pct(curr_win, prev_win)

                if win_change < -WOW_CHANGE_THRESHOLD:
                    anomaly = Anomaly(
                        metric=f"geo.{state}.win_rate",
                        change_pct=round(win_change, 1),
                        direction="down",
                        severity="warning",
                        message=f"{state} win rate dropped {abs(win_change):.1f}% WoW "
                                f"({prev_win:.1f}% -> {curr_win:.1f}%)",
                        current_value=curr_win,
                        previous_value=prev_win,
                    )
                    anomalies.append(asdict(anomaly))

            # Check toxic count increases
            curr_toxic = market.get("toxic_count", 0)
            prev_toxic = prev_market.get("toxic_count", 0)

            if prev_toxic > 0:
                toxic_change = self._calculate_change_pct(curr_toxic, prev_toxic)

                if toxic_change > WOW_CHANGE_THRESHOLD:
                    anomaly = Anomaly(
                        metric=f"geo.{state}.toxic",
                        change_pct=round(toxic_change, 1),
                        direction="up",
                        severity="warning",
                        message=f"{state} toxic inventory increased {toxic_change:.1f}% WoW "
                                f"({prev_toxic} -> {curr_toxic})",
                        current_value=curr_toxic,
                        previous_value=prev_toxic,
                    )
                    anomalies.append(asdict(anomaly))

        return anomalies

    def generate_thesis_checkpoints(self, current: dict) -> List[dict]:
        """
        Generate factual checkpoints (no "on track" language).

        Checkpoints:
        - Kaz-era win rate >90%?
        - Kaz-era underwater <10%?
        - Legacy toxic declining?
        - Price discipline holding?
        - Q1 revenue on pace?

        Returns:
            List of {checkpoint, status: 'pass'|'warn'|'fail', value, target}
        """
        checkpoints = []

        if not current:
            logger.warning("No current data for thesis checkpoint generation")
            return checkpoints

        # 1. Kaz-era win rate >90%
        kaz_win_rate = self._safe_get(current, "v3.portfolio.kaz_era.sold_win_rate")
        # Also check kaz_era.realized path
        if kaz_win_rate == 0:
            kaz_win_rate = self._safe_get(current, "kaz_era.realized.win_rate")

        if kaz_win_rate > 0:
            checkpoints.append(asdict(ThesisCheckpoint(
                checkpoint="kaz_era_win_rate",
                status="pass" if kaz_win_rate >= KAZ_ERA_WIN_RATE_TARGET else "fail",
                value=round(kaz_win_rate, 1),
                target=KAZ_ERA_WIN_RATE_TARGET,
                description=f"Kaz-era sold win rate: {kaz_win_rate:.1f}%",
            )))

        # 2. Kaz-era underwater <10%
        kaz_underwater_pct = self._safe_get(current, "v3.portfolio.kaz_era.listed_underwater_pct")
        # Also check kaz_era.unrealized path
        if kaz_underwater_pct == 0:
            kaz_above = self._safe_get(current, "kaz_era.unrealized.above_water_pct")
            if kaz_above > 0:
                kaz_underwater_pct = 100 - kaz_above

        checkpoints.append(asdict(ThesisCheckpoint(
            checkpoint="kaz_era_underwater",
            status="pass" if kaz_underwater_pct < KAZ_ERA_UNDERWATER_TARGET else
                   ("warn" if kaz_underwater_pct < 15 else "fail"),
            value=round(kaz_underwater_pct, 1),
            target=KAZ_ERA_UNDERWATER_TARGET,
            description=f"Kaz-era listed underwater: {kaz_underwater_pct:.1f}%",
        )))

        # 3. Legacy toxic declining
        toxic_remaining = self._safe_get(current, "toxic.remaining_count")
        toxic_sold = self._safe_get(current, "toxic.sold_count")
        clearance_pct = self._safe_get(current, "toxic.clearance_pct")

        # Determine if toxic is declining based on clearance progress
        toxic_status = "pass" if clearance_pct > 50 else ("warn" if clearance_pct > 25 else "fail")

        checkpoints.append(asdict(ThesisCheckpoint(
            checkpoint="legacy_toxic_declining",
            status=toxic_status,
            value=round(clearance_pct, 1),
            target=50.0,  # Target: 50%+ cleared
            description=f"Toxic clearance: {toxic_sold:.0f} sold, {toxic_remaining:.0f} remaining ({clearance_pct:.1f}%)",
        )))

        # 4. Price discipline holding
        # Check price cut metrics
        price_cuts_pct = self._safe_get(current, "v3.price_cut_severity.0.pct_with_cuts")
        # Also check pricing path
        if price_cuts_pct == 0:
            price_cuts_pct = self._safe_get(current, "pricing.homes_with_price_cuts_pct")

        # Lower price cuts = better discipline
        price_status = "pass" if price_cuts_pct < 30 else ("warn" if price_cuts_pct < 50 else "fail")

        checkpoints.append(asdict(ThesisCheckpoint(
            checkpoint="price_discipline",
            status=price_status,
            value=round(price_cuts_pct, 1),
            target=30.0,  # Target: <30% with cuts
            description=f"Homes with price cuts: {price_cuts_pct:.1f}%",
        )))

        # 5. Q1 revenue on pace
        guidance = current.get("guidance", {})
        if guidance:
            pct_to_target = guidance.get("pct_to_target", 0)
            days_elapsed = guidance.get("days_elapsed", 0)
            total_days = 90  # Q1 days

            expected_pct = (days_elapsed / total_days * 100) if total_days > 0 else 0

            # Status based on comparison to expected pace
            if pct_to_target >= expected_pct:
                q1_status = "pass"
            elif pct_to_target >= expected_pct - 5:
                q1_status = "warn"
            else:
                q1_status = "fail"

            checkpoints.append(asdict(ThesisCheckpoint(
                checkpoint="q1_revenue_pace",
                status=q1_status,
                value=round(pct_to_target, 1),
                target=round(expected_pct, 1),
                description=f"Q1 revenue: {pct_to_target:.1f}% of target (expected: {expected_pct:.1f}%)",
            )))

        return checkpoints

    def generate_weekly_summary(self, history: list, current: dict) -> str:
        """
        Generate a natural language summary (template-based, not AI-generated).

        Example output:
        "SOLD: 142 homes (+8% WoW) for $52.3M revenue
        KAZ-ERA: 43 sold at 95.3% win rate..."

        Args:
            history: List of historical data snapshots
            current: Current metrics snapshot

        Returns:
            Template-based summary string
        """
        if not current:
            return "No current data available for summary."

        lines = []

        # Get previous week data for WoW comparison
        previous = history[-1] if history else {}

        # SOLD section
        kaz_sold = self._safe_get(current, "v3.portfolio.kaz_era.sold_count")
        legacy_sold = self._safe_get(current, "v3.portfolio.legacy.sold_count")
        total_sold = kaz_sold + legacy_sold

        kaz_revenue = self._safe_get(current, "v3.portfolio.kaz_era.sold_total_realized")
        legacy_revenue = self._safe_get(current, "v3.portfolio.legacy.sold_total_realized")
        total_revenue = kaz_revenue + legacy_revenue

        # Calculate WoW change if we have previous data
        prev_total_sold = (
            self._safe_get(previous, "v3.portfolio.kaz_era.sold_count") +
            self._safe_get(previous, "v3.portfolio.legacy.sold_count")
        )

        wow_change = ""
        if prev_total_sold > 0:
            change_pct = self._calculate_change_pct(total_sold, prev_total_sold)
            wow_change = f" ({change_pct:+.0f}% WoW)"

        lines.append(f"SOLD: {total_sold:.0f} homes{wow_change} for ${total_revenue/1_000_000:.1f}M revenue")

        # KAZ-ERA section
        kaz_win_rate = self._safe_get(current, "v3.portfolio.kaz_era.sold_win_rate")
        if kaz_win_rate == 0:
            kaz_win_rate = self._safe_get(current, "kaz_era.realized.win_rate")

        kaz_underwater = self._safe_get(current, "v3.portfolio.kaz_era.listed_underwater")
        kaz_listed = self._safe_get(current, "v3.portfolio.kaz_era.listed_count")

        if kaz_sold > 0:
            lines.append(f"KAZ-ERA: {kaz_sold:.0f} sold at {kaz_win_rate:.1f}% win rate, "
                        f"{kaz_underwater:.0f}/{kaz_listed:.0f} listed underwater")

        # LEGACY section
        legacy_win_rate = self._safe_get(current, "v3.portfolio.legacy.sold_win_rate")

        if legacy_sold > 0:
            lines.append(f"LEGACY: {legacy_sold:.0f} sold at {legacy_win_rate:.1f}% win rate")

        # TOXIC section
        toxic_remaining = self._safe_get(current, "toxic.remaining_count")
        toxic_sold = self._safe_get(current, "toxic.sold_count")
        clearance_pct = self._safe_get(current, "toxic.clearance_pct")

        if toxic_remaining > 0 or toxic_sold > 0:
            lines.append(f"TOXIC: {toxic_sold:.0f} cleared, {toxic_remaining:.0f} remaining "
                        f"({clearance_pct:.1f}% cleared)")

        # VELOCITY section
        velocity = self._safe_get(current, "velocity.sales_per_day_avg")
        if velocity > 0:
            lines.append(f"VELOCITY: {velocity:.1f} homes/day avg")

        # GUIDANCE section
        guidance = current.get("guidance", {})
        if guidance:
            pct_to_target = guidance.get("pct_to_target", 0)
            pace = guidance.get("pace_vs_required", "unknown")
            revenue_to_date = guidance.get("revenue_to_date", 0)

            lines.append(f"Q1 GUIDANCE: ${revenue_to_date/1_000_000:.1f}M "
                        f"({pct_to_target:.1f}% of $595M target, pace: {pace})")

        return "\n".join(lines)

    def prepare_alert_data(self, anomalies: List[dict], checkpoints: List[dict]) -> List[dict]:
        """
        Prepare alert data for notification systems.

        Args:
            anomalies: List of detected anomalies
            checkpoints: List of thesis checkpoints

        Returns:
            List of AlertData dictionaries ready for notifications
        """
        alerts = []

        # Convert anomalies to alerts
        for anomaly in anomalies:
            alert = AlertData(
                alert_type="anomaly",
                severity=anomaly.get("severity", "warning"),
                title=f"Anomaly: {anomaly.get('metric', 'Unknown')}",
                message=anomaly.get("message", ""),
                metric=anomaly.get("metric", ""),
                value=anomaly.get("current_value", 0),
                timestamp=self.timestamp,
            )
            alerts.append(asdict(alert))

        # Convert failed checkpoints to alerts
        for checkpoint in checkpoints:
            if checkpoint.get("status") == "fail":
                alert = AlertData(
                    alert_type="checkpoint_fail",
                    severity="critical",
                    title=f"Checkpoint Failed: {checkpoint.get('checkpoint', 'Unknown')}",
                    message=checkpoint.get("description", ""),
                    metric=checkpoint.get("checkpoint", ""),
                    value=checkpoint.get("value", 0),
                    timestamp=self.timestamp,
                )
                alerts.append(asdict(alert))
            elif checkpoint.get("status") == "warn":
                alert = AlertData(
                    alert_type="checkpoint_warn",
                    severity="warning",
                    title=f"Checkpoint Warning: {checkpoint.get('checkpoint', 'Unknown')}",
                    message=checkpoint.get("description", ""),
                    metric=checkpoint.get("checkpoint", ""),
                    value=checkpoint.get("value", 0),
                    timestamp=self.timestamp,
                )
                alerts.append(asdict(alert))

        return alerts

    def generate_full_insights(
        self,
        current: dict,
        previous: Optional[dict] = None,
        history: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Generate complete insights package.

        Args:
            current: Current metrics snapshot
            previous: Previous week's metrics (for WoW comparison)
            history: Historical data for trend analysis

        Returns:
            Complete insights dictionary with anomalies, checkpoints, summary, and alerts
        """
        history = history or []
        previous = previous or (history[-1] if history else {})

        # Detect anomalies
        anomalies = self.detect_anomalies(current, previous)

        # Generate checkpoints
        checkpoints = self.generate_thesis_checkpoints(current)

        # Generate summary
        summary = self.generate_weekly_summary(history, current)

        # Prepare alerts
        alerts = self.prepare_alert_data(anomalies, checkpoints)

        # Calculate overall status
        failed_checkpoints = sum(1 for c in checkpoints if c.get("status") == "fail")
        critical_anomalies = sum(1 for a in anomalies if a.get("severity") == "critical")

        if failed_checkpoints >= 2 or critical_anomalies >= 2:
            overall_status = "critical"
        elif failed_checkpoints >= 1 or critical_anomalies >= 1:
            overall_status = "warning"
        else:
            overall_status = "healthy"

        return {
            "generated_at": self.timestamp,
            "overall_status": overall_status,
            "anomalies": anomalies,
            "checkpoints": checkpoints,
            "summary": summary,
            "alerts": alerts,
            "stats": {
                "anomaly_count": len(anomalies),
                "checkpoint_pass": sum(1 for c in checkpoints if c.get("status") == "pass"),
                "checkpoint_warn": sum(1 for c in checkpoints if c.get("status") == "warn"),
                "checkpoint_fail": failed_checkpoints,
                "alert_count": len(alerts),
            }
        }


def generate_insights(
    current: dict,
    previous: Optional[dict] = None,
    history: Optional[list] = None
) -> Dict[str, Any]:
    """
    Convenience function to generate insights.

    Args:
        current: Current metrics snapshot
        previous: Previous week's metrics
        history: Historical data list

    Returns:
        Complete insights dictionary
    """
    generator = InsightsGenerator()
    return generator.generate_full_insights(current, previous, history)
