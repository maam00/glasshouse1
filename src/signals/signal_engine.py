"""
Signal Engine - OPS Score Calculation
======================================
Computes 6 top-level signals and combines them into an OPS Score.

Signals:
    1. guidance_pace     - Q1 revenue guidance tracking
    2. kaz_econ          - Kaz-era unit economics health
    3. legacy_burndown   - Legacy inventory reduction velocity
    4. inventory_aging   - DOM distribution and stale inventory
    5. pricecut_stress   - Price reduction frequency/depth
    6. underwater_exposure - % of inventory below purchase cost

OPS_SCORE = sum(weight_i * normalized_signal_i)
where weight_i = base_weight * conf_mult(A=1, B=0.65, C=0.30) * sqrt(coverage_pct/100)
"""

import logging
import math
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config import (
    KAZ_ERA_START,
    PACE_GREEN_MIN, PACE_YELLOW_MIN,
    KAZ_WIN_RATE_GREEN_MIN, KAZ_WIN_RATE_YELLOW_MIN,
    TOXIC_PCT_GREEN_MAX, TOXIC_PCT_YELLOW_MAX,
    MONTHS_INV_GREEN_MAX, MONTHS_INV_YELLOW_MAX,
    PRICE_CUT_GREEN_MAX, PRICE_CUT_YELLOW_MAX,
    get_confidence_grade,
)
from src.db.database import Database

logger = logging.getLogger(__name__)


# =============================================================================
# SIGNAL WEIGHTS AND CONFIDENCE MULTIPLIERS
# =============================================================================

BASE_WEIGHTS = {
    'guidance_pace': 0.25,      # Most important - are we hitting targets?
    'kaz_econ': 0.20,           # New strategy working?
    'legacy_burndown': 0.15,    # Cleaning up old inventory?
    'inventory_aging': 0.15,    # How fresh is inventory?
    'pricecut_stress': 0.15,    # Market stress indicator
    'underwater_exposure': 0.10, # Risk exposure
}

CONFIDENCE_MULTIPLIERS = {
    'A': 1.00,   # High confidence - full weight
    'B': 0.65,   # Medium confidence - reduced weight
    'C': 0.30,   # Low confidence - heavily discounted
}


@dataclass
class Signal:
    """Individual signal result."""
    name: str
    value: float                    # Raw metric value
    normalized: float               # 0-100 normalized score (100 = best)
    delta_7d: Optional[float]       # Change over 7 days
    delta_28d: Optional[float]      # Change over 28 days
    state: str                      # 'G', 'Y', or 'R'
    confidence: str                 # 'A', 'B', or 'C'
    rationale: str                  # Human-readable explanation
    weight_used: float              # Actual weight after confidence adjustment
    coverage_pct: float             # Data coverage percentage
    sample_size: int                # Sample size (n)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OpsScore:
    """Combined OPS Score result."""
    score: float                    # 0-100 composite score
    grade: str                      # A+, A, A-, B+, B, B-, C+, C, C-, D, F
    signals: Dict[str, Signal]      # Individual signals
    breakdown: List[Dict]           # Contribution by signal
    timestamp: datetime
    rationale: str                  # Overall assessment

    def to_dict(self) -> Dict[str, Any]:
        return {
            'score': self.score,
            'grade': self.grade,
            'signals': {k: v.to_dict() for k, v in self.signals.items()},
            'breakdown': self.breakdown,
            'timestamp': self.timestamp.isoformat(),
            'rationale': self.rationale,
        }


def score_to_grade(score: float) -> str:
    """Convert 0-100 score to letter grade."""
    if score >= 97: return 'A+'
    if score >= 93: return 'A'
    if score >= 90: return 'A-'
    if score >= 87: return 'B+'
    if score >= 83: return 'B'
    if score >= 80: return 'B-'
    if score >= 77: return 'C+'
    if score >= 73: return 'C'
    if score >= 70: return 'C-'
    if score >= 60: return 'D'
    return 'F'


class SignalEngine:
    """Calculate OPS Score from 6 top-level signals."""

    def __init__(self, db: Database = None):
        self.db = db or Database()

    def calculate_ops_score(self) -> OpsScore:
        """Calculate the full OPS Score with all signals."""
        signals = {}

        # Calculate each signal
        signals['guidance_pace'] = self._calc_guidance_pace()
        signals['kaz_econ'] = self._calc_kaz_econ()
        signals['legacy_burndown'] = self._calc_legacy_burndown()
        signals['inventory_aging'] = self._calc_inventory_aging()
        signals['pricecut_stress'] = self._calc_pricecut_stress()
        signals['underwater_exposure'] = self._calc_underwater_exposure()

        # Calculate composite score
        total_weight = 0
        weighted_sum = 0
        breakdown = []

        for name, signal in signals.items():
            contribution = signal.normalized * signal.weight_used
            weighted_sum += contribution
            total_weight += signal.weight_used

            breakdown.append({
                'signal': name,
                'value': signal.value,
                'normalized': signal.normalized,
                'state': signal.state,
                'confidence': signal.confidence,
                'base_weight': BASE_WEIGHTS[name],
                'weight_used': round(signal.weight_used, 4),
                'contribution': round(contribution, 2),
                'pct_of_total': 0,  # Calculate after loop
            })

        # Normalize to 0-100
        score = (weighted_sum / total_weight) if total_weight > 0 else 0

        # Calculate % contribution for each signal
        for item in breakdown:
            item['pct_of_total'] = round(
                (item['contribution'] / weighted_sum * 100) if weighted_sum > 0 else 0, 1
            )

        # Sort breakdown by contribution
        breakdown.sort(key=lambda x: -x['contribution'])

        # Generate overall rationale
        rationale = self._generate_rationale(signals, score)

        return OpsScore(
            score=round(score, 1),
            grade=score_to_grade(score),
            signals=signals,
            breakdown=breakdown,
            timestamp=datetime.now(),
            rationale=rationale,
        )

    def _calc_weight(self, name: str, confidence: str, coverage_pct: float) -> float:
        """Calculate adjusted weight for a signal."""
        base = BASE_WEIGHTS.get(name, 0.1)
        conf_mult = CONFIDENCE_MULTIPLIERS.get(confidence, 0.3)
        coverage_mult = math.sqrt(coverage_pct / 100) if coverage_pct > 0 else 0.1
        return base * conf_mult * coverage_mult

    def _get_state(self, value: float, green_thresh: float, yellow_thresh: float,
                   direction: str = 'higher_better') -> str:
        """Determine G/Y/R state based on thresholds."""
        if direction == 'higher_better':
            if value >= green_thresh:
                return 'G'
            elif value >= yellow_thresh:
                return 'Y'
            else:
                return 'R'
        else:  # lower_better
            if value <= green_thresh:
                return 'G'
            elif value <= yellow_thresh:
                return 'Y'
            else:
                return 'R'

    def _normalize_score(self, value: float, worst: float, best: float) -> float:
        """Normalize a value to 0-100 scale."""
        if best == worst:
            return 50.0
        normalized = ((value - worst) / (best - worst)) * 100
        return max(0, min(100, normalized))

    # =========================================================================
    # SIGNAL CALCULATORS
    # =========================================================================

    def _calc_guidance_pace(self) -> Signal:
        """
        Signal 1: Guidance Pace
        Are we on track to hit Q1 revenue guidance?
        """
        # Get daily metrics for pace calculation
        conn = self.db._get_conn()
        cursor = conn.cursor()

        # Get last 30 days of sales data from sales_log
        cursor.execute("""
            SELECT sale_date, COUNT(*) as sales_count, SUM(sale_price) as total_revenue
            FROM sales_log
            WHERE sale_date >= date('now', '-30 days')
            GROUP BY sale_date
            ORDER BY sale_date DESC
        """)
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return Signal(
                name='guidance_pace',
                value=0,
                normalized=50,
                delta_7d=None,
                delta_28d=None,
                state='Y',
                confidence='C',
                rationale='Insufficient data for guidance tracking',
                weight_used=self._calc_weight('guidance_pace', 'C', 30),
                coverage_pct=30,
                sample_size=0,
            )

        # Calculate current pace vs target
        # Q1 target: ~$1B over 90 days = ~$11.1M/day
        q1_daily_target = 11_100_000
        recent_days = [dict(r) for r in rows]

        # Last 7 days average
        last_7 = [r['total_revenue'] or 0 for r in recent_days[:7]]
        avg_7d = sum(last_7) / len(last_7) if last_7 else 0

        # Calculate pace percentage
        pace_pct = (avg_7d / q1_daily_target * 100) if q1_daily_target > 0 else 0

        # 7d and 28d deltas
        if len(recent_days) >= 14:
            prev_7 = [r['total_revenue'] or 0 for r in recent_days[7:14]]
            prev_avg = sum(prev_7) / len(prev_7) if prev_7 else 0
            delta_7d = pace_pct - (prev_avg / q1_daily_target * 100) if prev_avg else None
        else:
            delta_7d = None

        delta_28d = None  # Would need historical comparison

        state = self._get_state(pace_pct, PACE_GREEN_MIN, PACE_YELLOW_MIN, 'higher_better')
        confidence = get_confidence_grade(len(recent_days) / 30 * 100, len(recent_days))
        normalized = self._normalize_score(pace_pct, 50, 120)  # 50% = terrible, 120% = excellent

        if pace_pct >= 100:
            rationale = f"On pace at {pace_pct:.0f}% of daily target"
        elif pace_pct >= 80:
            rationale = f"Slightly behind at {pace_pct:.0f}% of target, needs {100-pace_pct:.0f}pp acceleration"
        else:
            rationale = f"Significantly behind at {pace_pct:.0f}%, risk of missing guidance"

        return Signal(
            name='guidance_pace',
            value=round(pace_pct, 1),
            normalized=round(normalized, 1),
            delta_7d=round(delta_7d, 1) if delta_7d else None,
            delta_28d=delta_28d,
            state=state,
            confidence=confidence,
            rationale=rationale,
            weight_used=self._calc_weight('guidance_pace', confidence, len(recent_days) / 30 * 100),
            coverage_pct=round(len(recent_days) / 30 * 100, 1),
            sample_size=len(recent_days),
        )

    def _calc_kaz_econ(self) -> Signal:
        """
        Signal 2: Kaz-Era Economics
        How well is the new CEO's strategy performing?
        Uses 'new' cohort as proxy for Kaz-era (homes held <90 days).
        """
        conn = self.db._get_conn()
        cursor = conn.cursor()

        # Get new cohort sales (proxy for Kaz-era: <90 days held)
        cursor.execute("""
            SELECT
                COUNT(*) as total_sold,
                SUM(CASE WHEN realized_net > 0 THEN 1 ELSE 0 END) as profitable,
                AVG(realized_net) as avg_profit
            FROM sales_log
            WHERE cohort = 'new'
        """)
        row = cursor.fetchone()
        conn.close()

        if not row or row['total_sold'] == 0:
            return Signal(
                name='kaz_econ',
                value=0,
                normalized=50,
                delta_7d=None,
                delta_28d=None,
                state='Y',
                confidence='C',
                rationale='No new cohort sales data yet',
                weight_used=self._calc_weight('kaz_econ', 'C', 20),
                coverage_pct=20,
                sample_size=0,
            )

        total = row['total_sold']
        profitable = row['profitable'] or 0
        win_rate = (profitable / total * 100) if total > 0 else 0

        state = self._get_state(win_rate, KAZ_WIN_RATE_GREEN_MIN, KAZ_WIN_RATE_YELLOW_MIN, 'higher_better')
        coverage = min(100, total / 50 * 100)  # 50 sales = 100% coverage
        confidence = get_confidence_grade(coverage, total)
        normalized = self._normalize_score(win_rate, 50, 100)

        if win_rate >= 95:
            rationale = f"Exceptional: {profitable}/{total} profitable ({win_rate:.1f}% win rate)"
        elif win_rate >= 85:
            rationale = f"Strong: {profitable}/{total} profitable ({win_rate:.1f}%)"
        elif win_rate >= 70:
            rationale = f"Moderate: {win_rate:.1f}% win rate, strategy showing mixed results"
        else:
            rationale = f"Concerning: Only {win_rate:.1f}% win rate, strategy needs adjustment"

        return Signal(
            name='kaz_econ',
            value=round(win_rate, 1),
            normalized=round(normalized, 1),
            delta_7d=None,
            delta_28d=None,
            state=state,
            confidence=confidence,
            rationale=rationale,
            weight_used=self._calc_weight('kaz_econ', confidence, coverage),
            coverage_pct=round(coverage, 1),
            sample_size=total,
        )

    def _calc_legacy_burndown(self) -> Signal:
        """
        Signal 3: Legacy Burndown
        How fast are we clearing old/toxic inventory?
        Uses age_bucket to identify legacy inventory.
        """
        conn = self.db._get_conn()
        cursor = conn.cursor()

        # Count legacy (old + toxic) listings still active
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN age_bucket IN ('old', 'toxic') THEN 1 END) as legacy_count
            FROM listings_log
        """)
        row = cursor.fetchone()
        conn.close()

        total_active = row['total'] if row else 0
        legacy_count = row['legacy_count'] if row else 0

        if total_active == 0:
            legacy_pct = 0
        else:
            legacy_pct = (legacy_count / total_active * 100)

        # Lower legacy % is better
        # Ideal: <10% legacy, Acceptable: <25%, Concerning: >25%
        state = self._get_state(legacy_pct, 10, 25, 'lower_better')
        coverage = min(100, total_active / 100 * 100)
        confidence = get_confidence_grade(coverage, total_active)

        # Normalize: 0% legacy = 100, 50% legacy = 0
        normalized = self._normalize_score(100 - legacy_pct, 50, 100)

        burndown_rate = 100 - legacy_pct
        if legacy_pct <= 10:
            rationale = f"Excellent: Only {legacy_pct:.0f}% legacy inventory remains"
        elif legacy_pct <= 25:
            rationale = f"Good progress: {legacy_pct:.0f}% legacy, burning down steadily"
        else:
            rationale = f"Slow burndown: {legacy_pct:.0f}% still legacy inventory"

        return Signal(
            name='legacy_burndown',
            value=round(100 - legacy_pct, 1),  # Burndown rate (higher = better)
            normalized=round(normalized, 1),
            delta_7d=None,
            delta_28d=None,
            state=state,
            confidence=confidence,
            rationale=rationale,
            weight_used=self._calc_weight('legacy_burndown', confidence, coverage),
            coverage_pct=round(coverage, 1),
            sample_size=total_active,
        )

    def _calc_inventory_aging(self) -> Signal:
        """
        Signal 4: Inventory Aging
        DOM distribution - how fresh is our inventory?
        """
        conn = self.db._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN days_on_market < 30 THEN 1 END) as fresh,
                COUNT(CASE WHEN days_on_market BETWEEN 30 AND 90 THEN 1 END) as normal,
                COUNT(CASE WHEN days_on_market BETWEEN 90 AND 180 THEN 1 END) as stale,
                COUNT(CASE WHEN days_on_market > 180 THEN 1 END) as very_stale,
                AVG(days_on_market) as avg_dom
            FROM listings_log
        """)
        row = cursor.fetchone()
        conn.close()

        if not row or row['total'] == 0:
            return Signal(
                name='inventory_aging',
                value=0,
                normalized=50,
                delta_7d=None,
                delta_28d=None,
                state='Y',
                confidence='C',
                rationale='No active inventory data',
                weight_used=self._calc_weight('inventory_aging', 'C', 20),
                coverage_pct=20,
                sample_size=0,
            )

        total = row['total']
        fresh = row['fresh'] or 0
        stale_plus = (row['stale'] or 0) + (row['very_stale'] or 0)
        avg_dom = row['avg_dom'] or 0

        # Calculate freshness score (% fresh + normal)
        fresh_pct = ((fresh + (row['normal'] or 0)) / total * 100) if total > 0 else 0

        # Ideal: <60 DOM avg, Acceptable: <90, Concerning: >90
        state = self._get_state(avg_dom, 60, 90, 'lower_better')
        coverage = min(100, total / 200 * 100)
        confidence = get_confidence_grade(coverage, total)

        # Normalize: 30 DOM = 100, 150 DOM = 0
        normalized = self._normalize_score(150 - avg_dom, 0, 120)

        if avg_dom <= 45:
            rationale = f"Fresh inventory: {avg_dom:.0f} avg DOM, {fresh_pct:.0f}% under 90 days"
        elif avg_dom <= 75:
            rationale = f"Healthy aging: {avg_dom:.0f} avg DOM"
        else:
            rationale = f"Aging concern: {avg_dom:.0f} avg DOM, {stale_plus} stale listings"

        return Signal(
            name='inventory_aging',
            value=round(avg_dom, 1),
            normalized=round(normalized, 1),
            delta_7d=None,
            delta_28d=None,
            state=state,
            confidence=confidence,
            rationale=rationale,
            weight_used=self._calc_weight('inventory_aging', confidence, coverage),
            coverage_pct=round(coverage, 1),
            sample_size=total,
        )

    def _calc_pricecut_stress(self) -> Signal:
        """
        Signal 5: Price Cut Stress
        How much price reduction has occurred? (based on list vs purchase price)
        """
        conn = self.db._get_conn()
        cursor = conn.cursor()

        # Calculate stress based on how much prices have been cut from purchase
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN list_price < purchase_price THEN 1 END) as with_cuts,
                COUNT(CASE WHEN list_price < purchase_price * 0.95 THEN 1 END) as heavy_cuts,
                AVG(CASE WHEN purchase_price > 0 THEN (purchase_price - list_price) / purchase_price * 100 ELSE 0 END) as avg_cut_pct
            FROM listings_log
            WHERE purchase_price > 0
        """)
        row = cursor.fetchone()
        conn.close()

        if not row or row['total'] == 0:
            return Signal(
                name='pricecut_stress',
                value=0,
                normalized=75,
                delta_7d=None,
                delta_28d=None,
                state='G',
                confidence='C',
                rationale='No price cut data available',
                weight_used=self._calc_weight('pricecut_stress', 'C', 20),
                coverage_pct=20,
                sample_size=0,
            )

        total = row['total']
        with_cuts = row['with_cuts'] or 0
        heavy_cuts = row['heavy_cuts'] or 0

        # % with significant cuts (>5% below purchase) is the stress metric
        heavy_cut_pct = (heavy_cuts / total * 100) if total > 0 else 0

        state = self._get_state(heavy_cut_pct, PRICE_CUT_GREEN_MAX, PRICE_CUT_YELLOW_MAX, 'lower_better')
        coverage = min(100, total / 200 * 100)
        confidence = get_confidence_grade(coverage, total)

        # Normalize: 0% heavy cuts = 100, 60% = 0
        normalized = self._normalize_score(60 - heavy_cut_pct, 0, 60)

        if heavy_cut_pct <= 20:
            rationale = f"Low stress: Only {heavy_cut_pct:.0f}% listed >5% below purchase"
        elif heavy_cut_pct <= 40:
            rationale = f"Moderate stress: {heavy_cut_pct:.0f}% with significant price cuts"
        else:
            rationale = f"High stress: {heavy_cut_pct:.0f}% heavily discounted"

        return Signal(
            name='pricecut_stress',
            value=round(heavy_cut_pct, 1),
            normalized=round(normalized, 1),
            delta_7d=None,
            delta_28d=None,
            state=state,
            confidence=confidence,
            rationale=rationale,
            weight_used=self._calc_weight('pricecut_stress', confidence, coverage),
            coverage_pct=round(coverage, 1),
            sample_size=total,
        )

    def _calc_underwater_exposure(self) -> Signal:
        """
        Signal 6: Underwater Exposure
        What % of inventory is below purchase cost?
        """
        conn = self.db._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN unrealized_pnl < 0 THEN 1 END) as underwater,
                SUM(CASE WHEN unrealized_pnl < 0 THEN unrealized_pnl ELSE 0 END) as total_underwater_value,
                AVG(unrealized_pnl) as avg_unrealized
            FROM listings_log
        """)
        row = cursor.fetchone()
        conn.close()

        if not row or row['total'] == 0:
            return Signal(
                name='underwater_exposure',
                value=0,
                normalized=75,
                delta_7d=None,
                delta_28d=None,
                state='G',
                confidence='C',
                rationale='No underwater exposure data',
                weight_used=self._calc_weight('underwater_exposure', 'C', 20),
                coverage_pct=20,
                sample_size=0,
            )

        total = row['total']
        underwater = row['underwater'] or 0
        underwater_pct = (underwater / total * 100) if total > 0 else 0

        # Lower underwater % is better
        # Ideal: <15%, Acceptable: <30%, Concerning: >30%
        state = self._get_state(underwater_pct, 15, 30, 'lower_better')
        coverage = min(100, total / 200 * 100)
        confidence = get_confidence_grade(coverage, total)

        # Normalize: 0% underwater = 100, 50% = 0
        normalized = self._normalize_score(50 - underwater_pct, 0, 50)

        if underwater_pct <= 10:
            rationale = f"Minimal exposure: Only {underwater:.0f} homes ({underwater_pct:.1f}%) underwater"
        elif underwater_pct <= 25:
            rationale = f"Moderate exposure: {underwater:.0f} homes ({underwater_pct:.1f}%) underwater"
        else:
            rationale = f"Elevated risk: {underwater:.0f} homes ({underwater_pct:.1f}%) underwater"

        return Signal(
            name='underwater_exposure',
            value=round(underwater_pct, 1),
            normalized=round(normalized, 1),
            delta_7d=None,
            delta_28d=None,
            state=state,
            confidence=confidence,
            rationale=rationale,
            weight_used=self._calc_weight('underwater_exposure', confidence, coverage),
            coverage_pct=round(coverage, 1),
            sample_size=total,
        )

    def _generate_rationale(self, signals: Dict[str, Signal], score: float) -> str:
        """Generate overall assessment rationale."""
        green_count = sum(1 for s in signals.values() if s.state == 'G')
        yellow_count = sum(1 for s in signals.values() if s.state == 'Y')
        red_count = sum(1 for s in signals.values() if s.state == 'R')

        grade = score_to_grade(score)

        if score >= 85:
            assessment = "Strong operational health"
        elif score >= 70:
            assessment = "Moderate operational health with some concerns"
        elif score >= 55:
            assessment = "Below-average operations requiring attention"
        else:
            assessment = "Critical operational issues"

        concerns = [s.name.replace('_', ' ').title()
                   for s in signals.values() if s.state == 'R']
        concern_str = f". Key concerns: {', '.join(concerns)}" if concerns else ""

        return f"{assessment} ({green_count}G/{yellow_count}Y/{red_count}R){concern_str}"


def calculate_ops_score() -> OpsScore:
    """Convenience function to calculate OPS Score."""
    engine = SignalEngine()
    return engine.calculate_ops_score()


def get_ops_score_for_dashboard() -> Dict[str, Any]:
    """Get OPS Score formatted for dashboard display."""
    score = calculate_ops_score()
    return score.to_dict()


if __name__ == "__main__":
    # Test the signal engine
    engine = SignalEngine()
    result = engine.calculate_ops_score()

    print(f"\n{'='*60}")
    print(f"  OPS SCORE: {result.score} ({result.grade})")
    print(f"{'='*60}")
    print(f"\n  {result.rationale}")
    print(f"\n  SIGNAL BREAKDOWN:")
    print(f"  {'-'*56}")
    for item in result.breakdown:
        state_icon = {'G': '✓', 'Y': '!', 'R': '✗'}.get(item['state'], '?')
        print(f"  {state_icon} {item['signal']:<22} {item['value']:>8.1f} "
              f"({item['confidence']}) wt={item['weight_used']:.3f} → {item['contribution']:>5.1f}")
    print(f"{'='*60}\n")
