"""
Glass House CEO Dashboard
==========================
Full executive dashboard with all advanced metrics.
"""

from datetime import datetime
from typing import Dict, Any, List
from dataclasses import asdict

from ..db.database import DailyMetrics
from ..metrics.advanced import AdvancedAnalytics, MarketPerformance


def fmt_currency(val: float, short: bool = True) -> str:
    """Format as currency."""
    if val is None:
        return "$0"
    if short:
        if abs(val) >= 1_000_000:
            return f"${val/1_000_000:.2f}M"
        elif abs(val) >= 1_000:
            return f"${val/1_000:.1f}K"
    return f"${val:,.0f}"


def fmt_pct(val: float) -> str:
    """Format as percentage."""
    if val is None:
        return "0%"
    return f"{val:.1f}%"


def fmt_num(val: int) -> str:
    """Format number with commas."""
    if val is None:
        return "0"
    return f"{val:,}"


def progress_bar(pct: float, width: int = 20) -> str:
    """Create ASCII progress bar."""
    filled = int(pct / 100 * width)
    empty = width - filled
    return f"[{'█' * filled}{'░' * empty}] {pct:.1f}%"


class CEODashboard:
    """Generate comprehensive CEO dashboard."""

    WIDTH = 70

    def __init__(
        self,
        metrics: DailyMetrics,
        advanced: Dict[str, Any],
        previous: Dict = None
    ):
        self.m = metrics
        self.adv = advanced
        self.prev = previous

    def _header(self) -> str:
        line = "═" * self.WIDTH
        return f"""
╔{line}╗
║  GLASS HOUSE — $OPEN CEO Dashboard                                   ║
║  {self.m.date}                                                          ║
╚{line}╝"""

    def _kaz_era_section(self) -> str:
        """Kaz-era performance (new strategy)."""
        kaz = self.adv.get("kaz_era", {})
        if not kaz:
            return ""

        realized = kaz.get("realized", {})
        unrealized = kaz.get("unrealized", {})
        total = kaz.get("total", 0)
        health = kaz.get("overall_health_pct", 0)
        vs_legacy = kaz.get("vs_legacy_improvement", 0)

        if total == 0:
            return ""

        # Icons based on thresholds
        sold_win = realized.get("win_rate", 0)
        sold_icon = "✓" if sold_win >= 95 else "!" if sold_win >= 85 else "·"

        above_pct = unrealized.get("above_water_pct", 0)
        market_icon = "✓" if above_pct >= 85 else "!" if above_pct >= 70 else "·"

        return f"""
┌─ KAZ-ERA PERFORMANCE (New Strategy) ────────────────────────────────┐
│                                                                      │
│  REALIZED (Sold)                  │  UNREALIZED (On Market)          │
│  {sold_icon} {realized.get('profitable', 0)}/{realized.get('count', 0)} profitable ({sold_win:.1f}%)    │  {market_icon} {unrealized.get('above_water', 0)}/{unrealized.get('count', 0)} above water ({above_pct:.1f}%)   │
│    Avg Profit: {fmt_currency(realized.get('avg_profit', 0)):>10}       │    Underwater: {unrealized.get('underwater', 0)} homes           │
│                                                                      │
│  Total: {total} homes  |  Health: {health:.1f}%  |  vs Legacy: +{vs_legacy:.0f}pp              │
└──────────────────────────────────────────────────────────────────────┘"""

    def _guidance_section(self) -> str:
        """Q1 guidance tracking."""
        g = self.adv.get("guidance", {})

        target = g.get("q1_target", 595_000_000)
        revenue = g.get("revenue_to_date", 0)
        pct = g.get("pct_to_target", 0)
        pace = g.get("pace_vs_required", "unknown")
        projected = g.get("projected_quarter_revenue", 0)
        days_left = g.get("days_remaining", 0)
        req_daily = g.get("required_daily_revenue", 0)
        curr_daily = g.get("current_daily_revenue", 0)

        pace_icon = {"ahead": "🟢", "on_track": "🟡", "behind": "🔴"}.get(pace, "⚪")

        return f"""
┌─ Q1 GUIDANCE TRACKING ──────────────────────────────────────────────┐
│                                                                      │
│  Target: {fmt_currency(target):>12}     Revenue: {fmt_currency(revenue):>12}              │
│  {progress_bar(pct, 40):55}│
│                                                                      │
│  {pace_icon} Pace: {pace.upper():10}  Days Left: {days_left:3}                             │
│  Daily Required: {fmt_currency(req_daily):>10}   Current: {fmt_currency(curr_daily):>10}            │
│  Projected Q1:   {fmt_currency(projected):>10}                                    │
└──────────────────────────────────────────────────────────────────────┘"""

    def _cohort_section(self) -> str:
        """Cohort performance grid."""
        cohorts = [
            ("NEW (<90d)", self.m.cohort_new, "The Signal"),
            ("MID (90-180d)", self.m.cohort_mid, "Transition"),
            ("OLD (180-365d)", self.m.cohort_old, "Legacy"),
            ("TOXIC (>365d)", self.m.cohort_toxic, "Clearing"),
        ]

        lines = ["""
┌─ COHORT PERFORMANCE ────────────────────────────────────────────────┐
│  Cohort          Win Rate    Avg Profit    Count    Margin   Status │
│  ─────────────────────────────────────────────────────────────────  │"""]

        for name, c, note in cohorts:
            win = c.win_rate
            profit = c.avg_profit
            count = c.count
            margin = c.contribution_margin

            # Status indicator
            if "NEW" in name:
                status = "✓" if win >= 95 else "!" if win >= 90 else "✗"
            elif "TOXIC" in name:
                status = "↓" if count > 0 else "✓"
            else:
                status = "·"

            profit_str = fmt_currency(profit)
            if profit < 0:
                profit_str = f"-{fmt_currency(abs(profit))}"

            lines.append(
                f"│  {name:14} {fmt_pct(win):>8}    {profit_str:>10}    {count:>5}    {fmt_pct(margin):>6}   {status:>3}  │"
            )

        lines.append("└──────────────────────────────────────────────────────────────────────┘")
        return "\n".join(lines)

    def _toxic_section(self) -> str:
        """Toxic countdown with visual."""
        t = self.m.toxic
        sold = t.sold_count
        remaining = t.remaining_count
        total = sold + remaining
        pct = t.clearance_pct

        return f"""
┌─ TOXIC COUNTDOWN ───────────────────────────────────────────────────┐
│                                                                      │
│  Cleared: {sold:>4}  ████████████░░░░░░░░░░  Remaining: {remaining:>4}          │
│  {progress_bar(pct, 40):55}│
│                                                                      │
│  Avg Loss: {fmt_currency(t.sold_avg_loss):>10}    Weeks to Clear: ~{t.weeks_to_clear:.0f}               │
└──────────────────────────────────────────────────────────────────────┘"""

    def _velocity_section(self) -> str:
        """Velocity metrics."""
        v = self.adv.get("velocity", {})

        return f"""
┌─ VELOCITY ──────────────────────────────────────────────────────────┐
│                                                                      │
│  Avg Days to Sale:    {v.get('avg_days_to_sale', 0):>6.0f}     Sales/Day:        {v.get('sales_per_day_avg', 0):>6.1f}  │
│  Median Days:         {v.get('median_days_to_sale', 0):>6.0f}     Last 7 Days:      {v.get('sales_last_7_days', 0):>6}  │
│  Inventory Turnover:  {v.get('inventory_turnover_days', 0):>6.0f}d    Last 30 Days:     {v.get('sales_last_30_days', 0):>6}  │
└──────────────────────────────────────────────────────────────────────┘"""

    def _pricing_section(self) -> str:
        """Pricing intelligence."""
        p = self.adv.get("pricing", {})

        return f"""
┌─ PRICING INTELLIGENCE ──────────────────────────────────────────────┐
│                                                                      │
│  Avg Spread (Buy→Sell): {fmt_currency(p.get('avg_spread', 0)):>10}                            │
│  Homes with Price Cuts: {p.get('homes_with_price_cuts', 0):>5} ({fmt_pct(p.get('homes_with_price_cuts_pct', 0)):>6})                  │
│  Avg Cuts per Home:     {p.get('avg_cuts_per_home', 0):>5.1f}                                    │
│  Avg Price Reduction:   {fmt_pct(p.get('avg_price_cut_pct', 0)):>6}                                   │
└──────────────────────────────────────────────────────────────────────┘"""

    def _risk_section(self) -> str:
        """Risk dashboard."""
        r = self.adv.get("risk", {})

        underwater = r.get("underwater_count", 0)
        underwater_pct = r.get("underwater_pct", 0)
        exposure = r.get("underwater_total_exposure", 0)
        aged_uw = r.get("aged_underwater_count", 0)
        top_market = r.get("top_concentration_market", "N/A")
        top_pct = r.get("top_concentration_pct", 0)

        return f"""
┌─ RISK DASHBOARD ────────────────────────────────────────────────────┐
│                                                                      │
│  UNDERWATER (List < Purchase)                                        │
│  Count: {underwater:>5} ({fmt_pct(underwater_pct):>6})    Exposure: {fmt_currency(exposure):>12}            │
│  Aged + Underwater (>180d): {aged_uw:>5}  ← Most at risk                  │
│                                                                      │
│  CONCENTRATION                                                       │
│  Top Market: {top_market:>5} ({fmt_pct(top_pct):>6})                                     │
│  Markets >10%: {r.get('markets_above_10pct', 0):>3}                                              │
└──────────────────────────────────────────────────────────────────────┘"""

    def _market_matrix(self) -> str:
        """Market performance matrix."""
        markets = self.adv.get("markets", [])

        if not markets:
            return ""

        lines = ["""
┌─ MARKET MATRIX ─────────────────────────────────────────────────────┐
│  State   Inventory   Toxic   DOM    Win%   Avg Profit   Conc%       │
│  ─────────────────────────────────────────────────────────────────  │"""]

        # Show top 10 markets by inventory
        for m in markets[:10]:
            profit_str = fmt_currency(m.get("avg_profit", 0))
            if m.get("avg_profit", 0) < 0:
                profit_str = f"-{fmt_currency(abs(m.get('avg_profit', 0)))}"

            toxic_flag = "⚠" if m.get("toxic_count", 0) > 5 else " "

            lines.append(
                f"│  {m.get('state', ''):>5}   {m.get('inventory_count', 0):>7}   "
                f"{m.get('toxic_count', 0):>5}{toxic_flag}  {m.get('avg_dom', 0):>4.0f}   "
                f"{fmt_pct(m.get('win_rate', 0)):>5}   {profit_str:>10}   {fmt_pct(m.get('concentration_pct', 0)):>5}    │"
            )

        lines.append("└──────────────────────────────────────────────────────────────────────┘")
        return "\n".join(lines)

    def _inventory_section(self) -> str:
        """Inventory health visual."""
        i = self.m.inventory

        total = i.total or 1
        fresh_pct = i.fresh_count / total * 100
        normal_pct = i.normal_count / total * 100
        stale_pct = i.stale_count / total * 100
        vs_pct = i.very_stale_count / total * 100
        toxic_pct = i.toxic_count / total * 100

        # Visual bar
        bar_width = 50
        fresh_w = int(fresh_pct / 100 * bar_width)
        normal_w = int(normal_pct / 100 * bar_width)
        stale_w = int(stale_pct / 100 * bar_width)
        vs_w = int(vs_pct / 100 * bar_width)
        toxic_w = bar_width - fresh_w - normal_w - stale_w - vs_w

        bar = f"{'🟢' * fresh_w}{'🟡' * normal_w}{'🟠' * stale_w}{'🔴' * vs_w}{'⚫' * toxic_w}"

        return f"""
┌─ INVENTORY HEALTH ──────────────────────────────────────────────────┐
│                                                                      │
│  Total: {i.total:>5}    Avg DOM: {i.avg_dom:>5.0f}d    Unrealized: {fmt_currency(i.total_unrealized_pnl):>10} │
│                                                                      │
│  {bar[:50]:50} │
│  🟢 Fresh:{i.fresh_count:>4} 🟡 Normal:{i.normal_count:>4} 🟠 Stale:{i.stale_count:>4} 🔴 VStale:{i.very_stale_count:>4} ⚫ Toxic:{i.toxic_count:>3}│
│                                                                      │
│  Legacy (>180d): {fmt_pct(i.legacy_pct):>6}                                          │
└──────────────────────────────────────────────────────────────────────┘"""

    def _alerts_section(self) -> str:
        """Alerts and flags."""
        if not self.m.alerts:
            return """
┌─ ALERTS ────────────────────────────────────────────────────────────┐
│  ✓ No alerts — all metrics within thresholds                         │
└──────────────────────────────────────────────────────────────────────┘"""

        lines = ["", "┌─ ALERTS ────────────────────────────────────────────────────────────┐"]
        for alert in self.m.alerts[:5]:
            lines.append(f"│  ⚠ {alert[:64]:64} │")
        lines.append("└──────────────────────────────────────────────────────────────────────┘")
        return "\n".join(lines)

    def generate(self) -> str:
        """Generate full CEO dashboard."""
        sections = [
            self._header(),
            self._kaz_era_section(),  # New strategy performance first!
            self._guidance_section(),
            self._cohort_section(),
            self._toxic_section(),
            self._velocity_section(),
            self._pricing_section(),
            self._inventory_section(),
            self._market_matrix(),
            self._risk_section(),
            self._alerts_section(),
        ]

        return "\n".join(s for s in sections if s)

    def print(self):
        """Print dashboard to terminal."""
        print(self.generate())
