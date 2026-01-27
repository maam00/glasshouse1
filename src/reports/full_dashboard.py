"""
Glass House Full Intelligence Dashboard
========================================
Complete dashboard with operational + market + macro data.
"""

from datetime import datetime
from typing import Dict, Any
from dataclasses import asdict

from ..db.database import DailyMetrics


def fmt_currency(val: float, short: bool = True) -> str:
    if val is None or val == 0:
        return "$0"
    if short:
        if abs(val) >= 1_000_000_000:
            return f"${val/1_000_000_000:.2f}B"
        if abs(val) >= 1_000_000:
            return f"${val/1_000_000:.2f}M"
        elif abs(val) >= 1_000:
            return f"${val/1_000:.1f}K"
    return f"${val:,.0f}"


def fmt_pct(val: float) -> str:
    if val is None:
        return "0%"
    return f"{val:.1f}%"


def fmt_num(val) -> str:
    if val is None:
        return "0"
    return f"{val:,}"


def fmt_change(val: float, suffix: str = "") -> str:
    if val is None:
        return "N/A"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}{suffix}"


class FullDashboard:
    """Complete intelligence dashboard."""

    WIDTH = 76

    def __init__(
        self,
        metrics: DailyMetrics,
        advanced: Dict[str, Any],
        market: Dict[str, Any],
        previous: Dict = None
    ):
        self.m = metrics
        self.adv = advanced
        self.mkt = market
        self.prev = previous

    def _header(self) -> str:
        line = "═" * self.WIDTH
        stock = self.mkt.get("stock", {})
        price = stock.get("price", 0)
        change_pct = stock.get("change_pct", 0)
        change_str = f"+{change_pct:.1f}%" if change_pct > 0 else f"{change_pct:.1f}%"
        color_indicator = "🟢" if change_pct > 0 else "🔴" if change_pct < 0 else "⚪"

        return f"""
╔{line}╗
║  GLASS HOUSE — $OPEN Intelligence Dashboard                                  ║
║  {self.m.date}                                                                    ║
║                                                                              ║
║  $OPEN: ${price:.2f} {color_indicator} {change_str}     Market Cap: {fmt_currency(stock.get('market_cap', 0))}                   ║
╚{line}╝"""

    def _kaz_era_section(self) -> str:
        """Kaz-era performance (new CEO strategy)."""
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
┌─ KAZ-ERA PERFORMANCE (New Strategy since Nov 2025) ────────────────────────┐
│                                                                              │
│  REALIZED (Sold)                    │  UNREALIZED (On Market)                │
│  {sold_icon} {realized.get('profitable', 0)}/{realized.get('count', 0)} profitable ({sold_win:.1f}%)      │  {market_icon} {unrealized.get('above_water', 0)}/{unrealized.get('count', 0)} above water ({above_pct:.1f}%)      │
│    Avg Profit: {fmt_currency(realized.get('avg_profit', 0)):>10}          │    Underwater: {unrealized.get('underwater', 0)} homes                │
│                                                                              │
│  Total: {total} homes  |  Health: {health:.1f}%  |  vs Legacy: +{vs_legacy:.0f}pp                      │
└──────────────────────────────────────────────────────────────────────────────┘"""

    def _earnings_countdown(self) -> str:
        """Earnings countdown section."""
        earnings = self.mkt.get("earnings", {})
        g = self.adv.get("guidance", {})

        next_date = earnings.get("next_date", "Unknown")
        days_until = earnings.get("days_until", 0)
        pct_to_target = g.get("pct_to_target", 0)
        projected = g.get("projected_quarter_revenue", 0)
        target = g.get("q1_target", 595_000_000)

        # Pace indicator
        pace = g.get("pace_vs_required", "unknown")
        pace_icon = {"ahead": "🟢", "on_track": "🟡", "behind": "🔴"}.get(pace, "⚪")

        return f"""
┌─ EARNINGS COUNTDOWN ────────────────────────────────────────────────────────┐
│                                                                              │
│  Next Earnings: {next_date:12}  ({days_until} days)                               │
│                                                                              │
│  Q1 Progress: {fmt_pct(pct_to_target):>6} of {fmt_currency(target)} target                              │
│  Projected:   {fmt_currency(projected):>10}  {pace_icon} {pace.upper():10}                                │
│                                                                              │
│  Last EPS: {earnings.get('last_eps_actual', 0):.2f} (est: {earnings.get('last_eps_estimate', 0):.2f})  Surprise: {fmt_pct(earnings.get('last_surprise_pct', 0)):>6}           │
└──────────────────────────────────────────────────────────────────────────────┘"""

    def _market_context(self) -> str:
        """Macro market context."""
        mortgage = self.mkt.get("mortgage", {})
        # housing = self.mkt.get("housing", {})
        # macro = self.mkt.get("macro", {})
        stock = self.mkt.get("stock", {})

        rate = mortgage.get("rate_30yr", 0)
        rate_month = mortgage.get("month_change")
        rate_year = mortgage.get("year_change")
        payment = mortgage.get("monthly_payment_400k", 0)

        # Stock position
        high_52 = stock.get("week_52_high", 0)
        low_52 = stock.get("week_52_low", 0)
        pct_from_high = stock.get("pct_from_52_high", 0)
        short_pct = stock.get("short_pct_float", 0)

        return f"""
┌─ MARKET CONTEXT ────────────────────────────────────────────────────────────┐
│                                                                              │
│  RATES                              │  STOCK POSITION                        │
│  30Y Mortgage: {rate:>5.2f}%               │  52W High: ${high_52:>6.2f} ({pct_from_high:>+5.1f}%)          │
│  Month Δ: {fmt_change(rate_month, '%'):>10}              │  52W Low:  ${low_52:>6.2f}                    │
│  Year Δ:  {fmt_change(rate_year, '%'):>10}              │  Short %:  {fmt_pct(short_pct):>6}                    │
│  Payment ($400K): {fmt_currency(payment):>8}/mo       │                                        │
└──────────────────────────────────────────────────────────────────────────────┘"""

    def _cohort_section(self) -> str:
        """Cohort performance."""
        cohorts = [
            ("NEW (<90d)", self.m.cohort_new, "The Signal"),
            ("MID (90-180d)", self.m.cohort_mid, ""),
            ("OLD (180-365d)", self.m.cohort_old, ""),
            ("TOXIC (>365d)", self.m.cohort_toxic, "Clearing"),
        ]

        lines = ["""
┌─ COHORT ANALYSIS ───────────────────────────────────────────────────────────┐
│  Cohort            Win Rate    Avg Profit    Count    Margin    Status      │
│  ─────────────────────────────────────────────────────────────────────────  │"""]

        for name, c, note in cohorts:
            win = c.win_rate
            profit = c.avg_profit
            count = c.count
            margin = c.contribution_margin

            if "NEW" in name:
                status = "✓ Signal" if win >= 95 else "! Watch" if win >= 90 else "✗ Alert"
            elif "TOXIC" in name:
                status = "↓ Clear" if count > 0 else "✓ Done"
            else:
                status = ""

            profit_str = fmt_currency(profit)
            if profit < 0:
                profit_str = f"-{fmt_currency(abs(profit))}"

            lines.append(
                f"│  {name:16} {fmt_pct(win):>8}    {profit_str:>10}    {count:>5}    {fmt_pct(margin):>6}    {status:10} │"
            )

        lines.append("└──────────────────────────────────────────────────────────────────────────────┘")
        return "\n".join(lines)

    def _toxic_section(self) -> str:
        """Toxic countdown."""
        t = self.m.toxic
        return f"""
┌─ TOXIC COUNTDOWN ───────────────────────────────────────────────────────────┐
│  Cleared: {t.sold_count:>5}     Remaining: {t.remaining_count:>5}     Progress: {fmt_pct(t.clearance_pct):>6}                  │
│  Avg Loss: {fmt_currency(t.sold_avg_loss):>10}     Weeks to Clear: ~{t.weeks_to_clear:.0f}                              │
└──────────────────────────────────────────────────────────────────────────────┘"""

    def _velocity_pricing(self) -> str:
        """Velocity and pricing."""
        v = self.adv.get("velocity", {})
        p = self.adv.get("pricing", {})

        return f"""
┌─ VELOCITY ──────────────────────────┬─ PRICING ────────────────────────────┐
│  Avg Days to Sale:    {v.get('avg_days_to_sale', 0):>6.0f}       │  Avg Spread:      {fmt_currency(p.get('avg_spread', 0)):>12}       │
│  Median Days:         {v.get('median_days_to_sale', 0):>6.0f}       │  Homes w/ Cuts:   {p.get('homes_with_price_cuts', 0):>5} ({fmt_pct(p.get('homes_with_price_cuts_pct', 0)):>5})  │
│  Sales/Day:           {v.get('sales_per_day_avg', 0):>6.1f}       │  Avg Cuts/Home:   {p.get('avg_cuts_per_home', 0):>6.1f}             │
│  Inventory Turnover:  {v.get('inventory_turnover_days', 0):>6.0f}d      │  Avg Reduction:   {fmt_pct(p.get('avg_price_cut_pct', 0)):>6}             │
└─────────────────────────────────────┴──────────────────────────────────────┘"""

    def _inventory_section(self) -> str:
        """Inventory health."""
        i = self.m.inventory

        total = i.total or 1
        bar_width = 60

        # Calculate proportions
        fresh_pct = i.fresh_count / total
        normal_pct = i.normal_count / total
        stale_pct = i.stale_count / total
        vs_pct = i.very_stale_count / total
        toxic_pct = i.toxic_count / total

        fresh_w = int(fresh_pct * bar_width)
        normal_w = int(normal_pct * bar_width)
        stale_w = int(stale_pct * bar_width)
        vs_w = int(vs_pct * bar_width)
        toxic_w = bar_width - fresh_w - normal_w - stale_w - vs_w

        bar = "█" * fresh_w + "▓" * normal_w + "▒" * stale_w + "░" * vs_w + "·" * toxic_w

        return f"""
┌─ INVENTORY ({i.total:,} homes) ────────────────────────────────────────────────┐
│  [{bar}] │
│  █ Fresh:{i.fresh_count:>4}  ▓ Normal:{i.normal_count:>4}  ▒ Stale:{i.stale_count:>4}  ░ VStale:{i.very_stale_count:>4}  · Toxic:{i.toxic_count:>4}  │
│                                                                              │
│  Avg DOM: {i.avg_dom:>5.0f}d     Legacy (>180d): {fmt_pct(i.legacy_pct):>6}     Unrealized: {fmt_currency(i.total_unrealized_pnl):>10}  │
└──────────────────────────────────────────────────────────────────────────────┘"""

    def _risk_section(self) -> str:
        """Risk dashboard."""
        r = self.adv.get("risk", {})

        return f"""
┌─ RISK ──────────────────────────────────────────────────────────────────────┐
│  Underwater:    {r.get('underwater_count', 0):>5} ({fmt_pct(r.get('underwater_pct', 0)):>5})     Exposure: {fmt_currency(r.get('underwater_total_exposure', 0)):>12}           │
│  Aged+UW:       {r.get('aged_underwater_count', 0):>5}  ← High Risk                                            │
│  Top Market:    {r.get('top_concentration_market', 'N/A'):>5} ({fmt_pct(r.get('top_concentration_pct', 0)):>5})     Markets >10%: {r.get('markets_above_10pct', 0)}                    │
└──────────────────────────────────────────────────────────────────────────────┘"""

    def _market_matrix(self) -> str:
        """Market breakdown."""
        markets = self.adv.get("markets", [])[:8]

        if not markets:
            return ""

        lines = ["""
┌─ MARKET BREAKDOWN ──────────────────────────────────────────────────────────┐
│  State    Inv   Toxic   DOM   Conc%  │  State    Inv   Toxic   DOM   Conc%  │
│  ────────────────────────────────────┼────────────────────────────────────  │"""]

        # Display in two columns
        mid = (len(markets) + 1) // 2
        for i in range(mid):
            left = markets[i] if i < len(markets) else None
            right = markets[i + mid] if i + mid < len(markets) else None

            left_str = f"  {left['state']:>5}  {left['inventory_count']:>5}   {left['toxic_count']:>5}   {left['avg_dom']:>3.0f}   {fmt_pct(left['concentration_pct']):>5}" if left else " " * 36
            right_str = f"  {right['state']:>5}  {right['inventory_count']:>5}   {right['toxic_count']:>5}   {right['avg_dom']:>3.0f}   {fmt_pct(right['concentration_pct']):>5}" if right else " " * 36

            lines.append(f"│{left_str}  │{right_str}  │")

        lines.append("└──────────────────────────────────────┴──────────────────────────────────────┘")
        return "\n".join(lines)

    def _sec_section(self) -> str:
        """SEC filings info."""
        sec = self.mkt.get("sec", {})

        last_10q = sec.get('last_10q') or 'N/A'
        last_10k = sec.get('last_10k') or 'N/A'
        last_8k = sec.get('last_8k') or 'N/A'

        # Financial data from filings
        revenue = sec.get('revenue_from_filing', 0)
        inventory = sec.get('inventory_from_filing', 0)
        cash = sec.get('cash_from_filing', 0)

        return f"""
┌─ SEC FILINGS ───────────────────────────────────────────────────────────────┐
│  Last 10-Q: {last_10q:12}    Last 10-K: {last_10k:12}    Last 8-K: {last_8k:10} │
│  Revenue: {fmt_currency(revenue):>12}    Inventory: {fmt_currency(inventory):>10}    Cash: {fmt_currency(cash):>10}   │
└──────────────────────────────────────────────────────────────────────────────┘"""

    def _parcl_context_section(self) -> str:
        """Parcl market context section."""
        parcl = self.mkt.get("parcl_context", {})
        if not parcl:
            return ""

        markets = parcl.get("markets", {})
        investor = parcl.get("investor_activity", {})

        if not markets:
            return ""

        lines = ["""
┌─ MARKET CONTEXT (Parcl Labs) ──────────────────────────────────────────────┐
│  Market         Listings   Med Sale$    Sales   Mo Supply   Inv Net        │
│  ─────────────────────────────────────────────────────────────────────────  │"""]

        for state, data in list(markets.items())[:6]:
            name = data.get("name", state)[:12]
            listings = data.get("active_listings", 0)
            med_sale = data.get("median_sale_price", 0)
            sales = data.get("sales_count", 0)
            supply = data.get("months_supply", 0)

            # Get investor activity
            inv = investor.get(state, {})
            inv_net = inv.get("large_net", 0)
            inv_str = f"{inv_net:+d}" if inv_net != 0 else "0"

            lines.append(
                f"│  {name:12} {listings:>8,}   {fmt_currency(med_sale):>10}    {sales:>5}   {supply:>6.1f}mo   {inv_str:>6}        │"
            )

        credits = parcl.get("credit_usage", 0)
        ts = parcl.get("timestamp", "")[:10]
        lines.append(f"│  ─────────────────────────────────────────────────────────────────────────  │")
        lines.append(f"│  Fetched: {ts}    Credits used: {credits}                                        │")
        lines.append("└──────────────────────────────────────────────────────────────────────────────┘")

        return "\n".join(lines)

    def _alerts_section(self) -> str:
        """Alerts."""
        if not self.m.alerts:
            return """
┌─ ALERTS ────────────────────────────────────────────────────────────────────┐
│  ✓ All metrics within thresholds                                             │
└──────────────────────────────────────────────────────────────────────────────┘"""

        lines = ["", "┌─ ALERTS ────────────────────────────────────────────────────────────────────┐"]
        for alert in self.m.alerts[:4]:
            lines.append(f"│  ⚠ {alert[:70]:70} │")
        lines.append("└──────────────────────────────────────────────────────────────────────────────┘")
        return "\n".join(lines)

    def _footer(self) -> str:
        return f"""
{'─' * 78}
  Data: Parcl Labs CSV + Yahoo Finance + FRED + SEC EDGAR
  Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
{'─' * 78}
"""

    def generate(self) -> str:
        """Generate full dashboard."""
        sections = [
            self._header(),
            self._kaz_era_section(),  # New CEO strategy first!
            self._earnings_countdown(),
            self._market_context(),
            self._cohort_section(),
            self._toxic_section(),
            self._velocity_pricing(),
            self._inventory_section(),
            self._risk_section(),
            self._market_matrix(),
            self._parcl_context_section(),
            self._sec_section(),
            self._alerts_section(),
            self._footer(),
        ]

        return "\n".join(s for s in sections if s)

    def print(self):
        """Print dashboard."""
        print(self.generate())
