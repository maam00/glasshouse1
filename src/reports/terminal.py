"""
Glass House Terminal Report
============================
Screenshot-ready terminal output.
"""

from typing import Dict, Any, Optional
from ..db.database import DailyMetrics


def fmt_currency(val: float) -> str:
    """Format as currency."""
    if val is None:
        return "$0"
    if abs(val) >= 1_000_000:
        return f"${val/1_000_000:.2f}M"
    elif abs(val) >= 1_000:
        return f"${val/1_000:.1f}K"
    else:
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


class TerminalReport:
    """Generate terminal-friendly reports."""

    WIDTH = 62

    def __init__(self, metrics: DailyMetrics, previous: Dict = None):
        self.m = metrics
        self.prev = previous

    def _header(self) -> str:
        line = "=" * self.WIDTH
        return f"""
{line}
  GLASS HOUSE \u2014 $OPEN Daily Brief
  {self.m.date}
{line}"""

    def _cohorts(self) -> str:
        """Cohort analysis section."""
        lines = ["\nCOHORT PERFORMANCE"]

        cohorts = [
            (self.m.cohort_new, "signal"),
            (self.m.cohort_mid, None),
            (self.m.cohort_old, "watch"),
            (self.m.cohort_toxic, "clear"),
        ]

        for c, note in cohorts:
            status = ""
            if c.name.startswith("New") and c.win_rate >= 95:
                status = " [OK]"
            elif c.name.startswith("New") and c.win_rate < 95:
                status = " [!]"

            profit_str = fmt_currency(c.avg_profit)
            if c.avg_profit < 0:
                profit_str = f"-{fmt_currency(abs(c.avg_profit))}"

            lines.append(
                f"  {c.name:16} {fmt_pct(c.win_rate):>7} win | "
                f"{profit_str:>10} avg | {fmt_num(c.count):>5} sold{status}"
            )

        return "\n".join(lines)

    def _toxic(self) -> str:
        """Toxic inventory section."""
        t = self.m.toxic
        weeks_str = f"~{t.weeks_to_clear:.0f} weeks" if t.weeks_to_clear < 100 else "N/A"

        return f"""
TOXIC COUNTDOWN
  Cleared:     {fmt_num(t.sold_count):>6}
  Remaining:   {fmt_num(t.remaining_count):>6}
  Progress:    {fmt_pct(t.clearance_pct):>6}
  Pace:        {weeks_str:>6} to clear
  Avg Loss:    {fmt_currency(t.sold_avg_loss):>10}"""

    def _performance(self) -> str:
        """Overall performance section."""
        p = self.m.performance

        margin_status = ""
        if 5 <= p.contribution_margin <= 7:
            margin_status = " [TARGET]"
        elif p.contribution_margin > 7:
            margin_status = " [ABOVE]"
        elif p.contribution_margin > 0:
            margin_status = " [BELOW]"

        return f"""
OVERALL
  Win Rate:    {fmt_pct(p.win_rate):>10}
  Margin:      {fmt_pct(p.contribution_margin):>10}{margin_status}
  Avg Profit:  {fmt_currency(p.avg_profit):>10}
  Sold Today:  {fmt_num(p.homes_sold_today):>10}
  Revenue:     {fmt_currency(p.revenue_today):>10} today
               {fmt_currency(p.revenue_total):>10} total"""

    def _inventory(self) -> str:
        """Inventory health section."""
        i = self.m.inventory

        return f"""
INVENTORY
  Total:       {fmt_num(i.total):>10}
  Fresh:       {fmt_num(i.fresh_count):>10} (<30d)
  Normal:      {fmt_num(i.normal_count):>10} (30-90d)
  Stale:       {fmt_num(i.stale_count):>10} (90-180d)
  Very Stale:  {fmt_num(i.very_stale_count):>10} (180-365d)
  Toxic:       {fmt_num(i.toxic_count):>10} (>365d)
  Legacy %:    {fmt_pct(i.legacy_pct):>10}
  Avg DOM:     {i.avg_dom:>10.0f} days
  Unrealized:  {fmt_currency(i.total_unrealized_pnl):>10}"""

    def _geographic(self) -> str:
        """Geographic breakdown section."""
        geo = self.m.geographic
        if not geo.get("sales_by_state"):
            return ""

        lines = ["\nTOP MARKETS (by sales today)"]

        # Sort by count
        sorted_states = sorted(
            geo["sales_by_state"].items(),
            key=lambda x: -x[1]
        )[:5]

        for state, count in sorted_states:
            win_rate = geo.get("win_rate_by_state", {}).get(state, 0)
            inv_count = geo.get("inventory_by_state", {}).get(state, 0)
            lines.append(
                f"  {state:>5}: {count:>4} sold | {fmt_pct(win_rate):>6} win | {inv_count:>4} inv"
            )

        return "\n".join(lines)

    def _alerts(self) -> str:
        """Alerts section."""
        if not self.m.alerts:
            return "\nALERTS\n  None today - metrics stable"

        lines = ["\nALERTS"]
        for alert in self.m.alerts:
            lines.append(f"  [!] {alert}")

        return "\n".join(lines)

    def _comparison(self) -> str:
        """Week-over-week comparison."""
        if not self.prev:
            return ""

        lines = ["\nWoW COMPARISON"]

        prev_perf = self.prev.get("performance", {})
        curr_perf = self.m.performance

        comparisons = [
            ("Win Rate", prev_perf.get("win_rate", 0), curr_perf.win_rate, "%"),
            ("Margin", prev_perf.get("contribution_margin", 0), curr_perf.contribution_margin, "%"),
            ("Homes Sold", prev_perf.get("homes_sold_total", 0), curr_perf.homes_sold_total, ""),
        ]

        for name, prev_val, curr_val, suffix in comparisons:
            if prev_val > 0:
                change = ((curr_val - prev_val) / prev_val) * 100
                direction = "+" if change > 0 else ""
                lines.append(
                    f"  {name:12} {prev_val:.1f}{suffix} -> {curr_val:.1f}{suffix} ({direction}{change:.1f}%)"
                )

        return "\n".join(lines)

    def generate(self) -> str:
        """Generate full terminal report."""
        sections = [
            self._header(),
            self._cohorts(),
            self._toxic(),
            self._performance(),
            self._inventory(),
            self._geographic(),
            self._alerts(),
            self._comparison(),
        ]

        report = "\n".join(s for s in sections if s)
        report += f"\n\n{'=' * self.WIDTH}\n"

        return report

    def print(self):
        """Print report to terminal."""
        print(self.generate())
