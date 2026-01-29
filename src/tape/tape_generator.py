"""
Tape Generator - Daily Intelligence Summary
===========================================
Combines signals from jobs_watcher and filings_watcher into a daily tape.
Outputs max 3 bullet items ranked by materiality.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
from dataclasses import dataclass, asdict

from .jobs_watcher import JobsWatcher
from .filings_watcher import FilingsWatcher

logger = logging.getLogger(__name__)


@dataclass
class TapeItem:
    """A single item on the daily tape."""
    type: str           # 'filing', 'jobs', 'market', 'news'
    category: str       # Subcategory
    materiality: str    # 'high', 'medium', 'low'
    headline: str       # Short headline
    detail: str         # Additional context
    timestamp: str      # When this happened
    source: str = ''    # Where this came from
    url: str = ''       # Link if available


@dataclass
class DailyTape:
    """The daily intelligence tape."""
    date: str
    items: List[TapeItem]
    summary: str
    generated_at: str


MATERIALITY_ORDER = {'high': 0, 'medium': 1, 'low': 2}


class TapeGenerator:
    """Generate the daily intel tape."""

    def __init__(self, output_dir: Path = None):
        self.output_dir = output_dir or Path(__file__).parent.parent.parent / "outputs" / "tape"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.jobs_watcher = JobsWatcher(self.output_dir)
        self.filings_watcher = FilingsWatcher(self.output_dir)

    def collect_items(self) -> List[Dict[str, Any]]:
        """Collect items from all sources."""
        items = []

        # Jobs changes
        try:
            jobs_diff = self.jobs_watcher.check_for_changes()
            if jobs_diff:
                items.extend(self.jobs_watcher.get_tape_items(jobs_diff))
        except Exception as e:
            logger.warning(f"Jobs watcher failed: {e}")

        # SEC filings
        try:
            filing_alerts = self.filings_watcher.check_for_new_filings(days=7)
            if filing_alerts:
                items.extend(self.filings_watcher.get_tape_items(filing_alerts))
        except Exception as e:
            logger.warning(f"Filings watcher failed: {e}")

        return items

    def rank_items(self, items: List[Dict[str, Any]], max_items: int = 3) -> List[TapeItem]:
        """Rank items by materiality and return top N."""
        # Sort by materiality
        sorted_items = sorted(
            items,
            key=lambda x: MATERIALITY_ORDER.get(x.get('materiality', 'low'), 3)
        )

        # Convert to TapeItem
        tape_items = []
        for item in sorted_items[:max_items]:
            tape_items.append(TapeItem(
                type=item.get('type', 'unknown'),
                category=item.get('category', ''),
                materiality=item.get('materiality', 'low'),
                headline=item.get('headline', ''),
                detail=item.get('detail', ''),
                timestamp=item.get('timestamp', datetime.now().isoformat()),
                source=item.get('source', ''),
                url=item.get('url', ''),
            ))

        return tape_items

    def generate_summary(self, items: List[TapeItem]) -> str:
        """Generate a one-line summary of the tape."""
        if not items:
            return "No material updates today"

        high_count = sum(1 for i in items if i.materiality == 'high')
        types = set(i.type for i in items)

        if high_count > 0:
            return f"{high_count} high-priority item(s): {items[0].headline}"
        else:
            return f"{len(items)} update(s) from {', '.join(types)}"

    def generate_tape(self, max_items: int = 3) -> DailyTape:
        """Generate the daily tape."""
        logger.info("Generating daily tape...")

        items = self.collect_items()
        ranked_items = self.rank_items(items, max_items)
        summary = self.generate_summary(ranked_items)

        tape = DailyTape(
            date=datetime.now().strftime('%Y-%m-%d'),
            items=ranked_items,
            summary=summary,
            generated_at=datetime.now().isoformat(),
        )

        # Save tape
        self.save_tape(tape)

        return tape

    def save_tape(self, tape: DailyTape):
        """Save tape to JSON file."""
        tape_file = self.output_dir / "tape.json"

        # Load existing tapes (keep last 7 days)
        existing = []
        if tape_file.exists():
            try:
                with open(tape_file) as f:
                    data = json.load(f)
                existing = data.get('tapes', [])
            except Exception:
                pass

        # Add current tape
        tape_dict = {
            'date': tape.date,
            'summary': tape.summary,
            'generated_at': tape.generated_at,
            'items': [asdict(i) for i in tape.items],
        }

        # Remove existing entry for today if present
        existing = [t for t in existing if t.get('date') != tape.date]
        existing.insert(0, tape_dict)

        # Keep only last 7 days
        cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        existing = [t for t in existing if t.get('date', '') >= cutoff]

        # Save
        output = {
            'last_updated': datetime.now().isoformat(),
            'tapes': existing,
        }

        with open(tape_file, 'w') as f:
            json.dump(output, f, indent=2)

        logger.info(f"Saved tape to {tape_file}")

    def load_tape(self, days: int = 7) -> List[Dict[str, Any]]:
        """Load recent tapes from file."""
        tape_file = self.output_dir / "tape.json"

        if not tape_file.exists():
            return []

        try:
            with open(tape_file) as f:
                data = json.load(f)
            return data.get('tapes', [])[:days]
        except Exception as e:
            logger.warning(f"Could not load tape: {e}")
            return []

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get tape data formatted for dashboard display."""
        tapes = self.load_tape(days=7)

        # Flatten items for display
        all_items = []
        for tape in tapes:
            date = tape.get('date', '')
            for item in tape.get('items', []):
                item['date'] = date
                all_items.append(item)

        return {
            'last_updated': datetime.now().isoformat(),
            'item_count': len(all_items),
            'items': all_items[:10],  # Max 10 items for UI
            'by_date': tapes,
        }


def generate_daily_tape() -> DailyTape:
    """Convenience function to generate daily tape."""
    generator = TapeGenerator()
    return generator.generate_tape()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    tape = generate_daily_tape()

    print(f"\n{'='*60}")
    print(f"  DAILY TAPE - {tape.date}")
    print(f"{'='*60}")
    print(f"  {tape.summary}")
    print(f"\n  Items:")
    for i, item in enumerate(tape.items, 1):
        mat_icon = {'high': '!!!', 'medium': '!!', 'low': '!'}.get(item.materiality, '')
        print(f"\n  {i}. {mat_icon} [{item.type}] {item.headline}")
        if item.detail:
            print(f"     {item.detail}")
    print(f"\n{'='*60}\n")
