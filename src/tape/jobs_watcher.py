"""
Jobs Watcher - Opendoor Careers Monitoring
==========================================
Scrapes Opendoor careers page, diffs postings, and classifies roles.

Role buckets:
- AI/Platform: ML, AI, data science, platform engineering
- Pricing/Risk: Pricing, valuation, risk, analytics
- Ops: Operations, customer experience, field ops
- Sales: Sales, BD, partnerships, marketing
- G&A: Finance, legal, HR, admin, recruiting
"""

import json
import logging
import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class JobPosting:
    """A single job posting."""
    id: str
    title: str
    department: str
    location: str
    url: str
    bucket: str
    first_seen: str
    last_seen: str


@dataclass
class JobsDiff:
    """Diff between two job snapshots."""
    new_postings: List[JobPosting]
    removed_postings: List[JobPosting]
    by_bucket: Dict[str, Dict[str, int]]
    total_open: int
    net_change: int
    timestamp: str


# Keywords for classifying roles into buckets
BUCKET_KEYWORDS = {
    'ai_platform': [
        'machine learning', 'ml ', 'ai ', 'artificial intelligence',
        'data science', 'data scientist', 'platform', 'infrastructure',
        'backend', 'systems', 'architect', 'engineering manager',
        'staff engineer', 'principal engineer', 'tech lead',
    ],
    'pricing_risk': [
        'pricing', 'valuation', 'risk', 'analytics', 'economist',
        'quantitative', 'modeling', 'forecasting', 'actuar',
        'underwriting', 'credit', 'data analyst',
    ],
    'ops': [
        'operations', 'customer', 'experience', 'field', 'market',
        'coordinator', 'specialist', 'support', 'service',
        'transaction', 'escrow', 'title', 'closing',
    ],
    'sales': [
        'sales', 'business development', 'partnership', 'marketing',
        'growth', 'acquisition', 'account', 'revenue', 'commercial',
    ],
    'ga': [
        'finance', 'accounting', 'legal', 'counsel', 'hr ', 'human resources',
        'recruiting', 'recruiter', 'talent', 'people', 'admin',
        'compliance', 'audit', 'controller', 'treasury',
    ],
}


def classify_role(title: str, department: str) -> str:
    """Classify a job posting into a bucket based on title and department."""
    text = f"{title} {department}".lower()

    # Check each bucket's keywords
    scores = {bucket: 0 for bucket in BUCKET_KEYWORDS}

    for bucket, keywords in BUCKET_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                scores[bucket] += 1

    # Return highest scoring bucket, or 'ga' as default
    best_bucket = max(scores, key=scores.get)
    if scores[best_bucket] == 0:
        return 'ga'  # Default to G&A if no matches
    return best_bucket


class JobsWatcher:
    """Watch Opendoor careers page for changes."""

    CAREERS_URL = "https://www.opendoor.com/careers"
    GREENHOUSE_API = "https://boards-api.greenhouse.io/v1/boards/opendoor/jobs"

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or Path(__file__).parent.parent.parent / "outputs" / "tape"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_job_id(self, job: Dict) -> str:
        """Generate stable ID for a job posting."""
        key = f"{job.get('title', '')}-{job.get('location', {}).get('name', '')}"
        return hashlib.md5(key.encode()).hexdigest()[:12]

    def fetch_current_jobs(self) -> List[JobPosting]:
        """Fetch current job postings from Greenhouse API."""
        logger.info("Fetching Opendoor job postings...")

        try:
            response = requests.get(self.GREENHOUSE_API, timeout=30)
            response.raise_for_status()
            data = response.json()

            jobs = []
            today = datetime.now().strftime('%Y-%m-%d')

            for job in data.get('jobs', []):
                title = job.get('title', '')
                dept = job.get('departments', [{}])[0].get('name', '') if job.get('departments') else ''
                location = job.get('location', {}).get('name', '')
                url = job.get('absolute_url', '')

                posting = JobPosting(
                    id=self._get_job_id(job),
                    title=title,
                    department=dept,
                    location=location,
                    url=url,
                    bucket=classify_role(title, dept),
                    first_seen=today,
                    last_seen=today,
                )
                jobs.append(posting)

            logger.info(f"Found {len(jobs)} open positions")
            return jobs

        except requests.RequestException as e:
            logger.error(f"Failed to fetch jobs: {e}")
            return []

    def load_previous_snapshot(self) -> Dict[str, JobPosting]:
        """Load the previous jobs snapshot."""
        snapshot_file = self.cache_dir / "jobs_snapshot.json"

        if not snapshot_file.exists():
            return {}

        try:
            with open(snapshot_file) as f:
                data = json.load(f)
            return {j['id']: JobPosting(**j) for j in data.get('jobs', [])}
        except Exception as e:
            logger.warning(f"Could not load previous snapshot: {e}")
            return {}

    def save_snapshot(self, jobs: List[JobPosting]):
        """Save current jobs snapshot."""
        snapshot_file = self.cache_dir / "jobs_snapshot.json"

        data = {
            'timestamp': datetime.now().isoformat(),
            'total': len(jobs),
            'jobs': [asdict(j) for j in jobs],
        }

        with open(snapshot_file, 'w') as f:
            json.dump(data, f, indent=2)

    def diff_jobs(self, current: List[JobPosting], previous: Dict[str, JobPosting]) -> JobsDiff:
        """Compare current jobs to previous snapshot."""
        current_ids = {j.id for j in current}
        previous_ids = set(previous.keys())

        new_ids = current_ids - previous_ids
        removed_ids = previous_ids - current_ids

        new_postings = [j for j in current if j.id in new_ids]
        removed_postings = [previous[id] for id in removed_ids]

        # Count by bucket
        by_bucket = {
            'ai_platform': {'current': 0, 'new': 0, 'removed': 0},
            'pricing_risk': {'current': 0, 'new': 0, 'removed': 0},
            'ops': {'current': 0, 'new': 0, 'removed': 0},
            'sales': {'current': 0, 'new': 0, 'removed': 0},
            'ga': {'current': 0, 'new': 0, 'removed': 0},
        }

        for j in current:
            by_bucket[j.bucket]['current'] += 1

        for j in new_postings:
            by_bucket[j.bucket]['new'] += 1

        for j in removed_postings:
            by_bucket[j.bucket]['removed'] += 1

        return JobsDiff(
            new_postings=new_postings,
            removed_postings=removed_postings,
            by_bucket=by_bucket,
            total_open=len(current),
            net_change=len(new_postings) - len(removed_postings),
            timestamp=datetime.now().isoformat(),
        )

    def check_for_changes(self) -> Optional[JobsDiff]:
        """Check for job posting changes and return diff."""
        current_jobs = self.fetch_current_jobs()

        if not current_jobs:
            return None

        previous = self.load_previous_snapshot()
        diff = self.diff_jobs(current_jobs, previous)

        # Update first_seen for existing jobs
        for job in current_jobs:
            if job.id in previous:
                job.first_seen = previous[job.id].first_seen

        self.save_snapshot(current_jobs)

        return diff

    def get_tape_items(self, diff: JobsDiff, max_items: int = 2) -> List[Dict[str, Any]]:
        """Generate tape items from job changes."""
        items = []

        # Significant new hires in strategic areas
        ai_new = [j for j in diff.new_postings if j.bucket == 'ai_platform']
        pricing_new = [j for j in diff.new_postings if j.bucket == 'pricing_risk']

        if ai_new:
            items.append({
                'type': 'jobs',
                'category': 'hiring',
                'materiality': 'medium' if len(ai_new) >= 3 else 'low',
                'headline': f"Opendoor posted {len(ai_new)} AI/Platform roles",
                'detail': f"New postings: {', '.join(j.title[:30] for j in ai_new[:3])}",
                'bucket': 'ai_platform',
                'timestamp': diff.timestamp,
            })

        if pricing_new:
            items.append({
                'type': 'jobs',
                'category': 'hiring',
                'materiality': 'medium' if len(pricing_new) >= 2 else 'low',
                'headline': f"Opendoor posted {len(pricing_new)} Pricing/Risk roles",
                'detail': f"New postings: {', '.join(j.title[:30] for j in pricing_new[:3])}",
                'bucket': 'pricing_risk',
                'timestamp': diff.timestamp,
            })

        # Net change signal
        if abs(diff.net_change) >= 5:
            direction = "added" if diff.net_change > 0 else "removed"
            items.append({
                'type': 'jobs',
                'category': 'workforce',
                'materiality': 'medium',
                'headline': f"Opendoor {direction} {abs(diff.net_change)} job postings net",
                'detail': f"Total open positions: {diff.total_open}",
                'bucket': 'workforce',
                'timestamp': diff.timestamp,
            })

        return items[:max_items]


def check_opendoor_jobs() -> Optional[JobsDiff]:
    """Convenience function to check for job changes."""
    watcher = JobsWatcher()
    return watcher.check_for_changes()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    watcher = JobsWatcher()
    diff = watcher.check_for_changes()

    if diff:
        print(f"\n{'='*60}")
        print(f"  OPENDOOR JOBS UPDATE")
        print(f"{'='*60}")
        print(f"  Total Open: {diff.total_open}")
        print(f"  Net Change: {diff.net_change:+d}")
        print(f"\n  By Bucket:")
        for bucket, counts in diff.by_bucket.items():
            print(f"    {bucket:<15} {counts['current']:>3} open (+{counts['new']}, -{counts['removed']})")
        print(f"\n  New Postings ({len(diff.new_postings)}):")
        for j in diff.new_postings[:5]:
            print(f"    - {j.title} ({j.bucket})")
        print(f"{'='*60}\n")
