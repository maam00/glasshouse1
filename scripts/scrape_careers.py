#!/usr/bin/env python3
"""
Scrape Opendoor careers data from Greenhouse API.
https://boards-api.greenhouse.io/v1/boards/opendoor/jobs

Tracks hiring trends and provides insights on company health.
"""

import json
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict

GREENHOUSE_API = "https://boards-api.greenhouse.io/v1/boards/opendoor/jobs"

def scrape_careers():
    """Fetch job listings from Greenhouse API."""

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
    }

    print(f"Fetching {GREENHOUSE_API}...")
    response = requests.get(GREENHOUSE_API, headers=headers)
    response.raise_for_status()

    data = response.json()
    jobs = data.get('jobs', [])

    print(f"Found {len(jobs)} open positions")

    # Process jobs
    careers_data = {
        "scraped_at": datetime.now().isoformat(),
        "source": GREENHOUSE_API,
        "total_jobs": len(jobs),
        "jobs": [],
        "by_department": {},
        "by_location": {},
        "summary": {}
    }

    # Department mapping (normalize department names)
    dept_mapping = {
        'sales': 'Sales & Customer Experience',
        'customer': 'Sales & Customer Experience',
        'experience': 'Sales & Customer Experience',
        'engineering': 'Engineering & Technology',
        'software': 'Engineering & Technology',
        'data': 'Engineering & Technology',
        'ai': 'Engineering & Technology',
        'ml': 'Engineering & Technology',
        'it': 'Engineering & Technology',
        'research': 'Engineering & Technology',
        'finance': 'Finance & Operations',
        'accounting': 'Finance & Operations',
        'audit': 'Finance & Operations',
        'payroll': 'Finance & Operations',
        'operations': 'Finance & Operations',
        'portfolio': 'Finance & Operations',
        'marketing': 'Marketing & Growth',
        'growth': 'Marketing & Growth',
        'brand': 'Marketing & Growth',
        'design': 'Marketing & Growth',
        'product': 'Product & Design',
        'ux': 'Product & Design',
    }

    by_dept = defaultdict(list)
    by_location = defaultdict(list)

    for job in jobs:
        job_id = job.get('id')
        title = job.get('title', 'Unknown')
        location = job.get('location', {}).get('name', 'Remote')
        updated_at = job.get('updated_at', '')
        absolute_url = job.get('absolute_url', '')

        # Determine department from title
        title_lower = title.lower()
        department = 'Other'
        for keyword, dept in dept_mapping.items():
            if keyword in title_lower:
                department = dept
                break

        # Special cases
        if 'manager' in title_lower and 'sales' not in title_lower:
            if 'finance' in title_lower or 'audit' in title_lower:
                department = 'Finance & Operations'
        if 'director' in title_lower:
            if 'sales' in title_lower:
                department = 'Sales & Customer Experience'
            elif 'brand' in title_lower or 'design' in title_lower:
                department = 'Marketing & Growth'
        if 'advisor' in title_lower or 'advocate' in title_lower:
            department = 'Sales & Customer Experience'
        if 'partner' in title_lower and 'trade' in title_lower:
            department = 'Operations'
        if 'listing partner' in title_lower or 'agent' in title_lower:
            department = 'Sales & Customer Experience'

        job_entry = {
            'id': job_id,
            'title': title,
            'department': department,
            'location': location,
            'url': absolute_url,
            'updated_at': updated_at
        }

        careers_data['jobs'].append(job_entry)
        by_dept[department].append(job_entry)

        # Parse location for grouping
        loc_parts = location.split(';')
        for loc in loc_parts:
            loc = loc.strip()
            if loc:
                by_location[loc].append(job_entry)

    # Build department summary
    for dept, dept_jobs in sorted(by_dept.items(), key=lambda x: -len(x[1])):
        careers_data['by_department'][dept] = {
            'count': len(dept_jobs),
            'jobs': [{'title': j['title'], 'location': j['location']} for j in dept_jobs]
        }

    # Build location summary
    for loc, loc_jobs in sorted(by_location.items(), key=lambda x: -len(x[1])):
        careers_data['by_location'][loc] = {
            'count': len(loc_jobs),
            'titles': [j['title'] for j in loc_jobs[:5]]  # Top 5 titles per location
        }

    # Generate summary
    dept_counts = {dept: len(jobs) for dept, jobs in by_dept.items()}
    top_dept = max(dept_counts, key=dept_counts.get) if dept_counts else 'Unknown'

    top_locations = sorted(by_location.items(), key=lambda x: -len(x[1]))[:5]

    # Identify key/senior roles
    senior_roles = [j for j in careers_data['jobs']
                    if any(x in j['title'].lower() for x in ['director', 'senior', 'manager', 'lead', 'head'])]

    # Identify engineering/AI roles specifically
    ai_roles = [j for j in careers_data['jobs']
                if any(x in j['title'].lower() for x in ['ai', 'ml', 'machine learning', 'data scientist', 'research'])]

    careers_data['summary'] = {
        'total_positions': len(jobs),
        'departments': len(by_dept),
        'locations': len(by_location),
        'top_department': top_dept,
        'top_department_count': dept_counts.get(top_dept, 0),
        'senior_roles_count': len(senior_roles),
        'ai_ml_roles_count': len(ai_roles),
        'top_locations': [{'name': loc, 'count': len(jobs)} for loc, jobs in top_locations],
        'hiring_signal': categorize_hiring(len(jobs), dept_counts)
    }

    return careers_data


def categorize_hiring(total_jobs, dept_counts):
    """Categorize hiring activity level and focus."""

    eng_count = dept_counts.get('Engineering & Technology', 0)
    sales_count = dept_counts.get('Sales & Customer Experience', 0)

    # Determine hiring intensity
    if total_jobs >= 75:
        intensity = 'aggressive'
    elif total_jobs >= 50:
        intensity = 'active'
    elif total_jobs >= 25:
        intensity = 'moderate'
    else:
        intensity = 'limited'

    # Determine focus area
    if eng_count > sales_count and eng_count > 10:
        focus = 'technology'
    elif sales_count > eng_count and sales_count > 10:
        focus = 'growth'
    elif eng_count > 0 and sales_count > 0:
        focus = 'balanced'
    else:
        focus = 'maintenance'

    return {
        'intensity': intensity,
        'focus': focus,
        'description': f"{intensity.capitalize()} hiring with {focus} focus"
    }


def main():
    output_dir = Path(__file__).parent.parent / "outputs"
    output_dir.mkdir(exist_ok=True)

    print("Scraping Opendoor careers...")
    data = scrape_careers()

    # Save data
    timestamp = datetime.now().strftime("%Y-%m-%d")
    output_file = output_dir / f"careers_{timestamp}.json"

    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\nSaved to: {output_file}")

    # Print summary
    summary = data.get('summary', {})
    print("\n" + "=" * 50)
    print("  CAREERS SUMMARY")
    print("=" * 50)
    print(f"  Total Positions: {summary.get('total_positions', 0)}")
    print(f"  Departments: {summary.get('departments', 0)}")
    print(f"  Top Department: {summary.get('top_department', 'N/A')} ({summary.get('top_department_count', 0)} roles)")
    print(f"  Senior/Leadership Roles: {summary.get('senior_roles_count', 0)}")
    print(f"  AI/ML Roles: {summary.get('ai_ml_roles_count', 0)}")
    print(f"  Hiring Signal: {summary.get('hiring_signal', {}).get('description', 'N/A')}")
    print("=" * 50)

    print("\n  BY DEPARTMENT:")
    for dept, info in data.get('by_department', {}).items():
        print(f"    {dept}: {info['count']} positions")

    print("\n  TOP LOCATIONS:")
    for loc_info in summary.get('top_locations', [])[:5]:
        print(f"    {loc_info['name']}: {loc_info['count']} positions")

    print("=" * 50)

    return data


if __name__ == "__main__":
    main()
