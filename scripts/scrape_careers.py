#!/usr/bin/env python3
"""
Scrape Opendoor careers data from Greenhouse API.
https://boards-api.greenhouse.io/v1/boards/opendoor/jobs

Tracks hiring trends and provides AI-powered strategic insights.
"""

import json
import os
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

GREENHOUSE_API = "https://boards-api.greenhouse.io/v1/boards/opendoor/jobs"
GREENHOUSE_API_FULL = "https://boards-api.greenhouse.io/v1/boards/opendoor/jobs?content=true"

def scrape_careers():
    """Fetch job listings from Greenhouse API with full content."""

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
    }

    # Fetch with full content for AI analysis
    print(f"Fetching {GREENHOUSE_API_FULL}...")
    response = requests.get(GREENHOUSE_API_FULL, headers=headers)
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

        # Get content/description if available
        content = job.get('content', '')

        job_entry = {
            'id': job_id,
            'title': title,
            'department': department,
            'location': location,
            'url': absolute_url,
            'updated_at': updated_at,
            'content': content[:2000] if content else ''  # Truncate for storage
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


def analyze_careers_with_ai(careers_data):
    """Use Claude to extract strategic insights from job listings."""

    if not HAS_ANTHROPIC:
        print("Anthropic library not available, skipping AI analysis")
        return None

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print("No ANTHROPIC_API_KEY found, skipping AI analysis")
        return None

    # Prepare job data for analysis - focus on senior/strategic roles
    strategic_jobs = []
    for job in careers_data.get('jobs', []):
        title_lower = job['title'].lower()
        # Include leadership, AI/ML, and other strategic roles
        is_strategic = any(x in title_lower for x in [
            'director', 'head', 'vp', 'vice president', 'chief',
            'senior', 'lead', 'principal', 'staff',
            'ai', 'ml', 'machine learning', 'data scientist', 'research',
            'strategy', 'analytics'
        ])
        if is_strategic and job.get('content'):
            strategic_jobs.append({
                'title': job['title'],
                'department': job['department'],
                'location': job['location'],
                'description': job['content'][:1500]  # Keep descriptions focused
            })

    if not strategic_jobs:
        print("No strategic jobs with descriptions found")
        return None

    # Build prompt for Claude
    jobs_text = ""
    for job in strategic_jobs[:20]:  # Limit to top 20 to manage token usage
        jobs_text += f"\n---\nTITLE: {job['title']}\nDEPT: {job['department']}\nLOCATION: {job['location']}\nDESCRIPTION:\n{job['description']}\n"

    prompt = f"""Analyze these Opendoor job listings and extract ONLY material strategic insights for shareholders/investors.

DO NOT include:
- Basic headcount numbers
- Generic observations like "they're hiring engineers"
- Obvious statements

DO extract specific insights like:
- Leadership hires that signal strategic shifts (e.g., "Hiring Head of AI Pricing suggests investment in algorithmic pricing")
- Specific AI/ML capabilities being built (what KIND of AI work based on job requirements)
- New market or product initiatives (based on new role types)
- Compensation competitiveness signals if mentioned
- Technology stack changes (specific technologies mentioned)
- Organizational changes (new teams being formed)

Format: Return 3-5 bullet points, each one sentence, focused on MATERIAL insights only. Start each with a clear signal/implication.

JOB LISTINGS:
{jobs_text}

Return JSON format:
{{"insights": ["insight 1", "insight 2", ...]}}"""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        print("Analyzing job listings with Claude...")

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = response.content[0].text.strip()

        # Parse JSON from response
        import re
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if json_match:
            result = json.loads(json_match.group())
            insights = result.get('insights', [])
            print(f"Generated {len(insights)} strategic insights")
            return insights
        else:
            print("Could not parse AI response")
            return None

    except Exception as e:
        print(f"AI analysis error: {e}")
        return None


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

    # Run AI analysis for strategic insights
    ai_insights = analyze_careers_with_ai(data)
    if ai_insights:
        data['ai_insights'] = ai_insights
    else:
        data['ai_insights'] = []

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

    # Print AI insights
    if data.get('ai_insights'):
        print("\n" + "=" * 50)
        print("  AI STRATEGIC INSIGHTS")
        print("=" * 50)
        for insight in data['ai_insights']:
            print(f"  • {insight}")

    print("=" * 50)

    return data


if __name__ == "__main__":
    main()
