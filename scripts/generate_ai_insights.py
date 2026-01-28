#!/usr/bin/env python3
"""
Generate AI insights for the Glass House dashboard using Claude API.
"""

import json
import os
from pathlib import Path
from datetime import datetime
import anthropic

def load_dashboard_data(path: Path) -> dict:
    """Load the unified dashboard data."""
    with open(path) as f:
        return json.load(f)

def generate_insights(data: dict) -> dict:
    """Call Claude API to generate insights from dashboard data."""

    client = anthropic.Anthropic()

    # Prepare context for Claude
    context = f"""You are an expert stock analyst providing insights on Opendoor ($OPEN).

Here is the current Q1 2026 dashboard data:

VELOCITY METRICS:
- Q1 Sales: {data['velocity']['q1_sales']}
- Q1 Revenue: ${data['velocity']['q1_revenue']:,.0f}
- Daily Avg Sales: {data['velocity']['daily_avg_sales']:.1f} homes/day
- Best Day: {data['velocity']['best_day']} ({data['velocity']['best_day_sales']} sales)

GUIDANCE TRACKING:
- Q1 Revenue Target: ${data['guidance']['q1_target']:,.0f}
- Current Pacing: {data['guidance']['pacing_pct']:.1f}%
- Days Elapsed: {data['guidance']['days_elapsed']}
- Days Remaining: {data['guidance']['days_remaining']}
- On Track: {data['guidance']['on_track']}

P&L METRICS:
- Win Rate: {data['pnl']['win_rate']:.1f}%
- Avg Profit (wins): ${data['pnl']['avg_profit']:,.0f}
- Avg Loss (losses): ${data['pnl']['avg_loss']:,.0f}
- Total Realized: ${data['pnl']['total_realized']:,.0f}

WEEKLY COMPARISON:
- This Week Sales: {data['this_week']['sales']}
- Last Week Sales: {data['last_week']['sales']}
- Week-over-Week Sales Change: {data['wow_change']['sales_pct']:.1f}%
- Week-over-Week Revenue Change: {data['wow_change']['revenue_pct']:.1f}%
"""

    prompt = """Based on this Opendoor data, generate 3 SHORT insights for the dashboard.

Requirements:
1. Each insight should be 1 sentence (max 15 words)
2. Be specific with numbers
3. Focus on: velocity trend, guidance risk, and one notable pattern
4. Tone: professional, direct, no fluff

Return ONLY valid JSON in this exact format:
{
    "velocity_insight": "...",
    "guidance_insight": "...",
    "pattern_insight": "..."
}"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[
            {"role": "user", "content": context + "\n\n" + prompt}
        ]
    )

    # Parse the JSON response
    response_text = response.content[0].text.strip()

    # Handle potential markdown code blocks
    if response_text.startswith("```"):
        response_text = response_text.split("```")[1]
        if response_text.startswith("json"):
            response_text = response_text[4:]

    insights = json.loads(response_text)
    insights['generated_at'] = datetime.now().isoformat()

    return insights

def update_dashboard_data(data_path: Path, insights: dict):
    """Add insights to dashboard data JSON."""
    with open(data_path) as f:
        data = json.load(f)

    data['ai_insights'] = insights

    with open(data_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Updated dashboard data with AI insights")

def main():
    output_dir = Path(__file__).parent.parent / "outputs"
    dashboard_path = output_dir / "unified_dashboard_data.json"

    if not dashboard_path.exists():
        print(f"Error: Dashboard data not found at {dashboard_path}")
        return 1

    print("Loading dashboard data...")
    data = load_dashboard_data(dashboard_path)

    print("Generating AI insights...")
    insights = generate_insights(data)

    print("\nGenerated Insights:")
    print(f"  Velocity: {insights['velocity_insight']}")
    print(f"  Guidance: {insights['guidance_insight']}")
    print(f"  Pattern:  {insights['pattern_insight']}")

    update_dashboard_data(dashboard_path, insights)

    return 0

if __name__ == "__main__":
    exit(main())
