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

    # Calculate additional metrics
    avg_price = data['velocity'].get('avg_home_price', 0)
    homes_needed_per_day = 29  # $1B / $388K avg / 90 days
    current_daily = data['velocity']['daily_avg_sales']
    velocity_gap = ((current_daily / homes_needed_per_day) - 1) * 100

    # Get top market if available
    top_market = data.get('top_markets', [{}])[0] if data.get('top_markets') else {}
    market_concentration = data.get('market_concentration', {})

    # Prepare context for Claude
    context = f"""You are an expert equity analyst writing for Opendoor ($OPEN) shareholders.

CURRENT Q1 2026 DATA (as of {data['as_of']}):

VELOCITY (Key metric for guidance):
- Daily Sales: {data['velocity']['daily_avg_sales']:.1f} homes/day
- Target Needed: 29 homes/day to hit $1B guidance
- Gap: {velocity_gap:+.1f}% vs target
- This Week: {data['this_week']['sales']} homes ({data['wow_change']['sales_pct']:+.1f}% WoW)
- Best Day: {data['velocity']['best_day_sales']} homes on {data['velocity']['best_day']}

GUIDANCE TRACKING:
- Q1 Target: $1B revenue
- Current: ${data['velocity']['q1_revenue']/1e6:.1f}M ({data['guidance']['pacing_pct']:.1f}% pacing)
- Days Elapsed: {data['guidance']['days_elapsed']} of 90
- Days Remaining: {data['guidance']['days_remaining']}
- Projected: ${data['guidance']['projected_revenue']/1e6:.0f}M at current pace

PRICING:
- Avg Home Price: ${avg_price:,.0f}
- Q1 Revenue: ${data['velocity']['q1_revenue']:,.0f}
- Q1 Homes Sold: {data['velocity']['q1_sales']}

PROFITABILITY:
- Win Rate: {data['pnl']['win_rate']:.1f}% ({data['pnl']['wins']} wins, {data['pnl']['losses']} losses)
- Avg Profit on Wins: ${data['pnl']['avg_profit']:,.0f}
- Avg Loss on Losses: ${data['pnl']['avg_loss']:,.0f}
- Total Realized P&L: ${data['pnl']['total_realized']:,.0f}

GEOGRAPHIC:
- Top Market: {top_market.get('city', 'N/A')} ({top_market.get('listings', 0)} listings, ${top_market.get('avg_price', 0)/1000:.0f}K avg)
- Market Spread: {market_concentration.get('total_markets', 0)} cities
- Concentration: Top 5 = {market_concentration.get('top5_pct', 0)}% of inventory
"""

    prompt = """Generate 3 insights for shareholders. Each insight should:
- Be exactly 1 sentence (10-18 words)
- Include specific numbers from the data
- Be actionable/interpretive, not just restating facts

The 3 insights must cover:
1. VELOCITY: Is daily sales pace accelerating or decelerating? How does it compare to the 29/day needed?
2. GUIDANCE RISK: Will they hit $1B? What would need to change? Be direct about probability.
3. SIGNAL: One bullish OR bearish signal from the data (profitability, pricing, geographic, or weekly trend)

Tone: Direct, analytical, no hedge words like "may" or "could". State conclusions confidently.

Return ONLY valid JSON:
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
