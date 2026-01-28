#!/usr/bin/env python3
"""
Generate Market Intelligence Brief for Glass House dashboard.

Includes:
1. Earnings countdown and analyst estimates
2. Mortgage rate tracking
3. AI-powered daily market brief with macro context
"""

import json
import os
import re
import requests
from pathlib import Path
from datetime import datetime, timedelta

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


def fetch_earnings_data():
    """Fetch Opendoor earnings data from Yahoo Finance."""

    earnings_data = {
        'next_earnings_date': None,
        'days_until_earnings': None,
        'analyst_estimates': {},
        'recent_history': [],
        'source': 'yahoo_finance'
    }

    try:
        # Yahoo Finance API for earnings
        url = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/OPEN"
        params = {
            'modules': 'earningsHistory,earningsTrend,calendarEvents,financialData'
        }
        headers = {'User-Agent': 'Mozilla/5.0'}

        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            result = data.get('quoteSummary', {}).get('result', [{}])[0]

            # Get next earnings date from calendar events
            calendar = result.get('calendarEvents', {})
            earnings_dates = calendar.get('earnings', {}).get('earningsDate', [])
            if earnings_dates:
                # Take the first date
                ts = earnings_dates[0].get('raw', 0)
                if ts:
                    next_date = datetime.fromtimestamp(ts)
                    earnings_data['next_earnings_date'] = next_date.strftime('%Y-%m-%d')
                    earnings_data['days_until_earnings'] = (next_date - datetime.now()).days

            # Get analyst estimates from earnings trend
            trend = result.get('earningsTrend', {}).get('trend', [])
            for t in trend:
                period = t.get('period', '')
                if period in ['0q', '+1q']:  # Current and next quarter
                    label = 'current_quarter' if period == '0q' else 'next_quarter'
                    earnings_data['analyst_estimates'][label] = {
                        'period': t.get('endDate', ''),
                        'eps_estimate': t.get('earningsEstimate', {}).get('avg', {}).get('raw'),
                        'revenue_estimate': t.get('revenueEstimate', {}).get('avg', {}).get('raw'),
                        'revenue_estimate_low': t.get('revenueEstimate', {}).get('low', {}).get('raw'),
                        'revenue_estimate_high': t.get('revenueEstimate', {}).get('high', {}).get('raw'),
                        'num_analysts': t.get('earningsEstimate', {}).get('numberOfAnalysts', {}).get('raw'),
                    }

            # Get earnings history
            history = result.get('earningsHistory', {}).get('history', [])
            for h in history[-4:]:  # Last 4 quarters
                earnings_data['recent_history'].append({
                    'quarter': h.get('quarter', {}).get('fmt', ''),
                    'eps_actual': h.get('epsActual', {}).get('raw'),
                    'eps_estimate': h.get('epsEstimate', {}).get('raw'),
                    'surprise_pct': h.get('surprisePercent', {}).get('raw'),
                })

            # Get financial data
            fin_data = result.get('financialData', {})
            earnings_data['analyst_estimates']['price_targets'] = {
                'current': fin_data.get('currentPrice', {}).get('raw'),
                'target_mean': fin_data.get('targetMeanPrice', {}).get('raw'),
                'target_low': fin_data.get('targetLowPrice', {}).get('raw'),
                'target_high': fin_data.get('targetHighPrice', {}).get('raw'),
                'recommendation': fin_data.get('recommendationKey', ''),
                'num_analysts': fin_data.get('numberOfAnalystOpinions', {}).get('raw'),
            }

            print(f"Fetched earnings data: next date = {earnings_data['next_earnings_date']}")

    except Exception as e:
        print(f"Error fetching earnings data: {e}")

    return earnings_data


def fetch_mortgage_rates():
    """Fetch current mortgage rates from Freddie Mac or fallback sources."""

    rates_data = {
        'rate_30yr': None,
        'rate_15yr': None,
        'rate_change_1w': None,
        'rate_change_1m': None,
        'as_of': None,
        'source': None,
        'historical': []
    }

    # Try Freddie Mac PMMS data (Primary Mortgage Market Survey)
    try:
        # Freddie Mac publishes weekly on Thursdays
        # We'll use a proxy API or scrape approach

        # Alternative: Use FRED API for mortgage rates
        # Series: MORTGAGE30US (30-Year Fixed Rate Mortgage Average)
        fred_url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            'series_id': 'MORTGAGE30US',
            'api_key': os.environ.get('FRED_API_KEY', ''),
            'file_type': 'json',
            'limit': 10,
            'sort_order': 'desc'
        }

        if params['api_key']:
            response = requests.get(fred_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                observations = data.get('observations', [])
                if observations:
                    latest = observations[0]
                    rates_data['rate_30yr'] = float(latest['value'])
                    rates_data['as_of'] = latest['date']
                    rates_data['source'] = 'fred'

                    # Calculate changes
                    if len(observations) >= 2:
                        prev = float(observations[1]['value'])
                        rates_data['rate_change_1w'] = round(rates_data['rate_30yr'] - prev, 2)
                    if len(observations) >= 5:
                        month_ago = float(observations[4]['value'])
                        rates_data['rate_change_1m'] = round(rates_data['rate_30yr'] - month_ago, 2)

                    # Store historical
                    for obs in observations[:8]:
                        rates_data['historical'].append({
                            'date': obs['date'],
                            'rate': float(obs['value'])
                        })

                    print(f"Fetched mortgage rate from FRED: {rates_data['rate_30yr']}%")
                    return rates_data
    except Exception as e:
        print(f"FRED API error: {e}")

    # Fallback: Try to scrape or use cached/estimated data
    try:
        # Use Yahoo Finance for mortgage rate proxy
        # or hardcode recent known rate as fallback
        # Current rates as of Jan 2026 (approximate)
        rates_data['rate_30yr'] = 6.89  # Fallback estimate
        rates_data['rate_15yr'] = 6.12
        rates_data['as_of'] = datetime.now().strftime('%Y-%m-%d')
        rates_data['source'] = 'estimate'
        rates_data['rate_change_1w'] = -0.03
        rates_data['rate_change_1m'] = -0.15
        print("Using estimated mortgage rates (no FRED API key)")
    except Exception as e:
        print(f"Mortgage rate fallback error: {e}")

    return rates_data


def fetch_housing_news():
    """Fetch recent housing/real estate news headlines."""

    news_data = {
        'headlines': [],
        'fed_news': [],
        'housing_news': [],
        'opendoor_news': []
    }

    try:
        # Use Yahoo Finance news API for Opendoor
        url = "https://query1.finance.yahoo.com/v1/finance/search"
        params = {'q': 'OPEN', 'newsCount': 5}
        headers = {'User-Agent': 'Mozilla/5.0'}

        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for item in data.get('news', []):
                news_data['opendoor_news'].append({
                    'title': item.get('title', ''),
                    'publisher': item.get('publisher', ''),
                    'link': item.get('link', ''),
                    'date': datetime.fromtimestamp(item.get('providerPublishTime', 0)).strftime('%Y-%m-%d')
                })

        # Search for housing market news
        params = {'q': 'housing market mortgage rates', 'newsCount': 5}
        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for item in data.get('news', []):
                news_data['housing_news'].append({
                    'title': item.get('title', ''),
                    'publisher': item.get('publisher', ''),
                    'date': datetime.fromtimestamp(item.get('providerPublishTime', 0)).strftime('%Y-%m-%d')
                })

        # Search for Fed/FOMC news
        params = {'q': 'federal reserve interest rates FOMC', 'newsCount': 3}
        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for item in data.get('news', []):
                news_data['fed_news'].append({
                    'title': item.get('title', ''),
                    'publisher': item.get('publisher', ''),
                    'date': datetime.fromtimestamp(item.get('providerPublishTime', 0)).strftime('%Y-%m-%d')
                })

        print(f"Fetched news: {len(news_data['opendoor_news'])} OPEN, {len(news_data['housing_news'])} housing, {len(news_data['fed_news'])} Fed")

    except Exception as e:
        print(f"Error fetching news: {e}")

    return news_data


def generate_market_brief(dashboard_data, earnings_data, rates_data, news_data, problem_homes_data=None):
    """Generate AI-powered daily market brief with macro context."""

    if not HAS_ANTHROPIC:
        print("Anthropic library not available")
        return None

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print("No ANTHROPIC_API_KEY found")
        return None

    # Build context
    today = datetime.now().strftime('%A, %B %d, %Y')

    # Opendoor operational context
    velocity = dashboard_data.get('velocity', {})
    guidance = dashboard_data.get('guidance', {})
    pnl = dashboard_data.get('pnl', {})

    # Problem homes context
    problem_summary = ""
    if problem_homes_data:
        ps = problem_homes_data.get('summary', {})
        problem_summary = f"""
PROBLEM INVENTORY:
- Toxic Homes (>365 days): {ps.get('total_toxic', 0)}
- Very Stale (180-365 days): {ps.get('total_very_stale', 0)}
- Unrealized Losses: ${ps.get('unrealized_pnl_millions', 0)}M
- Critical Risk Markets: {ps.get('critical_markets_count', 0)}
"""

    # News headlines
    opendoor_headlines = "\n".join([f"- {n['title']}" for n in news_data.get('opendoor_news', [])[:3]])
    housing_headlines = "\n".join([f"- {n['title']}" for n in news_data.get('housing_news', [])[:3]])
    fed_headlines = "\n".join([f"- {n['title']}" for n in news_data.get('fed_news', [])[:2]])

    # Earnings context
    earnings_ctx = ""
    if earnings_data.get('next_earnings_date'):
        days_until = earnings_data.get('days_until_earnings', 0)
        estimates = earnings_data.get('analyst_estimates', {}).get('current_quarter', {})
        targets = earnings_data.get('analyst_estimates', {}).get('price_targets', {})

        earnings_ctx = f"""
EARNINGS:
- Next Report: {earnings_data['next_earnings_date']} ({days_until} days away)
- Q1 Revenue Estimate: ${(estimates.get('revenue_estimate', 0) or 0)/1e9:.2f}B
- Analyst Consensus: {targets.get('recommendation', 'N/A').upper()}
- Price Target: ${targets.get('target_mean', 0):.2f} (range: ${targets.get('target_low', 0):.2f}-${targets.get('target_high', 0):.2f})
"""

    prompt = f"""You are a senior equity analyst writing the daily market brief for Opendoor ($OPEN) shareholders.

TODAY: {today}

MORTGAGE RATES:
- 30-Year Fixed: {rates_data.get('rate_30yr', 'N/A')}%
- Weekly Change: {rates_data.get('rate_change_1w', 'N/A'):+.2f}%
- Monthly Change: {rates_data.get('rate_change_1m', 'N/A'):+.2f}%
{earnings_ctx}
OPENDOOR OPERATIONS:
- Daily Sales Pace: {velocity.get('daily_avg_sales', 0):.1f} homes/day
- Q1 Revenue: ${velocity.get('q1_revenue', 0)/1e6:.1f}M of $1B target ({guidance.get('pacing_pct', 0):.0f}% pacing)
- Win Rate: {pnl.get('win_rate', 0):.1f}%
- Days Remaining in Q1: {guidance.get('days_remaining', 0)}
{problem_summary}
RECENT NEWS - OPENDOOR:
{opendoor_headlines or "No recent headlines"}

RECENT NEWS - HOUSING MARKET:
{housing_headlines or "No recent headlines"}

RECENT NEWS - FED/MACRO:
{fed_headlines or "No recent headlines"}

Write a 3-4 sentence DAILY MARKET BRIEF that:
1. Opens with the most important macro factor affecting Opendoor today (rates, Fed, housing data)
2. Connects it to Opendoor's current operational performance
3. Gives a clear near-term outlook or risk to watch
4. Mentions earnings countdown if <30 days away

Tone: Confident, direct, no hedge words. Write as if briefing a portfolio manager.

Return JSON:
{{
    "market_brief": "Your 3-4 sentence brief here...",
    "sentiment": "bullish|neutral|bearish",
    "key_risk": "One sentence on the biggest near-term risk",
    "key_catalyst": "One sentence on the biggest near-term catalyst"
}}"""

    try:
        client = anthropic.Anthropic(api_key=api_key)

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = response.content[0].text.strip()

        # Parse JSON
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if json_match:
            result = json.loads(json_match.group())
            result['generated_at'] = datetime.now().isoformat()
            print(f"Generated market brief: {result.get('sentiment', 'unknown')} sentiment")
            return result

    except Exception as e:
        print(f"Error generating market brief: {e}")

    return None


def main():
    output_dir = Path(__file__).parent.parent / "outputs"
    today = datetime.now().strftime("%Y-%m-%d")

    print("=" * 60)
    print("  MARKET INTELLIGENCE BRIEF")
    print("=" * 60)

    # 1. Fetch earnings data
    print("\n[1/5] Fetching earnings data...")
    earnings_data = fetch_earnings_data()

    # 2. Fetch mortgage rates
    print("\n[2/5] Fetching mortgage rates...")
    rates_data = fetch_mortgage_rates()

    # 3. Fetch news
    print("\n[3/5] Fetching market news...")
    news_data = fetch_housing_news()

    # 4. Load existing dashboard data
    print("\n[4/5] Loading dashboard data...")
    dashboard_path = output_dir / "unified_dashboard_data.json"
    dashboard_data = {}
    if dashboard_path.exists():
        with open(dashboard_path) as f:
            dashboard_data = json.load(f)

    # Load problem homes data
    problem_path = output_dir / f"problem_homes_{today}.json"
    problem_data = None
    if problem_path.exists():
        with open(problem_path) as f:
            problem_data = json.load(f)

    # 5. Generate AI market brief
    print("\n[5/5] Generating AI market brief...")
    market_brief = generate_market_brief(dashboard_data, earnings_data, rates_data, news_data, problem_data)

    # Compile all market intelligence
    market_intel = {
        'generated_at': datetime.now().isoformat(),
        'earnings': earnings_data,
        'mortgage_rates': rates_data,
        'news': news_data,
        'market_brief': market_brief
    }

    # Save to file
    output_file = output_dir / f"market_intel_{today}.json"
    with open(output_file, 'w') as f:
        json.dump(market_intel, f, indent=2)
    print(f"\nSaved to: {output_file}")

    # Also update the main dashboard data
    if dashboard_path.exists():
        with open(dashboard_path) as f:
            dash = json.load(f)
        dash['market_intel'] = market_intel
        with open(dashboard_path, 'w') as f:
            json.dump(dash, f, indent=2)
        print(f"Updated: {dashboard_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)

    if earnings_data.get('next_earnings_date'):
        print(f"\n  EARNINGS: {earnings_data['next_earnings_date']} ({earnings_data['days_until_earnings']} days)")
        targets = earnings_data.get('analyst_estimates', {}).get('price_targets', {})
        if targets.get('target_mean'):
            print(f"  TARGET: ${targets['target_mean']:.2f} ({targets.get('recommendation', '').upper()})")

    print(f"\n  MORTGAGE: {rates_data.get('rate_30yr', 'N/A')}% (30yr)")
    if rates_data.get('rate_change_1w'):
        direction = "down" if rates_data['rate_change_1w'] < 0 else "up"
        print(f"  CHANGE: {abs(rates_data['rate_change_1w']):.2f}% {direction} this week")

    if market_brief:
        print(f"\n  SENTIMENT: {market_brief.get('sentiment', 'N/A').upper()}")
        print(f"\n  BRIEF: {market_brief.get('market_brief', 'N/A')}")
        print(f"\n  KEY RISK: {market_brief.get('key_risk', 'N/A')}")
        print(f"  KEY CATALYST: {market_brief.get('key_catalyst', 'N/A')}")

    print("\n" + "=" * 60)

    return market_intel


if __name__ == "__main__":
    main()
