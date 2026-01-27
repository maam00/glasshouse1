# Glass House Assessment — CEO Perspective

*From: Khosrowshahi mindset*
*Re: What I'd want to see every morning*

---

## What's Working

The dashboard captures the core story:
- **Cohort segmentation** — New cohort win rate is THE leading indicator. 90.3% tells me our pricing models are recalibrating but not yet dialed in.
- **Toxic countdown** — 84 remaining, 8 weeks to clear. This is the legacy drag we're managing.
- **Guidance tracking** — 20.1% to Q1 target, behind pace. This is the board conversation.

---

## What's Missing (Priority Order)

### 1. PER-MARKET UNIT ECONOMICS ← Critical

I can't see which markets are making money and which are burning. The sales CSV has no state column.

**Impact**: Can't answer "Should we pause acquisitions in Atlanta?" or "Is Phoenix turning around?"

**Fix**: Join sales to listings by property_id to get state.

---

### 2. HOLDING COST MODEL

`realized_net` in the CSV is just (sale_price - purchase_price). It doesn't include:
- Renovation costs (~$15-25K/home)
- Holding costs (~$50-75/day × days_held)
- Transaction costs (commissions, title, etc.)

**Impact**: A home showing $30K profit might actually be break-even.

**Fix**: Build a true unit economics model with estimated costs.

---

### 3. VELOCITY BREAKDOWN

I see avg days to sale (207), but I need to understand WHERE time is being lost:
- Days from offer acceptance → close (acquisition velocity)
- Days from purchase → list (renovation cycle)
- Days on market → sale (market velocity)

**Impact**: If DOM is high, is it pricing or market conditions?

**Fix**: We have purchase_date and sale_date in sales, initial_list_date in listings. Can calculate more.

---

### 4. PRICE CUT ANALYSIS BY COHORT

87% of homes have price cuts. But are NEW cohort homes getting cut less?

**Impact**: Price cuts destroy margin. Need to know if the new model is reducing them.

**Fix**: Join listings to understand cut patterns by age.

---

### 5. COMPETITOR SHARE

Parcl investor data shows market-level investor activity, but not Opendoor's share specifically.

**Impact**: Am I gaining or losing share in Phoenix? Are competitors pulling back?

**Fix**: Would need Parcl portfolio-level data or scrape competitor listings.

---

### 6. SPREAD DECOMPOSITION

Current spread calculation: sale_price - purchase_price = $30.3K avg

I need:
- Gross spread (sale_price - purchase_price)
- Net spread (after reno, holding, transaction costs)
- Spread by market
- Spread trend over time

---

## Dashboard Changes I'd Make

### Header Should Show:
```
$OPEN: $5.81 (-46% from 52W high)  |  Q1: 20% to $595M (62 days left)  |  Cash: $962M
```

### Add These Sections:

**MARKET P&L MATRIX**
```
Market   Inv   Sales   Win%   Margin   Trend   Action
NC       127      45    92%    11.2%     ↑     GROW
GA       102      38    78%     4.1%     ↓     HOLD
TX        89      52    85%     8.3%     →     GROW
FL        85      29    71%     2.1%     ↓     PAUSE?
```

**VELOCITY FUNNEL**
```
Acquisition → Renovation → Listed → Sold
    12 days      18 days     45 days   [75 days avg cycle]
```

**PRICING HEALTH**
```
New Cohort:  12% cut within 30d  (target: <10%)
Mid Cohort:  34% cut
Old Cohort:  67% cut
```

---

## Build List

1. **State enrichment** — Join sales to listings
2. **Unit economics model** — Estimate true margin
3. **Market P&L view** — Per-market performance
4. **Velocity decomposition** — Where is time lost
5. **Cohort price cut analysis** — Are new homes priced better
6. **Competitor tracker** — Share of market activity

---

## Data Sources to Add

| Data | Source | Effort |
|------|--------|--------|
| Zillow home values | Zillow API (free) | Low |
| Redfin market data | Redfin Data Center | Low |
| Offerpad listings | Scraper | Medium |
| County records | County APIs | High |
| MLS data | Would need license | High |

