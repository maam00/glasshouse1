/**
 * Glass House V5 Dashboard Scripts
 * Interactive dashboard with Plotly charts and dynamic data rendering
 */

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Format currency values as $X.XM or $X.XK
 */
function formatCurrency(value) {
  if (value === null || value === undefined) return '$0';
  const num = parseFloat(value);
  if (Math.abs(num) >= 1000000) {
    return '$' + (num / 1000000).toFixed(1) + 'M';
  } else if (Math.abs(num) >= 1000) {
    return '$' + (num / 1000).toFixed(1) + 'K';
  }
  return '$' + num.toFixed(0);
}

/**
 * Format as percentage with one decimal
 */
function formatPercent(value) {
  if (value === null || value === undefined) return '0%';
  return parseFloat(value).toFixed(1) + '%';
}

/**
 * Format number with commas
 */
function formatNumber(value) {
  if (value === null || value === undefined) return '0';
  return parseInt(value).toLocaleString();
}

/**
 * Format date for chart labels
 */
function formatDate(dateStr) {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

// ============================================================================
// DATA LOADING
// ============================================================================

/**
 * Load dashboard data from JSON file
 */
async function loadDashboardData() {
  try {
    const response = await fetch('outputs/dashboard_data.json');
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return response.json();
  } catch (error) {
    console.error('Failed to load dashboard data:', error);
    // Return mock data for development/testing
    return getMockData();
  }
}

/**
 * Mock data for development when JSON not available
 */
function getMockData() {
  return {
    current: {
      date: new Date().toISOString().split('T')[0],
      daily_revenue: 8500000,
      toxic_debt_ratio: 12.5,
      v3: {
        portfolio: {
          total_loans: 15234,
          total_principal: 485000000,
          avg_loan_size: 31845
        },
        underwater_watchlist: [
          { loan_id: 'L-001', property_address: '123 Main St, Phoenix, AZ', ltv: 125, days_delinquent: 45 },
          { loan_id: 'L-002', property_address: '456 Oak Ave, Tampa, FL', ltv: 118, days_delinquent: 32 },
          { loan_id: 'L-003', property_address: '789 Pine Rd, Austin, TX', ltv: 112, days_delinquent: 28 }
        ]
      }
    },
    history: generateMockHistory(),
    trends: {
      revenue_chart: generateMockRevenueChart()
    },
    insights: {
      verdict: 'Monitor closely - revenue trending above target but toxic debt rising'
    }
  };
}

function generateMockHistory() {
  const history = [];
  const today = new Date();
  for (let i = 30; i >= 0; i--) {
    const date = new Date(today);
    date.setDate(date.getDate() - i);
    history.push({
      date: date.toISOString().split('T')[0],
      daily_revenue: 4000000 + Math.random() * 11000000,
      toxic_debt_ratio: 10 + Math.random() * 5
    });
  }
  return history;
}

function generateMockRevenueChart() {
  const data = [];
  const today = new Date();
  for (let i = 13; i >= 0; i--) {
    const date = new Date(today);
    date.setDate(date.getDate() - i);
    data.push({
      date: date.toISOString().split('T')[0],
      revenue: 4000000 + Math.random() * 11000000
    });
  }
  return data;
}

// ============================================================================
// REVENUE CHART WITH PLOTLY
// ============================================================================

/**
 * Render the revenue bar chart with Plotly
 * CRITICAL: Y-axis scaled 0-20M with target line at $6.8M
 */
function renderRevenueChart(data) {
  const chartContainer = document.getElementById('revenue-chart');
  if (!chartContainer) {
    console.warn('Revenue chart container not found');
    return;
  }

  // Extract revenue data from trends or history
  let revenueData = [];

  if (data.trends && data.trends.revenue_chart) {
    revenueData = data.trends.revenue_chart;
  } else if (data.history && data.history.length > 0) {
    // Use last 14 days from history
    revenueData = data.history.slice(-14).map(d => ({
      date: d.date,
      revenue: d.daily_revenue || d.revenue || 0
    }));
  }

  if (revenueData.length === 0) {
    chartContainer.innerHTML = '<p style="color: #6b7280; text-align: center; padding: 40px;">No revenue data available</p>';
    return;
  }

  const TARGET_REVENUE = 6800000; // $6.8M target
  const MAX_Y = 20000000; // $20M max

  // Prepare chart data
  const dates = revenueData.map(d => formatDate(d.date));
  const revenues = revenueData.map(d => d.revenue);

  // Color bars based on target
  const barColors = revenues.map(rev =>
    rev >= TARGET_REVENUE ? '#22c55e' : '#f59e0b'
  );

  // Create hover text
  const hoverText = revenueData.map(d =>
    `${formatDate(d.date)}<br>Revenue: ${formatCurrency(d.revenue)}<br>${d.revenue >= TARGET_REVENUE ? 'Above target' : 'Below target'}`
  );

  // Bar trace
  const barTrace = {
    x: dates,
    y: revenues,
    type: 'bar',
    marker: {
      color: barColors,
      line: {
        color: barColors.map(c => c === '#22c55e' ? '#16a34a' : '#d97706'),
        width: 1
      }
    },
    hoverinfo: 'text',
    hovertext: hoverText,
    name: 'Daily Revenue'
  };

  // Target line trace
  const targetLine = {
    x: dates,
    y: Array(dates.length).fill(TARGET_REVENUE),
    type: 'scatter',
    mode: 'lines',
    line: {
      color: '#ef4444',
      width: 2,
      dash: 'dash'
    },
    hoverinfo: 'name',
    name: 'Target: $6.8M'
  };

  // Layout configuration - dark theme
  const layout = {
    paper_bgcolor: '#141414',
    plot_bgcolor: '#141414',
    font: {
      family: 'Inter, system-ui, sans-serif',
      color: '#9ca3af'
    },
    margin: {
      l: 70,
      r: 30,
      t: 40,
      b: 60
    },
    xaxis: {
      tickangle: -45,
      tickfont: {
        size: 11,
        color: '#9ca3af'
      },
      gridcolor: '#374151',
      gridwidth: 1,
      showgrid: false,
      linecolor: '#374151'
    },
    yaxis: {
      title: {
        text: 'Daily Revenue',
        font: {
          size: 12,
          color: '#9ca3af'
        }
      },
      range: [0, MAX_Y],
      tickmode: 'array',
      tickvals: [0, 5000000, 10000000, 15000000, 20000000],
      ticktext: ['$0', '$5M', '$10M', '$15M', '$20M'],
      tickfont: {
        size: 11,
        color: '#9ca3af'
      },
      gridcolor: '#374151',
      gridwidth: 1,
      showgrid: true,
      zeroline: true,
      zerolinecolor: '#374151'
    },
    showlegend: true,
    legend: {
      x: 0,
      y: 1.15,
      orientation: 'h',
      font: {
        size: 11,
        color: '#9ca3af'
      },
      bgcolor: 'transparent'
    },
    hovermode: 'closest',
    bargap: 0.3,
    annotations: [
      {
        x: dates[dates.length - 1],
        y: TARGET_REVENUE,
        xanchor: 'right',
        yanchor: 'bottom',
        text: 'Target: $6.8M',
        showarrow: false,
        font: {
          size: 10,
          color: '#ef4444'
        },
        bgcolor: '#141414',
        opacity: 0.9
      }
    ]
  };

  // Config for chart
  const config = {
    responsive: true,
    displayModeBar: false,
    staticPlot: false
  };

  Plotly.newPlot(chartContainer, [barTrace, targetLine], layout, config);
}

// ============================================================================
// RENDER FUNCTIONS
// ============================================================================

/**
 * Update the verdict text based on current metrics
 */
function renderVerdict(data) {
  const verdictEl = document.getElementById('verdict-text');
  if (!verdictEl) return;

  let verdict = '';

  if (data.insights && data.insights.verdict) {
    verdict = data.insights.verdict;
  } else if (data.current) {
    const revenue = data.current.daily_revenue || 0;
    const toxicRatio = data.current.toxic_debt_ratio || 0;

    if (revenue >= 6800000 && toxicRatio < 15) {
      verdict = 'Strong performance - revenue above target, toxic debt under control';
    } else if (revenue >= 6800000 && toxicRatio >= 15) {
      verdict = 'Mixed signals - revenue healthy but toxic debt needs attention';
    } else if (revenue < 6800000 && toxicRatio < 15) {
      verdict = 'Revenue below target - focus on collection efficiency';
    } else {
      verdict = 'Critical attention needed - both revenue and toxic debt concerning';
    }
  }

  verdictEl.textContent = verdict;
}

/**
 * Populate Kaz-era metrics card
 */
function renderKazEraCard(data) {
  const container = document.getElementById('kaz-era-metrics');
  if (!container || !data.current) return;

  const kazData = data.current.kaz_era || data.current;

  // Update individual metric elements if they exist
  updateMetric('kaz-loans', formatNumber(kazData.total_loans || kazData.v3?.portfolio?.total_loans));
  updateMetric('kaz-principal', formatCurrency(kazData.total_principal || kazData.v3?.portfolio?.total_principal));
  updateMetric('kaz-avg-loan', formatCurrency(kazData.avg_loan_size || kazData.v3?.portfolio?.avg_loan_size));
  updateMetric('kaz-delinquent', formatPercent(kazData.delinquency_rate));
}

/**
 * Populate Legacy metrics card
 */
function renderLegacyCard(data) {
  const container = document.getElementById('legacy-metrics');
  if (!container || !data.current) return;

  const legacyData = data.current.legacy || {};

  updateMetric('legacy-loans', formatNumber(legacyData.total_loans));
  updateMetric('legacy-principal', formatCurrency(legacyData.total_principal));
  updateMetric('legacy-avg-loan', formatCurrency(legacyData.avg_loan_size));
  updateMetric('legacy-delinquent', formatPercent(legacyData.delinquency_rate));
}

/**
 * Update toxic debt countdown progress bar and weekly trend
 */
function renderToxicCountdown(data) {
  const progressBar = document.getElementById('toxic-progress');
  const percentLabel = document.getElementById('toxic-percent');
  const trendLabel = document.getElementById('toxic-trend');

  if (!data.current) return;

  const toxicRatio = data.current.toxic_debt_ratio || 0;
  const weeklyChange = data.current.toxic_weekly_change || 0;

  if (progressBar) {
    // Scale: 0-25% toxic ratio maps to 0-100% progress bar
    const progressPercent = Math.min((toxicRatio / 25) * 100, 100);
    progressBar.style.width = progressPercent + '%';

    // Color based on severity
    if (toxicRatio < 10) {
      progressBar.style.backgroundColor = '#22c55e';
    } else if (toxicRatio < 15) {
      progressBar.style.backgroundColor = '#f59e0b';
    } else {
      progressBar.style.backgroundColor = '#ef4444';
    }
  }

  if (percentLabel) {
    percentLabel.textContent = formatPercent(toxicRatio);
  }

  if (trendLabel) {
    const trendIcon = weeklyChange > 0 ? '↑' : weeklyChange < 0 ? '↓' : '→';
    const trendColor = weeklyChange > 0 ? '#ef4444' : weeklyChange < 0 ? '#22c55e' : '#9ca3af';
    trendLabel.innerHTML = `<span style="color: ${trendColor}">${trendIcon} ${Math.abs(weeklyChange).toFixed(1)}% WoW</span>`;
  }
}

/**
 * Populate cohort table with vintage analysis
 */
function renderCohortTable(data) {
  const tableBody = document.getElementById('cohort-table-body');
  if (!tableBody) return;

  const cohorts = data.current?.cohorts || data.cohorts || [];

  if (cohorts.length === 0) {
    tableBody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #6b7280;">No cohort data available</td></tr>';
    return;
  }

  tableBody.innerHTML = cohorts.map(cohort => `
    <tr>
      <td>${cohort.vintage || cohort.period}</td>
      <td>${formatNumber(cohort.loans)}</td>
      <td>${formatCurrency(cohort.principal)}</td>
      <td>${formatPercent(cohort.delinquency_rate)}</td>
      <td class="${getTrendClass(cohort.trend)}">${cohort.trend || 'Stable'}</td>
    </tr>
  `).join('');
}

/**
 * Populate geographic risk table
 */
function renderGeographicRisk(data) {
  const tableBody = document.getElementById('geo-risk-table-body');
  if (!tableBody) return;

  const geoData = data.current?.geographic_risk || data.geographic || [];

  if (geoData.length === 0) {
    tableBody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #6b7280;">No geographic data available</td></tr>';
    return;
  }

  tableBody.innerHTML = geoData.map(state => `
    <tr>
      <td>${state.state}</td>
      <td>${formatCurrency(state.exposure)}</td>
      <td>${formatPercent(state.delinquency_rate)}</td>
      <td class="${getRiskClass(state.risk_level)}">${state.risk_level || 'Medium'}</td>
    </tr>
  `).join('');
}

/**
 * Populate price cut statistics
 */
function renderPriceCuts(data) {
  const container = document.getElementById('price-cuts');
  if (!container) return;

  const priceCuts = data.current?.price_cuts || {};

  updateMetric('properties-with-cuts', formatNumber(priceCuts.count));
  updateMetric('avg-cut-percent', formatPercent(priceCuts.avg_percent));
  updateMetric('total-cut-value', formatCurrency(priceCuts.total_value));
}

/**
 * Populate underwater property watchlist
 */
function renderWatchlist(data) {
  const tableBody = document.getElementById('watchlist-table-body');
  if (!tableBody) return;

  const watchlist = data.current?.v3?.underwater_watchlist ||
                    data.current?.underwater_watchlist ||
                    [];

  if (watchlist.length === 0) {
    tableBody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #6b7280;">No underwater properties</td></tr>';
    return;
  }

  tableBody.innerHTML = watchlist.slice(0, 10).map(property => `
    <tr>
      <td>${property.loan_id}</td>
      <td>${property.property_address || property.address}</td>
      <td class="${property.ltv > 120 ? 'text-red' : 'text-amber'}">${formatPercent(property.ltv)}</td>
      <td>${property.days_delinquent || 0} days</td>
    </tr>
  `).join('');
}

/**
 * Populate Week-over-Week changes
 */
function renderThisWeek(data) {
  const container = document.getElementById('this-week');
  if (!container) return;

  const weekChanges = data.current?.week_over_week || data.wow_changes || {};

  updateMetricWithTrend('wow-revenue', weekChanges.revenue);
  updateMetricWithTrend('wow-delinquency', weekChanges.delinquency);
  updateMetricWithTrend('wow-collections', weekChanges.collections);
  updateMetricWithTrend('wow-originations', weekChanges.originations);
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Update a metric element by ID
 */
function updateMetric(elementId, value) {
  const el = document.getElementById(elementId);
  if (el) {
    el.textContent = value || '--';
  }
}

/**
 * Update metric with trend indicator
 */
function updateMetricWithTrend(elementId, change) {
  const el = document.getElementById(elementId);
  if (!el || change === undefined) return;

  const value = parseFloat(change);
  const isPositive = value > 0;
  const icon = isPositive ? '↑' : value < 0 ? '↓' : '→';
  const color = isPositive ? '#22c55e' : value < 0 ? '#ef4444' : '#9ca3af';

  el.innerHTML = `<span style="color: ${color}">${icon} ${Math.abs(value).toFixed(1)}%</span>`;
}

/**
 * Get CSS class for trend indicators
 */
function getTrendClass(trend) {
  if (!trend) return '';
  const t = trend.toLowerCase();
  if (t.includes('improv') || t.includes('down')) return 'text-green';
  if (t.includes('worsen') || t.includes('up')) return 'text-red';
  return 'text-amber';
}

/**
 * Get CSS class for risk levels
 */
function getRiskClass(riskLevel) {
  if (!riskLevel) return '';
  const r = riskLevel.toLowerCase();
  if (r === 'low') return 'text-green';
  if (r === 'high' || r === 'critical') return 'text-red';
  return 'text-amber';
}

// ============================================================================
// SCREENSHOT MODE
// ============================================================================

/**
 * Toggle screenshot mode for clean exports
 */
function toggleScreenshotMode() {
  document.body.classList.toggle('screenshot-mode');

  const isActive = document.body.classList.contains('screenshot-mode');
  console.log(`Screenshot mode: ${isActive ? 'ON' : 'OFF'}`);

  // Show notification
  showNotification(`Screenshot mode ${isActive ? 'enabled' : 'disabled'}`);
}

/**
 * Show temporary notification
 */
function showNotification(message) {
  const existing = document.getElementById('notification-toast');
  if (existing) existing.remove();

  const toast = document.createElement('div');
  toast.id = 'notification-toast';
  toast.style.cssText = `
    position: fixed;
    bottom: 20px;
    right: 20px;
    background: #1f2937;
    color: #f3f4f6;
    padding: 12px 20px;
    border-radius: 8px;
    font-size: 14px;
    z-index: 10000;
    animation: fadeIn 0.3s ease;
  `;
  toast.textContent = message;
  document.body.appendChild(toast);

  setTimeout(() => {
    toast.style.animation = 'fadeOut 0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, 2000);
}

// Keyboard shortcut: Cmd+Shift+S or Ctrl+Shift+S
document.addEventListener('keydown', (e) => {
  if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key === 'S') {
    e.preventDefault();
    toggleScreenshotMode();
  }
});

// ============================================================================
// RENDER ALL
// ============================================================================

/**
 * Render all dashboard components
 */
function renderAll(data) {
  console.log('Rendering dashboard with data:', data);

  renderVerdict(data);
  renderRevenueChart(data);
  renderKazEraCard(data);
  renderLegacyCard(data);
  renderToxicCountdown(data);
  renderCohortTable(data);
  renderGeographicRisk(data);
  renderPriceCuts(data);
  renderWatchlist(data);
  renderThisWeek(data);

  // Update last refresh timestamp
  const refreshEl = document.getElementById('last-refresh');
  if (refreshEl) {
    refreshEl.textContent = new Date().toLocaleString();
  }
}

// ============================================================================
// INITIALIZE
// ============================================================================

document.addEventListener('DOMContentLoaded', async () => {
  console.log('Glass House V5 Dashboard initializing...');

  try {
    const data = await loadDashboardData();
    renderAll(data);
    console.log('Dashboard initialized successfully');
  } catch (error) {
    console.error('Failed to initialize dashboard:', error);

    // Show error message to user
    const main = document.querySelector('main') || document.body;
    const errorDiv = document.createElement('div');
    errorDiv.style.cssText = `
      position: fixed;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      background: #1f2937;
      color: #ef4444;
      padding: 20px 30px;
      border-radius: 8px;
      border: 1px solid #ef4444;
      z-index: 10000;
    `;
    errorDiv.innerHTML = `
      <h3 style="margin: 0 0 10px">Failed to load dashboard</h3>
      <p style="margin: 0; color: #9ca3af">${error.message}</p>
    `;
    main.appendChild(errorDiv);
  }
});

// ============================================================================
// EXPORT FOR TESTING
// ============================================================================

if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    formatCurrency,
    formatPercent,
    formatNumber,
    loadDashboardData,
    renderRevenueChart,
    renderAll
  };
}
