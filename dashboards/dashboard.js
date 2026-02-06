/**
 * Foodservice Analytics Dashboard JavaScript
 * Handles data loading, chart rendering, and interactivity
 */

// Configuration
const DATA_PATH = './data';
const CHART_COLORS = {
    primary: '#6366f1',
    primaryLight: '#818cf8',
    secondary: '#10b981',
    accent: '#f59e0b',
    danger: '#ef4444',
    gray: '#64748b',
    gradients: {
        primary: ['#6366f1', '#8b5cf6'],
        success: ['#10b981', '#34d399'],
        accent: ['#f59e0b', '#fbbf24']
    }
};

// Utility Functions
function formatCurrency(value) {
    if (value >= 1e9) return `$${(value / 1e9).toFixed(2)}B`;
    if (value >= 1e6) return `$${(value / 1e6).toFixed(2)}M`;
    if (value >= 1e3) return `$${(value / 1e3).toFixed(1)}K`;
    return `$${value.toFixed(2)}`;
}

function formatNumber(value) {
    if (value >= 1e9) return `${(value / 1e9).toFixed(2)}B`;
    if (value >= 1e6) return `${(value / 1e6).toFixed(2)}M`;
    if (value >= 1e3) return `${(value / 1e3).toFixed(1)}K`;
    return value.toLocaleString();
}

function formatPercent(value) {
    return `${value.toFixed(1)}%`;
}

// Create gradient for charts
function createGradient(ctx, colors) {
    const gradient = ctx.createLinearGradient(0, 0, 0, 300);
    gradient.addColorStop(0, colors[0] + 'CC');
    gradient.addColorStop(1, colors[1] + '33');
    return gradient;
}

// Data Loading Functions
async function loadJSON(filename) {
    try {
        const response = await fetch(`${DATA_PATH}/${filename}`);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        return await response.json();
    } catch (error) {
        console.error(`Error loading ${filename}:`, error);
        return null;
    }
}

// Executive Summary
async function loadExecutiveSummary() {
    const data = await loadJSON('executive_summary.json');
    if (!data) return;

    // Update KPI cards
    document.getElementById('totalNetSales').textContent = formatCurrency(data.total_net_sales || 0);
    document.getElementById('totalUnits').textContent = formatNumber(data.total_units || 0);
    document.getElementById('activeOperators').textContent = formatNumber(data.total_operators || 0);
    document.getElementById('winRate').textContent = formatPercent(data.win_rate || 0);
    document.getElementById('avgDealSize').textContent = formatCurrency(data.avg_deal_size || 0);
    document.getElementById('pipelineValue').textContent = formatCurrency(data.pipeline_value || 0);

    // Update trends (placeholder - would calculate from data)
    document.getElementById('salesTrend').textContent = '↑ 6.2% YoY';
    document.getElementById('salesTrend').classList.add('positive');
}

// Monthly Trends Chart
async function loadMonthlyTrends() {
    const data = await loadJSON('monthly_trends.json');
    if (!data || data.length === 0) return;

    const ctx = document.getElementById('salesTrendChart');
    if (!ctx) return;

    const labels = data.map(d => d.month);
    const netSales = data.map(d => d.net_sales);
    const margin = data.map(d => d.gross_margin);

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Net Sales',
                    data: netSales,
                    borderColor: CHART_COLORS.primary,
                    backgroundColor: createGradient(ctx.getContext('2d'), CHART_COLORS.gradients.primary),
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 6,
                    borderWidth: 2
                },
                {
                    label: 'Gross Margin',
                    data: margin,
                    borderColor: CHART_COLORS.secondary,
                    backgroundColor: 'transparent',
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 6,
                    borderWidth: 2,
                    borderDash: [5, 5]
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                intersect: false,
                mode: 'index'
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    align: 'end',
                    labels: {
                        color: '#94a3b8',
                        usePointStyle: true,
                        padding: 20
                    }
                },
                tooltip: {
                    backgroundColor: '#1e293b',
                    titleColor: '#f1f5f9',
                    bodyColor: '#94a3b8',
                    borderColor: '#334155',
                    borderWidth: 1,
                    padding: 12,
                    callbacks: {
                        label: function (context) {
                            return `${context.dataset.label}: ${formatCurrency(context.raw)}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        color: '#64748b',
                        maxRotation: 0,
                        maxTicksLimit: 12
                    }
                },
                y: {
                    grid: {
                        color: '#334155',
                        drawBorder: false
                    },
                    ticks: {
                        color: '#64748b',
                        callback: function (value) {
                            return formatCurrency(value);
                        }
                    }
                }
            }
        }
    });
}

// Region Chart
async function loadDistributorData() {
    const data = await loadJSON('distributor_scorecards.json');
    if (!data || data.length === 0) return;

    // Region summary
    const regionData = data.reduce((acc, d) => {
        const region = d.region || 'Unknown';
        if (!acc[region]) acc[region] = 0;
        acc[region] += d.net_sales || 0;
        return acc;
    }, {});

    const ctx = document.getElementById('regionChart');
    if (!ctx) return;

    const regions = Object.keys(regionData);
    const sales = Object.values(regionData);

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: regions,
            datasets: [{
                data: sales,
                backgroundColor: [
                    CHART_COLORS.primary,
                    CHART_COLORS.secondary,
                    CHART_COLORS.accent,
                    CHART_COLORS.danger,
                    CHART_COLORS.primaryLight
                ],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '65%',
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: '#94a3b8',
                        padding: 15,
                        usePointStyle: true
                    }
                },
                tooltip: {
                    backgroundColor: '#1e293b',
                    callbacks: {
                        label: function (context) {
                            return `${context.label}: ${formatCurrency(context.raw)}`;
                        }
                    }
                }
            }
        }
    });

    // Top distributors bar chart
    const distCtx = document.getElementById('distributorChart');
    if (!distCtx) return;

    const topDist = data.slice(0, 8);

    new Chart(distCtx, {
        type: 'bar',
        data: {
            labels: topDist.map(d => d.distributor_name.substring(0, 15)),
            datasets: [{
                label: 'Net Sales',
                data: topDist.map(d => d.net_sales),
                backgroundColor: CHART_COLORS.primary + '99',
                borderColor: CHART_COLORS.primary,
                borderWidth: 1,
                borderRadius: 4
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            return formatCurrency(context.raw);
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        color: '#334155'
                    },
                    ticks: {
                        color: '#64748b',
                        callback: function (value) {
                            return formatCurrency(value);
                        }
                    }
                },
                y: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        color: '#94a3b8'
                    }
                }
            }
        }
    });
}

// Rep Rankings Table
async function loadRepRankings() {
    const data = await loadJSON('rep_rankings.json');
    if (!data || data.length === 0) return;

    const tbody = document.getElementById('topRepsTable');
    if (!tbody) return;

    const topReps = data.slice(0, 5);

    tbody.innerHTML = topReps.map(rep => `
        <tr>
            <td><strong>${rep.rep_name}</strong></td>
            <td>${rep.territory_name || 'N/A'}</td>
            <td>${formatCurrency(rep.revenue || 0)}</td>
            <td>
                <span class="badge ${(rep.win_rate || 0) >= 40 ? 'badge-success' : 'badge-warning'}">
                    ${formatPercent(rep.win_rate || 0)}
                </span>
            </td>
            <td>${rep.won || 0}</td>
        </tr>
    `).join('');
}

// Pipeline Chart
async function loadPipelineData() {
    const data = await loadJSON('pipeline_health.json');
    if (!data || data.length === 0) return;

    const ctx = document.getElementById('pipelineChart');
    if (!ctx) return;

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.stage),
            datasets: [{
                label: 'Pipeline Value',
                data: data.map(d => d.value),
                backgroundColor: data.map((_, i) => {
                    const colors = [CHART_COLORS.primary, CHART_COLORS.primaryLight,
                    CHART_COLORS.secondary, CHART_COLORS.accent, CHART_COLORS.danger];
                    return colors[i % colors.length];
                }),
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            const d = data[context.dataIndex];
                            return [
                                `Value: ${formatCurrency(context.raw)}`,
                                `Count: ${d.count}`,
                                `Weighted: ${formatCurrency(d.weighted_value)}`
                            ];
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        color: '#94a3b8',
                        maxRotation: 45
                    }
                },
                y: {
                    grid: {
                        color: '#334155'
                    },
                    ticks: {
                        color: '#64748b',
                        callback: function (value) {
                            return formatCurrency(value);
                        }
                    }
                }
            }
        }
    });
}

// YoY Growth Table
async function loadYoYData() {
    const data = await loadJSON('yoy_growth.json');
    if (!data || data.length === 0) return;

    const tbody = document.getElementById('yoyTable');
    if (!tbody) return;

    // Get 2024 data
    const yearData = data.filter(d => d.year === '2024').slice(0, 5);

    tbody.innerHTML = yearData.map(d => `
        <tr>
            <td><strong>${d.distributor_name}</strong></td>
            <td><span class="badge badge-primary">${d.distributor_type}</span></td>
            <td>${formatCurrency(d.net_sales || 0)}</td>
            <td>
                <span class="badge ${(d.yoy_growth || 0) >= 0 ? 'badge-success' : 'badge-danger'}">
                    ${d.yoy_growth !== null ? formatPercent(d.yoy_growth) : 'N/A'}
                </span>
            </td>
        </tr>
    `).join('');
}

// Load Distributor Scorecards (for scorecard page)
async function loadDistributorScorecards() {
    const data = await loadJSON('distributor_scorecards.json');
    if (!data || data.length === 0) return;

    const container = document.getElementById('scorecardGrid');
    if (!container) return;

    container.innerHTML = data.map(d => `
        <div class="scorecard">
            <div class="scorecard-header">
                <div>
                    <h4 class="scorecard-title">${d.distributor_name}</h4>
                    <span class="scorecard-subtitle">${d.distributor_type} • ${d.region || 'N/A'}</span>
                </div>
                <span class="badge badge-primary">${formatCurrency(d.net_sales || 0)}</span>
            </div>
            <div class="scorecard-metrics">
                <div class="metric">
                    <span class="metric-value">${formatNumber(d.active_operators || 0)}</span>
                    <span class="metric-label">Operators</span>
                </div>
                <div class="metric">
                    <span class="metric-value">${formatNumber(d.products_sold || 0)}</span>
                    <span class="metric-label">Products</span>
                </div>
                <div class="metric">
                    <span class="metric-value">${formatPercent(d.return_rate || 0)}</span>
                    <span class="metric-label">Return Rate</span>
                </div>
            </div>
            <div style="margin-top: 1rem;">
                <div style="display: flex; justify-content: space-between; font-size: 0.75rem; color: #64748b; margin-bottom: 0.25rem;">
                    <span>Margin</span>
                    <span>${formatCurrency(d.gross_margin || 0)}</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${Math.min(100, ((d.gross_margin || 0) / (d.net_sales || 1)) * 100 * 3)}%"></div>
                </div>
            </div>
        </div>
    `).join('');
}

// Load Territory Heatmap (for territory page)
async function loadTerritoryHeatmap() {
    const data = await loadJSON('territory_heatmap.json');
    if (!data || data.length === 0) return;

    // Group by region
    const regions = data.reduce((acc, t) => {
        const region = t.region || 'Unknown';
        if (!acc[region]) {
            acc[region] = {
                territories: [],
                totalSales: 0,
                totalOperators: 0
            };
        }
        acc[region].territories.push(t);
        acc[region].totalSales += t.net_sales || 0;
        acc[region].totalOperators += t.operators || 0;
        return acc;
    }, {});

    const container = document.getElementById('regionGrid');
    if (!container) return;

    container.innerHTML = Object.entries(regions).map(([name, data]) => `
        <div class="region-card" onclick="showTerritoryDetails('${name}')">
            <div class="region-name">${name}</div>
            <div class="region-sales">${formatCurrency(data.totalSales)}</div>
            <div class="region-operators">${formatNumber(data.totalOperators)} operators</div>
        </div>
    `).join('');

    // Territory details table
    const tbody = document.getElementById('territoryTable');
    if (!tbody) return;

    tbody.innerHTML = data.slice(0, 10).map(t => `
        <tr>
            <td><strong>${t.territory_name}</strong></td>
            <td>${t.state}</td>
            <td><span class="badge badge-primary">${t.region}</span></td>
            <td>${formatCurrency(t.net_sales || 0)}</td>
            <td>${formatNumber(t.operators || 0)}</td>
            <td>${t.reps || 0}</td>
        </tr>
    `).join('');
}

// Load Rep Performance (for rep page)
async function loadRepPerformance() {
    const data = await loadJSON('rep_rankings.json');
    if (!data || data.length === 0) return;

    const tbody = document.getElementById('repTable');
    if (!tbody) return;

    tbody.innerHTML = data.map((rep, index) => `
        <tr>
            <td>
                <span class="badge ${index < 3 ? 'badge-success' : 'badge-primary'}">#${index + 1}</span>
            </td>
            <td><strong>${rep.rep_name}</strong></td>
            <td><span class="badge badge-primary">${rep.rep_tier || 'N/A'}</span></td>
            <td>${rep.territory_name || 'N/A'}</td>
            <td>${formatCurrency(rep.revenue || 0)}</td>
            <td>
                <span class="badge ${(rep.win_rate || 0) >= 40 ? 'badge-success' : (rep.win_rate || 0) >= 25 ? 'badge-warning' : 'badge-danger'}">
                    ${formatPercent(rep.win_rate || 0)}
                </span>
            </td>
            <td>${rep.won || 0} / ${rep.opportunities || 0}</td>
            <td>${formatCurrency(rep.avg_deal || 0)}</td>
            <td>
                <span class="badge ${(rep.quota_attainment || 0) >= 100 ? 'badge-success' : 'badge-warning'}">
                    ${formatPercent(rep.quota_attainment || 0)}
                </span>
            </td>
            <td>${rep.activities || 0}</td>
        </tr>
    `).join('');

    // Activity correlation chart
    loadActivityCorrelation(data);
}

// Activity-Revenue Correlation Chart
async function loadActivityCorrelation(repData) {
    const ctx = document.getElementById('correlationChart');
    if (!ctx) return;

    const correlation = await loadJSON('activity_correlation.json');
    const data = correlation || repData;

    new Chart(ctx, {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Reps',
                data: data.map(d => ({
                    x: d.total_activities || d.activities || 0,
                    y: d.revenue || 0,
                    name: d.rep_name
                })),
                backgroundColor: CHART_COLORS.primary + '99',
                borderColor: CHART_COLORS.primary,
                pointRadius: 8,
                pointHoverRadius: 12
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            return [
                                `${context.raw.name}`,
                                `Activities: ${context.raw.x}`,
                                `Revenue: ${formatCurrency(context.raw.y)}`
                            ];
                        }
                    }
                }
            },
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Total Activities',
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#334155'
                    },
                    ticks: {
                        color: '#64748b'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'Revenue',
                        color: '#94a3b8'
                    },
                    grid: {
                        color: '#334155'
                    },
                    ticks: {
                        color: '#64748b',
                        callback: function (value) {
                            return formatCurrency(value);
                        }
                    }
                }
            }
        }
    });
}

// Load Pipeline Health (for pipeline page)
async function loadPipelineHealth() {
    const data = await loadJSON('pipeline_health.json');
    if (!data || data.length === 0) return;

    const container = document.getElementById('pipelineFunnel');
    if (!container) return;

    const maxValue = Math.max(...data.map(d => d.value));

    container.innerHTML = data.map(d => `
        <div class="funnel-stage">
            <div class="funnel-label">${d.stage}</div>
            <div class="funnel-bar">
                <div class="funnel-fill" style="width: ${(d.value / maxValue) * 100}%">
                    ${d.count}
                </div>
            </div>
            <div class="funnel-value">${formatCurrency(d.value)}</div>
        </div>
    `).join('');

    // Win rate trend chart
    const ctx = document.getElementById('winRateTrendChart');
    if (!ctx) return;

    // Simulated monthly win rate data
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const winRates = [32, 35, 31, 38, 42, 40, 45, 43, 41, 44, 46, 48];

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: months,
            datasets: [{
                label: 'Win Rate %',
                data: winRates,
                borderColor: CHART_COLORS.secondary,
                backgroundColor: createGradient(ctx.getContext('2d'), CHART_COLORS.gradients.success),
                fill: true,
                tension: 0.4,
                pointRadius: 4,
                pointHoverRadius: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            return `Win Rate: ${context.raw}%`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        color: '#94a3b8'
                    }
                },
                y: {
                    min: 0,
                    max: 60,
                    grid: {
                        color: '#334155'
                    },
                    ticks: {
                        color: '#64748b',
                        callback: function (value) {
                            return value + '%';
                        }
                    }
                }
            }
        }
    });
}

// Export for use in HTML
window.loadExecutiveSummary = loadExecutiveSummary;
window.loadMonthlyTrends = loadMonthlyTrends;
window.loadDistributorData = loadDistributorData;
window.loadRepRankings = loadRepRankings;
window.loadPipelineData = loadPipelineData;
window.loadYoYData = loadYoYData;
window.loadDistributorScorecards = loadDistributorScorecards;
window.loadTerritoryHeatmap = loadTerritoryHeatmap;
window.loadRepPerformance = loadRepPerformance;
window.loadPipelineHealth = loadPipelineHealth;
