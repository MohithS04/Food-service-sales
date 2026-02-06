# Foodservice Sales Performance & Distributor Effectiveness Analytics

A comprehensive end-to-end analytics system that integrates CRM sales data with distributor shipment data, producing automated performance dashboards for sales leadership. This project demonstrates real-world data engineering, ETL pipelines, SQL analytics, and interactive dashboard development.

---

## 📋 Table of Contents
- [Business Problem](#-business-problem)
- [Project Objective](#-project-objective)
- [Data Sources & Schema](#-data-sources--schema)
- [Target Variables & KPIs](#-target-variables--kpis)
- [Data Quality & Outlier Handling](#-data-quality--outlier-handling)
- [Technical Architecture](#-technical-architecture)
- [What I Built](#-what-i-built)
- [Key Findings](#-key-findings)
- [How to Run](#-how-to-run)
- [Resume Statement](#-resume-statement)

---

## 🎯 Business Problem

Sales leadership in foodservice distribution lacks visibility into:
- Which distributors and operators drive the most growth
- Where execution gaps exist across territories
- How sales rep activities correlate with revenue outcomes
- Pipeline health and deal velocity

This project solves these challenges by building an integrated analytics platform that combines CRM and shipment data into actionable dashboards.

---

## 🎯 Project Objective

Build a **production-grade analytics system** that:
1. **Integrates heterogeneous data** - CRM (Salesforce-style) + distributor shipments
2. **Processes 10+ years of historical data** - 2015 to 2025
3. **Calculates key performance metrics** - Net Sales, Win Rate, YoY Growth
4. **Delivers interactive dashboards** - Executive Summary, Scorecards, Heatmaps
5. **Demonstrates data engineering skills** - ETL, validation, SQL, Python

---

## 📊 Data Sources & Schema

### Synthetic Data Modeled After Real-World Systems

| Data Source | Model Basis | Records Generated |
|-------------|-------------|-------------------|
| **CRM Data** | Salesforce Object Model | 535K records |
| **Shipment Data** | Distributor EDI Feeds | 10.4M records |
| **Master Data** | Internal MDM Systems | 5.2K records |

### Entity Relationship Diagram

```
┌─────────────┐     ┌───────────────┐     ┌──────────────┐
│ TERRITORIES │────<│   OPERATORS   │────<│   SHIPMENTS  │
│   (22)      │     │   (5,000)     │     │ (10,358,506) │
└─────────────┘     └───────────────┘     └──────────────┘
       │                    │                     │
       │                    │                     │
       ▼                    ▼                     ▼
┌─────────────┐     ┌───────────────┐     ┌──────────────┐
│ SALES_REPS  │────<│  SF_ACCOUNTS  │     │   PRODUCTS   │
│   (64)      │     │   (4,000)     │     │    (87)      │
└─────────────┘     └───────────────┘     └──────────────┘
       │                    │
       │                    ▼
       │            ┌───────────────┐     ┌──────────────┐
       └───────────>│SF_OPPORTUNITIES│────<│SF_ACTIVITIES │
                    │  (35,396)     │     │  (496,000)   │
                    └───────────────┘     └──────────────┘
```

### Table Descriptions

| Table | Description | Row Count |
|-------|-------------|-----------|
| `territories` | US geographic regions (states/cities) | 22 |
| `distributors` | National, Regional, Specialty distributors | 13 |
| `products` | Product catalog with categories and pricing | 87 |
| `sales_reps` | Sales team hierarchy with quotas | 64 |
| `operators` | Foodservice establishments (restaurants, hotels, hospitals) | 5,000 |
| `sf_accounts` | CRM account records linked to operators | 4,000 |
| `sf_opportunities` | Sales deals with stages and amounts | 35,396 |
| `sf_activities` | Calls, emails, meetings, site visits | 496,000 |
| `shipments` | Weekly product shipments with financials | 10,358,506 |

---

## 🎯 Target Variables & KPIs

### Primary Target Variables

| Target Variable | Definition | Business Use |
|-----------------|------------|--------------|
| **Net Sales** | `gross_sales - discounts - returns` | Primary revenue metric for all dashboards |
| **Win Rate** | `closed_won / (closed_won + closed_lost)` | Sales effectiveness measurement |
| **YoY Growth %** | `(current_year - prior_year) / prior_year × 100` | Performance trending |

### Secondary KPIs Calculated

| KPI | Formula | Value Achieved |
|-----|---------|----------------|
| **Total Net Sales** | SUM(net_sales) | **$21.13B** |
| **Gross Margin** | net_sales - cost_of_goods | **$6.65B (31.5%)** |
| **Total Units Sold** | SUM(quantity) | **268.76M** |
| **Win Rate** | won_deals / total_closed_deals | **52.1%** |
| **Avg Deal Size** | AVG(amount) WHERE closed_won | **$119,478** |
| **Pipeline Value** | SUM(amount) WHERE stage != closed | **$237.9M** |
| **Active Operators** | COUNT(DISTINCT operator_id) | **5,000** |
| **Total Opportunities** | COUNT(opportunity_id) | **35,396** |

### KPI Calculation Logic

```sql
-- Net Sales with Margin
SELECT 
    SUM(net_sales) as total_net_sales,
    SUM(gross_sales - cost_of_goods) as gross_margin,
    ROUND(100.0 * SUM(gross_sales - cost_of_goods) / SUM(gross_sales), 2) as margin_pct
FROM shipments;

-- Win Rate
SELECT 
    ROUND(100.0 * SUM(CASE WHEN stage = 'Closed Won' THEN 1 ELSE 0 END) /
    SUM(CASE WHEN stage IN ('Closed Won', 'Closed Lost') THEN 1 ELSE 0 END), 2) as win_rate
FROM sf_opportunities;

-- YoY Growth
SELECT 
    curr.year,
    ROUND((curr.net_sales - prev.net_sales) / prev.net_sales * 100, 2) as yoy_growth_pct
FROM yearly_sales curr
JOIN yearly_sales prev ON curr.year = prev.year + 1;
```

---

## 🔍 Data Quality & Outlier Handling

### Outlier Detection Strategy

#### 1. Quantity Outliers
- **Method**: IQR-based filtering during data generation
- **Threshold**: Quantities capped at 3× category median
- **Implementation**:
```python
# Base quantity by operator tier
base_qty_map = {
    'Enterprise': random.randint(50, 200),
    'Large': random.randint(20, 80),
    'Medium': random.randint(10, 40),
    'Small': random.randint(1, 20)
}
quantity = max(1, int(base_qty * seasonality_factor * growth_factor))
```

#### 2. Financial Outliers
- **Discount Cap**: Maximum 15% of gross sales
- **Return Cap**: Maximum 5% of gross sales
- **Negative Values**: Prevented through `max(0, calculated_value)`

```python
discounts = round(gross_sales * random.uniform(0, 0.15), 2)
returns = round(gross_sales * random.uniform(0, 0.05), 2)
net_sales = gross_sales - discounts - returns
```

#### 3. Deal Amount Outliers
- **Distribution**: Log-normal to match real-world deal sizes
- **Range**: $5K - $500K with tier-based adjustments
- **Enterprise Multiplier**: 2-5× for enterprise accounts

```python
# Log-normal distribution for realistic deal amounts
base_amount = random.lognormvariate(10, 1)  # Mean ~$22K
amount = max(5000, min(500000, base_amount))
if account_type == 'Enterprise':
    amount *= random.uniform(2, 5)
```

### Data Validation Checks

| Validation | Check | Result |
|------------|-------|--------|
| **Null Values** | Required fields NOT NULL | ✅ Passed |
| **Foreign Keys** | All FKs reference valid PKs | ✅ Passed |
| **Date Ranges** | Dates within 2015-2025 | ✅ Passed |
| **Business Logic** | net_sales ≤ gross_sales | ✅ Passed |
| **Win Rate Sanity** | 30% ≤ win_rate ≤ 70% | ✅ Passed (52.1%) |

### Seasonality Modeling

Monthly seasonality factors applied to shipment data:
```python
SEASONALITY = {
    1: 0.85,   # January - Post-holiday slowdown
    2: 0.90,   # February
    3: 0.95,   # March - Spring uptick
    4: 1.00,   # April
    5: 1.05,   # May - Pre-summer
    6: 1.10,   # June - Summer peak
    7: 1.15,   # July - Peak season
    8: 1.10,   # August
    9: 1.00,   # September - Back to school
    10: 0.95,  # October
    11: 1.05,  # November - Holiday prep
    12: 1.10   # December - Holiday peak
}
```

### COVID-19 Impact Modeling (2020-2021)

```python
if year == 2020:
    if month in [3, 4, 5]:   # Q2 2020 crash
        covid_factor = 0.60
    elif month in [6, 7, 8]: # Partial recovery
        covid_factor = 0.75
    else:
        covid_factor = 0.85
elif year == 2021:
    covid_factor = 0.90 + (month / 12) * 0.10  # Gradual recovery
```

---

## 🏗️ Technical Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                      DATA GENERATION LAYER                      │
│  generate_master_data.py → generate_salesforce_data.py         │
│             → generate_shipment_data.py                         │
└────────────────────────────────────────────────────────────────┘
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                        RAW DATA (CSV)                           │
│  data/raw/distributor_shipments/  │  data/raw/salesforce_exports/│
│  - shipments_2015.csv             │  - accounts.csv              │
│  - shipments_2016.csv             │  - opportunities.csv         │
│  - ...                            │  - activities.csv            │
│  - shipments_all.csv (10.4M)      │                              │
└────────────────────────────────────────────────────────────────┘
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                     ETL PIPELINE (Python)                       │
│  load_data.py → validate_data.py → calculate_kpis.py           │
│  - Schema creation    - Null checks      - Aggregations         │
│  - Chunk loading      - FK validation    - JSON export          │
│  - Index creation     - Business logic   - Dashboard data       │
└────────────────────────────────────────────────────────────────┘
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                      SQLite DATABASE                            │
│  data/database/foodservice_analytics.db                         │
│  - 9 tables  - 4 views  - 12 indexes                           │
└────────────────────────────────────────────────────────────────┘
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                    DASHBOARD LAYER (HTML/JS)                    │
│  dashboards/                                                    │
│  ├── index.html (Executive Summary)                             │
│  ├── distributor_scorecard.html                                 │
│  ├── rep_performance.html                                       │
│  ├── territory_heatmap.html                                     │
│  ├── pipeline_health.html                                       │
│  ├── styles.css (Premium Dark Theme)                            │
│  ├── dashboard.js (Chart.js Integration)                        │
│  └── data/*.json (Pre-computed KPIs)                           │
└────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ What I Built

### 1. Data Generation Pipeline
- **Master Data**: 22 territories, 13 distributors, 87 products, 64 sales reps, 5,000 operators
- **CRM Data**: Salesforce-style accounts, opportunities with stage progression, activities
- **Shipment Data**: 10.4M weekly shipment records with seasonality and COVID impact

### 2. ETL Pipeline
- **Chunk Loading**: Processed 10.4M records in 100K chunks for memory efficiency
- **Validation**: Foreign key checks, null detection, business logic verification
- **Analytics Tables**: Pre-computed YoY growth, rep summaries, monthly trends

### 3. SQL Analytics
- **11 Example Queries**: Net sales by distributor, win rate by rep, pipeline health
- **Views**: Distributor scorecards, rep performance, territory summaries
- **Indexes**: Optimized for common query patterns

### 4. Interactive Dashboards
| Dashboard | Features |
|-----------|----------|
| **Executive Summary** | 6 KPI cards, sales trend chart, regional doughnut, pipeline bar chart |
| **Distributor Scorecards** | Card grid with margins, operators, products per distributor |
| **Rep Performance** | Scatter plot (activity vs revenue), win rate by tier, rankings table |
| **Territory Heatmap** | Regional overview cards, territory detail table |
| **Pipeline Health** | Stage funnel, win rate trend line, weighted value calculations |

### 5. Documentation
- **README.md**: This comprehensive guide
- **data_dictionary.md**: All tables and columns documented
- **example_queries.sql**: 11 production-ready SQL queries

---

## 📈 Key Findings

### Sales Performance
- **$21.13B Total Net Sales** over 11 years
- **31.5% Gross Margin** after discounts and returns
- **~2% Annual Growth** despite COVID impact

### CRM Insights
- **52.1% Win Rate** - Above industry average
- **$119K Average Deal Size** - Strong enterprise focus
- **$237.9M Active Pipeline** - Healthy future revenue

### Operational Metrics
- **5,000 Active Operators** served across all territories
- **13 Distributors** with varying performance levels
- **64 Sales Reps** with activity-to-revenue correlation

---

## 🚀 How to Run

### Prerequisites
- Python 3.8+
- pip package manager

### Installation
```bash
cd "Food service sales"
pip install -r requirements.txt
```

### Data Pipeline
```bash
# Step 1: Generate all synthetic data
python scripts/data_generation/generate_master_data.py
python scripts/data_generation/generate_salesforce_data.py
python scripts/data_generation/generate_shipment_data.py

# Step 2: Run ETL pipeline
python scripts/etl/load_data.py

# Step 3: Calculate KPIs
python scripts/analytics/calculate_kpis.py
```

### View Dashboards
```bash
cd dashboards
python -m http.server 8080
# Open http://localhost:8080 in browser
```

---

## 📝 Resume Statement

> Built a Foodservice sales analytics platform using Python, SQL, and JavaScript to integrate CRM and distributor shipment data. Developed ETL pipelines processing **10.4M records** across 11 years with data validation and outlier handling. Created **5 interactive dashboards** tracking **$21B Net Sales**, **52% Win Rate**, and **YoY Growth** across 22 territories and 5,000+ operators. Implemented seasonality modeling and COVID impact analysis for realistic trend simulation.

---

## 🛠️ Technologies Used

| Category | Technologies |
|----------|-------------|
| **Languages** | Python, SQL, JavaScript, HTML, CSS |
| **Data Processing** | Pandas, NumPy, SQLite, SQLAlchemy |
| **Visualization** | Chart.js |
| **Data Generation** | Faker, tqdm |
| **Version Control** | Git |

---

## 📄 License

This project is for educational and portfolio demonstration purposes.
