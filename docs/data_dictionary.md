# Data Dictionary

## Dimension Tables

### territories
| Column | Type | Description |
|--------|------|-------------|
| territory_id | TEXT | Primary key (e.g., "NE-NY-NYC") |
| territory_name | TEXT | Human-readable name |
| region | TEXT | Geographic region (Northeast, Southeast, Midwest, West) |
| state | TEXT | US state code |
| timezone | TEXT | IANA timezone identifier |

### distributors
| Column | Type | Description |
|--------|------|-------------|
| distributor_id | TEXT | Primary key (e.g., "DIST-001") |
| distributor_name | TEXT | Company name |
| distributor_type | TEXT | National, Regional, or Specialty |
| headquarters_state | TEXT | HQ state code |
| territory_id | TEXT | FK to territories |
| active_since | DATE | Start date |

### operators
| Column | Type | Description |
|--------|------|-------------|
| operator_id | TEXT | Primary key (e.g., "OP-000001") |
| operator_name | TEXT | Business name |
| operator_type | TEXT | Restaurant, Hotel, Hospital, etc. |
| cuisine_type | TEXT | Restaurant cuisine (if applicable) |
| city | TEXT | City name |
| state | TEXT | State code |
| county | TEXT | County name |
| zip_code | TEXT | ZIP code |
| territory_id | TEXT | FK to territories |
| opening_date | DATE | Business opening date |
| annual_revenue_tier | TEXT | Small, Medium, Large, Enterprise |
| primary_distributor_id | TEXT | FK to distributors |

### products
| Column | Type | Description |
|--------|------|-------------|
| product_id | TEXT | Primary key (e.g., "PROD-00001") |
| product_name | TEXT | Product name |
| brand | TEXT | Brand name |
| category | TEXT | Proteins, Dairy, Produce, etc. |
| subcategory | TEXT | Detailed category |
| unit_of_measure | TEXT | LB, CS, EA, GAL, OZ |
| standard_price | REAL | List price |
| cost | REAL | Product cost |
| active | INTEGER | 1 = active, 0 = inactive |

### sales_reps
| Column | Type | Description |
|--------|------|-------------|
| rep_id | TEXT | Primary key (e.g., "REP-001") |
| rep_name | TEXT | Full name |
| email | TEXT | Email address |
| hire_date | DATE | Employment start date |
| territory_id | TEXT | FK to territories |
| manager_id | TEXT | FK to sales_reps (self-ref) |
| quota_annual | REAL | Annual quota amount |
| rep_tier | TEXT | Junior, Senior, Director |

---

## CRM Tables (Salesforce Model)

### sf_accounts
| Column | Type | Description |
|--------|------|-------------|
| account_id | TEXT | Primary key (e.g., "ACC-000001") |
| operator_id | TEXT | FK to operators |
| account_name | TEXT | Account name |
| account_type | TEXT | Prospect, Customer, Former Customer |
| industry | TEXT | Industry classification |
| owner_id | TEXT | FK to sales_reps |
| created_date | DATE | Account creation date |
| last_activity_date | DATE | Most recent activity |
| account_status | TEXT | Active, Inactive, Churned |

### sf_opportunities
| Column | Type | Description |
|--------|------|-------------|
| opportunity_id | TEXT | Primary key (e.g., "OPP-0000001") |
| account_id | TEXT | FK to sf_accounts |
| opportunity_name | TEXT | Deal name |
| stage | TEXT | Prospecting → Closed Won/Lost |
| amount | REAL | Deal value |
| probability | INTEGER | Win probability % |
| close_date | DATE | Expected/actual close date |
| created_date | DATE | Opportunity creation date |
| owner_id | TEXT | FK to sales_reps |
| lead_source | TEXT | Trade Show, Referral, etc. |
| product_interest | TEXT | Primary product category |
| competitor | TEXT | Competing company (if known) |
| loss_reason | TEXT | Reason for loss (if applicable) |

### sf_activities
| Column | Type | Description |
|--------|------|-------------|
| activity_id | TEXT | Primary key (e.g., "ACT-00000001") |
| account_id | TEXT | FK to sf_accounts |
| opportunity_id | TEXT | FK to sf_opportunities (nullable) |
| owner_id | TEXT | FK to sales_reps |
| activity_type | TEXT | Call, Email, Meeting, Demo, Site Visit |
| activity_date | DATE | Activity date |
| duration_minutes | INTEGER | Duration in minutes |
| subject | TEXT | Activity subject line |
| outcome | TEXT | Connected, Left Voicemail, etc. |
| next_steps | TEXT | Follow-up notes |

---

## Fact Tables

### shipments
| Column | Type | Description |
|--------|------|-------------|
| shipment_id | TEXT | Primary key (e.g., "SHIP-0000000001") |
| shipment_date | DATE | Actual shipment date |
| week_ending | DATE | Week-ending Saturday |
| distributor_id | TEXT | FK to distributors |
| operator_id | TEXT | FK to operators |
| product_id | TEXT | FK to products |
| quantity | INTEGER | Units shipped |
| gross_sales | REAL | List price × quantity |
| discounts | REAL | Applied discounts |
| returns | REAL | Return value |
| net_sales | REAL | gross_sales - discounts - returns |
| cost_of_goods | REAL | Product cost × quantity |

---

## Analytics Views

### vw_distributor_scorecard
Pre-aggregated distributor metrics by year/month.

### vw_rep_performance
Sales rep performance with win rates and activity counts.

### vw_pipeline_health
Open pipeline by stage with weighted values.

### vw_territory_summary
Territory-level aggregations for heatmaps.
