-- Foodservice Sales Analytics Database Schema
-- Enterprise-style schema modeled after Salesforce CRM + Distributor systems

-- ============================================
-- DIMENSION TABLES (Master Data)
-- ============================================

-- Regions/Territories
CREATE TABLE IF NOT EXISTS territories (
    territory_id TEXT PRIMARY KEY,
    territory_name TEXT NOT NULL,
    region TEXT NOT NULL,
    state TEXT NOT NULL,
    timezone TEXT
);

-- Distributors (Sysco, US Foods, etc.)
CREATE TABLE IF NOT EXISTS distributors (
    distributor_id TEXT PRIMARY KEY,
    distributor_name TEXT NOT NULL,
    distributor_type TEXT,  -- National, Regional, Specialty
    headquarters_state TEXT,
    territory_id TEXT,
    active_since DATE,
    FOREIGN KEY (territory_id) REFERENCES territories(territory_id)
);

-- Operators (Restaurants, Hotels, Institutions)
CREATE TABLE IF NOT EXISTS operators (
    operator_id TEXT PRIMARY KEY,
    operator_name TEXT NOT NULL,
    operator_type TEXT,  -- Restaurant, Hotel, Hospital, School, etc.
    cuisine_type TEXT,
    city TEXT,
    state TEXT,
    county TEXT,
    zip_code TEXT,
    territory_id TEXT,
    opening_date DATE,
    annual_revenue_tier TEXT,  -- Small, Medium, Large, Enterprise
    primary_distributor_id TEXT,
    FOREIGN KEY (territory_id) REFERENCES territories(territory_id),
    FOREIGN KEY (primary_distributor_id) REFERENCES distributors(distributor_id)
);

-- Products
CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    brand TEXT,
    category TEXT,  -- Proteins, Dairy, Produce, Beverages, etc.
    subcategory TEXT,
    unit_of_measure TEXT,
    standard_price REAL,
    cost REAL,
    active INTEGER DEFAULT 1
);

-- Sales Representatives
CREATE TABLE IF NOT EXISTS sales_reps (
    rep_id TEXT PRIMARY KEY,
    rep_name TEXT NOT NULL,
    email TEXT,
    hire_date DATE,
    territory_id TEXT,
    manager_id TEXT,
    quota_annual REAL,
    rep_tier TEXT,  -- Junior, Senior, Director
    FOREIGN KEY (territory_id) REFERENCES territories(territory_id),
    FOREIGN KEY (manager_id) REFERENCES sales_reps(rep_id)
);

-- ============================================
-- SALESFORCE CRM TABLES
-- ============================================

-- Accounts (linked to Operators)
CREATE TABLE IF NOT EXISTS sf_accounts (
    account_id TEXT PRIMARY KEY,
    operator_id TEXT NOT NULL,
    account_name TEXT NOT NULL,
    account_type TEXT,  -- Prospect, Customer, Former Customer
    industry TEXT,
    owner_id TEXT,  -- Sales Rep
    created_date DATE,
    last_activity_date DATE,
    account_status TEXT,  -- Active, Inactive, Churned
    FOREIGN KEY (operator_id) REFERENCES operators(operator_id),
    FOREIGN KEY (owner_id) REFERENCES sales_reps(rep_id)
);

-- Opportunities
CREATE TABLE IF NOT EXISTS sf_opportunities (
    opportunity_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    opportunity_name TEXT NOT NULL,
    stage TEXT,  -- Prospecting, Qualification, Proposal, Negotiation, Closed Won, Closed Lost
    amount REAL,
    probability INTEGER,
    close_date DATE,
    created_date DATE,
    owner_id TEXT,
    lead_source TEXT,  -- Trade Show, Referral, Cold Call, Website
    product_interest TEXT,
    competitor TEXT,
    loss_reason TEXT,
    FOREIGN KEY (account_id) REFERENCES sf_accounts(account_id),
    FOREIGN KEY (owner_id) REFERENCES sales_reps(rep_id)
);

-- Activities (Calls, Emails, Meetings)
CREATE TABLE IF NOT EXISTS sf_activities (
    activity_id TEXT PRIMARY KEY,
    account_id TEXT,
    opportunity_id TEXT,
    owner_id TEXT NOT NULL,
    activity_type TEXT,  -- Call, Email, Meeting, Demo, Site Visit
    activity_date DATE,
    duration_minutes INTEGER,
    subject TEXT,
    outcome TEXT,  -- Connected, Left Voicemail, No Answer, Completed
    next_steps TEXT,
    FOREIGN KEY (account_id) REFERENCES sf_accounts(account_id),
    FOREIGN KEY (opportunity_id) REFERENCES sf_opportunities(opportunity_id),
    FOREIGN KEY (owner_id) REFERENCES sales_reps(rep_id)
);

-- ============================================
-- DISTRIBUTOR FACT TABLES
-- ============================================

-- Weekly Shipments
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id TEXT PRIMARY KEY,
    shipment_date DATE NOT NULL,
    week_ending DATE NOT NULL,
    distributor_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER,
    gross_sales REAL,
    discounts REAL DEFAULT 0,
    returns REAL DEFAULT 0,
    net_sales REAL,
    cost_of_goods REAL,
    FOREIGN KEY (distributor_id) REFERENCES distributors(distributor_id),
    FOREIGN KEY (operator_id) REFERENCES operators(operator_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- ============================================
-- ANALYTICS VIEWS
-- ============================================

-- Distributor Scorecard View
CREATE VIEW IF NOT EXISTS vw_distributor_scorecard AS
SELECT 
    d.distributor_id,
    d.distributor_name,
    d.distributor_type,
    t.region,
    strftime('%Y', s.week_ending) as year,
    strftime('%m', s.week_ending) as month,
    COUNT(DISTINCT s.operator_id) as active_operators,
    COUNT(DISTINCT s.product_id) as products_sold,
    SUM(s.quantity) as total_units,
    SUM(s.gross_sales) as gross_sales,
    SUM(s.net_sales) as net_sales,
    SUM(s.returns) as total_returns,
    ROUND(SUM(s.net_sales) - SUM(s.cost_of_goods), 2) as gross_margin
FROM distributors d
LEFT JOIN territories t ON d.territory_id = t.territory_id
LEFT JOIN shipments s ON d.distributor_id = s.distributor_id
GROUP BY d.distributor_id, d.distributor_name, d.distributor_type, t.region,
         strftime('%Y', s.week_ending), strftime('%m', s.week_ending);

-- Sales Rep Performance View
CREATE VIEW IF NOT EXISTS vw_rep_performance AS
SELECT 
    sr.rep_id,
    sr.rep_name,
    sr.rep_tier,
    t.territory_name,
    t.region,
    strftime('%Y', o.close_date) as year,
    COUNT(DISTINCT o.opportunity_id) as total_opportunities,
    SUM(CASE WHEN o.stage = 'Closed Won' THEN 1 ELSE 0 END) as won_deals,
    SUM(CASE WHEN o.stage = 'Closed Lost' THEN 1 ELSE 0 END) as lost_deals,
    ROUND(100.0 * SUM(CASE WHEN o.stage = 'Closed Won' THEN 1 ELSE 0 END) / 
          NULLIF(COUNT(DISTINCT o.opportunity_id), 0), 1) as win_rate,
    SUM(CASE WHEN o.stage = 'Closed Won' THEN o.amount ELSE 0 END) as total_revenue,
    ROUND(AVG(CASE WHEN o.stage = 'Closed Won' THEN o.amount END), 2) as avg_deal_size,
    COUNT(DISTINCT a.activity_id) as total_activities
FROM sales_reps sr
LEFT JOIN territories t ON sr.territory_id = t.territory_id
LEFT JOIN sf_opportunities o ON sr.rep_id = o.owner_id
LEFT JOIN sf_activities a ON sr.rep_id = a.owner_id 
    AND strftime('%Y', a.activity_date) = strftime('%Y', o.close_date)
GROUP BY sr.rep_id, sr.rep_name, sr.rep_tier, t.territory_name, t.region,
         strftime('%Y', o.close_date);

-- Pipeline Health View
CREATE VIEW IF NOT EXISTS vw_pipeline_health AS
SELECT 
    strftime('%Y-%m', o.created_date) as period,
    o.stage,
    COUNT(*) as opportunity_count,
    SUM(o.amount) as pipeline_value,
    AVG(o.probability) as avg_probability,
    SUM(o.amount * o.probability / 100.0) as weighted_pipeline
FROM sf_opportunities o
WHERE o.stage NOT IN ('Closed Won', 'Closed Lost')
GROUP BY strftime('%Y-%m', o.created_date), o.stage;

-- Territory Summary View
CREATE VIEW IF NOT EXISTS vw_territory_summary AS
SELECT 
    t.territory_id,
    t.territory_name,
    t.region,
    t.state,
    COUNT(DISTINCT o.operator_id) as total_operators,
    COUNT(DISTINCT sr.rep_id) as total_reps,
    COUNT(DISTINCT d.distributor_id) as total_distributors,
    SUM(s.net_sales) as total_net_sales,
    COUNT(DISTINCT opp.opportunity_id) as total_opportunities
FROM territories t
LEFT JOIN operators o ON t.territory_id = o.territory_id
LEFT JOIN sales_reps sr ON t.territory_id = sr.territory_id
LEFT JOIN distributors d ON t.territory_id = d.territory_id
LEFT JOIN shipments s ON o.operator_id = s.operator_id
LEFT JOIN sf_accounts a ON o.operator_id = a.operator_id
LEFT JOIN sf_opportunities opp ON a.account_id = opp.account_id
GROUP BY t.territory_id, t.territory_name, t.region, t.state;

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

CREATE INDEX IF NOT EXISTS idx_shipments_date ON shipments(week_ending);
CREATE INDEX IF NOT EXISTS idx_shipments_distributor ON shipments(distributor_id);
CREATE INDEX IF NOT EXISTS idx_shipments_operator ON shipments(operator_id);
CREATE INDEX IF NOT EXISTS idx_opportunities_stage ON sf_opportunities(stage);
CREATE INDEX IF NOT EXISTS idx_opportunities_date ON sf_opportunities(close_date);
CREATE INDEX IF NOT EXISTS idx_activities_date ON sf_activities(activity_date);
CREATE INDEX IF NOT EXISTS idx_activities_owner ON sf_activities(owner_id);
