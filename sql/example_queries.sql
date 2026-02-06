-- ================================================
-- Foodservice Analytics - SQL Query Examples
-- ================================================

-- ================================================
-- 1. NET SALES BY DISTRIBUTOR (YoY Comparison)
-- ================================================
WITH yearly_sales AS (
    SELECT 
        d.distributor_name,
        d.distributor_type,
        strftime('%Y', s.week_ending) AS year,
        SUM(s.net_sales) AS net_sales
    FROM shipments s
    JOIN distributors d ON s.distributor_id = d.distributor_id
    GROUP BY d.distributor_name, d.distributor_type, strftime('%Y', s.week_ending)
)
SELECT 
    curr.distributor_name,
    curr.distributor_type,
    curr.year,
    printf('$%,.2f', curr.net_sales) AS current_year_sales,
    printf('$%,.2f', prev.net_sales) AS prior_year_sales,
    ROUND((curr.net_sales - prev.net_sales) / prev.net_sales * 100, 1) || '%' AS yoy_growth
FROM yearly_sales curr
LEFT JOIN yearly_sales prev 
    ON curr.distributor_name = prev.distributor_name 
    AND CAST(curr.year AS INT) = CAST(prev.year AS INT) + 1
WHERE curr.year = '2024'
ORDER BY curr.net_sales DESC;


-- ================================================
-- 2. WIN RATE BY SALES REP
-- ================================================
SELECT 
    sr.rep_name,
    sr.rep_tier,
    t.territory_name,
    COUNT(DISTINCT o.opportunity_id) AS total_opportunities,
    SUM(CASE WHEN o.stage = 'Closed Won' THEN 1 ELSE 0 END) AS won,
    SUM(CASE WHEN o.stage = 'Closed Lost' THEN 1 ELSE 0 END) AS lost,
    ROUND(
        100.0 * SUM(CASE WHEN o.stage = 'Closed Won' THEN 1 ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN o.stage IN ('Closed Won', 'Closed Lost') THEN 1 ELSE 0 END), 0)
    , 1) AS win_rate_pct,
    printf('$%,.2f', SUM(CASE WHEN o.stage = 'Closed Won' THEN o.amount ELSE 0 END)) AS revenue_won
FROM sales_reps sr
JOIN territories t ON sr.territory_id = t.territory_id
LEFT JOIN sf_opportunities o ON sr.rep_id = o.owner_id
WHERE sr.rep_tier != 'Director'
GROUP BY sr.rep_id, sr.rep_name, sr.rep_tier, t.territory_name
HAVING total_opportunities > 0
ORDER BY revenue_won DESC
LIMIT 20;


-- ================================================
-- 3. AVERAGE DEAL SIZE BY LEAD SOURCE
-- ================================================
SELECT 
    lead_source,
    COUNT(*) AS total_deals,
    SUM(CASE WHEN stage = 'Closed Won' THEN 1 ELSE 0 END) AS won_deals,
    ROUND(100.0 * SUM(CASE WHEN stage = 'Closed Won' THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate,
    printf('$%,.2f', AVG(CASE WHEN stage = 'Closed Won' THEN amount END)) AS avg_deal_size,
    printf('$%,.2f', SUM(CASE WHEN stage = 'Closed Won' THEN amount ELSE 0 END)) AS total_revenue
FROM sf_opportunities
WHERE stage IN ('Closed Won', 'Closed Lost')
GROUP BY lead_source
ORDER BY total_revenue DESC;


-- ================================================
-- 4. ACTIVE OPERATORS PER DISTRIBUTOR
-- ================================================
SELECT 
    d.distributor_name,
    d.distributor_type,
    COUNT(DISTINCT s.operator_id) AS active_operators,
    COUNT(DISTINCT s.product_id) AS unique_products,
    SUM(s.quantity) AS total_units,
    printf('$%,.2f', SUM(s.net_sales)) AS net_sales,
    printf('$%,.2f', SUM(s.net_sales) / COUNT(DISTINCT s.operator_id)) AS revenue_per_operator
FROM distributors d
JOIN shipments s ON d.distributor_id = s.distributor_id
WHERE s.week_ending >= date('now', '-1 year')
GROUP BY d.distributor_id, d.distributor_name, d.distributor_type
ORDER BY net_sales DESC;


-- ================================================
-- 5. SALES REP ACTIVITY → REVENUE CORRELATION
-- ================================================
WITH rep_metrics AS (
    SELECT 
        sr.rep_id,
        sr.rep_name,
        COUNT(DISTINCT a.activity_id) AS total_activities,
        SUM(CASE WHEN a.activity_type = 'Call' THEN 1 ELSE 0 END) AS calls,
        SUM(CASE WHEN a.activity_type = 'Meeting' THEN 1 ELSE 0 END) AS meetings,
        SUM(CASE WHEN a.activity_type = 'Site Visit' THEN 1 ELSE 0 END) AS site_visits,
        SUM(CASE WHEN o.stage = 'Closed Won' THEN o.amount ELSE 0 END) AS revenue
    FROM sales_reps sr
    LEFT JOIN sf_activities a ON sr.rep_id = a.owner_id
    LEFT JOIN sf_opportunities o ON sr.rep_id = o.owner_id AND o.stage = 'Closed Won'
    WHERE sr.rep_tier != 'Director'
    GROUP BY sr.rep_id, sr.rep_name
)
SELECT 
    rep_name,
    total_activities,
    calls,
    meetings,
    site_visits,
    printf('$%,.2f', revenue) AS revenue,
    ROUND(revenue / NULLIF(total_activities, 0), 2) AS revenue_per_activity
FROM rep_metrics
WHERE total_activities > 0
ORDER BY revenue DESC
LIMIT 20;


-- ================================================
-- 6. PIPELINE HEALTH BY STAGE
-- ================================================
SELECT 
    stage,
    COUNT(*) AS opportunity_count,
    printf('$%,.2f', SUM(amount)) AS pipeline_value,
    ROUND(AVG(probability), 0) AS avg_probability,
    printf('$%,.2f', SUM(amount * probability / 100.0)) AS weighted_value,
    ROUND(100.0 * SUM(amount) / (SELECT SUM(amount) FROM sf_opportunities WHERE stage NOT IN ('Closed Won', 'Closed Lost')), 1) || '%' AS pct_of_pipeline
FROM sf_opportunities
WHERE stage NOT IN ('Closed Won', 'Closed Lost')
GROUP BY stage
ORDER BY avg_probability;


-- ================================================
-- 7. MONTHLY SALES TREND
-- ================================================
SELECT 
    strftime('%Y-%m', week_ending) AS month,
    printf('$%,.2f', SUM(net_sales)) AS net_sales,
    SUM(quantity) AS units,
    COUNT(DISTINCT operator_id) AS active_operators,
    printf('$%,.2f', SUM(net_sales) - SUM(cost_of_goods)) AS gross_margin,
    ROUND(100.0 * (SUM(net_sales) - SUM(cost_of_goods)) / SUM(net_sales), 1) || '%' AS margin_pct
FROM shipments
GROUP BY strftime('%Y-%m', week_ending)
ORDER BY month DESC
LIMIT 24;


-- ================================================
-- 8. TERRITORY PERFORMANCE RANKING
-- ================================================
SELECT 
    t.territory_name,
    t.region,
    t.state,
    COUNT(DISTINCT o.operator_id) AS operators,
    COUNT(DISTINCT sr.rep_id) AS reps,
    printf('$%,.2f', COALESCE(SUM(s.net_sales), 0)) AS net_sales,
    printf('$%,.2f', COALESCE(SUM(s.net_sales) / NULLIF(COUNT(DISTINCT sr.rep_id), 0), 0)) AS sales_per_rep
FROM territories t
LEFT JOIN operators o ON t.territory_id = o.territory_id
LEFT JOIN sales_reps sr ON t.territory_id = sr.territory_id
LEFT JOIN shipments s ON o.operator_id = s.operator_id
GROUP BY t.territory_id, t.territory_name, t.region, t.state
ORDER BY net_sales DESC;


-- ================================================
-- 9. TOP PRODUCTS BY REVENUE
-- ================================================
SELECT 
    p.product_name,
    p.brand,
    p.category,
    p.subcategory,
    SUM(s.quantity) AS units_sold,
    printf('$%,.2f', SUM(s.net_sales)) AS net_sales,
    printf('$%,.2f', SUM(s.net_sales) - SUM(s.cost_of_goods)) AS gross_margin,
    ROUND(100.0 * (SUM(s.net_sales) - SUM(s.cost_of_goods)) / SUM(s.net_sales), 1) || '%' AS margin_pct
FROM products p
JOIN shipments s ON p.product_id = s.product_id
GROUP BY p.product_id, p.product_name, p.brand, p.category, p.subcategory
ORDER BY net_sales DESC
LIMIT 25;


-- ================================================
-- 10. LOSS ANALYSIS BY REASON
-- ================================================
SELECT 
    COALESCE(loss_reason, 'Unknown') AS loss_reason,
    COUNT(*) AS lost_deals,
    printf('$%,.2f', SUM(amount)) AS lost_revenue,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM sf_opportunities WHERE stage = 'Closed Lost'), 1) || '%' AS pct_of_losses,
    ROUND(AVG(amount), 2) AS avg_deal_size
FROM sf_opportunities
WHERE stage = 'Closed Lost'
GROUP BY loss_reason
ORDER BY lost_deals DESC;


-- ================================================
-- 11. SEASONALITY ANALYSIS
-- ================================================
SELECT 
    strftime('%m', week_ending) AS month_num,
    CASE strftime('%m', week_ending)
        WHEN '01' THEN 'January'
        WHEN '02' THEN 'February'
        WHEN '03' THEN 'March'
        WHEN '04' THEN 'April'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'June'
        WHEN '07' THEN 'July'
        WHEN '08' THEN 'August'
        WHEN '09' THEN 'September'
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
    END AS month_name,
    printf('$%,.2f', AVG(monthly_sales)) AS avg_monthly_sales,
    ROUND(AVG(monthly_sales) / (SELECT AVG(net_sales) FROM shipments) * 100 / COUNT(DISTINCT strftime('%Y', week_ending)), 1) AS seasonality_index
FROM (
    SELECT 
        strftime('%Y-%m', week_ending) AS year_month,
        strftime('%m', week_ending) AS month,
        SUM(net_sales) AS monthly_sales
    FROM shipments
    GROUP BY strftime('%Y-%m', week_ending)
)
GROUP BY month_num
ORDER BY month_num;
