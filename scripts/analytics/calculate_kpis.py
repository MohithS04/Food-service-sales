"""
Analytics KPI Calculator
Computes all key performance indicators for the dashboard
"""

import sqlite3
import pandas as pd
import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'database', 'foodservice_analytics.db')
OUTPUT_DIR = os.path.join(BASE_DIR, 'dashboards', 'data')


def calculate_kpis():
    """Calculate all KPIs and export to JSON"""
    
    print("Calculating KPIs...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Executive Summary KPIs
    exec_summary = pd.read_sql("""
        SELECT 
            -- Sales Metrics
            ROUND(SUM(net_sales), 2) as total_net_sales,
            ROUND(SUM(gross_sales), 2) as total_gross_sales,
            ROUND(SUM(net_sales) - SUM(cost_of_goods), 2) as gross_margin,
            ROUND(100.0 * (SUM(net_sales) - SUM(cost_of_goods)) / SUM(net_sales), 1) as margin_pct,
            SUM(quantity) as total_units,
            COUNT(DISTINCT operator_id) as total_operators,
            COUNT(DISTINCT distributor_id) as total_distributors,
            COUNT(*) as total_shipments
        FROM shipments
    """, conn)
    
    # Add opportunity metrics
    opp_metrics = pd.read_sql("""
        SELECT 
            COUNT(*) as total_opportunities,
            SUM(CASE WHEN stage = 'Closed Won' THEN 1 ELSE 0 END) as won_deals,
            SUM(CASE WHEN stage = 'Closed Lost' THEN 1 ELSE 0 END) as lost_deals,
            ROUND(100.0 * SUM(CASE WHEN stage = 'Closed Won' THEN 1 ELSE 0 END) / 
                  NULLIF(SUM(CASE WHEN stage IN ('Closed Won', 'Closed Lost') THEN 1 ELSE 0 END), 0), 1) as win_rate,
            SUM(CASE WHEN stage = 'Closed Won' THEN amount ELSE 0 END) as revenue_won,
            ROUND(AVG(CASE WHEN stage = 'Closed Won' THEN amount END), 2) as avg_deal_size,
            SUM(CASE WHEN stage NOT IN ('Closed Won', 'Closed Lost') THEN amount ELSE 0 END) as pipeline_value
        FROM sf_opportunities
    """, conn)
    
    summary = {**exec_summary.iloc[0].to_dict(), **opp_metrics.iloc[0].to_dict()}
    
    with open(os.path.join(OUTPUT_DIR, 'executive_summary.json'), 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    print("  ✓ Executive Summary")
    
    # 2. YoY Growth by Distributor
    yoy_growth = pd.read_sql("""
        WITH yearly AS (
            SELECT 
                strftime('%Y', week_ending) as year,
                d.distributor_name,
                d.distributor_type,
                SUM(s.net_sales) as net_sales
            FROM shipments s
            JOIN distributors d ON s.distributor_id = d.distributor_id
            GROUP BY strftime('%Y', week_ending), d.distributor_name, d.distributor_type
        )
        SELECT 
            curr.year,
            curr.distributor_name,
            curr.distributor_type,
            curr.net_sales,
            prev.net_sales as prior_year,
            ROUND((curr.net_sales - COALESCE(prev.net_sales, 0)) / NULLIF(prev.net_sales, 0) * 100, 1) as yoy_growth
        FROM yearly curr
        LEFT JOIN yearly prev ON curr.distributor_name = prev.distributor_name 
            AND CAST(curr.year AS INT) = CAST(prev.year AS INT) + 1
        ORDER BY curr.year DESC, curr.net_sales DESC
    """, conn)
    
    yoy_growth.to_json(os.path.join(OUTPUT_DIR, 'yoy_growth.json'), orient='records', indent=2)
    print("  ✓ YoY Growth")
    
    # 3. Distributor Scorecards
    distributor_cards = pd.read_sql("""
        SELECT 
            d.distributor_id,
            d.distributor_name,
            d.distributor_type,
            t.region,
            COUNT(DISTINCT s.operator_id) as active_operators,
            COUNT(DISTINCT s.product_id) as products_sold,
            SUM(s.quantity) as total_units,
            ROUND(SUM(s.net_sales), 2) as net_sales,
            ROUND(SUM(s.gross_sales), 2) as gross_sales,
            ROUND(100.0 * SUM(s.returns) / NULLIF(SUM(s.gross_sales), 0), 2) as return_rate,
            ROUND(SUM(s.net_sales) - SUM(s.cost_of_goods), 2) as gross_margin
        FROM distributors d
        LEFT JOIN territories t ON d.territory_id = t.territory_id
        LEFT JOIN shipments s ON d.distributor_id = s.distributor_id
        GROUP BY d.distributor_id, d.distributor_name, d.distributor_type, t.region
        ORDER BY net_sales DESC
    """, conn)
    
    distributor_cards.to_json(os.path.join(OUTPUT_DIR, 'distributor_scorecards.json'), orient='records', indent=2)
    print("  ✓ Distributor Scorecards")
    
    # 4. Rep Performance Rankings
    rep_rankings = pd.read_sql("""
        SELECT 
            sr.rep_id,
            sr.rep_name,
            sr.rep_tier,
            t.territory_name,
            t.region,
            sr.quota_annual,
            COUNT(DISTINCT o.opportunity_id) as opportunities,
            SUM(CASE WHEN o.stage = 'Closed Won' THEN 1 ELSE 0 END) as won,
            SUM(CASE WHEN o.stage = 'Closed Lost' THEN 1 ELSE 0 END) as lost,
            ROUND(100.0 * SUM(CASE WHEN o.stage = 'Closed Won' THEN 1 ELSE 0 END) / 
                  NULLIF(SUM(CASE WHEN o.stage IN ('Closed Won', 'Closed Lost') THEN 1 ELSE 0 END), 0), 1) as win_rate,
            SUM(CASE WHEN o.stage = 'Closed Won' THEN o.amount ELSE 0 END) as revenue,
            ROUND(AVG(CASE WHEN o.stage = 'Closed Won' THEN o.amount END), 2) as avg_deal,
            ROUND(100.0 * SUM(CASE WHEN o.stage = 'Closed Won' THEN o.amount ELSE 0 END) / 
                  NULLIF(sr.quota_annual, 0), 1) as quota_attainment,
            COUNT(DISTINCT a.activity_id) as activities,
            ROUND(1.0 * COUNT(DISTINCT a.activity_id) / NULLIF(COUNT(DISTINCT o.opportunity_id), 0), 1) as activities_per_opp
        FROM sales_reps sr
        LEFT JOIN territories t ON sr.territory_id = t.territory_id
        LEFT JOIN sf_opportunities o ON sr.rep_id = o.owner_id
        LEFT JOIN sf_activities a ON sr.rep_id = a.owner_id
        WHERE sr.rep_tier != 'Director'
        GROUP BY sr.rep_id, sr.rep_name, sr.rep_tier, t.territory_name, t.region, sr.quota_annual
        ORDER BY revenue DESC
    """, conn)
    
    rep_rankings.to_json(os.path.join(OUTPUT_DIR, 'rep_rankings.json'), orient='records', indent=2)
    print("  ✓ Rep Rankings")
    
    # 5. Territory Heatmap Data
    territory_data = pd.read_sql("""
        SELECT 
            t.territory_id,
            t.territory_name,
            t.region,
            t.state,
            COUNT(DISTINCT o.operator_id) as operators,
            COUNT(DISTINCT sr.rep_id) as reps,
            ROUND(SUM(s.net_sales), 2) as net_sales,
            COUNT(DISTINCT opp.opportunity_id) as opportunities,
            SUM(CASE WHEN opp.stage = 'Closed Won' THEN opp.amount ELSE 0 END) as revenue_won
        FROM territories t
        LEFT JOIN operators o ON t.territory_id = o.territory_id
        LEFT JOIN sales_reps sr ON t.territory_id = sr.territory_id
        LEFT JOIN shipments s ON o.operator_id = s.operator_id
        LEFT JOIN sf_accounts a ON o.operator_id = a.operator_id
        LEFT JOIN sf_opportunities opp ON a.account_id = opp.account_id
        GROUP BY t.territory_id, t.territory_name, t.region, t.state
        ORDER BY net_sales DESC
    """, conn)
    
    territory_data.to_json(os.path.join(OUTPUT_DIR, 'territory_heatmap.json'), orient='records', indent=2)
    print("  ✓ Territory Heatmap")
    
    # 6. Pipeline Health
    pipeline_data = pd.read_sql("""
        SELECT 
            stage,
            COUNT(*) as count,
            SUM(amount) as value,
            AVG(probability) as avg_probability,
            SUM(amount * probability / 100.0) as weighted_value
        FROM sf_opportunities
        WHERE stage NOT IN ('Closed Won', 'Closed Lost')
        GROUP BY stage
        ORDER BY avg_probability
    """, conn)
    
    pipeline_data.to_json(os.path.join(OUTPUT_DIR, 'pipeline_health.json'), orient='records', indent=2)
    print("  ✓ Pipeline Health")
    
    # 7. Monthly Trends
    monthly_trends = pd.read_sql("""
        SELECT 
            strftime('%Y-%m', week_ending) as month,
            SUM(net_sales) as net_sales,
            SUM(quantity) as units,
            COUNT(DISTINCT operator_id) as active_operators,
            ROUND(SUM(net_sales) - SUM(cost_of_goods), 2) as gross_margin
        FROM shipments
        GROUP BY strftime('%Y-%m', week_ending)
        ORDER BY month
    """, conn)
    
    monthly_trends.to_json(os.path.join(OUTPUT_DIR, 'monthly_trends.json'), orient='records', indent=2)
    print("  ✓ Monthly Trends")
    
    # 8. Activity-Revenue Correlation
    activity_correlation = pd.read_sql("""
        SELECT 
            sr.rep_id,
            sr.rep_name,
            COUNT(DISTINCT a.activity_id) as total_activities,
            SUM(CASE WHEN a.activity_type = 'Call' THEN 1 ELSE 0 END) as calls,
            SUM(CASE WHEN a.activity_type = 'Meeting' THEN 1 ELSE 0 END) as meetings,
            SUM(CASE WHEN a.activity_type = 'Site Visit' THEN 1 ELSE 0 END) as site_visits,
            SUM(CASE WHEN o.stage = 'Closed Won' THEN o.amount ELSE 0 END) as revenue
        FROM sales_reps sr
        LEFT JOIN sf_activities a ON sr.rep_id = a.owner_id
        LEFT JOIN sf_opportunities o ON sr.rep_id = o.owner_id AND o.stage = 'Closed Won'
        WHERE sr.rep_tier != 'Director'
        GROUP BY sr.rep_id, sr.rep_name
        HAVING total_activities > 0
    """, conn)
    
    activity_correlation.to_json(os.path.join(OUTPUT_DIR, 'activity_correlation.json'), orient='records', indent=2)
    print("  ✓ Activity Correlation")
    
    conn.close()
    print("\nAll KPIs calculated and exported!")
    
    return summary


if __name__ == "__main__":
    summary = calculate_kpis()
    print("\nExecutive Summary:")
    for k, v in summary.items():
        if isinstance(v, float):
            print(f"  {k}: {v:,.2f}")
        else:
            print(f"  {k}: {v:,}" if isinstance(v, int) else f"  {k}: {v}")
