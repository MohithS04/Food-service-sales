"""
ETL Pipeline - Data Loader
Loads all generated CSV data into SQLite database
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
DB_DIR = os.path.join(DATA_DIR, 'database')
DB_PATH = os.path.join(DB_DIR, 'foodservice_analytics.db')
SCHEMA_PATH = os.path.join(BASE_DIR, 'sql', 'schema.sql')


def create_database():
    """Create SQLite database and apply schema"""
    
    print("Creating database...")
    os.makedirs(DB_DIR, exist_ok=True)
    
    # Remove existing database if exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print("  Removed existing database")
    
    # Create new database and apply schema
    conn = sqlite3.connect(DB_PATH)
    
    with open(SCHEMA_PATH, 'r') as f:
        schema_sql = f.read()
    
    conn.executescript(schema_sql)
    conn.commit()
    
    print(f"  Database created: {DB_PATH}")
    return conn


def load_master_data(conn):
    """Load dimension tables (master data)"""
    
    print("\nLoading master data...")
    
    # Territories
    territories_df = pd.read_csv(os.path.join(RAW_DIR, 'territories.csv'))
    territories_df.to_sql('territories', conn, if_exists='replace', index=False)
    print(f"  Loaded {len(territories_df)} territories")
    
    # Distributors
    distributors_df = pd.read_csv(os.path.join(RAW_DIR, 'distributors.csv'))
    distributors_df.to_sql('distributors', conn, if_exists='replace', index=False)
    print(f"  Loaded {len(distributors_df)} distributors")
    
    # Products
    products_df = pd.read_csv(os.path.join(RAW_DIR, 'products.csv'))
    products_df.to_sql('products', conn, if_exists='replace', index=False)
    print(f"  Loaded {len(products_df)} products")
    
    # Sales Reps
    sales_reps_df = pd.read_csv(os.path.join(RAW_DIR, 'sales_reps.csv'))
    sales_reps_df.to_sql('sales_reps', conn, if_exists='replace', index=False)
    print(f"  Loaded {len(sales_reps_df)} sales reps")
    
    # Operators
    operators_df = pd.read_csv(os.path.join(RAW_DIR, 'operators.csv'))
    operators_df.to_sql('operators', conn, if_exists='replace', index=False)
    print(f"  Loaded {len(operators_df)} operators")


def load_salesforce_data(conn):
    """Load Salesforce CRM data"""
    
    print("\nLoading Salesforce CRM data...")
    sf_dir = os.path.join(RAW_DIR, 'salesforce_exports')
    
    # Accounts
    accounts_df = pd.read_csv(os.path.join(sf_dir, 'sf_accounts.csv'))
    accounts_df.to_sql('sf_accounts', conn, if_exists='replace', index=False)
    print(f"  Loaded {len(accounts_df)} accounts")
    
    # Opportunities
    opportunities_df = pd.read_csv(os.path.join(sf_dir, 'sf_opportunities.csv'))
    opportunities_df.to_sql('sf_opportunities', conn, if_exists='replace', index=False)
    print(f"  Loaded {len(opportunities_df)} opportunities")
    
    # Activities
    activities_df = pd.read_csv(os.path.join(sf_dir, 'sf_activities.csv'))
    activities_df.to_sql('sf_activities', conn, if_exists='replace', index=False)
    print(f"  Loaded {len(activities_df)} activities")


def load_shipment_data(conn):
    """Load distributor shipment data"""
    
    print("\nLoading shipment data...")
    shipments_dir = os.path.join(RAW_DIR, 'distributor_shipments')
    
    # Load combined file
    shipments_path = os.path.join(shipments_dir, 'shipments_all.csv')
    
    if os.path.exists(shipments_path):
        # Load in chunks for large file
        chunk_size = 100000
        total_rows = 0
        
        for i, chunk in enumerate(pd.read_csv(shipments_path, chunksize=chunk_size)):
            if i == 0:
                chunk.to_sql('shipments', conn, if_exists='replace', index=False)
            else:
                chunk.to_sql('shipments', conn, if_exists='append', index=False)
            total_rows += len(chunk)
            print(f"  Loaded chunk {i+1}: {len(chunk)} rows (total: {total_rows:,})")
        
        print(f"  Total shipments loaded: {total_rows:,}")
    else:
        print("  WARNING: shipments_all.csv not found!")


def validate_data(conn):
    """Run data validation queries"""
    
    print("\n" + "=" * 50)
    print("Data Validation")
    print("=" * 50)
    
    validations = [
        ("Territories", "SELECT COUNT(*) FROM territories"),
        ("Distributors", "SELECT COUNT(*) FROM distributors"),
        ("Products", "SELECT COUNT(*) FROM products"),
        ("Sales Reps", "SELECT COUNT(*) FROM sales_reps"),
        ("Operators", "SELECT COUNT(*) FROM operators"),
        ("Accounts", "SELECT COUNT(*) FROM sf_accounts"),
        ("Opportunities", "SELECT COUNT(*) FROM sf_opportunities"),
        ("Activities", "SELECT COUNT(*) FROM sf_activities"),
        ("Shipments", "SELECT COUNT(*) FROM shipments"),
    ]
    
    cursor = conn.cursor()
    
    for name, query in validations:
        result = cursor.execute(query).fetchone()[0]
        print(f"  {name}: {result:,}")
    
    # Check for foreign key issues
    print("\nForeign Key Validation:")
    
    fk_checks = [
        ("Operators without valid territory", 
         "SELECT COUNT(*) FROM operators o LEFT JOIN territories t ON o.territory_id = t.territory_id WHERE t.territory_id IS NULL"),
        ("Accounts without valid operator",
         "SELECT COUNT(*) FROM sf_accounts a LEFT JOIN operators o ON a.operator_id = o.operator_id WHERE o.operator_id IS NULL"),
        ("Opportunities without valid account",
         "SELECT COUNT(*) FROM sf_opportunities op LEFT JOIN sf_accounts a ON op.account_id = a.account_id WHERE a.account_id IS NULL"),
    ]
    
    all_valid = True
    for name, query in fk_checks:
        result = cursor.execute(query).fetchone()[0]
        status = "✓ OK" if result == 0 else f"⚠ {result} issues"
        if result > 0:
            all_valid = False
        print(f"  {name}: {status}")
    
    return all_valid


def create_analytics_tables(conn):
    """Create pre-computed analytics tables for faster dashboard queries"""
    
    print("\nCreating analytics tables...")
    
    cursor = conn.cursor()
    
    # YoY Growth Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analytics_yoy_growth AS
        WITH yearly_sales AS (
            SELECT 
                strftime('%Y', week_ending) as year,
                distributor_id,
                SUM(net_sales) as total_net_sales,
                SUM(quantity) as total_quantity,
                COUNT(DISTINCT operator_id) as active_operators
            FROM shipments
            GROUP BY strftime('%Y', week_ending), distributor_id
        )
        SELECT 
            curr.year,
            curr.distributor_id,
            curr.total_net_sales,
            curr.total_quantity,
            curr.active_operators,
            prev.total_net_sales as prior_year_sales,
            ROUND((curr.total_net_sales - COALESCE(prev.total_net_sales, 0)) / 
                  NULLIF(prev.total_net_sales, 0) * 100, 2) as yoy_growth_pct
        FROM yearly_sales curr
        LEFT JOIN yearly_sales prev 
            ON curr.distributor_id = prev.distributor_id 
            AND CAST(curr.year AS INTEGER) = CAST(prev.year AS INTEGER) + 1
    """)
    print("  Created analytics_yoy_growth table")
    
    # Rep Performance Summary
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analytics_rep_summary AS
        SELECT 
            sr.rep_id,
            sr.rep_name,
            sr.rep_tier,
            t.territory_name,
            t.region,
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
        GROUP BY sr.rep_id, sr.rep_name, sr.rep_tier, t.territory_name, t.region
    """)
    print("  Created analytics_rep_summary table")
    
    # Monthly Sales Trend
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analytics_monthly_trend AS
        SELECT 
            strftime('%Y-%m', week_ending) as month,
            SUM(net_sales) as net_sales,
            SUM(gross_sales) as gross_sales,
            SUM(returns) as returns,
            SUM(quantity) as quantity,
            COUNT(DISTINCT operator_id) as active_operators,
            COUNT(DISTINCT distributor_id) as active_distributors,
            COUNT(DISTINCT product_id) as products_sold
        FROM shipments
        GROUP BY strftime('%Y-%m', week_ending)
        ORDER BY month
    """)
    print("  Created analytics_monthly_trend table")
    
    conn.commit()


def generate_dashboard_data(conn):
    """Export analytics data to JSON for dashboard consumption"""
    
    print("\nExporting dashboard data...")
    
    dashboard_dir = os.path.join(BASE_DIR, 'dashboards', 'data')
    os.makedirs(dashboard_dir, exist_ok=True)
    
    exports = [
        ('monthly_trend', 'SELECT * FROM analytics_monthly_trend'),
        ('yoy_growth', 'SELECT * FROM analytics_yoy_growth'),
        ('rep_summary', 'SELECT * FROM analytics_rep_summary'),
        ('distributor_scorecard', 'SELECT * FROM vw_distributor_scorecard'),
        ('territory_summary', 'SELECT * FROM vw_territory_summary'),
    ]
    
    for name, query in exports:
        try:
            df = pd.read_sql(query, conn)
            df.to_json(os.path.join(dashboard_dir, f'{name}.json'), orient='records', indent=2)
            print(f"  Exported {name}.json ({len(df)} rows)")
        except Exception as e:
            print(f"  Error exporting {name}: {e}")


def main():
    """Main ETL pipeline"""
    
    print("=" * 50)
    print("Foodservice Analytics - ETL Pipeline")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    # Create database
    conn = create_database()
    
    try:
        # Load data
        load_master_data(conn)
        load_salesforce_data(conn)
        load_shipment_data(conn)
        
        # Create indexes (already in schema, but ensure)
        conn.commit()
        
        # Validate
        is_valid = validate_data(conn)
        
        # Create analytics tables
        create_analytics_tables(conn)
        
        # Export dashboard data
        generate_dashboard_data(conn)
        
        print("\n" + "=" * 50)
        print("ETL Pipeline Complete!")
        print(f"Database: {DB_PATH}")
        print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
