"""
Data Validation Script
Validates data integrity and quality after ETL
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'database', 'foodservice_analytics.db')


def run_validation():
    """Run comprehensive data validation"""
    
    print("=" * 60)
    print("Foodservice Analytics - Data Validation Report")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    issues = []
    
    # 1. Row Counts
    print("\n1. TABLE ROW COUNTS")
    print("-" * 40)
    tables = ['territories', 'distributors', 'products', 'sales_reps', 'operators',
              'sf_accounts', 'sf_opportunities', 'sf_activities', 'shipments']
    
    for table in tables:
        try:
            count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            status = "✓" if count > 0 else "⚠"
            print(f"  {status} {table}: {count:,}")
            if count == 0:
                issues.append(f"Table {table} is empty")
        except Exception as e:
            print(f"  ✗ {table}: ERROR - {e}")
            issues.append(f"Table {table} error: {e}")
    
    # 2. Null Checks
    print("\n2. NULL VALUE CHECKS (Required Fields)")
    print("-" * 40)
    
    null_checks = [
        ('distributors', 'distributor_name'),
        ('operators', 'operator_name'),
        ('products', 'product_name'),
        ('sales_reps', 'rep_name'),
        ('sf_opportunities', 'stage'),
        ('shipments', 'net_sales'),
    ]
    
    for table, column in null_checks:
        try:
            nulls = cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL").fetchone()[0]
            status = "✓" if nulls == 0 else "⚠"
            print(f"  {status} {table}.{column}: {nulls} nulls")
            if nulls > 0:
                issues.append(f"{table}.{column} has {nulls} null values")
        except Exception as e:
            print(f"  ✗ {table}.{column}: ERROR")
    
    # 3. Foreign Key Integrity
    print("\n3. FOREIGN KEY INTEGRITY")
    print("-" * 40)
    
    fk_checks = [
        ('operators → territories', 
         "SELECT COUNT(*) FROM operators o WHERE o.territory_id NOT IN (SELECT territory_id FROM territories)"),
        ('operators → distributors',
         "SELECT COUNT(*) FROM operators o WHERE o.primary_distributor_id NOT IN (SELECT distributor_id FROM distributors)"),
        ('sf_accounts → operators',
         "SELECT COUNT(*) FROM sf_accounts a WHERE a.operator_id NOT IN (SELECT operator_id FROM operators)"),
        ('sf_opportunities → accounts',
         "SELECT COUNT(*) FROM sf_opportunities o WHERE o.account_id NOT IN (SELECT account_id FROM sf_accounts)"),
        ('sf_activities → accounts',
         "SELECT COUNT(*) FROM sf_activities a WHERE a.account_id IS NOT NULL AND a.account_id NOT IN (SELECT account_id FROM sf_accounts)"),
        ('shipments → distributors',
         "SELECT COUNT(*) FROM shipments s WHERE s.distributor_id NOT IN (SELECT distributor_id FROM distributors)"),
        ('shipments → operators',
         "SELECT COUNT(*) FROM shipments s WHERE s.operator_id NOT IN (SELECT operator_id FROM operators)"),
        ('shipments → products',
         "SELECT COUNT(*) FROM shipments s WHERE s.product_id NOT IN (SELECT product_id FROM products)"),
    ]
    
    for name, query in fk_checks:
        try:
            orphans = cursor.execute(query).fetchone()[0]
            status = "✓" if orphans == 0 else "⚠"
            print(f"  {status} {name}: {orphans} orphan records")
            if orphans > 0:
                issues.append(f"{name} has {orphans} orphan records")
        except Exception as e:
            print(f"  ✗ {name}: ERROR - {e}")
    
    # 4. Date Range Validation
    print("\n4. DATE RANGE VALIDATION")
    print("-" * 40)
    
    date_queries = [
        ('Opportunities (close_date)', 
         "SELECT MIN(close_date), MAX(close_date) FROM sf_opportunities"),
        ('Activities (activity_date)',
         "SELECT MIN(activity_date), MAX(activity_date) FROM sf_activities"),
        ('Shipments (week_ending)',
         "SELECT MIN(week_ending), MAX(week_ending) FROM shipments"),
    ]
    
    for name, query in date_queries:
        try:
            min_date, max_date = cursor.execute(query).fetchone()
            print(f"  ✓ {name}: {min_date} to {max_date}")
        except Exception as e:
            print(f"  ✗ {name}: ERROR - {e}")
    
    # 5. Business Logic Validation
    print("\n5. BUSINESS LOGIC VALIDATION")
    print("-" * 40)
    
    # Win rate sanity check
    win_rate_query = """
        SELECT 
            ROUND(100.0 * SUM(CASE WHEN stage = 'Closed Won' THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate,
            COUNT(*) as total
        FROM sf_opportunities 
        WHERE stage IN ('Closed Won', 'Closed Lost')
    """
    win_rate, total = cursor.execute(win_rate_query).fetchone()
    expected_range = (25, 55)
    status = "✓" if expected_range[0] <= win_rate <= expected_range[1] else "⚠"
    print(f"  {status} Overall Win Rate: {win_rate}% (expected: {expected_range[0]}-{expected_range[1]}%)")
    
    # Net sales vs gross sales
    sales_check = """
        SELECT 
            SUM(net_sales) as net,
            SUM(gross_sales) as gross,
            ROUND(100.0 * SUM(net_sales) / SUM(gross_sales), 1) as net_pct
        FROM shipments
    """
    net, gross, net_pct = cursor.execute(sales_check).fetchone()
    status = "✓" if 80 <= net_pct <= 100 else "⚠"
    print(f"  {status} Net Sales %: {net_pct}% of gross (expected: 80-100%)")
    
    # Average deal size
    avg_deal = cursor.execute("""
        SELECT ROUND(AVG(amount), 2) FROM sf_opportunities WHERE stage = 'Closed Won'
    """).fetchone()[0]
    status = "✓" if 5000 <= avg_deal <= 200000 else "⚠"
    print(f"  {status} Average Deal Size: ${avg_deal:,.2f} (expected: $5K-$200K)")
    
    # 6. Summary Statistics
    print("\n6. SUMMARY STATISTICS")
    print("-" * 40)
    
    stats = cursor.execute("""
        SELECT 
            (SELECT SUM(net_sales) FROM shipments) as total_net_sales,
            (SELECT COUNT(*) FROM shipments) as total_shipments,
            (SELECT COUNT(DISTINCT operator_id) FROM shipments) as active_operators,
            (SELECT SUM(CASE WHEN stage = 'Closed Won' THEN amount ELSE 0 END) FROM sf_opportunities) as pipeline_won,
            (SELECT COUNT(*) FROM sf_opportunities WHERE stage NOT IN ('Closed Won', 'Closed Lost')) as open_opps
    """).fetchone()
    
    print(f"  Total Net Sales: ${stats[0]:,.2f}")
    print(f"  Total Shipments: {stats[1]:,}")
    print(f"  Active Operators: {stats[2]:,}")
    print(f"  Pipeline Won Value: ${stats[3]:,.2f}")
    print(f"  Open Opportunities: {stats[4]:,}")
    
    # Final Report
    print("\n" + "=" * 60)
    if len(issues) == 0:
        print("✓ ALL VALIDATIONS PASSED")
    else:
        print(f"⚠ {len(issues)} ISSUES FOUND:")
        for issue in issues:
            print(f"  - {issue}")
    print("=" * 60)
    
    conn.close()
    return len(issues) == 0


if __name__ == "__main__":
    run_validation()
