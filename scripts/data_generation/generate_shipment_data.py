"""
Distributor Shipment Data Generator for Foodservice Sales Analytics
Generates: Weekly shipment CSVs from 2015-2025
Includes seasonality, growth trends, and realistic patterns
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
from tqdm import tqdm

np.random.seed(42)
random.seed(42)

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
OUTPUT_DIR = os.path.join(RAW_DIR, 'distributor_shipments')

# Date range
START_DATE = datetime(2015, 1, 1)
END_DATE = datetime(2025, 12, 31)

# Seasonality factors by month (foodservice patterns)
SEASONALITY = {
    1: 0.85,   # January - post-holiday slowdown
    2: 0.90,   # February - Valentine's boost
    3: 0.95,   # March - spring pickup
    4: 1.00,   # April - steady
    5: 1.05,   # May - graduations, Mother's Day
    6: 1.10,   # June - summer surge begins
    7: 1.15,   # July - peak summer
    8: 1.10,   # August - back to school
    9: 1.00,   # September - normalizing
    10: 1.00,  # October - steady
    11: 1.15,  # November - Thanksgiving peak
    12: 1.20   # December - holiday peak
}

# Year-over-year growth rates (reflecting industry trends)
YOY_GROWTH = {
    2015: 1.00,
    2016: 1.03,
    2017: 1.05,
    2018: 1.04,
    2019: 1.06,
    2020: 0.65,  # COVID impact
    2021: 0.85,  # Recovery
    2022: 1.10,  # Rebound
    2023: 1.08,
    2024: 1.06,
    2025: 1.04
}


def load_master_data():
    """Load previously generated master data"""
    distributors_df = pd.read_csv(os.path.join(RAW_DIR, 'distributors.csv'))
    operators_df = pd.read_csv(os.path.join(RAW_DIR, 'operators.csv'))
    products_df = pd.read_csv(os.path.join(RAW_DIR, 'products.csv'))
    return distributors_df, operators_df, products_df


def get_week_ending_dates(start_date, end_date):
    """Generate list of week-ending Saturdays"""
    weeks = []
    current = start_date
    
    # Move to first Saturday
    while current.weekday() != 5:  # Saturday
        current += timedelta(days=1)
    
    while current <= end_date:
        weeks.append(current)
        current += timedelta(days=7)
    
    return weeks


def generate_shipments(distributors_df, operators_df, products_df):
    """Generate weekly shipment records"""
    
    print("Generating shipment data...")
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Get all week-ending dates
    weeks = get_week_ending_dates(START_DATE, END_DATE)
    print(f"  Generating data for {len(weeks)} weeks")
    
    # Create operator-distributor relationships
    # Each operator has 1-3 distributors they order from
    operator_distributors = {}
    for _, op in operators_df.iterrows():
        primary = op['primary_distributor_id']
        num_secondary = random.randint(0, 2)
        secondary = distributors_df[distributors_df['distributor_id'] != primary].sample(
            n=min(num_secondary, len(distributors_df) - 1)
        )['distributor_id'].tolist()
        operator_distributors[op['operator_id']] = [primary] + secondary
    
    # Create product preferences by operator type
    product_categories = products_df['category'].unique()
    
    all_shipments = []
    shipment_id = 1
    
    # Process by year to manage memory
    for year in range(START_DATE.year, END_DATE.year + 1):
        print(f"  Processing year {year}...")
        
        year_weeks = [w for w in weeks if w.year == year]
        year_growth = YOY_GROWTH.get(year, 1.0)
        
        # Base cumulative growth from 2015
        cumulative_growth = 1.0
        for y in range(2015, year):
            cumulative_growth *= YOY_GROWTH.get(y, 1.0)
        
        for week_ending in tqdm(year_weeks, desc=f"  Year {year}", leave=False):
            month = week_ending.month
            seasonality_factor = SEASONALITY[month]
            
            # Sample operators for this week (not all operators order every week)
            active_operators = operators_df.sample(frac=random.uniform(0.3, 0.5))
            
            for _, op in active_operators.iterrows():
                # Get distributors for this operator
                distributors = operator_distributors.get(op['operator_id'], [op['primary_distributor_id']])
                
                # Usually order from primary, sometimes from secondary
                distributor_id = random.choices(
                    distributors,
                    weights=[0.8] + [0.2 / max(len(distributors) - 1, 1)] * (len(distributors) - 1)
                )[0]
                
                # Sample products (operators typically order 3-15 products per shipment)
                num_products = random.randint(3, 15)
                ordered_products = products_df.sample(n=num_products)
                
                for _, product in ordered_products.iterrows():
                    # Base quantity based on operator size
                    if op['annual_revenue_tier'] == 'Enterprise':
                        base_qty = random.randint(20, 200)
                    elif op['annual_revenue_tier'] == 'Large':
                        base_qty = random.randint(10, 100)
                    elif op['annual_revenue_tier'] == 'Medium':
                        base_qty = random.randint(5, 50)
                    else:
                        base_qty = random.randint(2, 20)
                    
                    # Apply factors
                    quantity = max(1, int(base_qty * seasonality_factor * cumulative_growth))
                    
                    # Calculate financials
                    unit_price = product['standard_price'] * random.uniform(0.9, 1.1)
                    gross_sales = round(quantity * unit_price, 2)
                    
                    # Discounts (volume discounts for larger orders)
                    discount_rate = 0
                    if quantity >= 50:
                        discount_rate = random.uniform(0.05, 0.15)
                    elif quantity >= 20:
                        discount_rate = random.uniform(0.02, 0.08)
                    
                    discounts = round(gross_sales * discount_rate, 2)
                    
                    # Returns (small percentage)
                    returns = round(gross_sales * random.uniform(0, 0.03), 2) if random.random() > 0.9 else 0
                    
                    net_sales = round(gross_sales - discounts - returns, 2)
                    cost_of_goods = round(quantity * product['cost'], 2)
                    
                    # Sample shipment date within the week
                    days_before = random.randint(1, 6)
                    shipment_date = week_ending - timedelta(days=days_before)
                    
                    all_shipments.append({
                        'shipment_id': f'SHIP-{str(shipment_id).zfill(10)}',
                        'shipment_date': shipment_date.strftime('%Y-%m-%d'),
                        'week_ending': week_ending.strftime('%Y-%m-%d'),
                        'distributor_id': distributor_id,
                        'operator_id': op['operator_id'],
                        'product_id': product['product_id'],
                        'quantity': quantity,
                        'gross_sales': gross_sales,
                        'discounts': discounts,
                        'returns': returns,
                        'net_sales': net_sales,
                        'cost_of_goods': cost_of_goods
                    })
                    shipment_id += 1
        
        # Save yearly file
        if all_shipments:
            year_df = pd.DataFrame([s for s in all_shipments if s['week_ending'].startswith(str(year))])
            if not year_df.empty:
                year_df.to_csv(os.path.join(OUTPUT_DIR, f'shipments_{year}.csv'), index=False)
                print(f"    Saved {len(year_df):,} shipments for {year}")
    
    # Create combined file
    print("  Creating combined shipments file...")
    shipments_df = pd.DataFrame(all_shipments)
    shipments_df.to_csv(os.path.join(OUTPUT_DIR, 'shipments_all.csv'), index=False)
    
    return shipments_df


def generate_summary_stats(shipments_df):
    """Generate summary statistics file"""
    
    shipments_df['week_ending'] = pd.to_datetime(shipments_df['week_ending'])
    shipments_df['year'] = shipments_df['week_ending'].dt.year
    shipments_df['month'] = shipments_df['week_ending'].dt.month
    
    # Monthly summary
    monthly_summary = shipments_df.groupby(['year', 'month']).agg({
        'shipment_id': 'count',
        'quantity': 'sum',
        'gross_sales': 'sum',
        'net_sales': 'sum',
        'returns': 'sum',
        'operator_id': 'nunique',
        'distributor_id': 'nunique'
    }).reset_index()
    
    monthly_summary.columns = ['year', 'month', 'shipment_count', 'total_quantity', 
                               'gross_sales', 'net_sales', 'returns', 
                               'active_operators', 'active_distributors']
    
    monthly_summary.to_csv(os.path.join(OUTPUT_DIR, 'monthly_summary.csv'), index=False)
    
    return monthly_summary


def main():
    """Generate all shipment data"""
    
    print("Generating Distributor Shipment Data...")
    print("=" * 50)
    
    # Load master data
    print("Loading master data...")
    distributors_df, operators_df, products_df = load_master_data()
    print(f"  Loaded {len(distributors_df)} distributors, {len(operators_df)} operators, {len(products_df)} products")
    
    # Generate shipments
    shipments_df = generate_shipments(distributors_df, operators_df, products_df)
    print(f"\nTotal shipments generated: {len(shipments_df):,}")
    
    # Generate summary stats
    print("\nGenerating summary statistics...")
    summary = generate_summary_stats(shipments_df)
    
    print("=" * 50)
    print("Shipment data generation complete!")
    print(f"Output directory: {OUTPUT_DIR}")
    
    # Print sample stats
    print("\nSample Statistics:")
    print(f"  Total Net Sales: ${shipments_df['net_sales'].sum():,.2f}")
    print(f"  Total Quantity: {shipments_df['quantity'].sum():,}")
    print(f"  Unique Operators: {shipments_df['operator_id'].nunique():,}")
    print(f"  Unique Products: {shipments_df['product_id'].nunique():,}")
    
    return shipments_df


if __name__ == "__main__":
    main()
