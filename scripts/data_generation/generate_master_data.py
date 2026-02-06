"""
Master Data Generator for Foodservice Sales Analytics
Generates: Territories, Distributors, Operators, Products, Sales Reps
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os
import uuid

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Configuration
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'raw')

# ============================================
# TERRITORY DATA
# ============================================
def generate_territories():
    """Generate US territories/regions for sales coverage"""
    
    territories_data = [
        # Northeast
        {'territory_id': 'NE-NY-NYC', 'territory_name': 'New York Metro', 'region': 'Northeast', 'state': 'NY', 'timezone': 'America/New_York'},
        {'territory_id': 'NE-NY-UPS', 'territory_name': 'Upstate New York', 'region': 'Northeast', 'state': 'NY', 'timezone': 'America/New_York'},
        {'territory_id': 'NE-MA-BOS', 'territory_name': 'Greater Boston', 'region': 'Northeast', 'state': 'MA', 'timezone': 'America/New_York'},
        {'territory_id': 'NE-PA-PHL', 'territory_name': 'Philadelphia Metro', 'region': 'Northeast', 'state': 'PA', 'timezone': 'America/New_York'},
        {'territory_id': 'NE-NJ-NJ', 'territory_name': 'New Jersey', 'region': 'Northeast', 'state': 'NJ', 'timezone': 'America/New_York'},
        
        # Southeast
        {'territory_id': 'SE-FL-MIA', 'territory_name': 'South Florida', 'region': 'Southeast', 'state': 'FL', 'timezone': 'America/New_York'},
        {'territory_id': 'SE-FL-ORL', 'territory_name': 'Central Florida', 'region': 'Southeast', 'state': 'FL', 'timezone': 'America/New_York'},
        {'territory_id': 'SE-GA-ATL', 'territory_name': 'Atlanta Metro', 'region': 'Southeast', 'state': 'GA', 'timezone': 'America/New_York'},
        {'territory_id': 'SE-NC-CLT', 'territory_name': 'Charlotte Metro', 'region': 'Southeast', 'state': 'NC', 'timezone': 'America/New_York'},
        {'territory_id': 'SE-TX-HOU', 'territory_name': 'Houston Metro', 'region': 'Southeast', 'state': 'TX', 'timezone': 'America/Chicago'},
        {'territory_id': 'SE-TX-DFW', 'territory_name': 'Dallas-Fort Worth', 'region': 'Southeast', 'state': 'TX', 'timezone': 'America/Chicago'},
        
        # Midwest
        {'territory_id': 'MW-IL-CHI', 'territory_name': 'Chicago Metro', 'region': 'Midwest', 'state': 'IL', 'timezone': 'America/Chicago'},
        {'territory_id': 'MW-OH-CLE', 'territory_name': 'Cleveland Metro', 'region': 'Midwest', 'state': 'OH', 'timezone': 'America/New_York'},
        {'territory_id': 'MW-MI-DET', 'territory_name': 'Detroit Metro', 'region': 'Midwest', 'state': 'MI', 'timezone': 'America/Detroit'},
        {'territory_id': 'MW-MN-MSP', 'territory_name': 'Twin Cities', 'region': 'Midwest', 'state': 'MN', 'timezone': 'America/Chicago'},
        
        # West
        {'territory_id': 'WE-CA-LA', 'territory_name': 'Los Angeles Metro', 'region': 'West', 'state': 'CA', 'timezone': 'America/Los_Angeles'},
        {'territory_id': 'WE-CA-SF', 'territory_name': 'San Francisco Bay', 'region': 'West', 'state': 'CA', 'timezone': 'America/Los_Angeles'},
        {'territory_id': 'WE-CA-SD', 'territory_name': 'San Diego Metro', 'region': 'West', 'state': 'CA', 'timezone': 'America/Los_Angeles'},
        {'territory_id': 'WE-WA-SEA', 'territory_name': 'Seattle Metro', 'region': 'West', 'state': 'WA', 'timezone': 'America/Los_Angeles'},
        {'territory_id': 'WE-AZ-PHX', 'territory_name': 'Phoenix Metro', 'region': 'West', 'state': 'AZ', 'timezone': 'America/Phoenix'},
        {'territory_id': 'WE-CO-DEN', 'territory_name': 'Denver Metro', 'region': 'West', 'state': 'CO', 'timezone': 'America/Denver'},
        {'territory_id': 'WE-NV-LAS', 'territory_name': 'Las Vegas Metro', 'region': 'West', 'state': 'NV', 'timezone': 'America/Los_Angeles'},
    ]
    
    df = pd.DataFrame(territories_data)
    return df

# ============================================
# DISTRIBUTOR DATA
# ============================================
def generate_distributors(territories_df):
    """Generate major foodservice distributors"""
    
    # Major national distributors
    national_distributors = [
        {'name': 'Sysco Corporation', 'type': 'National', 'hq_state': 'TX'},
        {'name': 'US Foods', 'type': 'National', 'hq_state': 'IL'},
        {'name': 'Performance Food Group', 'type': 'National', 'hq_state': 'VA'},
        {'name': 'Gordon Food Service', 'type': 'National', 'hq_state': 'MI'},
    ]
    
    # Regional distributors
    regional_distributors = [
        {'name': 'Reinhart Foodservice', 'type': 'Regional', 'hq_state': 'WI'},
        {'name': 'Ben E. Keith Foods', 'type': 'Regional', 'hq_state': 'TX'},
        {'name': 'Shamrock Foods', 'type': 'Regional', 'hq_state': 'AZ'},
        {'name': 'Labatt Food Service', 'type': 'Regional', 'hq_state': 'TX'},
        {'name': 'Nicholas & Co', 'type': 'Regional', 'hq_state': 'UT'},
        {'name': 'Cheney Brothers', 'type': 'Regional', 'hq_state': 'FL'},
    ]
    
    # Specialty distributors
    specialty_distributors = [
        {'name': 'Baldor Specialty Foods', 'type': 'Specialty', 'hq_state': 'NY'},
        {'name': 'FreshPoint', 'type': 'Specialty', 'hq_state': 'TX'},
        {'name': 'Saval Foodservice', 'type': 'Specialty', 'hq_state': 'MD'},
    ]
    
    all_distributors = national_distributors + regional_distributors + specialty_distributors
    
    distributors = []
    for i, dist in enumerate(all_distributors):
        territory = territories_df[territories_df['state'] == dist['hq_state']]
        if territory.empty:
            territory = territories_df.sample(1)
        
        distributors.append({
            'distributor_id': f'DIST-{str(i+1).zfill(3)}',
            'distributor_name': dist['name'],
            'distributor_type': dist['type'],
            'headquarters_state': dist['hq_state'],
            'territory_id': territory.iloc[0]['territory_id'],
            'active_since': fake.date_between(start_date='-20y', end_date='-5y').isoformat()
        })
    
    return pd.DataFrame(distributors)

# ============================================
# PRODUCT DATA
# ============================================
def generate_products():
    """Generate foodservice product catalog"""
    
    product_categories = {
        'Proteins': {
            'Beef': ['Ground Beef 80/20', 'Prime Rib', 'Ribeye Steak', 'Beef Tenderloin', 'Brisket'],
            'Poultry': ['Chicken Breast', 'Chicken Wings', 'Turkey Breast', 'Duck Breast', 'Whole Chicken'],
            'Pork': ['Pork Chops', 'Bacon', 'Ham', 'Pork Belly', 'Pulled Pork'],
            'Seafood': ['Salmon Fillet', 'Shrimp 16/20', 'Lobster Tail', 'Crab Meat', 'Cod Fillet']
        },
        'Dairy': {
            'Cheese': ['Cheddar Block', 'Mozzarella Shredded', 'Parmesan Wheel', 'Blue Cheese', 'Brie'],
            'Milk': ['Whole Milk', '2% Milk', 'Heavy Cream', 'Half & Half', 'Buttermilk'],
            'Butter': ['Unsalted Butter', 'Clarified Butter', 'Whipped Butter', 'Cultured Butter']
        },
        'Produce': {
            'Vegetables': ['Romaine Lettuce', 'Tomatoes', 'Onions', 'Bell Peppers', 'Carrots'],
            'Fruits': ['Lemons', 'Limes', 'Berries Mix', 'Apples', 'Oranges'],
            'Herbs': ['Fresh Basil', 'Cilantro', 'Parsley', 'Thyme', 'Rosemary']
        },
        'Beverages': {
            'Soft Drinks': ['Cola Syrup', 'Lemon-Lime Syrup', 'Orange Soda Syrup', 'Root Beer Syrup'],
            'Coffee': ['Espresso Beans', 'House Blend', 'Decaf Coffee', 'Cold Brew Concentrate'],
            'Juice': ['Orange Juice', 'Apple Juice', 'Cranberry Juice', 'Tomato Juice']
        },
        'Dry Goods': {
            'Grains': ['Long Grain Rice', 'Pasta Spaghetti', 'Flour All Purpose', 'Quinoa', 'Oats'],
            'Canned': ['Diced Tomatoes', 'Black Beans', 'Corn', 'Coconut Milk', 'Tomato Paste'],
            'Oils': ['Olive Oil Extra Virgin', 'Canola Oil', 'Vegetable Oil', 'Sesame Oil']
        },
        'Frozen': {
            'Appetizers': ['Mozzarella Sticks', 'Jalapeno Poppers', 'Egg Rolls', 'Onion Rings'],
            'Desserts': ['Cheesecake', 'Chocolate Cake', 'Ice Cream Vanilla', 'Sorbet Mixed'],
            'Prepared': ['French Fries', 'Onion Rings', 'Hash Browns', 'Frozen Vegetables Mix']
        }
    }
    
    brands = ['Sysco Classic', 'Sysco Imperial', 'US Foods Chef\'s Line', 
              'Gordon Choice', 'Performance Select', 'Restaurant Pride',
              'Kitchen Essentials', 'Premium Reserve', 'Value Line']
    
    products = []
    product_id = 1
    
    for category, subcategories in product_categories.items():
        for subcategory, items in subcategories.items():
            for item in items:
                base_price = random.uniform(5, 150)
                products.append({
                    'product_id': f'PROD-{str(product_id).zfill(5)}',
                    'product_name': item,
                    'brand': random.choice(brands),
                    'category': category,
                    'subcategory': subcategory,
                    'unit_of_measure': random.choice(['LB', 'CS', 'EA', 'GAL', 'OZ']),
                    'standard_price': round(base_price, 2),
                    'cost': round(base_price * random.uniform(0.55, 0.75), 2),
                    'active': 1
                })
                product_id += 1
    
    return pd.DataFrame(products)

# ============================================
# SALES REP DATA
# ============================================
def generate_sales_reps(territories_df, num_reps=60):
    """Generate sales representatives"""
    
    reps = []
    managers = []
    
    # First create managers (1 per region)
    regions = territories_df['region'].unique()
    for i, region in enumerate(regions):
        manager_id = f'REP-MGR-{str(i+1).zfill(3)}'
        region_territories = territories_df[territories_df['region'] == region]
        managers.append({
            'rep_id': manager_id,
            'rep_name': fake.name(),
            'email': fake.company_email(),
            'hire_date': fake.date_between(start_date='-15y', end_date='-5y').isoformat(),
            'territory_id': region_territories.iloc[0]['territory_id'],
            'manager_id': None,
            'quota_annual': round(random.uniform(2000000, 5000000), 2),
            'rep_tier': 'Director'
        })
    
    # Create individual contributors
    for i in range(num_reps):
        territory = territories_df.sample(1).iloc[0]
        region = territory['region']
        
        # Find manager for this region
        manager = next((m for m in managers if m['rep_id'].startswith('REP-MGR') 
                       and territories_df[territories_df['territory_id'] == m['territory_id']].iloc[0]['region'] == region), None)
        
        reps.append({
            'rep_id': f'REP-{str(i+1).zfill(3)}',
            'rep_name': fake.name(),
            'email': fake.company_email(),
            'hire_date': fake.date_between(start_date='-10y', end_date='-6m').isoformat(),
            'territory_id': territory['territory_id'],
            'manager_id': manager['rep_id'] if manager else None,
            'quota_annual': round(random.uniform(500000, 1500000), 2),
            'rep_tier': random.choice(['Junior', 'Senior', 'Senior', 'Senior'])
        })
    
    return pd.DataFrame(managers + reps)

# ============================================
# OPERATOR DATA
# ============================================
def generate_operators(territories_df, distributors_df, num_operators=5000):
    """Generate foodservice operators (restaurants, hotels, etc.)"""
    
    operator_types = ['Restaurant', 'Restaurant', 'Restaurant', 'Restaurant',  # 4x weight
                     'Hotel', 'Hospital', 'School', 'Corporate Cafeteria', 
                     'Sports Venue', 'Catering', 'Country Club']
    
    cuisine_types = ['American', 'Italian', 'Mexican', 'Asian', 'Mediterranean',
                    'Steakhouse', 'Seafood', 'Fast Casual', 'Fine Dining', 
                    'Breakfast/Brunch', 'BBQ', 'Pizza', 'Sushi', 'Indian']
    
    revenue_tiers = {
        'Small': (100000, 500000),
        'Medium': (500000, 2000000),
        'Large': (2000000, 10000000),
        'Enterprise': (10000000, 50000000)
    }
    
    cities_by_state = {
        'NY': ['New York', 'Brooklyn', 'Queens', 'Buffalo', 'Rochester', 'Albany'],
        'CA': ['Los Angeles', 'San Francisco', 'San Diego', 'Sacramento', 'San Jose', 'Oakland'],
        'TX': ['Houston', 'Dallas', 'Austin', 'San Antonio', 'Fort Worth', 'El Paso'],
        'FL': ['Miami', 'Orlando', 'Tampa', 'Jacksonville', 'Fort Lauderdale'],
        'IL': ['Chicago', 'Aurora', 'Naperville', 'Joliet', 'Rockford'],
        'PA': ['Philadelphia', 'Pittsburgh', 'Allentown', 'Erie'],
        'OH': ['Columbus', 'Cleveland', 'Cincinnati', 'Toledo'],
        'GA': ['Atlanta', 'Augusta', 'Columbus', 'Savannah'],
        'NC': ['Charlotte', 'Raleigh', 'Greensboro', 'Durham'],
        'MI': ['Detroit', 'Grand Rapids', 'Warren', 'Ann Arbor'],
        'MA': ['Boston', 'Worcester', 'Springfield', 'Cambridge'],
        'NJ': ['Newark', 'Jersey City', 'Paterson', 'Elizabeth'],
        'WA': ['Seattle', 'Spokane', 'Tacoma', 'Vancouver'],
        'AZ': ['Phoenix', 'Tucson', 'Mesa', 'Chandler', 'Scottsdale'],
        'CO': ['Denver', 'Colorado Springs', 'Aurora', 'Fort Collins'],
        'MN': ['Minneapolis', 'Saint Paul', 'Rochester', 'Duluth'],
        'NV': ['Las Vegas', 'Henderson', 'Reno', 'North Las Vegas']
    }
    
    operators = []
    national_distributors = distributors_df[distributors_df['distributor_type'] == 'National']['distributor_id'].tolist()
    
    for i in range(num_operators):
        territory = territories_df.sample(1).iloc[0]
        state = territory['state']
        cities = cities_by_state.get(state, ['Metro Area'])
        
        op_type = random.choice(operator_types)
        tier = random.choices(['Small', 'Medium', 'Large', 'Enterprise'], 
                             weights=[0.5, 0.3, 0.15, 0.05])[0]
        
        # Generate operator name based on type
        if op_type == 'Restaurant':
            name_templates = [
                f"{fake.last_name()}'s {random.choice(cuisine_types)}",
                f"The {fake.word().title()} Kitchen",
                f"{random.choice(cuisine_types)} House",
                f"{fake.last_name()} & Co.",
                f"Cafe {fake.word().title()}",
                f"{random.choice(['Urban', 'Classic', 'Modern', 'Old Town'])} {random.choice(['Grill', 'Bistro', 'Eatery', 'Kitchen'])}"
            ]
            name = random.choice(name_templates)
            cuisine = random.choice(cuisine_types)
        else:
            name = f"{fake.company()} {op_type}"
            cuisine = None
        
        operators.append({
            'operator_id': f'OP-{str(i+1).zfill(6)}',
            'operator_name': name,
            'operator_type': op_type,
            'cuisine_type': cuisine,
            'city': random.choice(cities),
            'state': state,
            'county': f"{fake.city()} County",
            'zip_code': fake.zipcode(),
            'territory_id': territory['territory_id'],
            'opening_date': fake.date_between(start_date='-25y', end_date='-1y').isoformat(),
            'annual_revenue_tier': tier,
            'primary_distributor_id': random.choice(national_distributors)
        })
    
    return pd.DataFrame(operators)


def main():
    """Generate all master data files"""
    
    print("Generating Foodservice Master Data...")
    print("=" * 50)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate territories
    print("Generating territories...")
    territories_df = generate_territories()
    territories_df.to_csv(os.path.join(OUTPUT_DIR, 'territories.csv'), index=False)
    print(f"  Created {len(territories_df)} territories")
    
    # Generate distributors
    print("Generating distributors...")
    distributors_df = generate_distributors(territories_df)
    distributors_df.to_csv(os.path.join(OUTPUT_DIR, 'distributors.csv'), index=False)
    print(f"  Created {len(distributors_df)} distributors")
    
    # Generate products
    print("Generating products...")
    products_df = generate_products()
    products_df.to_csv(os.path.join(OUTPUT_DIR, 'products.csv'), index=False)
    print(f"  Created {len(products_df)} products")
    
    # Generate sales reps
    print("Generating sales reps...")
    sales_reps_df = generate_sales_reps(territories_df)
    sales_reps_df.to_csv(os.path.join(OUTPUT_DIR, 'sales_reps.csv'), index=False)
    print(f"  Created {len(sales_reps_df)} sales reps")
    
    # Generate operators
    print("Generating operators...")
    operators_df = generate_operators(territories_df, distributors_df)
    operators_df.to_csv(os.path.join(OUTPUT_DIR, 'operators.csv'), index=False)
    print(f"  Created {len(operators_df)} operators")
    
    print("=" * 50)
    print("Master data generation complete!")
    print(f"Output directory: {OUTPUT_DIR}")
    
    return territories_df, distributors_df, products_df, sales_reps_df, operators_df


if __name__ == "__main__":
    main()
