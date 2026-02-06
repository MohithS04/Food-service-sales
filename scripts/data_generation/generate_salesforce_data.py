"""
Salesforce CRM Data Generator for Foodservice Sales Analytics
Generates: Accounts, Opportunities, Activities
Modeled after Salesforce object structures
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
OUTPUT_DIR = os.path.join(RAW_DIR, 'salesforce_exports')

# Date range for data generation
START_DATE = datetime(2015, 1, 1)
END_DATE = datetime(2025, 12, 31)

# Opportunity stages with probabilities
STAGES = {
    'Prospecting': {'probability': 10, 'avg_days': 14},
    'Qualification': {'probability': 25, 'avg_days': 21},
    'Needs Analysis': {'probability': 40, 'avg_days': 30},
    'Proposal': {'probability': 60, 'avg_days': 21},
    'Negotiation': {'probability': 80, 'avg_days': 14},
    'Closed Won': {'probability': 100, 'avg_days': 0},
    'Closed Lost': {'probability': 0, 'avg_days': 0}
}

LEAD_SOURCES = ['Trade Show', 'Referral', 'Cold Call', 'Website', 'Partner', 
                'LinkedIn', 'Industry Event', 'Existing Customer']

COMPETITORS = ['Sysco', 'US Foods', 'Performance Food Group', 'Gordon Food Service',
              'Regional Competitor', 'Local Supplier', 'Direct from Manufacturer', None]

LOSS_REASONS = ['Price', 'Competitor', 'No Decision', 'Budget Constraints', 
               'Changed Requirements', 'Poor Fit', 'Timing', None]

ACTIVITY_TYPES = ['Call', 'Email', 'Meeting', 'Demo', 'Site Visit', 'Follow-up']

ACTIVITY_OUTCOMES = ['Connected', 'Left Voicemail', 'No Answer', 'Completed', 
                    'Rescheduled', 'Cancelled']


def load_master_data():
    """Load previously generated master data"""
    operators_df = pd.read_csv(os.path.join(RAW_DIR, 'operators.csv'))
    sales_reps_df = pd.read_csv(os.path.join(RAW_DIR, 'sales_reps.csv'))
    products_df = pd.read_csv(os.path.join(RAW_DIR, 'products.csv'))
    return operators_df, sales_reps_df, products_df


def generate_accounts(operators_df, sales_reps_df):
    """Generate Salesforce Account records linked to operators"""
    
    accounts = []
    # Select ~80% of operators to have accounts (some operators not in CRM yet)
    selected_operators = operators_df.sample(frac=0.8)
    
    # Get reps that are not managers
    ic_reps = sales_reps_df[sales_reps_df['rep_tier'] != 'Director']['rep_id'].tolist()
    
    for _, op in selected_operators.iterrows():
        # Determine account type based on revenue tier and randomness
        if op['annual_revenue_tier'] in ['Large', 'Enterprise']:
            account_type = random.choices(['Customer', 'Customer', 'Prospect'], 
                                         weights=[0.6, 0.3, 0.1])[0]
        else:
            account_type = random.choices(['Customer', 'Prospect', 'Former Customer'], 
                                         weights=[0.4, 0.4, 0.2])[0]
        
        # Created date should be after operator opening date
        try:
            opening_date = datetime.strptime(op['opening_date'], '%Y-%m-%d')
        except:
            opening_date = START_DATE
        
        created_date = fake.date_between(
            start_date=max(opening_date, START_DATE),
            end_date=END_DATE - timedelta(days=180)
        )
        
        # Last activity is after created date
        last_activity = fake.date_between(
            start_date=created_date,
            end_date=END_DATE
        )
        
        accounts.append({
            'account_id': f'ACC-{op["operator_id"].split("-")[1]}',
            'operator_id': op['operator_id'],
            'account_name': op['operator_name'],
            'account_type': account_type,
            'industry': 'Foodservice' if op['operator_type'] == 'Restaurant' else op['operator_type'],
            'owner_id': random.choice(ic_reps),
            'created_date': created_date.isoformat(),
            'last_activity_date': last_activity.isoformat(),
            'account_status': 'Active' if account_type != 'Former Customer' else 'Churned'
        })
    
    return pd.DataFrame(accounts)


def generate_opportunities(accounts_df, sales_reps_df, products_df):
    """Generate Salesforce Opportunity records"""
    
    opportunities = []
    opp_id = 1
    
    ic_reps = sales_reps_df[sales_reps_df['rep_tier'] != 'Director']['rep_id'].tolist()
    product_interests = products_df['category'].unique().tolist()
    
    # Generate opportunities over time
    for _, account in accounts_df.iterrows():
        created_date = datetime.strptime(account['created_date'], '%Y-%m-%d')
        
        # Determine number of opportunities based on account type and duration
        years_active = (END_DATE - created_date).days / 365
        
        if account['account_type'] == 'Customer':
            num_opps = int(random.uniform(3, 8) * years_active)
        elif account['account_type'] == 'Prospect':
            num_opps = int(random.uniform(1, 3) * min(years_active, 2))
        else:  # Former Customer
            num_opps = int(random.uniform(1, 2) * min(years_active, 1))
        
        num_opps = max(1, min(num_opps, 20))  # Cap at 20 opps per account
        
        for _ in range(num_opps):
            # Random opportunity creation date
            opp_created = fake.date_between(
                start_date=created_date,
                end_date=END_DATE - timedelta(days=30)
            )
            
            # Determine final stage
            is_won = random.random() < 0.35  # 35% base win rate
            
            # Adjust win rate based on account type
            if account['account_type'] == 'Customer':
                is_won = random.random() < 0.55  # Higher for existing customers
            elif account['account_type'] == 'Former Customer':
                is_won = random.random() < 0.15  # Lower for churned
            
            # Determine if deal is still open
            days_since_created = (END_DATE - datetime.combine(opp_created, datetime.min.time())).days
            is_closed = days_since_created > random.randint(30, 180)
            
            if is_closed:
                final_stage = 'Closed Won' if is_won else 'Closed Lost'
            else:
                # Pick a random open stage
                open_stages = [s for s in STAGES.keys() if s not in ['Closed Won', 'Closed Lost']]
                final_stage = random.choice(open_stages)
            
            # Calculate close date
            days_to_close = sum(STAGES[s]['avg_days'] for s in STAGES.keys() 
                              if STAGES[s]['probability'] <= STAGES.get(final_stage, {}).get('probability', 100))
            close_date = opp_created + timedelta(days=max(7, days_to_close + random.randint(-14, 30)))
            
            if close_date > END_DATE.date():
                close_date = END_DATE.date()
            
            # Calculate deal amount based on account type and product
            base_amount = random.uniform(5000, 150000)
            if account['account_type'] == 'Customer':
                base_amount *= random.uniform(1.2, 2.0)  # Larger deals for customers
            
            opportunities.append({
                'opportunity_id': f'OPP-{str(opp_id).zfill(7)}',
                'account_id': account['account_id'],
                'opportunity_name': f"{account['account_name']} - {random.choice(product_interests)} Deal",
                'stage': final_stage,
                'amount': round(base_amount, 2),
                'probability': STAGES.get(final_stage, {}).get('probability', 50),
                'close_date': close_date.isoformat() if isinstance(close_date, datetime) else close_date,
                'created_date': opp_created.isoformat(),
                'owner_id': account['owner_id'] if random.random() > 0.1 else random.choice(ic_reps),
                'lead_source': random.choice(LEAD_SOURCES),
                'product_interest': random.choice(product_interests),
                'competitor': random.choice(COMPETITORS) if final_stage == 'Closed Lost' or random.random() > 0.6 else None,
                'loss_reason': random.choice(LOSS_REASONS) if final_stage == 'Closed Lost' else None
            })
            opp_id += 1
    
    return pd.DataFrame(opportunities)


def generate_activities(accounts_df, opportunities_df, sales_reps_df):
    """Generate Salesforce Activity records (Calls, Emails, Meetings)"""
    
    activities = []
    activity_id = 1
    
    ic_reps = sales_reps_df[sales_reps_df['rep_tier'] != 'Director']['rep_id'].tolist()
    
    # Generate activities for each opportunity
    for _, opp in opportunities_df.iterrows():
        opp_created = datetime.strptime(opp['created_date'], '%Y-%m-%d')
        opp_close = datetime.strptime(str(opp['close_date']), '%Y-%m-%d')
        
        # Number of activities correlates with deal size and stage
        base_activities = max(3, int(opp['amount'] / 10000))
        
        if opp['stage'] == 'Closed Won':
            num_activities = int(base_activities * random.uniform(1.5, 2.5))
        elif opp['stage'] == 'Closed Lost':
            num_activities = int(base_activities * random.uniform(0.5, 1.0))
        else:
            num_activities = int(base_activities * random.uniform(0.8, 1.5))
        
        num_activities = min(num_activities, 30)  # Cap at 30 activities per opp
        
        for _ in range(num_activities):
            activity_date = fake.date_between(
                start_date=opp_created,
                end_date=min(opp_close, datetime.now())
            )
            
            activity_type = random.choices(
                ACTIVITY_TYPES,
                weights=[0.25, 0.30, 0.15, 0.10, 0.10, 0.10]
            )[0]
            
            # Duration based on activity type
            if activity_type == 'Call':
                duration = random.randint(5, 45)
            elif activity_type == 'Email':
                duration = random.randint(2, 15)
            elif activity_type == 'Meeting':
                duration = random.randint(30, 120)
            elif activity_type == 'Demo':
                duration = random.randint(45, 90)
            elif activity_type == 'Site Visit':
                duration = random.randint(60, 180)
            else:
                duration = random.randint(5, 30)
            
            activities.append({
                'activity_id': f'ACT-{str(activity_id).zfill(8)}',
                'account_id': opp['account_id'],
                'opportunity_id': opp['opportunity_id'],
                'owner_id': opp['owner_id'],
                'activity_type': activity_type,
                'activity_date': activity_date.isoformat(),
                'duration_minutes': duration,
                'subject': f"{activity_type}: {opp['opportunity_name'][:50]}",
                'outcome': random.choice(ACTIVITY_OUTCOMES),
                'next_steps': fake.sentence() if random.random() > 0.3 else None
            })
            activity_id += 1
    
    # Add some standalone activities not linked to opportunities
    for _, account in accounts_df.sample(frac=0.3).iterrows():
        account_created = datetime.strptime(account['created_date'], '%Y-%m-%d')
        
        for _ in range(random.randint(1, 5)):
            activity_date = fake.date_between(
                start_date=account_created,
                end_date=END_DATE
            )
            
            activity_type = random.choice(ACTIVITY_TYPES)
            
            activities.append({
                'activity_id': f'ACT-{str(activity_id).zfill(8)}',
                'account_id': account['account_id'],
                'opportunity_id': None,
                'owner_id': account['owner_id'],
                'activity_type': activity_type,
                'activity_date': activity_date.isoformat(),
                'duration_minutes': random.randint(5, 60),
                'subject': f"{activity_type}: General check-in",
                'outcome': random.choice(ACTIVITY_OUTCOMES),
                'next_steps': fake.sentence() if random.random() > 0.5 else None
            })
            activity_id += 1
    
    return pd.DataFrame(activities)


def main():
    """Generate all Salesforce CRM data"""
    
    print("Generating Salesforce CRM Data...")
    print("=" * 50)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Load master data
    print("Loading master data...")
    operators_df, sales_reps_df, products_df = load_master_data()
    
    # Generate accounts
    print("Generating accounts...")
    accounts_df = generate_accounts(operators_df, sales_reps_df)
    accounts_df.to_csv(os.path.join(OUTPUT_DIR, 'sf_accounts.csv'), index=False)
    print(f"  Created {len(accounts_df)} accounts")
    
    # Generate opportunities
    print("Generating opportunities...")
    opportunities_df = generate_opportunities(accounts_df, sales_reps_df, products_df)
    opportunities_df.to_csv(os.path.join(OUTPUT_DIR, 'sf_opportunities.csv'), index=False)
    print(f"  Created {len(opportunities_df)} opportunities")
    
    # Generate activities
    print("Generating activities...")
    activities_df = generate_activities(accounts_df, opportunities_df, sales_reps_df)
    activities_df.to_csv(os.path.join(OUTPUT_DIR, 'sf_activities.csv'), index=False)
    print(f"  Created {len(activities_df)} activities")
    
    print("=" * 50)
    print("Salesforce CRM data generation complete!")
    print(f"Output directory: {OUTPUT_DIR}")
    
    return accounts_df, opportunities_df, activities_df


if __name__ == "__main__":
    main()
