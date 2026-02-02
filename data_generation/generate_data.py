import os
import random
from datetime import datetime, timedelta
from google.cloud import bigquery
from faker import Faker
import pandas as pd

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
DATASET_ID = "retail_syn_data"
LOCATION = "europe-west1"

client = bigquery.Client(project=PROJECT_ID)
fake = Faker(['en_GB', 'fr_FR', 'de_DE', 'sv_SE']) # European locales

def create_dataset():
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = LOCATION
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id}")

def generate_raw_customers(n=1000):
    data = []
    countries = ['en_GB', 'fr_FR', 'de_DE', 'sv_SE']
    for _ in range(n):
        country_code = random.choice(countries)
        fake_loc = Faker(country_code)
        
        # Introduce quality issues
        email = fake_loc.email() if random.random() > 0.1 else None # 10% missing email
        phone = fake_loc.phone_number()
        if random.random() > 0.9: # 10% invalid phone format
            phone = "INVALID-" + phone
            
        data.append({
            "customer_id": fake_loc.uuid4(),
            "name": fake_loc.name(),
            "email": email,
            "phone": phone,
            "country": fake_loc.country(),
            "registration_date": fake_loc.date_between(start_date='-2y', end_date='today').isoformat(),
            "card_number": fake_loc.credit_card_number(),
            "card_expiry": fake_loc.credit_card_expire(),
            "membership_level": random.choice(["Gold", "Silver", "Bronze", "Standard"])
        })
    return pd.DataFrame(data)

def generate_raw_products(n=500):
    categories = ["Clothes", "Home", "Fashion Accessories"]
    data = []
    for _ in range(n):
        # Introduce quality issues
        price = round(random.uniform(5.0, 500.0), 2)
        if random.random() > 0.95: # 5% missing price
            price = None
            
        data.append({
            "product_id": fake.uuid4(),
            "name": fake.word().capitalize() + " " + random.choice(["Shirt", "Pants", "Chair", "Lamp", "Watch", "Bag"]),
            "category": random.choice(categories),
            "price": price
        })
    
    # Introduce duplicates
    if n > 10:
        for _ in range(5):
            data.append(data[random.randint(0, n-1)])
            
    return pd.DataFrame(data)

def generate_orders(customers_df, products_df, n=5000):
    orders_data = []
    transactions_data = []
    
    for _ in range(n):
        customer = customers_df.sample(1).iloc[0]
        order_id = fake.uuid4()
        order_date = fake.date_between(start_date='-1y', end_date='today')
        
        # Generate transactions for this order
        num_items = random.randint(1, 5)
        total_amount = 0
        
        for _ in range(num_items):
            product = products_df.sample(1).iloc[0]
            quantity = random.randint(1, 3)
            amount = product['price'] * quantity if pd.notnull(product['price']) else 0
            total_amount += amount
            
            transactions_data.append({
                "transaction_id": fake.uuid4(),
                "order_id": order_id,
                "product_id": product['product_id'],
                "quantity": quantity,
                "amount": amount
            })
            
        orders_data.append({
            "order_id": order_id,
            "customer_id": customer['customer_id'],
            "order_date": order_date.isoformat(),
            "total_amount": total_amount
        })
    return pd.DataFrame(orders_data), pd.DataFrame(transactions_data)

def load_to_bigquery(df, table_name):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows to {table_id}")

def create_derived_table(source_table, target_table):
    """Creates a table using CTAS to establish lineage."""
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{target_table}` AS
    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{source_table}`
    """
    job = client.query(query)
    job.result()
    print(f"Created {target_table} from {source_table} (Lineage established)")

if __name__ == "__main__":
    if not PROJECT_ID:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
        exit(1)
        
    create_dataset()
    
    print("Generating raw data...")
    raw_customers_df = generate_raw_customers()
    raw_products_df = generate_raw_products()
    
    # Use raw data frames for orders generation
    orders_df, transactions_df = generate_orders(raw_customers_df, raw_products_df)
    
    print("Loading RAW tables to BigQuery...")
    load_to_bigquery(raw_customers_df, "raw_customers")
    load_to_bigquery(raw_products_df, "raw_products")
    load_to_bigquery(orders_df, "raw_orders")
    load_to_bigquery(transactions_df, "raw_transactions")
    
    print("Creating Main tables (CTAS from Raw) for Lineage...")
    create_derived_table("raw_customers", "customers")
    create_derived_table("raw_products", "products")
    create_derived_table("raw_orders", "orders")
    create_derived_table("raw_transactions", "transactions")
    
    print("Done.")
