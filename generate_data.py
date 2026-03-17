import pandas as pd
import numpy as np
from utils.db_connector import get_engine
from datetime import datetime, timedelta
import random

engine = get_engine()

"""Da bi podaci bili realistični — koristimo ISTE 
kupce i proizvode koji već postoje u bazi.
Ako bismo izmislili nove kupce, JOIN u 
fact_orders bi pao jer ih nema u dim_customers!"""

def load_existing_data():
    """Učitaj postojeće dimenzije iz baze"""
    
    customers = pd.read_sql("""
        SELECT customer_id, customer_name 
        FROM dim_customers
    """, engine)
    
    products = pd.read_sql("""
        SELECT product_name, product_category, 
               product_sub_category, product_container,
               product_base_margin
        FROM dim_product
    """, engine)
    
    geographies = pd.read_sql("""
        SELECT country, region, state_or_province, 
               city, postal_code
        FROM dim_geography
    """, engine)
    
    users = pd.read_sql("""
        SELECT manager_name, 
               CASE manager_name
                   WHEN 'Erin' THEN 'East'
                   WHEN 'William' THEN 'West'
                   WHEN 'Sam' THEN 'South'
                   WHEN 'Chris' THEN 'Central'
               END as region
        FROM dim_manager
    """, engine)
    
    max_row_id = pd.read_sql(
        "SELECT MAX(row_id) as max_id FROM fact_orders", engine
    ).iloc[0]['max_id']
    
    max_order_id = pd.read_sql(
        "SELECT MAX(order_id) as max_id FROM fact_orders", engine
    ).iloc[0]['max_id']
    
    return customers, products, geographies, users, max_row_id, max_order_id

"""Gledao sam originalne podatke i koristio
slične raspone vrednosti da podaci izgledaju
realistično i konzistentno sa postojećim!"""

def generate_orders(start_date, end_date, num_orders, 
                    customers, products, geographies, 
                    start_row_id, start_order_id):
    """Generiši narudžbine za određeni period"""
    
    rows = []
    current_row_id = start_row_id + 1
    current_order_id = start_order_id + 1
    
    date_range = pd.date_range(start=start_date, end=end_date)
    
    segments = ['Consumer', 'Corporate', 'Home Office', 'Small Business']
    ship_modes = ['Regular Air', 'Express Air', 'Delivery Truck']
    priorities = ['Low', 'Medium', 'High', 'Critical', 'Not Specified']
    
    for _ in range(num_orders):
        # Nasumični kupac i proizvod
        customer = customers.sample(1).iloc[0]
        product = products.sample(1).iloc[0]
        geography = geographies.sample(1).iloc[0]
        
        # Datumi
        order_date = random.choice(date_range)
        ship_days = random.randint(1, 7)
        ship_date = order_date + timedelta(days=ship_days)
        
        # Finansije
        unit_price = round(random.uniform(10, 500), 2)
        quantity = random.randint(1, 10)
        discount = round(random.choice([0, 0.05, 0.1, 0.15, 0.2]), 2)
        sales = round(unit_price * quantity * (1 - discount), 2)
        profit = round(sales * random.uniform(0.05, 0.35), 2)
        shipping_cost = round(random.uniform(2, 50), 2)
        
        rows.append({
            'Row ID': current_row_id,
            'Order ID': current_order_id,
            'Order Date': order_date.strftime('%Y-%m-%d'),
            'Ship Date': ship_date.strftime('%Y-%m-%d'),
            'Ship Mode': random.choice(ship_modes),
            'Customer ID': customer['customer_id'],
            'Customer Name': customer['customer_name'],
            'Segment': random.choice(segments),
            'Country': geography['country'],
            'City': geography['city'],
            'State or Province': geography['state_or_province'],
            'Postal Code': geography['postal_code'],
            'Region': geography['region'],
            'Product Name': product['product_name'],
            'Product Category': product['product_category'],
            'Product Sub-Category': product['product_sub_category'],
            'Product Container': product['product_container'],
            'Product Base Margin': product['product_base_margin'],
            'Sales': sales,
            'Quantity': quantity,
            'Discount': discount,
            'Profit': profit,
            'Unit Price': unit_price,
            'Shipping Cost': shipping_cost,
            'Order Priority': random.choice(priorities),
            'is_returned': 1 if random.random() < 0.15 else 0,
        })
        
        current_row_id += 1
        current_order_id += 1
    
    return pd.DataFrame(rows)

def main():
    print("Učitavam postojeće podatke iz baze...")
    customers, products, geographies, users, max_row_id, max_order_id = load_existing_data()
    print(f"Max Row ID: {max_row_id}, Max Order ID: {max_order_id}")
    
    # Period 1 — Jul/Avg 2015
    print("Generišem Period 1 (Jul/Avg 2015)...")
    df1 = generate_orders(
        start_date='2015-07-01',
        end_date='2015-08-31',
        num_orders=300,
        customers=customers,
        products=products,
        geographies=geographies,
        start_row_id=max_row_id,
        start_order_id=max_order_id
    )
    df1.to_csv('data/raw/new_orders_1.csv', sep='|', index=False)
    print(f"✅ new_orders_1.csv — {len(df1)} redova")
    
    # Period 2 — Sep/Okt 2015
    print("Generišem Period 2 (Sep/Okt 2015)...")
    df2 = generate_orders(
        start_date='2015-09-01',
        end_date='2015-10-31',
        num_orders=300,
        customers=customers,
        products=products,
        geographies=geographies,
        start_row_id=max_row_id + len(df1),
        start_order_id=max_order_id + len(df1)
    )
    df2.to_csv('data/raw/new_orders_2.csv', sep='|', index=False)
    print(f"✅ new_orders_2.csv — {len(df2)} redova")
    
    # Period 3 — Nov/Dec 2015
    print("Generišem Period 3 (Nov/Dec 2015)...")
    df3 = generate_orders(
        start_date='2015-11-01',
        end_date='2015-12-31',
        num_orders=300,
        customers=customers,
        products=products,
        geographies=geographies,
        start_row_id=max_row_id + len(df1) + len(df2),
        start_order_id=max_order_id + len(df1) + len(df2)
    )
    df3.to_csv('data/raw/new_orders_3.csv', sep='|', index=False)
    print(f"✅ new_orders_3.csv — {len(df3)} redova")
    
    print("\n Sva 3 CSV fajla su generisana u data/raw/!")

if __name__ == "__main__":
    main()