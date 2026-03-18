import pandas as pd
from utils.data_loader import load_orders
from utils.data_cleaner import clean_orders
from utils.db_connector import get_engine
import logging
import os

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/incremental_load.log',
    level=logging.INFO,
    format='%(asctime)s — %(message)s'
)
#Uzmi sve postojeće Row_ID iz baze
def get_existing_row_ids(engine):
    """Uzmi sve postojeće Row_ID iz baze"""
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT row_id FROM fact_orders"
        )
        return set(row[0] for row in result)
#Ovo je KLJUČ Incremental Load-a
#Svaki put kada se skripta pokrene
#zna tačno koji fajl treba da učita sledeći.
#Sprečava duplikate i osigurava redosled.
def get_next_csv(engine):
    """Pronađi sledeći CSV koji nije učitan"""
    files = ['new_orders_1.csv', 'new_orders_2.csv', 'new_orders_3.csv']
    
    loaded = pd.read_sql("""
        SELECT file_name FROM loaded_files
    """, engine) if table_exists(engine, 'loaded_files') else pd.DataFrame(columns=['file_name'])
    
    for f in files:
        if f not in loaded['file_name'].values:
            return f
    return None

from sqlalchemy import text
# Proverava da li loaded_files tabela postoji
# Zašto? Prva pokretanja skripta tabela ne postoji
# pa moramo proveriti pre čitanja iz nje
def table_exists(engine, table_name):
    """Proverava da li tabela postoji"""
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'smart_store' 
            AND table_name = '{table_name}'
        """))
        return result.fetchone()[0] > 0

def mark_file_as_loaded(engine, filename):
    """Zabeleži da je fajl učitan"""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS loaded_files (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_name VARCHAR(100),
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text(f"""
            INSERT INTO loaded_files (file_name) VALUES ('{filename}')
        """))
        conn.commit()

def append_to_staging(df, engine):
    """Dodaj nove redove u staging"""
    # Preimenuj kolone da odgovaraju staging tabeli
    df = df.rename(columns={
        'Segment': 'Customer Segment',
        'Quantity': 'Quantity ordered new',
        'Order Priority': 'Order Priority',
    })
    
    # Zadrži samo kolone koje postoje u staging tabeli
    staging_columns = [
        'Row ID', 'Order Priority', 'Discount', 'Unit Price',
        'Shipping Cost', 'Customer ID', 'Customer Name', 'Ship Mode',
        'Customer Segment', 'Product Category', 'Product Sub-Category',
        'Product Container', 'Product Name', 'Product Base Margin',
        'Country', 'Region', 'State or Province', 'City', 'Postal Code',
        'Order Date', 'Ship Date', 'Profit', 'Quantity ordered new',
        'Sales', 'Order ID', 'Days Between Order and Ship Date', 'is_returned'
    ]
    
    df = df[staging_columns]
    df.to_sql('staging_orders', engine, if_exists='append', index=False)
    logging.info(f"Staging ažuriran — {len(df)} redova")
def update_dimensions(engine):
    """Ažuriraj dimenzije sa INSERT IGNORE"""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT IGNORE INTO dim_customers (customer_id, customer_name)
            SELECT DISTINCT `Customer ID`, `Customer Name`
            FROM staging_orders
            WHERE `Customer ID` IS NOT NULL
        """))
        conn.execute(text("""
            INSERT IGNORE INTO dim_geography 
            (country, region, state_or_province, city, postal_code)
            SELECT DISTINCT Country, Region, 
                   `State or Province`, City, `Postal Code`
            FROM staging_orders
        """))
        conn.execute(text("""
            INSERT IGNORE INTO dim_product 
            (product_name, product_category, product_sub_category,
             product_container, product_base_margin)
            SELECT DISTINCT `Product Name`, `Product Category`, 
                   `Product Sub-Category`, `Product Container`,
                   `Product Base Margin`
            FROM staging_orders
        """))
        conn.execute(text("""
            INSERT IGNORE INTO dim_calendar (
                date_id, full_date, year, quarter, 
                month_number, month_name, week_number
            )
            SELECT DISTINCT
                DATE_FORMAT(STR_TO_DATE(`Order Date`, '%Y-%m-%d'), '%Y%m%d'),
                STR_TO_DATE(`Order Date`, '%Y-%m-%d'),
                YEAR(STR_TO_DATE(`Order Date`, '%Y-%m-%d')),
                QUARTER(STR_TO_DATE(`Order Date`, '%Y-%m-%d')),
                MONTH(STR_TO_DATE(`Order Date`, '%Y-%m-%d')),
                MONTHNAME(STR_TO_DATE(`Order Date`, '%Y-%m-%d')),
                WEEK(STR_TO_DATE(`Order Date`, '%Y-%m-%d'))
            FROM staging_orders
            UNION
            SELECT DISTINCT
                DATE_FORMAT(STR_TO_DATE(`Ship Date`, '%Y-%m-%d'), '%Y%m%d'),
                STR_TO_DATE(`Ship Date`, '%Y-%m-%d'),
                YEAR(STR_TO_DATE(`Ship Date`, '%Y-%m-%d')),
                QUARTER(STR_TO_DATE(`Ship Date`, '%Y-%m-%d')),
                MONTH(STR_TO_DATE(`Ship Date`, '%Y-%m-%d')),
                MONTHNAME(STR_TO_DATE(`Ship Date`, '%Y-%m-%d')),
                WEEK(STR_TO_DATE(`Ship Date`, '%Y-%m-%d'))
            FROM staging_orders
        """))
        conn.commit()
    logging.info("Dimenzije ažurirane!")

def append_to_fact(engine):
    """Dodaj nove redove u fact_orders"""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT IGNORE INTO fact_orders (
                row_id, order_id, customer_id, product_id,
                geography_id, order_date_id, ship_date_id,
                segment_id, orderpriority_id, manager_id,
                shipmode_id, sales, profit, quantity,
                discount, unit_price, shipping_cost, is_returned
            )
            SELECT 
                s.`Row ID`,
                s.`Order ID`,
                c.customer_key,
                p.product_key,
                g.geography_id,
                DATE_FORMAT(STR_TO_DATE(s.`Order Date`, '%Y-%m-%d'), '%Y%m%d'),
                DATE_FORMAT(STR_TO_DATE(s.`Ship Date`, '%Y-%m-%d'), '%Y%m%d'),
                seg.segment_id,
                op.orderpriority_id,
                m.manager_id,
                sm.shipmode_id,
                s.Sales,
                s.Profit,
                s.`Quantity ordered new`,
                s.Discount,
                s.`Unit Price`,
                s.`Shipping Cost`,
                s.is_returned
            FROM staging_orders s
            JOIN dim_customers c ON c.customer_id = s.`Customer ID`
            JOIN dim_product p ON p.product_name = s.`Product Name`
            JOIN dim_geography g ON g.city = s.City 
                AND g.postal_code = s.`Postal Code`
            JOIN dim_segment seg ON seg.segment_name = s.`Customer Segment`
            JOIN dim_orderpriority op ON op.order_priority = s.`Order Priority`
        JOIN dim_manager m ON m.manager_name = (
    SELECT dm.manager_name 
    FROM dim_manager dm
    WHERE dm.manager_name = CASE s.Region
        WHEN 'West'    THEN 'William'
        WHEN 'East'    THEN 'Erin'
        WHEN 'South'   THEN 'Sam'
        WHEN 'Central' THEN 'Chris'
    END
    LIMIT 1
            )
            JOIN dim_shipmode sm ON sm.ship_mode = s.`Ship Mode`
        """))
        conn.commit()
    logging.info("Fact tabela ažurirana!")

def clear_staging(engine):
    """Očisti staging tabelu"""
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM staging_orders"))
        conn.commit()
    logging.info("Staging očišćen!")
def main():
    engine = get_engine()
    logging.info("=== Incremental Load Started ===")
    
    # 1. Pronađi sledeći CSV
    next_file = get_next_csv(engine)
    if not next_file:
        logging.info("Nema novih fajlova za učitavanje!")
        print("✅ Nema novih fajlova za učitavanje!")
        return
    
    filepath = f'data/raw/{next_file}'
    print(f"📂 Učitavam: {next_file}")
    logging.info(f"Fajl: {next_file}")
    
 # 2. Učitaj i očisti podatke
    df = load_orders(filepath)
    from utils.data_loader import load_returns
    returns_df = load_returns('data/raw/Returns.csv')

# Sačuvaj is_returned pre clean_orders
    is_returned_backup = df['is_returned'].copy()

    df = clean_orders(df, returns_df)

# Vrati is_returned vrednosti iz generisanih podataka
    df['is_returned'] = is_returned_backup.values
    print(f"is_returned vrednosti vraćene: {df['is_returned'].sum()} vraćenih")
    
    # 3. Append u staging
    append_to_staging(df, engine)
    
    # 4. Ažuriraj dimenzije
    print("🔄 Ažuriram dimenzije...")
    update_dimensions(engine)
    
    # 5. Append u fact
    print("🔄 Ažuriram fact_orders...")
    append_to_fact(engine)
    
    # 6. Označi fajl kao učitan
    mark_file_as_loaded(engine, next_file)
    
    # 7. Očisti staging
    clear_staging(engine)
    
    print(f"🎉 {next_file} uspešno učitan u bazu!")
    logging.info("=== Incremental Load Completed ===")

if __name__ == "__main__":
    main()