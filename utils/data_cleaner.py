"""
utils/data_cleaner.py
=====================
Funkcije za čišćenje i pripremu podataka.
Rešava sve data quality probleme identifikovane u analizi:
  - TRIM Order Priority ("Critical " → "Critical")
  - LPAD Postal Code na 5 cifara ("7203" → "07203")
  - Zaokruživanje decimalnih kolona na 2 mesta
  - Uklanjanje duplikata Row ID
  - Dodavanje is_returned kolone
  - Dodavanje Days_Between_Order_And_Ship kolone
"""

import pandas as pd


# Kolone koje zaokružujemo na 2 decimale
DECIMAL_COLS = [
    'Discount',
    'Unit Price',
    'Shipping Cost',
    'Product Base Margin',
    'Profit',
    'Sales'
]


def trim_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Uklanja whitespace iz string kolona.
    Rešava problem: "Critical " → "Critical"
    """
    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())
    print("TRIM primenjen na sve string kolone")
    return df


def format_postal_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formatira Postal Code na 5 cifara.
    Rešava problem: "7203" → "07203"
    """
    df['Postal Code'] = (
        df['Postal Code']
        .astype(str)
        .str.strip()
        .str.zfill(5)  # ekvivalent LPAD sa '0' na 5 karaktera
    )
    print("Postal Code formatiran na 5 cifara")
    return df


def round_decimal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Zaokružuje decimalne kolone na 2 mesta.
    """
    existing_cols = [c for c in DECIMAL_COLS if c in df.columns]
    df[existing_cols] = df[existing_cols].round(2)
    print(f"Zaokružene kolone: {existing_cols}")
    return df


def remove_duplicates(df: pd.DataFrame, subset: str = 'Row ID') -> pd.DataFrame:
    """
    Uklanja duplikate na osnovu Row ID.
    Analiza je pokazala 2 duplikata u izvornim podacima.
    """
    before = len(df)
    df = df.drop_duplicates(subset=subset, keep='first')
    after = len(df)
    removed = before - after
    print(f"Uklonjeno duplikata: {removed} (bilo {before}, ostalo {after})")
    return df


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Konvertuje Order Date i Ship Date u datetime format.
    """
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df['Ship Date'] = pd.to_datetime(df['Ship Date'])
    print("✅ Datumi konvertovani u datetime format")
    return df


def add_days_between(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dodaje kolonu Days_Between_Order_And_Ship.
    Zahtev iz Python sekcije zadatka.
    """
    df['Days Between Order and Ship Date'] = (
        df['Ship Date'] - df['Order Date']
    ).dt.days
    print("Dodata kolona: Days Between Order and Ship Date")
    return df


def add_is_returned(df: pd.DataFrame, returns_df: pd.DataFrame) -> pd.DataFrame:
    """
    Dodaje is_returned Boolean kolonu u orders DataFrame.
    LEFT JOIN sa Returns tabelom — ako Order ID postoji → True, inače → False.
    
    Args:
        df: Orders DataFrame
        returns_df: Returns DataFrame
    
    Returns:
        Orders DataFrame sa is_returned kolonom
    """
    returned_ids = set(returns_df['Order ID'].astype(str).str.strip())
    df['is_returned'] = df['Order ID'].astype(str).str.strip().isin(returned_ids)
    count = df['is_returned'].sum()
    print(f"Dodata kolona: is_returned ({count} vraćenih narudžbina)")
    return df


def check_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Proverava null vrednosti i zamenjuje ih medijanom numeričkih kolona.
    Ispisuje broj redova sa null vrednostima po koloni.
    
    Returns:
        Očišćen DataFrame
    """
    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]

    if len(null_cols) == 0:
        print("Nema null vrednosti u DataFrame-u")
        return df

    print(f"⚠️  Pronađene null vrednosti:")
    for col, count in null_cols.items():
        print(f"   - {col}: {count} null vrednosti")

    # Zameni null vrednosti medijanom za numeričke kolone
    numeric_cols = df.select_dtypes(include='number').columns
    for col in numeric_cols:
        if df[col].isnull().sum() > 0:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            print(f"   ✅ {col}: null zamenjeni medijanom ({median_val:.2f})")

    total_null_rows = null_counts.sum()
    print(f"\n📊 Ukupno redova sa null vrednostima: {total_null_rows}")
    return df


def clean_orders(df: pd.DataFrame, returns_df: pd.DataFrame) -> pd.DataFrame:
    """
    Master funkcija — primenjuje sve korake čišćenja redom.
    
    Args:
        df: Sirovi orders DataFrame
        returns_df: Returns DataFrame
    
    Returns:
        Očišćen DataFrame spreman za upload u SQL
    """
    print("🔄 Počinje čišćenje podataka...\n")

    df = trim_string_columns(df)
    df = format_postal_code(df)
    df = round_decimal_columns(df)
    df = remove_duplicates(df)
    df = parse_dates(df)
    df = add_days_between(df)
    df = add_is_returned(df, returns_df)
    df = check_nulls(df)

    print(f"\n✅ Čišćenje završeno! Finalni DataFrame: {len(df)} redova, {len(df.columns)} kolona")
    return df