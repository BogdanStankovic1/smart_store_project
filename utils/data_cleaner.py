# Funkcije za ciscenje i pripremu podataka pre ucitavanja u bazu

import pandas as pd

DECIMAL_COLS = [
    'Discount',
    'Unit Price',
    'Shipping Cost',
    'Product Base Margin',
    'Profit',
    'Sales'
]


# Uklanja whitespace iz svih string kolona ("Critical " -> "Critical")
def trim_string_columns(df):
    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())
    print("TRIM primenjen na sve string kolone")
    return df


# Formatira Postal Code na 5 cifara ("7203" -> "07203")
def format_postal_code(df):
    df['Postal Code'] = (
        df['Postal Code']
        .astype(str)
        .str.strip()
        .str.zfill(5)
    )
    print("Postal Code formatiran na 5 cifara")
    return df


# Zaokruzuje decimalne kolone na 2 mesta
def round_decimal_columns(df):
    existing_cols = [c for c in DECIMAL_COLS if c in df.columns]
    df[existing_cols] = df[existing_cols].round(2)
    print(f"Zaokruzene kolone: {existing_cols}")
    return df


# Uklanja duplikate na osnovu Row ID
def remove_duplicates(df, subset='Row ID'):
    before = len(df)
    df = df.drop_duplicates(subset=subset, keep='first')
    after = len(df)
    removed = before - after
    print(f"Uklonjeno duplikata: {removed} (bilo {before}, ostalo {after})")
    return df


# Konvertuje Order Date i Ship Date iz stringa u datetime format
def parse_dates(df):
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df['Ship Date'] = pd.to_datetime(df['Ship Date'])
    print("Datumi konvertovani u datetime format")
    return df


# Dodaje kolonu koja racuna broj dana između Order Date i Ship Date
def add_days_between(df):
    df['Days Between Order and Ship Date'] = (
        df['Ship Date'] - df['Order Date']
    ).dt.days
    print("Dodata kolona: Days Between Order and Ship Date")
    return df


# Dodaje is_returned Boolean kolonu na osnovu Returns tabele
def add_is_returned(df, returns_df):
    returned_ids = set(returns_df['Order ID'].astype(str).str.strip())
    df['is_returned'] = df['Order ID'].astype(str).str.strip().isin(returned_ids)
    count = df['is_returned'].sum()
    print(f"Dodata kolona: is_returned ({count} vracenih narudzbina)")
    return df


# Proverava null vrednosti i zamenjuje ih medijanom numeričkih kolona
def check_nulls(df):
    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]

    if len(null_cols) == 0:
        print("Nema null vrednosti u DataFrame-u")
        return df

    print("Pronadjene null vrednosti:")
    for col, count in null_cols.items():
        print(f"  - {col}: {count} null vrednosti")

    numeric_cols = df.select_dtypes(include='number').columns
    for col in numeric_cols:
        if df[col].isnull().sum() > 0:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            print(f"  {col}: null zamenjeni medijanom ({median_val:.2f})")

    print(f"Ukupno redova sa null vrednostima: {null_counts.sum()}")
    return df


# Master funkcija koja poziva sve korake ciscenja redom
def clean_orders(df, returns_df):
    print("Pocinje ciscenje podataka...\n")

    df = trim_string_columns(df)
    df = format_postal_code(df)
    df = round_decimal_columns(df)
    df = remove_duplicates(df)
    df = parse_dates(df)
    df = add_days_between(df)
    df = add_is_returned(df, returns_df)
    df = check_nulls(df)

    print(f"\nCiscenje zavrseno! Finalni DataFrame: {len(df)} redova, {len(df.columns)} kolona")
    return df