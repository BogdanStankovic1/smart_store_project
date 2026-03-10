"""
utils/data_loader.py
====================
Funkcije za učitavanje CSV fajlova u pandas DataFrame-ove.
"""

import pandas as pd
import os


def load_orders(filepath: str) -> pd.DataFrame:
    """
    Učitava jedan Orders CSV fajl.
    
    Args:
        filepath: Putanja do CSV fajla
    
    Returns:
        DataFrame sa podacima
    """
    df = pd.read_csv(
        filepath,
        sep='|',
        encoding='utf-8',
        dtype={'Postal Code': str}  # Postal Code čuvamo kao string od početka
    )
    return df


def load_all_orders(data_dir: str) -> pd.DataFrame:
    """
    Učitava sve 4 Orders CSV fajlove i spaja ih u jedan DataFrame.
    
    Args:
        data_dir: Putanja do foldera sa CSV fajlovima (npr. 'data/raw/')
    
    Returns:
        Jedan DataFrame sa svim narudžbinama
    """
    files = {
        'East':    'Orders_East.csv',
        'West':    'Orders_West.csv',
        'South':   'Orders_South.csv',
        'Central': 'Orders_Central.csv',
    }

    dfs = []
    for region, filename in files.items():
        path = os.path.join(data_dir, filename)
        df = load_orders(path)
        print(f"✅ Učitan {filename}: {len(df)} redova")
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    print(f"\n📦 Ukupno redova nakon spajanja: {len(combined)}")
    return combined


def load_returns(filepath: str) -> pd.DataFrame:
    """
    Učitava Returns CSV fajl.
    
    Args:
        filepath: Putanja do Returns.csv
    
    Returns:
        DataFrame sa vraćenim narudžbinama
    """
    df = pd.read_csv(filepath, sep='|', encoding='utf-8')
    print(f"✅ Učitan Returns: {len(df)} redova")
    return df


def load_users(filepath: str) -> pd.DataFrame:
    """
    Učitava Users CSV fajl.
    
    Args:
        filepath: Putanja do Users.csv
    
    Returns:
        DataFrame sa managerima po regionima
    """
    df = pd.read_csv(filepath, sep='|', encoding='utf-8')
    print(f"✅ Učitan Users: {len(df)} redova")
    return df