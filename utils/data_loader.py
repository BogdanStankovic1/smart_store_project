# Ucitavanje CSV fajlova u pandas DataFrame-ove

import pandas as pd
import os


# Ucitava jedan Orders CSV fajl sa zadatom putanjom
def load_orders(filepath):
    df = pd.read_csv(
        filepath,
        sep='|',
        encoding='utf-8',
        dtype={'Postal Code': str}
    )
    return df


# Ucitava sva 4 Orders CSV fajla i spaja ih u jedan DataFrame
def load_all_orders(data_dir):
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
        print(f"Ucitan {filename}: {len(df)} redova")
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    print(f"Ukupno redova nakon spajanja: {len(combined)}")
    return combined


# Ucitava Returns CSV fajl sa informacijama o vracenim narudzbinama
def load_returns(filepath):
    df = pd.read_csv(filepath, sep='|', encoding='utf-8')
    print(f"Ucitan Returns: {len(df)} redova")
    return df


# Ucitava Users CSV fajl sa informacijama o managerima po regionima
def load_users(filepath):
    df = pd.read_csv(filepath, sep='|', encoding='utf-8')
    print(f"Ucitan Users: {len(df)} redova")
    return df