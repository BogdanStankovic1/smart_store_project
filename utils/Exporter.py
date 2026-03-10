"""
utils/exporter.py
=================
Funkcije za export DataFrame-ova u CSV fajlove.
"""

import pandas as pd
import os


def export_csv(df: pd.DataFrame, filepath: str, index: bool = False) -> None:
    """
    Exportuje DataFrame u CSV fajl.
    Automatski kreira folder ako ne postoji.
    
    Args:
        df:       DataFrame za export
        filepath: Putanja do output CSV fajla
        index:    Da li uključiti index (default: False)
    """
    # Kreiraj folder ako ne postoji
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    df.to_csv(filepath, index=index, encoding='utf-8')
    print(f"✅ Exportovano: {filepath} ({len(df)} redova)")


def export_multiple(dfs: dict, output_dir: str) -> None:
    """
    Exportuje više DataFrame-ova odjednom.
    
    Args:
        dfs:        Dict sa {naziv_fajla: DataFrame}
        output_dir: Folder za export
    
    Primer:
        export_multiple({
            'pivot_order_priority': df1,
            'pivot_segment':        df2,
        }, 'exports/pivot_tables/')
    """
    for filename, df in dfs.items():
        filepath = os.path.join(output_dir, f"{filename}.csv")
        export_csv(df, filepath)

    print(f"\n✅ Exportovano {len(dfs)} fajlova u: {output_dir}")