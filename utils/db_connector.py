"""
utils/db_connector.py
=====================
Funkcije za konekciju sa MySQL bazom podataka.
Koristi SQLAlchemy + pymysql driver.

Instalacija potrebnih paketa:
    pip install sqlalchemy pymysql
"""

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine



DB_CONFIG = {
    'user':     'root',
    'password': 'admin',   
    'host':     'localhost',
    'port':     3306,
    'database': 'smart_store'
}



def get_engine(database: str = None) -> Engine:
    """
    Kreira SQLAlchemy engine za konekciju sa MySQL.
    
    Args:
        database: Naziv baze (default: iz DB_CONFIG)
    
    Returns:
        SQLAlchemy Engine objekat
    """
    db = database or DB_CONFIG['database']

    connection_string = (
        f"mysql+pymysql://"
        f"{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
        f"/{db}"
    )

    engine = create_engine(connection_string, echo=False)
    print(f"✅ Konekcija kreirana: {DB_CONFIG['host']}/{db}")
    return engine


def create_database(database: str = None) -> None:
    """
    Kreira bazu podataka ako ne postoji.
    Konekcija bez database parametra (na MySQL server nivo).
    """
    db = database or DB_CONFIG['database']

    # Konekcija na server bez specificiranja baze
    connection_string = (
        f"mysql+pymysql://"
        f"{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
    )

    engine = create_engine(connection_string)

    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db}` "
                          f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.commit()

    print(f"✅ Baza '{db}' kreirana (ili već postoji)")


def test_connection(engine: Engine) -> bool:
    """
    Testira konekciju sa bazom.
    
    Returns:
        True ako je konekcija uspešna
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Konekcija uspešna!")
        return True
    except Exception as e:
        print(f"❌ Greška pri konekciji: {e}")
        return False