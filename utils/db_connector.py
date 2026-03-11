# Konekcija sa MySQL bazom podataka koriscenjem SQLAlchemy i pymysql drivera

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DB_CONFIG = {
    'user':     'root',
    'password': 'admin',
    'host':     'localhost',
    'port':     3306,
    'database': 'smart_store'
}

def get_engine(database=None):
    db = database or DB_CONFIG['database']
    connection_string = (
        f"mysql+pymysql://"
        f"{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
        f"/{db}"
    )
    engine = create_engine(connection_string, echo=False)
    print(f"Konekcija kreirana: {DB_CONFIG['host']}/{db}")
    return engine


def create_database(database=None):
    db = database or DB_CONFIG['database']
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
    print(f"Baza '{db}' kreirana (ili vec postoji)")


def test_connection(engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Konekcija uspesna!")
        return True
    except Exception as e:
        print(f"Greska pri konekciji: {e}")
        return False