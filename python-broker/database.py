import sqlite3
from config import DB_PATH

def init_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_candles (
                symbol TEXT,
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        conn.commit()
        conn.close()
        print(f"SQLite database initialized at {DB_PATH}")
    except Exception as e:
        print(f"Database initialization error: {e}")
