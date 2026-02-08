import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "metastore",
    "user": "hive",
    "password": "hive"
}

@contextmanager
def get_db_connection():
    conn = psycopg2.connect(**DATABASE_CONFIG, cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()

def get_db_cursor(conn):
    return conn.cursor()