import os
import psycopg2
from psycopg2.extras import RealDictCursor

# PostgreSQL Database Configuration
DB_HOST = os.getenv("DB_HOST", "130.61.180.68")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fraud_detection_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "your_secure_password")

def get_db_connection():
    """
    Establishes and returns a connection to the OCI PostgreSQL database.
    """
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return connection
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL database: {e}")
        raise e