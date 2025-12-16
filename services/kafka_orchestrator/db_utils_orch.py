from dotenv import load_dotenv
import sqlite3
import logging
import os

load_dotenv()
logger = logging.getLogger(__name__)

DB_FILENAME = os.getenv('DB_NAME') or 'garmin_tokens.db'
DB_PATH = os.path.join('/app/data/', DB_FILENAME)

def get_orchestrator_conn():
    """
    Return a sqlite3 connection read only using the configured DB path.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30) 
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL') 
        logger.info(f"SQLite connection (RO) OK: {DB_PATH}")
        return conn
    except Exception as e:
        logger.error(f"Errore connessione SQLite (mappature): {e}")
        raise

def get_garmin_id_by_email(email):
    """
    Retrieve the User Garmin ID for the give email.

    Args: 
        email (str): The user's email address.
    """
    conn = None
    try:
        conn = get_orchestrator_conn()
        c = conn.cursor()
        stmt = 'SELECT garmin_subscriber_id FROM user_mappings WHERE user_email = ?'
        c.execute(stmt, (email,))
        row = c.fetchone()
        return row['garmin_subscriber_id'] if row else None
        
    except Exception as e:
        logger.error(f"Error retreiving mapping for {email}: {e}")
        return None
    finally:
        if conn:
            conn.close()
            