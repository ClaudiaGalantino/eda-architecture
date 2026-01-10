from dotenv import load_dotenv
import sqlite3
import logging
import os

load_dotenv()
logger = logging.getLogger(__name__)

DB_FILENAME = os.getenv('DB_NAME') or 'garmin_tokens.db'
# The DB is mounted into /app/data via docker-compose
DB_PATH = os.path.join('/app/data/', DB_FILENAME)

def _resolve_db_uri() -> str:
    """Return the SQLite URI for read-only access, with existence check for clearer errors."""
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found at {DB_PATH}")
        raise FileNotFoundError(f"Database file not found at {DB_PATH}")
    # SQLite URI for read-only access
    return f"file:{DB_PATH}?mode=ro"

def get_orchestrator_conn():
    """
    Return a sqlite3 connection read only using the configured DB path.
    """
    try:
        db_uri = _resolve_db_uri()
        conn = sqlite3.connect(db_uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        #conn.execute('PRAGMA journal_mode=WAL') 
        logger.info(f"SQLite connection (ro) OK: {DB_PATH}")
        return conn
    except FileNotFoundError as e:
        logger.error(f"Database file not found: {e}")
        raise
    except Exception as e:
        logger.error(f"SQLite connection error: {e}")
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
            