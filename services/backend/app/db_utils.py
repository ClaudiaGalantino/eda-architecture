from dotenv import load_dotenv
import sqlite3
import logging
import os

load_dotenv()
logger = logging.getLogger(__name__)
_data_initilized = False

DB_FILENAME = os.getenv('DB_NAME') or 'garmin_tokens.db'
DB_PATH = os.path.join('/app/data/', DB_FILENAME)

def get_conn():
    """Return a sqlite3 connection using the configured DB path."""
    global _data_initilized
    abs_path = os.path.abspath(DB_PATH)
    dir_path = os.path.dirname(DB_PATH)

    if not _data_initilized:
        logger.info(f"Using database at: {abs_path}")
        logger.info(f"DB directory: {dir_path}, exists: {os.path.exists(dir_path)}, is_dir: {os.path.isdir(dir_path)}")
        try:
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"makedirs succeeded for {dir_path}")
        except Exception as e:
            logger.error(f"makedirs failed for {dir_path}: {e}")
            raise
        
        _data_initilized = True
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        # C
        conn.execute('PRAGMA busy_timeout=5000')
    except Exception:
        pass
    logger.info(f"Connected successfully to: {DB_PATH}")
    return conn


def init_db():
    '''
    Initialise the SQLite database with necessary tables.'''
    conn = get_conn()
    c = conn.cursor()
    # Ensure WAL mode so other containers can read while it writes
    try:
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('PRAGMA synchronous=NORMAL')
        c.execute('PRAGMA wal_autocheckpoint=1000')
        logger.info("SQLite PRAGMAs applied: WAL, synchronous=NORMAL, wal_autocheckpoint=1000")
    except Exception as e:
        logger.warning(f"Failed to apply SQLite PRAGMAs for WAL: {e}")
    c.execute(
        '''CREATE TABLE IF NOT EXISTS tokens (
            garmin_subscriber_id TEXT PRIMARY KEY,
            session_id TEXT UNIQUE,
            oauth_token TEXT,
            oauth_token_secret TEXT
        )'''
    )
    c.execute(
        '''CREATE TABLE IF NOT EXISTS user_mappings (
            garmin_subscriber_id TEXT PRIMARY KEY,
            user_email TEXT
        )'''
    )
    conn.commit()
    conn.close()
    logger.info(f"Database schemas initialized at: {os.path.abspath(DB_PATH)}")


def save_token_by_garmin_id(garmin_subscriber_id, token, secret, session_id):
    """
    Save or update the OAuth token and secret using the Garmin ID as primary key.
    """
    conn = get_conn()
    c = conn.cursor()
    stmt = '''
        INSERT OR REPLACE INTO tokens (garmin_subscriber_id, oauth_token, oauth_token_secret, session_id) 
        VALUES (?, ?, ?, ?)
    '''
    params = (garmin_subscriber_id, token, secret, session_id)
    c.execute(stmt, params)
    conn.commit()
    conn.close()
    logger.info(f"Saved/Updated OAuth token for Garmin ID: {garmin_subscriber_id}")


def get_token_internal(session_id):
    """
    Retrieve the OAuth token and secret for a given session ID.
    Uses the user_mappings table to find the associated Garmin ID.
    """
    conn = get_conn()
    c = conn.cursor()
    stmt_token = 'SELECT oauth_token, oauth_token_secret FROM tokens WHERE session_id = ?'
    c.execute(stmt_token, (session_id,))
    token_row = c.fetchone()
    conn.close()
    return (token_row['oauth_token'], token_row['oauth_token_secret']) if token_row else None


def get_token(garmin_id):
    """Retrieve the OAuth token and secret for a given garmin user ID."""
    conn = get_conn()
    c = conn.cursor()
    stmt = 'SELECT oauth_token, oauth_token_secret FROM tokens WHERE garmin_subscriber_id = ?'
    c.execute(stmt, (garmin_id,))
    row = c.fetchone()
    conn.close()
    return (row['oauth_token'], row['oauth_token_secret']) if row else None


def save_garmin_mapping(garmin_subscriber_id, user_email, session_id):
    """Save or update the mapping between Garmin subscriber ID and user email."""
    conn = get_conn()
    c = conn.cursor()
    stmt = '''INSERT OR REPLACE INTO user_mappings (garmin_subscriber_id, user_email) VALUES (?, ?)'''
    params = (garmin_subscriber_id, user_email)
    c.execute(stmt, params)
    conn.commit()
    conn.close()
    logger.info(f"Saved mapping: Garmin ID {garmin_subscriber_id}, Email {user_email}")


def get_email(garmin_id):
    """Retrieve the email associated with a given internal user ID."""
    conn = get_conn()
    c = conn.cursor()
    stmt = 'SELECT user_email FROM user_mappings WHERE garmin_subscriber_id = ?'
    c.execute(stmt, (garmin_id,))
    row = c.fetchone()
    conn.close()
    return row['user_email'] if row else None


def delete_user(user_id):
    """Delete a user from tokens and user_mappings tables.
    Returns a dict with deletion counts or None on error.
    """
    conn = get_conn()
    c = conn.cursor()
    try:
        # Delete token from tokens table
        stmt = 'DELETE FROM tokens WHERE garmin_subscriber_id = ?'
        c.execute(stmt, (user_id,))
        tokens_deleted = c.rowcount
        
        # Delete mapping from user_mappings table
        stmt = 'DELETE FROM user_mappings WHERE garmin_subscriber_id = ?'
        c.execute(stmt, (user_id,))
        mappings_deleted = c.rowcount
        
        conn.commit()
        logger.info(f"Deleted user {user_id}: tokens_deleted={tokens_deleted}, mappings_deleted={mappings_deleted}")
        return {'tokens_deleted': tokens_deleted, 'mappings_deleted': mappings_deleted}
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting user {user_id}: {e}")
        return None
    finally:
        conn.close()
