import sqlite3
import logging
from dotenv import load_dotenv
import os

load_dotenv()
logger = logging.getLogger(__name__)

default_db = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'garmin_tokens.db'))
db_name = os.getenv('DB_NAME') or default_db

def get_conn():
    """Return a sqlite3 connection using the configured DB path."""
    path = os.path.abspath(db_name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    '''
    Initialise the SQLite database with necessary tables.'''
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        '''CREATE TABLE IF NOT EXISTS tokens (
            user_id TEXT PRIMARY KEY,
            oauth_token TEXT,
            oauth_token_secret TEXT
        )'''
    )
    c.execute(
        '''CREATE TABLE IF NOT EXISTS user_mappings (
            garmin_subscriber_id TEXT PRIMARY KEY,
            user_email TEXT,
            internal_user_id TEXT UNIQUE
        )'''
    )
    conn.commit()
    conn.close()
    logger.info(f"Database schemas initialized at: {os.path.abspath(db_name)}")


def save_token(user_id, token, secret):
    """
    Save or update the OAuth token and secret for a given internal user ID.
    """
    conn = get_conn()
    c = conn.cursor()
    stmt = 'INSERT OR REPLACE INTO tokens (user_id, oauth_token, oauth_token_secret) VALUES (?, ?, ?)'
    params = (user_id, token, secret)
    c.execute(stmt, params)
    conn.commit()
    conn.close()
    logger.info(f"Saved/Updated OAuth token for internal user: {user_id}")


def get_token(user_id):
    """Retrieve the OAuth token and secret for a given internal user ID."""
    conn = get_conn()
    c = conn.cursor()
    stmt = 'SELECT oauth_token, oauth_token_secret FROM tokens WHERE user_id = ?'
    c.execute(stmt, (user_id,))
    row = c.fetchone()
    conn.close()
    return (row['oauth_token'], row['oauth_token_secret']) if row else None


def save_garmin_mapping(garmin_subscriber_id,user_email, internal_user_id):
    """Save or update the mapping between Garmin subscriber ID and internal user ID."""
    conn = get_conn()
    c = conn.cursor()
    stmt = '''INSERT OR REPLACE INTO user_mappings (garmin_subscriber_id, user_email, internal_user_id) VALUES (?, ?, ?)'''
    params = (garmin_subscriber_id, user_email, internal_user_id)
    c.execute(stmt, params)
    conn.commit()
    conn.close()
    logger.info(f"Saved mapping: Garmin ID {garmin_subscriber_id}, Email {user_email} -> Internal ID {internal_user_id}")


def is_garmin_user(garmin_subscriber_id):
    """Check if a Garmin subscriber ID is already mapped to an internal user ID."""
    conn = get_conn()
    c = conn.cursor()
    stmt = 'SELECT 1 FROM user_mappings WHERE garmin_subscriber_id = ? LIMIT 1'
    c.execute(stmt, (garmin_subscriber_id,))
    row = c.fetchone()
    conn.close()
    return True if row else False

def get_email(user_id):
    """Retrieve the email associated with a given internal user ID."""
    conn = get_conn()
    c = conn.cursor()
    stmt = 'SELECT user_email FROM user_mappings WHERE internal_user_id = ?'
    c.execute(stmt, (user_id,))
    row = c.fetchone()
    conn.close()
    return row['user_email'] if row else None
