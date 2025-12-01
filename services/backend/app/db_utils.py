import sqlite3
from venv import logger
from dotenv import load_dotenv
import os

load_dotenv()
db_name = os.getenv('DB_NAME')

def init_db():
    '''
    Initialise the SQLite database with necessary tables.'''
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            user_id TEXT PRIMARY KEY, 
            oauth_token TEXT,
            oauth_token_secret TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_mappings (
            garmin_subscriber_id TEXT PRIMARY KEY,
            internal_user_id TEXT UNIQUE
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Database schemas initialized.")

def save_token(user_id, token, secret):
    """
    Save or update the OAuth token and secret for a given internal user ID."""
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO tokens (user_id, oauth_token, oauth_token_secret) 
        VALUES (?, ?, ?)
    ''', (user_id, token, secret))
    conn.commit()
    conn.close()
    logger.info(f"Saved/Updated OAuth token for internal user: {user_id}")

def get_token(user_id):
    """Retrieve the OAuth token and secret for a given internal user ID."""
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('SELECT oauth_token, oauth_token_secret FROM tokens WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    conn.close()
    return row

def save_garmin_mapping(garmin_subscriber_id, internal_user_id):
    """Save or update the mapping between Garmin subscriber ID and internal user ID."""
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO user_mappings (garmin_subscriber_id, internal_user_id) 
        VALUES (?, ?)
    ''', (garmin_subscriber_id, internal_user_id))
    conn.commit()
    conn.close()
    logger.info(f"Saved mapping: Garmin ID {garmin_subscriber_id} -> Internal ID {internal_user_id}")

def is_garmin_user(garmin_subscriber_id):
    """Check if a Garmin subscriber ID is already mapped to an internal user ID."""
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('SELECT internal_user_id FROM user_mappings WHERE garmin_subscriber_id = ?', (garmin_subscriber_id,))
    row = c.fetchone()
    conn.close()
    return row is not None