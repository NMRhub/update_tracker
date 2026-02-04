
import importlib.metadata 
import logging
import sqlite3
from pathlib import Path
from typing import Protocol

update_tracker_logger = logging.getLogger(__name__)

__version__ =  importlib.metadata.version('update_tracker')

class SshUser(Protocol):
    account: str
    keyfile: Path


def init_database(db_path: str) -> sqlite3.Connection:
    """Initialize the database and create table if it doesn't exist. """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if table exists
    cursor.execute('''SELECT sql FROM sqlite_master WHERE type='table' AND name='host_updates' ''')
    result = cursor.fetchone()

    if not result:
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS host_updates (
                hostname TEXT PRIMARY KEY,
                last_update DATE,
                uptime_days REAL NOT NULL,
                sample_time TIMESTAMP NOT NULL
            )
        ''')

    conn.commit()
    return conn
