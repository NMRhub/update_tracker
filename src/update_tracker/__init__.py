import dataclasses
import importlib.metadata 
import logging
import sqlite3
from dataclasses import dataclass
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
                sample_time TIMESTAMP NOT NULL,
                kernel_needs_reboot INTEGER,
                kernel_available INTEGER
            )
        ''')
    else:
        # Migrate existing table: add new columns if missing
        cursor.execute("PRAGMA table_info(host_updates)")
        columns = {row[1] for row in cursor.fetchall()}
        if 'kernel_needs_reboot' not in columns:
            cursor.execute('ALTER TABLE host_updates ADD COLUMN kernel_needs_reboot INTEGER')
        if 'kernel_available' not in columns:
            cursor.execute('ALTER TABLE host_updates ADD COLUMN kernel_available INTEGER')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS conffile_choices (
            hostname TEXT NOT NULL,
            conffile TEXT NOT NULL,
            choice TEXT NOT NULL CHECK (choice IN ('old', 'new')),
            recorded_at TIMESTAMP NOT NULL,
            PRIMARY KEY (hostname, conffile)
        )
    ''')

    conn.commit()
    return conn

from update_tracker.query import query_ansible

HostLimit = dict[str, tuple[int, int]]


@dataclass
class HostSpec:
    only_these : list[str] = dataclasses.field(default_factory=list)
    host_limits : HostLimit = dataclasses.field(default_factory=dict)

    def filter(self,hostname:str)->bool:
        return self.only_these is None or len(self.only_these) == 0 or hostname in self.only_these
