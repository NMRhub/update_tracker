#!/usr/bin/env python3
import argparse
import datetime
import logging
import sqlite3

import yaml

from update_tracker import update_tracker_logger, last_update
from update_tracker.last_update import get_last
from update_tracker.query import query_ansible


def init_database(db_path: str) -> sqlite3.Connection:
    """Initialize the database and create table if it doesn't exist.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        Database connection
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if it doesn't exist - one row per host
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS host_updates (
            hostname TEXT PRIMARY KEY,
            last_update DATE NOT NULL,
            uptime_days REAL NOT NULL,
            sample_time TIMESTAMP NOT NULL
        )
    ''')

    conn.commit()
    return conn


def store_update(conn: sqlite3.Connection, hostname: str,
                 last_update_date: datetime.date, uptime: datetime.timedelta,
                 sample_time: datetime.datetime):
    """Store host update information in the database.

    Args:
        conn: Database connection
        hostname: Host name
        last_update_date: Date of last apt upgrade
        uptime: System uptime as timedelta
        sample_time: Timestamp when the sample was taken
    """
    cursor = conn.cursor()

    # Convert uptime to days (as float for precision)
    uptime_days = uptime.total_seconds() / 86400.0

    cursor.execute('''
        INSERT OR REPLACE INTO host_updates
        (hostname, last_update, uptime_days, sample_time)
        VALUES (?, ?, ?, ?)
    ''', (hostname, last_update_date, uptime_days, sample_time))

    conn.commit()


def main():
    logging.basicConfig()
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--loglevel', default='WARN', help="Python logging level")
    parser.add_argument('--yaml',default="/etc/nmrhub.d/update_tracker.yaml",
                        help="YAML configuration file")

    args = parser.parse_args()
    update_tracker_logger.setLevel(getattr(logging,args.loglevel))
    with open(args.yaml) as f:
        config = yaml.safe_load((f))
    a = config['ansible']
    database_file = config['data']

    # Initialize database
    conn = init_database(database_file)
    sample_time = datetime.datetime.now()

    inv = query_ansible(a['config'],a['inventory'])
    update_tracker_logger.info(f"Found {len(inv.inventory)} hosts")

    for host in inv.inventory:
        try:
            r = get_last(host, inv)
            update_tracker_logger.info(f"{host}: update={r.update}, uptime={r.uptime}")
            store_update(conn, host, r.update, r.uptime, sample_time)
        except Exception as e:
            update_tracker_logger.error(f"Failed to process {host}: {e}")

    conn.close()
    update_tracker_logger.info(f"Successfully stored data for {len(inv.inventory)} hosts")


if __name__ == "__main__":
    main()

