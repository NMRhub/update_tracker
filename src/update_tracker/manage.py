#!/usr/bin/env python3
import argparse
import datetime
import logging
import sqlite3

import yaml

from update_tracker import update_tracker_logger, init_database
from update_tracker.database import report

def delete_host(conn: sqlite3.Connection, hostname: str) -> bool:
    """Delete a hostname from the database.

    Args:
        conn: Database connection
        hostname: Hostname to delete

    Returns:
        True if host was deleted, False if not found
    """
    cursor = conn.cursor()

    # Check if host exists
    cursor.execute('SELECT hostname FROM host_updates WHERE hostname = ?', (hostname,))
    if not cursor.fetchone():
        return False

    # Delete the host
    cursor.execute('DELETE FROM host_updates WHERE hostname = ?', (hostname,))
    conn.commit()
    return True


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Manage update tracker database'
    )
    parser.add_argument('-l', '--loglevel', default='WARN', help="Python logging level")
    parser.add_argument('--yaml', default="/etc/nmrhub.d/update_tracker.yaml",
                        help="YAML configuration file")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--delete', metavar='HOSTNAME',
                       help="Remove this hostname from database")

    args = parser.parse_args()
    log_level = getattr(logging, args.loglevel)

    # Configure logging with the specified level and force to stderr
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True
    )

    with open(args.yaml) as f:
        config = yaml.safe_load(f)
    database_file = config['data']

    conn = init_database(database_file)

    # Handle delete operation
    if args.delete:
        hostname = args.delete
        if delete_host(conn, hostname):
            print(f"✓ Deleted {hostname} from database")
            update_tracker_logger.info(f"Deleted {hostname}")
        else:
            print(f"✗ Host {hostname} not found in database")
            update_tracker_logger.warning(f"Host {hostname} not found")

    conn.close()


if __name__ == "__main__":
    main()
