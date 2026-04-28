#!/usr/bin/env python3
import argparse
import datetime

import psycopg

from update_tracker import update_tracker_logger, postgres_connect, add_common_args, setup_logging, load_config
from update_tracker.database import report

def delete_host(conn: psycopg.Connection, hostname: str) -> bool:
    """Delete a hostname from the database.

    Args:
        conn: Database connection
        hostname: Hostname to delete

    Returns:
        True if host was deleted, False if not found
    """
    cursor = conn.cursor()

    # Check if host exists
    cursor.execute('SELECT hostname FROM audit.host_updates WHERE hostname = %s', (hostname,))
    if not cursor.fetchone():
        return False

    # Delete the host
    cursor.execute('DELETE FROM audit.host_updates WHERE hostname = %s', (hostname,))
    conn.commit()
    return True


def mark_updated(conn: psycopg.Connection, hostname: str) -> bool:
    """Mark a hostname as updated today."""
    cursor = conn.cursor()

    cursor.execute('SELECT hostname FROM audit.host_updates WHERE hostname = %s', (hostname,))
    if not cursor.fetchone():
        return False

    today = datetime.date.today()
    cursor.execute('''UPDATE audit.host_updates
        SET last_update = %s
        WHERE hostname = %s''', (today, hostname))
    conn.commit()
    return True


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Manage update tracker database'
    )
    add_common_args(parser)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--delete', help="Remove this hostname from database")
    group.add_argument('--mark-updated',help="Manually mark this hostname as updated today")

    args = parser.parse_args()
    setup_logging(args)
    config = load_config(args)
    conn = postgres_connect(config)

    # Handle delete operation
    if (hostname := args.delete):
        if delete_host(conn, hostname):
            print(f"✓ Deleted {hostname} from database")
            update_tracker_logger.info(f"Deleted {hostname}")
        else:
            print(f"✗ Host {hostname} not found in database")
            update_tracker_logger.warning(f"Host {hostname} not found")

    # Handle mark-updated operation
    if (hostname := args.mark_updated):
        if mark_updated(conn, hostname):
            today = datetime.date.today()
            print(f"✓ Marked {hostname} as updated on {today}")
            update_tracker_logger.info(f"Marked {hostname} as updated on {today}")
        else:
            print(f"✗ Host {hostname} not found in database")
            update_tracker_logger.warning(f"Host {hostname} not found")

    conn.close()


if __name__ == "__main__":
    main()
