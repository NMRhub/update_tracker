#!/usr/bin/env python3
import argparse
import datetime
import logging
import sqlite3

import yaml

from update_tracker import update_tracker_logger, init_database
from update_tracker.last_update import get_last
from update_tracker.query import query_ansible


def get_last_sample_time(conn: sqlite3.Connection, hostname: str) -> datetime.datetime | None:
    """Get the last sample time for a host."""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT sample_time FROM host_updates
        WHERE hostname = ?
    ''', (hostname,))

    row = cursor.fetchone()
    if row:
        # Parse the timestamp string back to datetime
        return datetime.datetime.fromisoformat(row[0])
    return None


def store_update(conn: sqlite3.Connection, hostname: str,
                 last_update_date: datetime.date | None,
                 uptime: datetime.timedelta,
                 sample_time: datetime.datetime):
    """Store host update information in the database."""
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
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--loglevel', default='WARN', help="Python logging level")
    parser.add_argument('--yaml',default="/etc/nmrhub.d/update_tracker.yaml",
                        help="YAML configuration file")
    parser.add_argument('-r','--resample',action='store_true',
                        help="Only sample overdue servers")

    args = parser.parse_args()
    log_level = getattr(logging, args.loglevel)

    # Configure logging with the specified level and force to stderr
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True
    )
    with open(args.yaml) as f:
        config = yaml.safe_load((f))
    a = config['ansible']
    database_file = config['data']

    # Initialize database
    conn = init_database(database_file)
    sample_time = datetime.datetime.now(datetime.timezone.utc)

    inv = query_ansible(a['config'],a['inventory'])
    update_tracker_logger.info(f"Found {len(inv.inventory)} hosts")

    c = config['cutoffs']
    ssh_seconds = c['ssh seconds']
    sample_cutoff_hours = c['sample hours']
    sample_cutoff_delta = datetime.timedelta(hours=sample_cutoff_hours)
    uptime_limit = datetime.timedelta(days=c['uptime days'])
    update_limit = datetime.timedelta(days=c['update days'])

    processed = 0
    skipped = 0

    for host in inv.inventory:
        try:
            update_tracker_logger.debug(f"host {host}")
                # Check if host was sampled recently
            last_sample = get_last_sample_time(conn, host)
            if last_sample:
                time_since_sample = sample_time - last_sample
                if time_since_sample < sample_cutoff_delta:
                    msg = f"{host}: skipped (last sampled {time_since_sample.total_seconds() / 3600:.1f} hours ago)"
                    update_tracker_logger.info(msg)
                    skipped += 1
                    continue

            r = get_last(host, inv, ssh_seconds)
            update_info = r.update if r.update else "never"
            update_tracker_logger.info(f"{host}: update={update_info}, uptime={r.uptime}")
            store_update(conn, host, r.update, r.uptime, sample_time)
            processed += 1
        except Exception as e:
            update_tracker_logger.error(f"Failed to process {host}: {e}")

    conn.close()
    update_tracker_logger.info(f"Processed {processed} hosts, skipped {skipped} hosts")


if __name__ == "__main__":
    main()

