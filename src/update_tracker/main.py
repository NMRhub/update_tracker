#!/usr/bin/env python3
import argparse
import datetime
import logging
import sqlite3

import yaml

from update_tracker import update_tracker_logger, init_database
from update_tracker.last_update import UpdateChecker
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
                 sample_time: datetime.datetime,
                 kernel_needs_reboot: bool | None = None,
                 kernel_available: bool | None = None):
    """Store host update information in the database."""
    cursor = conn.cursor()

    # Convert uptime to days (as float for precision)
    uptime_days = uptime.total_seconds() / 86400.0

    # SQLite stores booleans as integers; None stays NULL
    def to_int(b: bool | None) -> int | None:
        return int(b) if b is not None else None

    cursor.execute('''
        INSERT OR REPLACE INTO host_updates
        (hostname, last_update, uptime_days, sample_time, kernel_needs_reboot, kernel_available)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (hostname, last_update_date, uptime_days, sample_time,
          to_int(kernel_needs_reboot), to_int(kernel_available)))

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
    c = config['cutoffs']
    ssh_seconds = c['ssh seconds']
    sample_cutoff_hours = c['sample hours']
    sample_cutoff_delta = datetime.timedelta(hours=sample_cutoff_hours)

    # Build per-host limits from each inventory group, using the more permissive
    # (longer) limits if a host appears in multiple inventories
    host_limits: dict[str, tuple[int, int]] = {}  # hostname -> (uptime_days, update_days)
    for inv_name in a['inventory']:
        inv_limits = c[inv_name]
        uptime_days = inv_limits['uptime days']
        update_days = inv_limits['update days']
        group_inv = query_ansible(a['config'], [inv_name])
        for host in group_inv.inventory:
            if host in host_limits:
                existing_uptime, existing_update = host_limits[host]
                host_limits[host] = (max(existing_uptime, uptime_days), max(existing_update, update_days))
            else:
                host_limits[host] = (uptime_days, update_days)

    # Get combined inventory (provides SSH credentials)
    inv = query_ansible(a['config'], a['inventory'])
    update_tracker_logger.info(f"Found {len(inv.inventory)} hosts")

    # Initialize database
    conn = init_database(database_file)
    sample_time = datetime.datetime.now(datetime.timezone.utc)

    # Determine which hosts to sample
    if args.resample:
        # Check each host against its per-host limits to find overdue ones
        cursor = conn.cursor()
        cursor.execute('SELECT hostname, last_update, uptime_days FROM host_updates')

        current_date = datetime.date.today()
        overdue_hosts = set()
        for hostname, last_update, uptime_days in cursor.fetchall():
            uptime_limit, update_limit = host_limits.get(hostname, (0, 0))

            if uptime_days > uptime_limit:
                overdue_hosts.add(hostname)
            elif last_update is None:
                overdue_hosts.add(hostname)
            elif (current_date - datetime.date.fromisoformat(last_update)).days > update_limit:
                overdue_hosts.add(hostname)

        hosts_to_sample = [host for host in inv.inventory if host in overdue_hosts]
        update_tracker_logger.info(
            f"Resample mode: sampling {len(hosts_to_sample)} overdue hosts out of {len(inv.inventory)} total"
        )
    else:
        hosts_to_sample = inv.inventory

    processed = 0
    skipped = 0

    with UpdateChecker(inv, ssh_seconds) as checker:
        for host in hosts_to_sample:
            try:
                update_tracker_logger.debug(f"host {host}")
                # Check if host was sampled recently
                if not args.resample:
                    last_sample = get_last_sample_time(conn, host)
                    if last_sample:
                        time_since_sample = sample_time - last_sample
                        if time_since_sample < sample_cutoff_delta:
                            msg = f"{host}: skipped (last sampled {time_since_sample.total_seconds() / 3600:.1f} hours ago)"
                            update_tracker_logger.info(msg)
                            skipped += 1
                            continue

                r = checker.get_last(host)
                update_info = r.update if r.update else "never"
                update_tracker_logger.info(
                    f"{host}: update={update_info}, uptime={r.uptime}, "
                    f"kernel_needs_reboot={r.kernel_needs_reboot}, kernel_available={r.kernel_available}"
                )
                store_update(conn, host, r.update, r.uptime, sample_time,
                             r.kernel_needs_reboot, r.kernel_available)
                processed += 1
            except Exception as e:
                update_tracker_logger.error(f"Failed to process {host}: {e}")

    conn.close()
    update_tracker_logger.info(f"Processed {processed} hosts, skipped {skipped} hosts")


if __name__ == "__main__":
    main()

