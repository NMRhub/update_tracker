#!/usr/bin/env python3
import argparse
import datetime
from concurrent.futures import Future

from update_tracker import update_tracker_logger, postgres_connect, add_common_args, setup_logging, load_config, build_host_limits
from update_tracker.last_update import UpdateChecker
from update_tracker.query import query_ansible


def get_last_sample_time(conn, hostname: str) -> datetime.datetime | None:
    """Get the last sample time for a host."""
    cursor = conn.cursor()
    cursor.execute(
        'SELECT sample_time FROM audit.host_updates WHERE hostname = %s',
        (hostname,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _is_old_ubuntu(version: str, current: str) -> bool:
    """Return True if version is older than current (e.g. '22.04' < '24.04')."""
    def parts(v: str) -> tuple:
        return tuple(int(x) for x in v.split('.') if x.isdigit())
    return parts(version) < parts(current)


def store_update(conn, hostname: str,
                 last_update_date: datetime.date | None,
                 sample_time: datetime.datetime,
                 kernel_needs_reboot: bool | None = None,
                 kernel_available: bool | None = None,
                 old_version: bool | None = None):
    """Store host update information in the database."""
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO audit.host_updates
            (hostname, last_update, sample_time, kernel_needs_reboot, kernel_available, old_version)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (hostname) DO UPDATE SET
            last_update = EXCLUDED.last_update,
            sample_time = EXCLUDED.sample_time,
            kernel_needs_reboot = EXCLUDED.kernel_needs_reboot,
            kernel_available = EXCLUDED.kernel_available,
            old_version = EXCLUDED.old_version
    ''', (hostname, last_update_date, sample_time,
          kernel_needs_reboot, kernel_available, old_version))
    conn.commit()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_common_args(parser)
    parser.add_argument('-r','--resample',action='store_true',
                        help="Only sample overdue servers")
    parser.add_argument('-s', '--server', default=None,
                        help="Only sample this single server")
    parser.add_argument('-n', '--now', action='store_true',
                        help="Resample all hosts regardless of last sample time")

    args = parser.parse_args()
    setup_logging(args)
    config = load_config(args)
    conn = postgres_connect(config)
    a = config['ansible']
    c = config['cutoffs']
    current_ubuntu = config.get('current ubuntu')
    ssh_seconds = c['ssh seconds']
    sample_cutoff_hours = c['sample hours']
    sample_cutoff_delta = datetime.timedelta(hours=sample_cutoff_hours)

    host_limits = build_host_limits(config)

    # Get combined inventory (provides SSH credentials)
    inv = query_ansible(a['config'], a['inventory'])
    update_tracker_logger.info(f"Found {len(inv.inventory)} hosts")

    sample_time = datetime.datetime.now(datetime.timezone.utc)

    # Determine which hosts to sample
    if args.server:
        hosts_to_sample = [args.server]
        update_tracker_logger.info(f"Single-server mode: sampling {args.server}")
    elif args.resample:
        # Check each host against its per-host limits to find overdue ones
        cursor = conn.cursor()
        cursor.execute('SELECT hostname, last_update FROM audit.host_updates')

        current_date = datetime.date.today()
        overdue_hosts = set()
        for hostname, last_update in cursor.fetchall():
            update_limit = host_limits.get(hostname, 0)

            if last_update is None:
                overdue_hosts.add(hostname)
            elif (current_date - last_update).days > update_limit:
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
        # Submit all hosts to thread pool, skipping recently-sampled ones
        futures: dict[str, Future] = {}
        for host in hosts_to_sample:
            update_tracker_logger.debug(f"host {host}")
            if not args.resample and not args.server and not args.now:
                last_sample = get_last_sample_time(conn, host)
                if last_sample:
                    time_since_sample = sample_time - last_sample
                    if time_since_sample < sample_cutoff_delta:
                        update_tracker_logger.info(
                            f"{host}: skipped (last sampled {time_since_sample.total_seconds() / 3600:.1f} hours ago)"
                        )
                        skipped += 1
                        continue
            futures[host] = checker.submit(host)

        # Collect results and write to database
        for host, future in futures.items():
            try:
                r = future.result(timeout=60)
                update_info = r.update if r.update else "never"
                old_version = None
                if r.ubuntu_version and current_ubuntu:
                    old_version = _is_old_ubuntu(r.ubuntu_version, str(current_ubuntu))
                update_tracker_logger.info(
                    f"{host}: update={update_info}, "
                    f"kernel_needs_reboot={r.kernel_needs_reboot}, kernel_available={r.kernel_available}, "
                    f"ubuntu={r.ubuntu_version}, old_version={old_version}"
                )
                store_update(conn, host, r.update, sample_time,
                             r.kernel_needs_reboot, r.kernel_available, old_version)
                processed += 1
            except KeyboardInterrupt:
                update_tracker_logger.warning(f"Interrupted while waiting for {host}, continuing")
            except Exception as e:
                update_tracker_logger.error(f"Failed to process {host}: {e}")

    conn.close()
    update_tracker_logger.info(f"Processed {processed} hosts, skipped {skipped} hosts")


if __name__ == "__main__":
    main()
