#!/usr/bin/env python3
import argparse
import datetime
import logging
import sqlite3

import yaml

from update_tracker import update_tracker_logger, init_database
from update_tracker.database import report

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--loglevel', default='WARN', help="Python logging level")
    parser.add_argument('--yaml',default="/etc/nmrhub.d/update_tracker.yaml",
                        help="YAML configuration file")

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
    database_file = config['data']

    # Initialize database
    conn = init_database(database_file)
    current_time = datetime.datetime.now(datetime.timezone.utc)

    c = config['cutoffs']
    uptime_limit_days = c['uptime days']
    update_limit_days = c['update days']

    issues = report(conn,uptime_limit_days,update_limit_days)
    conn.close()
    # Display results
    print("=" * 70)
    print("UPDATE TRACKER REPORT")
    print(f"Generated: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Uptime limit: {uptime_limit_days} days")
    print(f"Update limit: {update_limit_days} days")
    print("=" * 70)

    # Display uptime issues
    if issues.uptime:
        print(f"\n⚠️  Servers with excessive uptime (>{uptime_limit_days} days):")
        for hostname, uptime_days in sorted(issues.uptime, key=lambda x: x[1], reverse=True):
            print(f"  • {hostname}: {uptime_days:.1f} days")
    else:
        print(f"\n✓ No servers with excessive uptime (>{uptime_limit_days} days)")

    # Display never updated servers
    if issues.never_updated:
        print(f"\n⚠️  Servers never updated:")
        for hostname in sorted(issues.never_updated):
            print(f"  • {hostname}")
    else:
        print(f"\n✓ No servers without update history")

    # Display outdated update issues
    if issues.update_old:
        print(f"\n⚠️  Servers with outdated updates (>{update_limit_days} days):")
        for hostname, last_update_date, days_since in sorted(issues.update_old, key=lambda x: x[2], reverse=True):
            print(f"  • {hostname}: last updated {last_update_date} ({days_since} days ago)")
    else:
        print(f"\n✓ No servers with outdated updates (>{update_limit_days} days)")

    # Summary
    total_issues = issues.total
    print("\n" + "=" * 70)
    if total_issues > 0:
        print(f"TOTAL: {total_issues} server(s) require attention")
    else:
        print("TOTAL: All servers are up to date!")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()

