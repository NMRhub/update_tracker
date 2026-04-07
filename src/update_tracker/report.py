#!/usr/bin/env python3
import argparse
import datetime
import logging

import yaml

from update_tracker import update_tracker_logger, postgres_connect, query_ansible, HostSpec
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
    a = config['ansible']
    c = config['cutoffs']

    # Build per-host limits from each inventory group, using the more permissive
    # (longer) limits if a host appears in multiple inventories
    host_limits: dict[str, tuple[int, int]] = {}
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

    conn = postgres_connect(config)
    current_time = datetime.datetime.now(datetime.timezone.utc)

    hs = HostSpec(host_limits=host_limits)

    issues = report(conn, hs)
    conn.close()

    # Display results
    print("=" * 70)
    print("UPDATE TRACKER REPORT")
    print(f"Generated: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    for inv_name in a['inventory']:
        inv_limits = c[inv_name]
        print(f"  {inv_name}: uptime={inv_limits['uptime days']}d, update={inv_limits['update days']}d")
    print("=" * 70)

    # Display uptime issues
    if issues.uptime:
        print(f"\n⚠️  Servers with excessive uptime:")
        for hostname, uptime_days in sorted(issues.uptime, key=lambda x: x[1], reverse=True):
            uptime_limit, _ = host_limits.get(hostname, (0, 0))
            print(f"  • {hostname}: {uptime_days:.1f} days (limit: {uptime_limit})")
    else:
        print(f"\n✓ No servers with excessive uptime")

    # Display never updated servers
    if issues.never_updated:
        print(f"\n⚠️  Servers never updated:")
        for hostname in sorted(issues.never_updated):
            print(f"  • {hostname}")
    else:
        print(f"\n✓ No servers without update history")

    # Display outdated update issues
    if issues.update_old:
        print(f"\n⚠️  Servers with outdated updates:")
        for hostname, last_update_date, days_since in sorted(issues.update_old, key=lambda x: x[2], reverse=True):
            _, update_limit = host_limits.get(hostname, (0, 0))
            print(f"  • {hostname}: last updated {last_update_date} ({days_since} days ago, limit: {update_limit})")
    else:
        print(f"\n✓ No servers with outdated updates")

    # Display kernel reboot needed
    if issues.kernel_needs_reboot:
        print(f"\n⚠️  Servers with newer kernel installed (reboot required):")
        for hostname in sorted(issues.kernel_needs_reboot):
            print(f"  • {hostname}")
    else:
        print(f"\n✓ No servers requiring kernel reboot")

    # Display kernel update available
    if issues.kernel_available:
        print(f"\n⚠️  Servers with kernel update available:")
        for hostname in sorted(issues.kernel_available):
            print(f"  • {hostname}")
    else:
        print(f"\n✓ No servers with kernel updates pending")

    # Summary
    total_issues = issues.total
    print("\n" + "=" * 70)
    if total_issues > 0:
        print(f"TOTAL: {total_issues} server(s) require attention")
    else:
        print("TOTAL: All servers are up to date!")
    print("=" * 70)


if __name__ == "__main__":
    main()

