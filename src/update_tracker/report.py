#!/usr/bin/env python3
import argparse
import datetime

from update_tracker import postgres_connect, HostSpec
from update_tracker.database import report
from update_tracker import add_common_args, setup_logging, load_config, build_host_limits

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_common_args(parser)
    parser.add_argument('--all', dest='show_all', action='store_true',
                        help="Include hosts that have a regular update schedule (suppressed by default)")

    args = parser.parse_args()
    setup_logging(args)
    config = load_config(args)
    a = config['ansible']
    c = config['cutoffs']

    host_limits = build_host_limits(config)

    conn = postgres_connect(config)
    current_time = datetime.datetime.now(datetime.timezone.utc)

    hs = HostSpec(host_limits=host_limits)

    issues = report(conn, hs, show_all=args.show_all)
    conn.close()

    # Display results
    print("=" * 70)
    print("UPDATE TRACKER REPORT")
    print(f"Generated: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    for inv_name in a['inventory']:
        inv_limits = c[inv_name]
        print(f"  {inv_name}: update={inv_limits['update days']}d")
    print("=" * 70)

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
            update_limit = host_limits.get(hostname, 0)
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

    # Display old Ubuntu version
    current_ubuntu = config.get('current ubuntu')
    if issues.old_version:
        print(f"\n⚠️  Servers running Ubuntu older than {current_ubuntu}:")
        for hostname in sorted(issues.old_version):
            print(f"  • {hostname}")
    else:
        print(f"\n✓ No servers with outdated Ubuntu version")

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
