#!/usr/bin/env python3
import datetime
from dataclasses import dataclass, field

from update_tracker import HostSpec


@dataclass
class Overdue:
    """Container for server issues found in the report."""
    never_updated: list[str] = field(default_factory=list)
    update_old: list[tuple[str, datetime.date, int]] = field(default_factory=list)
    kernel_needs_reboot: list[str] = field(default_factory=list)
    kernel_available: list[str] = field(default_factory=list)
    old_version: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Total number of servers with issues."""
        return (len(self.never_updated) + len(self.update_old)
                + len(self.kernel_needs_reboot) + len(self.kernel_available) + len(self.old_version))


def report(conn, host_spec: HostSpec, show_all: bool = False):
    """Query database and return overdue hosts checked against per-host limits.

    Args:
        conn: Database connection
        host_spec: HostSpec containing host_limits (hostname -> update_days)
        show_all: If False (default), suppress hosts that have an update_schedule in
                  audit.update_schedule (they are managed via a regular schedule).
    """
    cursor = conn.cursor()

    scheduled_hosts: set[str] = set()
    if not show_all:
        cursor.execute('SELECT hostname FROM audit.update_schedule where update_schedule is not null')
        scheduled_hosts = {row[0] for row in cursor.fetchall()}

    cursor.execute('''SELECT hostname, last_update, sample_time, kernel_needs_reboot, kernel_available, old_version
        FROM audit.host_updates
        ORDER BY hostname''')

    current_date = datetime.date.today()
    issues = Overdue()

    for row in cursor.fetchall():
        hostname, last_update, sample_time, kernel_needs_reboot, kernel_available, old_version = row
        if not host_spec.filter(hostname):
            continue
        if hostname in scheduled_hosts:
            continue

        update_limit = host_spec.host_limits.get(hostname, 0)

        if last_update is None:
            issues.never_updated.append(hostname)
        else:
            if (current_date - last_update).days > update_limit:
                days_since_update = (current_date - last_update).days
                issues.update_old.append((hostname, last_update, days_since_update))

        if kernel_needs_reboot:
            issues.kernel_needs_reboot.append(hostname)
        if kernel_available:
            issues.kernel_available.append(hostname)
        if old_version:
            issues.old_version.append(hostname)

    return issues
