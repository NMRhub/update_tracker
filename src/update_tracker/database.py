#!/usr/bin/env python3
import datetime
from dataclasses import dataclass, field


@dataclass
class Overdue:
    """Container for server issues found in the report."""
    uptime: list[tuple[str, float]] = field(default_factory=list)
    never_updated: list[str] = field(default_factory=list)
    update_old: list[tuple[str, datetime.date, int]] = field(default_factory=list)
    kernel_needs_reboot: list[str] = field(default_factory=list)
    kernel_available: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Total number of servers with issues."""
        return len(self.uptime) + len(self.never_updated) + len(self.update_old) + len(self.kernel_needs_reboot) + len(self.kernel_available)


def report(conn, host_limits: dict[str, tuple[int, int]]) -> Overdue:
    """Query database and return overdue hosts checked against per-host limits.

    Args:
        conn: Database connection
        host_limits: Mapping of hostname -> (uptime_days, update_days)
    """
    cursor = conn.cursor()
    cursor.execute('''SELECT hostname, last_update, uptime_days, sample_time, kernel_needs_reboot, kernel_available
        FROM host_updates
        ORDER BY hostname''')

    current_date = datetime.date.today()
    issues = Overdue()

    for row in cursor.fetchall():
        hostname, last_update, uptime_days, sample_time, kernel_needs_reboot, kernel_available = row

        uptime_limit, update_limit = host_limits.get(hostname, (0, 0))

        if uptime_days > uptime_limit:
            issues.uptime.append((hostname, uptime_days))

        if last_update is None:
            issues.never_updated.append(hostname)
        else:
            last_update_date = datetime.date.fromisoformat(last_update)
            if (current_date - last_update_date).days > update_limit:
                days_since_update = (current_date - last_update_date).days
                issues.update_old.append((hostname, last_update_date, days_since_update))

        if kernel_needs_reboot:
            issues.kernel_needs_reboot.append(hostname)
        if kernel_available:
            issues.kernel_available.append(hostname)

    return issues

