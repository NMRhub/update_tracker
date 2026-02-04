#!/usr/bin/env python3
import datetime
from dataclasses import dataclass, field


@dataclass
class Overdue:
    """Container for server issues found in the report."""
    uptime: list[tuple[str, float]] = field(default_factory=list)
    never_updated: list[str] = field(default_factory=list)
    update_old: list[tuple[str, datetime.date, int]] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Total number of servers with issues."""
        return len(self.uptime) + len(self.never_updated) + len(self.update_old)


def report(conn, uptime_limit_days, update_limit_days) -> Overdue:
    current_time = datetime.datetime.now(datetime.timezone.utc)


    cursor = conn.cursor()
    cursor.execute('''SELECT hostname, last_update, uptime_days, sample_time
        FROM host_updates
        ORDER BY hostname''')

    current_date = datetime.date.today()
    update_cutoff_date = current_date - datetime.timedelta(days=update_limit_days)

    issues = Overdue()

    for row in cursor.fetchall():
        hostname, last_update, uptime_days, sample_time = row

        if uptime_days > uptime_limit_days:
            issues.uptime.append((hostname, uptime_days))

        if last_update is None:
            issues.never_updated.append(hostname)
        else:
            # Check if update is too old
            last_update_date = datetime.date.fromisoformat(last_update)
            if last_update_date < update_cutoff_date:
                days_since_update = (current_date - last_update_date).days
                issues.update_old.append((hostname, last_update_date, days_since_update))

    return issues

