#!/usr/bin/env python3
import datetime


def report( conn,uptime_limit_days,update_limit_days):
    current_time = datetime.datetime.now(datetime.timezone.utc)


    cursor = conn.cursor()
    cursor.execute('''SELECT hostname, last_update, uptime_days, sample_time
        FROM host_updates
        ORDER BY hostname''')

    current_date = datetime.date.today()
    update_cutoff_date = current_date - datetime.timedelta(days=update_limit_days)

    issues = {
        'uptime': [],
        'never_updated': [],
        'update_old': []
    }

    for row in cursor.fetchall():
        hostname, last_update, uptime_days, sample_time = row

        if uptime_days > uptime_limit_days:
            issues['uptime'].append((hostname, uptime_days))

        if last_update is None:
            issues['never_updated'].append(hostname)
        else:
            # Check if update is too old
            last_update_date = datetime.date.fromisoformat(last_update)
            if last_update_date < update_cutoff_date:
                days_since_update = (current_date - last_update_date).days
                issues['update_old'].append((hostname, last_update_date, days_since_update))

    conn.close()
    return issues

