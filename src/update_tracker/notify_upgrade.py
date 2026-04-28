#!/usr/bin/env python3
import argparse
import datetime

from mailer.email_template import EmailTemplate
from nmrboxemail import SmtpMailer, Email
from postgresql_access import DatabaseDict

from update_tracker import update_tracker_logger, postgres_connect, HostSpec
from update_tracker import add_common_args, setup_logging, load_config, build_host_limits


def next_upgrade_date() -> datetime.date:
    """Return the next weekday at least 7 days from today."""
    target = datetime.date.today() + datetime.timedelta(days=7)
    while target.weekday() >= 5:  # Saturday=5, Sunday=6
        target += datetime.timedelta(days=1)
    return target


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_common_args(parser)
    parser.add_argument('--dry-run', action='store_true',
                        help="Show what would be done without updating database or sending email")

    args = parser.parse_args()
    setup_logging(args)
    config = load_config(args)

    host_limits = build_host_limits(config)

    hs = HostSpec(host_limits=host_limits)
    conn = postgres_connect(config)

    db = DatabaseDict(dictionary=config['database'])
    template = EmailTemplate(db=db)
    template_name = config['mail template']
    mailer = SmtpMailer(config)
    mailer.reply = config['reply']

    upgrade_date = next_upgrade_date()
    current_date = datetime.date.today()

    cursor = conn.cursor()
    cursor.execute('''
        SELECT hu.hostname, hu.last_update, hu.uptime_days, hu.kernel_needs_reboot,
               hu.kernel_available, hu.old_version, us.person_id
        FROM audit.host_updates hu
        JOIN audit.update_schedule us ON hu.hostname = us.hostname
        WHERE us.update_schedule IS NULL
          AND us.next_upgrade IS NULL
          AND us.person_id IS NOT NULL
          AND NOT us.user_managed
        ORDER BY hu.hostname
    ''')
    rows = cursor.fetchall()

    processed = 0
    for hostname, last_update, uptime_days, kernel_needs_reboot, kernel_available, old_version, person_id in rows:
        if not hs.filter(hostname):
            continue

        uptime_limit, update_limit = host_limits.get(hostname, (0, 0))

        actions = []
        reboot = False
        if last_update is None or (current_date - last_update).days > update_limit:
            actions.append("system updated")
        if kernel_needs_reboot:
            actions.append("rebooted for new kernel")
            reboot = True
        elif kernel_available:
            actions.append("kernel updated")
        if not reboot and uptime_days > uptime_limit:
            actions.append("reboot")

        if not old_version and not actions:
            continue

        action = " and ".join(actions)

        person_cursor = conn.cursor()
        person_cursor.execute(
            "SELECT first_name, email FROM public.persons WHERE id = %s",
            (person_id,)
        )
        person_row = person_cursor.fetchone()
        if person_row is None:
            update_tracker_logger.warning("No person found for person_id %s (host %s)", person_id, hostname)
            continue
        first_name, email_address = person_row
        if old_version:
            update_tracker_logger.warning(f"{first_name} {email_address} {hostname} old version")
            continue

        upgrade_dt = datetime.datetime.combine(upgrade_date, datetime.time(), tzinfo=datetime.timezone.utc)

        data = {
            'first_name': first_name,
            'vmname': hostname,
            'action': action,
            'date': upgrade_date.strftime('%A, %B %d, %Y'),
        }

        subject, content = template.format(template_name, data)

        print(f"{hostname}: notify {email_address}, scheduled {upgrade_date}, action: {action}")
        update_tracker_logger.info(content)

        if not args.dry_run:
            email = Email(subject, content, to=(email_address,))
            email.type = 'html'
            mailer.send(email)

            update_cursor = conn.cursor()
            update_cursor.execute(
                "UPDATE audit.update_schedule SET next_upgrade = %s WHERE hostname = %s",
                (upgrade_dt, hostname)
            )
            conn.commit()

        processed += 1

    conn.close()
    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{prefix}Processed {processed} host(s)")


if __name__ == "__main__":
    main()
