import datetime
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from ansible_collections.community.general.plugins.modules.scaleway_sshkey import sshkey_user_patch

from update_tracker import SshUser
def to_delta(uptime:str)->datetime.timedelta:
    """Parse uptime command output to timedelta.

    Args:
        uptime: Output from 'uptime' command
        Example: "16:07:47 up 4 days, 22:42,  9 users,  load average: 0.59, 0.84, 0.62"

    Returns:
        timedelta representing the system uptime
    """
    # Extract the uptime portion after "up" and before user count or load average
    match = re.search(r'up\s+(.+?)(?:,\s+\d+\s+users?|,\s+load)', uptime)
    if not match:
        raise ValueError(f"Could not parse uptime from: {uptime}")

    uptime_str = match.group(1).strip()

    days = 0
    hours = 0
    minutes = 0

    # Parse days: "4 days" or "1 day"
    day_match = re.search(r'(\d+)\s+days?', uptime_str)
    if day_match:
        days = int(day_match.group(1))

    # Parse hours:minutes: "22:42" or "1:30"
    time_match = re.search(r'(\d+):(\d+)', uptime_str)
    if time_match:
        hours = int(time_match.group(1))
        minutes = int(time_match.group(2))

    # Parse minutes only: "42 min"
    min_match = re.search(r'(\d+)\s+min', uptime_str)
    if min_match and not time_match:
        minutes = int(min_match.group(1))

    return datetime.timedelta(days=days, hours=hours, minutes=minutes)



@dataclass
class LastUpdate:
    update: datetime.date | None
    uptime: datetime.timedelta

def get_last(hostname:str,ssh_user:SshUser,timeout:int)->LastUpdate:
    """Get last apt upgrade and uptime times from remote host via SSH.

    Args:
        hostname: Remote host to connect to
        ssh_user: SSH user information (account and keyfile)
        timeout: SSH command timeout in seconds

    Returns:
        LastUpdate object with update and uptime dates
    """
    ssh_base = [
        'ssh',
        '-i', str(ssh_user.keyfile),
        '-o', f'ConnectTimeout={timeout}',
        '-o', 'ServerAliveInterval=5',
        '-o', 'ServerAliveCountMax=3',
        f'{ssh_user.account}@{hostname}'
    ]

    # Get last apt-get upgrade time
    # Add a buffer to subprocess timeout to let SSH handle its own timeout
    subprocess_timeout = timeout + 5
    apt_cmd = ssh_base + ['zless /var/log/apt/history*']
    apt_result = subprocess.run(apt_cmd, capture_output=True, text=True, timeout=subprocess_timeout)

    if apt_result.returncode != 0:
        raise RuntimeError(f"Failed to get apt history: {apt_result.stderr}")

    # Parse apt history for Start-Date entries followed by apt-get upgrade commands
    last_upgrade_date = None
    lines = apt_result.stdout.splitlines()
    for i, line in enumerate(lines):
        if line.startswith('Start-Date:'):
            # Format: Start-Date: 2024-01-25  10:30:15
            match = re.search(r'Start-Date:\s+(\d{4}-\d{2}-\d{2})', line)
            if match and i + 1 < len(lines):
                # Check next line contains both "apt-get" and "upgrade"
                next_line = lines[i + 1]
                if 'apt-get' in next_line and 'upgrade' in next_line:
                    date_str = match.group(1)
                    current_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                    if last_upgrade_date is None or current_date > last_upgrade_date:
                        last_upgrade_date = current_date

    # last_upgrade_date will be None if no apt-get upgrade found in history

    # Get last uptime time
    uptime_cmd = ssh_base + ['uptime']
    uptime_result = subprocess.run(uptime_cmd, capture_output=True, text=True, timeout=subprocess_timeout)

    if uptime_result.returncode != 0:
        raise RuntimeError(f"Failed to get uptime history: {uptime_result.stderr}")

    last_uptime_date = to_delta(uptime_result.stdout)

    return LastUpdate(update=last_upgrade_date, uptime=last_uptime_date)
