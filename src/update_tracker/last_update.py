import concurrent.futures
import datetime
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from update_tracker import SshUser, update_tracker_logger


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
class KernelStatus:
    needs_reboot: bool | None  # newer kernel installed but not running
    available: bool | None     # newer kernel available in apt


@dataclass
class LastUpdate:
    update: datetime.date | None
    uptime: datetime.timedelta
    # Both are None when the host is not Ubuntu
    kernel_needs_reboot: bool | None = field(default=None)  # newer kernel installed but not running
    kernel_available: bool | None = field(default=None)     # newer kernel available in apt

class UpdateChecker:
    _REMOTE_SCRIPT = '/tmp/_check_kernel.py'
    _KERNEL_SCRIPT = """\
import re, subprocess, sys

try:
    with open('/etc/os-release') as f:
        if 'ubuntu' not in f.read().lower():
            print('not-ubuntu')
            sys.exit(0)
except FileNotFoundError:
    print('not-ubuntu')
    sys.exit(0)

current = subprocess.run(['uname', '-r'], capture_output=True, text=True).stdout.strip()

dpkg = subprocess.run(['dpkg', '-l', 'linux-image-[0-9]*'], capture_output=True, text=True)
versions = [current]
for line in dpkg.stdout.splitlines():
    if line.startswith('ii'):
        pkg = line.split()[1]
        versions.append(pkg.replace('linux-image-', ''))

newest = sorted(set(versions), key=lambda v: tuple(int(x) for x in re.findall(r'\\d+', v)))[-1]
needs_reboot = 1 if newest != current else 0

subprocess.run(['apt-get', 'update', '-qq'], capture_output=True)
apt_list = subprocess.run(['apt', 'list', '--upgradable'], capture_output=True, text=True)
available = sum(1 for line in apt_list.stdout.splitlines() if 'linux-image' in line)

print(f'ubuntu:{needs_reboot}:{available}')
"""

    def __init__(self, ssh_user: SshUser, timeout: int):
        self.subprocess_timeout = timeout + 5
        self._account = ssh_user.account
        self._ssh_opts = [
            '-i', str(ssh_user.keyfile),
            '-o', f'ConnectTimeout={timeout}',
            '-o', 'ServerAliveInterval=5',
            '-o', 'ServerAliveCountMax=3',
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
        ]
        self.scp_base = ['scp'] + self._ssh_opts
        self._local_script: Path | None = None
        self._executor = concurrent.futures.ThreadPoolExecutor()

    def __enter__(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(self._KERNEL_SCRIPT)
            self._local_script = Path(f.name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=False)
        if self._local_script is not None:
            self._local_script.unlink(missing_ok=True)
        return False

    def submit(self, hostname: str) -> concurrent.futures.Future:
        """Submit get_last for hostname to the thread pool and return the Future."""
        return self._executor.submit(self.get_last, hostname)

    def get_last(self, hostname: str) -> LastUpdate:
        update_tracker_logger.info(f"Sampling {hostname}")
        remote_user_host = f'{self._account}@{hostname}'
        ssh_base = ['ssh'] + self._ssh_opts + [remote_user_host]

        apt_cmd = ssh_base + ['zless /var/log/apt/history*']
        apt_result = subprocess.run(apt_cmd, capture_output=True, text=True, timeout=self.subprocess_timeout)

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
                    next_line = lines[i + 1]
                    if 'apt-get' in next_line and 'upgrade' in next_line:
                        date_str = match.group(1)
                        current_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                        if last_upgrade_date is None or current_date > last_upgrade_date:
                            last_upgrade_date = current_date

        uptime_cmd = ssh_base + ['uptime']
        uptime_result = subprocess.run(uptime_cmd, capture_output=True, text=True, timeout=self.subprocess_timeout)

        if uptime_result.returncode != 0:
            raise RuntimeError(f"Failed to get uptime history: {uptime_result.stderr}")

        kernel_status = self._check_newer_kernel(ssh_base, remote_user_host)

        return LastUpdate(update=last_upgrade_date, uptime=to_delta(uptime_result.stdout),
                          kernel_needs_reboot=kernel_status.needs_reboot, kernel_available=kernel_status.available)

    def _check_newer_kernel(self, ssh_base: list, remote_user_host: str) -> KernelStatus:
        scp_cmd = self.scp_base + [str(self._local_script), f'{remote_user_host}:{self._REMOTE_SCRIPT}']
        subprocess.run(scp_cmd, capture_output=True, timeout=self.subprocess_timeout)

        result = subprocess.run(
            ssh_base + [f'python3 {self._REMOTE_SCRIPT}'],
            capture_output=True,
            text=True,
            timeout=self.subprocess_timeout,
        )

        output = result.stdout.strip()
        if output == 'not-ubuntu':
            return KernelStatus(needs_reboot=None, available=None)
        if output.startswith('ubuntu:'):
            parts = output.split(':')
            if len(parts) == 3:
                try:
                    return KernelStatus(needs_reboot=bool(int(parts[1])), available=bool(int(parts[2])))
                except ValueError:
                    pass
        return KernelStatus(needs_reboot=None, available=None)
