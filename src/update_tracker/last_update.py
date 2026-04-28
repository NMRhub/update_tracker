import concurrent.futures
import datetime
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from update_tracker import SshUser, update_tracker_logger


@dataclass
class KernelStatus:
    needs_reboot: bool | None  # newer kernel installed but not running
    available: bool | None     # newer kernel available in apt
    ubuntu_version: str | None = None  # e.g. "22.04"; None if not Ubuntu


@dataclass
class LastUpdate:
    update: datetime.date | None
    # All None when the host is not Ubuntu
    kernel_needs_reboot: bool | None = field(default=None)  # newer kernel installed but not running
    kernel_available: bool | None = field(default=None)     # newer kernel available in apt
    ubuntu_version: str | None = field(default=None)        # e.g. "22.04"; None if not Ubuntu

class UpdateChecker:
    _REMOTE_SCRIPT = '/tmp/_check_kernel.py'
    _KERNEL_SCRIPT = """\
import re, subprocess, sys

try:
    with open('/etc/os-release') as f:
        content = f.read()
        if 'ubuntu' not in content.lower():
            print('not-ubuntu')
            sys.exit(0)
        m = re.search(r'VERSION_ID="?([\\d.]+)"?', content)
        ubuntu_version = m.group(1) if m else ''
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

print(f'ubuntu:{needs_reboot}:{available}:{ubuntu_version}')
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

        kernel_status = self._check_newer_kernel(ssh_base, remote_user_host)

        return LastUpdate(update=last_upgrade_date,
                          kernel_needs_reboot=kernel_status.needs_reboot,
                          kernel_available=kernel_status.available,
                          ubuntu_version=kernel_status.ubuntu_version)

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
            if len(parts) >= 3:
                try:
                    version = parts[3] if len(parts) >= 4 else None
                    return KernelStatus(
                        needs_reboot=bool(int(parts[1])),
                        available=bool(int(parts[2])),
                        ubuntu_version=version or None,
                    )
                except ValueError:
                    pass
        return KernelStatus(needs_reboot=None, available=None)
