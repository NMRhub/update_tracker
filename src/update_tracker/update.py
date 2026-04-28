#!/usr/bin/env python3
import argparse
import concurrent.futures
import datetime
import os
import queue
import re
import select
import subprocess
import threading
import time
from pathlib import Path

import psycopg

from update_tracker import postgres_connect, update_tracker_logger, HostLimit, HostSpec, add_common_args, setup_logging, load_config, build_host_limits
from update_tracker.query import query_ansible

REBOOT_TIMEOUT = 300      # seconds to wait for host to come back
POLL_INTERVAL = 15        # seconds between SSH reconnect attempts
SHUTDOWN_WAIT = 30        # seconds to wait before polling (let host start rebooting)
APT_UPGRADE_TIMEOUT = 600 # seconds for apt-get upgrade to complete

# apt output patterns that indicate manual intervention is required
_MANUAL_INTERVENTION_PATTERNS = [
    'dpkg was interrupted',
    'dpkg --configure',
    'apt --fix-broken',
    'unmet dependencies',
    'held broken packages',
    'requires manual',
]

def _ssh_opts(keyfile: Path, timeout: int) -> list[str]:
    return [
        '-i', str(keyfile),
        '-o', f'ConnectTimeout={timeout}',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
    ]


def send_reboot(hostname: str, account: str, keyfile: Path, timeout: int):
    """Issue reboot over SSH. Connection drop is expected and ignored."""
    try:
        subprocess.run(
            ['ssh'] + _ssh_opts(keyfile, timeout) + [f'{account}@{hostname}', '/usr/bin/sudo','/usr/sbin/reboot'],
            capture_output=True, text=True, timeout=timeout + 5
        )
    except subprocess.TimeoutExpired:
        pass  # connection drops when host reboots — expected


def monitor_reboot(hostname: str, account: str, keyfile: Path, results: dict):
    """Poll host after reboot until it responds or REBOOT_TIMEOUT is reached."""
    time.sleep(SHUTDOWN_WAIT)
    deadline = time.monotonic() + (REBOOT_TIMEOUT - SHUTDOWN_WAIT)

    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                ['ssh'] + _ssh_opts(keyfile, 10) + [f'{account}@{hostname}', 'uptime'],
                capture_output=True, text=True, timeout=15
            )
            if result.returncode == 0:
                results[hostname] = 'ok'
                update_tracker_logger.info(f"{hostname}: reboot confirmed")
                return
        except (subprocess.TimeoutExpired, Exception):
            pass
        time.sleep(POLL_INTERVAL)

    results[hostname] = 'timeout'
    update_tracker_logger.error(f"{hostname}: did not come back within {REBOOT_TIMEOUT}s")


def get_kernel_issues(conn: psycopg.Connection) -> list[tuple[str, bool, bool]]:
    """Return hosts with kernel issues as (hostname, needs_reboot, available)."""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT hostname, kernel_needs_reboot, kernel_available
        FROM audit.host_updates
        WHERE kernel_needs_reboot = true OR kernel_available = true
        ORDER BY hostname
    ''')
    return [(row[0], bool(row[1]), bool(row[2])) for row in cursor.fetchall()]


def do_kernel(conn: psycopg.Connection, account: str, keyfile: Path, timeout: int):
    hosts = get_kernel_issues(conn)
    if not hosts:
        print("No servers with kernel issues.")
        return

    print(f"Found {len(hosts)} server(s) with kernel issues.\n")

    results: dict[str, str] = {}
    threads: list[threading.Thread] = []

    for hostname, needs_reboot, available in hosts:
        parts = []
        if needs_reboot:
            parts.append("newer kernel installed (reboot required)")
        if available:
            parts.append("kernel update available in apt")
        print(f"{hostname}: {', '.join(parts)}")
        answer = input("  Reboot (N/y)? ").strip().lower()
        if answer == 'y':
            try:
                send_reboot(hostname, account, keyfile, timeout)
                print(f"  Reboot sent. Monitoring in background...")
                update_tracker_logger.info(f"Reboot sent to {hostname}")
                t = threading.Thread(
                    target=monitor_reboot,
                    args=(hostname, account, keyfile, results),
                    daemon=True,
                )
                t.start()
                threads.append(t)
            except Exception as e:
                print(f"  Failed to send reboot: {e}")
                update_tracker_logger.error(f"Failed to reboot {hostname}: {e}")
        else:
            print("  Skipped.")

    if threads:
        print(f"\nWaiting for {len(threads)} server(s) to come back online "
              f"(timeout: {REBOOT_TIMEOUT}s)...")
        for t in threads:
            t.join()

        print("\nReboot results:")
        for hostname, status in results.items():
            if status == 'ok':
                print(f"  {hostname}: back online")
            else:
                print(f"  {hostname}: ERROR - did not respond within {REBOOT_TIMEOUT}s")


_CONFFILE_PROMPT = re.compile(r'\*\*\* (\S+) \(Y/I/N/O/D/Z\)')
_CONFIG_FILE_RE = re.compile(r"Configuration file '(.+?)'")
# Queue item: (hostname, conffile_path, response_event, response_holder)
ConffilePrompt = tuple[str, str, threading.Event, list]


def run_apt_upgrade(hostname: str, account: str, keyfile: Path, timeout: int,
                    conffile_choices: dict[str, str] | None = None,
                    prompt_queue: queue.Queue | None = None) -> tuple[bool, str]:
    """Run apt-get update (subprocess.run) then apt-get -y upgrade (Popen).

    When a dpkg conffile prompt appears:
    - If conffile_choices has a stored answer, use it automatically.
    - Otherwise, put a ConffilePrompt on prompt_queue and block until the
      main thread answers (or respond N if no queue is provided).
    Returns (success, message).
    """
    # Step 1: refresh package cache
    update_result = subprocess.run(
        ['ssh'] + _ssh_opts(keyfile, timeout) + [
            f'{account}@{hostname}',
            'DEBIAN_FRONTEND=noninteractive /usr/bin/sudo apt-get update -qq',
        ],
        capture_output=True, text=True, timeout=timeout + 5,
    )
    if update_result.returncode != 0:
        detail = update_result.stderr.strip() or f"exit {update_result.returncode}"
        return False, f"apt-get update failed: {detail}"

    # Step 2: upgrade packages, streaming output via Popen
    proc = subprocess.Popen(
        ['ssh'] + _ssh_opts(keyfile, timeout) + [
            f'{account}@{hostname}',
            'DEBIAN_FRONTEND=noninteractive /usr/bin/sudo apt-get -y upgrade --allow-downgrades',
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    output_lines: list[str] = []
    buf = ''
    last_conffile_path: str | None = None
    fd = proc.stdout.fileno()
    try:
        deadline = time.monotonic() + APT_UPGRADE_TIMEOUT
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(proc.args, APT_UPGRADE_TIMEOUT)
            ready, _, _ = select.select([fd], [], [], min(remaining, 1.0))
            if ready:
                chunk = os.read(fd, 4096).decode('utf-8', errors='replace')
                if not chunk:
                    break  # EOF
                buf += chunk
                # Flush complete lines
                while '\n' in buf:
                    line, buf = buf.split('\n', 1)
                    line += '\n'
                    output_lines.append(line)
                    update_tracker_logger.debug(f"{hostname}: {line.rstrip()}")
                    cf = _CONFIG_FILE_RE.search(line)
                    if cf:
                        last_conffile_path = cf.group(1)
                # Check if the partial buffer (no newline) is a conffile prompt
                m = _CONFFILE_PROMPT.search(buf)
                if m:
                    conffile_path = last_conffile_path or m.group(1)
                    response = 'N'  # default: keep old
                    if conffile_choices and conffile_path in conffile_choices:
                        response = 'Y' if conffile_choices[conffile_path] == 'new' else 'N'
                        update_tracker_logger.debug(
                            f"{hostname}: conffile {conffile_path} auto-responding {response}")
                    elif prompt_queue is not None:
                        event: threading.Event = threading.Event()
                        holder: list = ['N']
                        prompt_queue.put((hostname, conffile_path, event, holder))
                        event.wait()  # block thread until main thread answers
                        response = holder[0]
                    else:
                        update_tracker_logger.debug(
                            f"{hostname}: conffile {conffile_path} no queue, defaulting N")
                    output_lines.append(buf + response + '\n')
                    buf = ''
                    last_conffile_path = None
                    proc.stdin.write(response + '\n')
                    proc.stdin.flush()
            elif proc.poll() is not None:
                break  # process exited, no more output
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        return False, f"timed out after {APT_UPGRADE_TIMEOUT}s"
    finally:
        proc.stdin.close()

    combined = ''.join(output_lines)
    if proc.returncode == 0:
        if 'kept back' in combined or 'not upgraded' in combined:
            return True, "done (some packages kept back — may require manual upgrade)"
        return True, "done"

    for pat in _MANUAL_INTERVENTION_PATTERNS:
        if pat in combined:
            return False, f"requires manual intervention: {combined.strip()[-500:]}"

    return False, combined.strip()[-500:] or f"exit code {proc.returncode}"


def find_dpkg_new_files(hostname: str, account: str, keyfile: Path, timeout: int) -> list[str]:
    """Return list of .dpkg-new paths on the remote host (conffile conflicts)."""
    result = subprocess.run(
        ['ssh'] + _ssh_opts(keyfile, timeout) + [
            f'{account}@{hostname}',
            'find /etc /usr/share -name "*.dpkg-new" 2>/dev/null'
        ],
        capture_output=True, text=True, timeout=30
    )
    return [f.strip() for f in result.stdout.splitlines() if f.strip()]


def save_conffile_choice(conn: psycopg.Connection, hostname: str, conffile: str, choice: str):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO audit.conffile_choices (hostname, conffile, choice, recorded_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (hostname, conffile) DO UPDATE SET
            choice      = EXCLUDED.choice,
            recorded_at = EXCLUDED.recorded_at
    ''', (hostname, conffile, choice, datetime.datetime.now(datetime.timezone.utc)))
    conn.commit()


def get_conffile_choices(conn: psycopg.Connection, hostname: str) -> dict[str, str]:
    cursor = conn.cursor()
    cursor.execute('SELECT conffile, choice FROM audit.conffile_choices WHERE hostname = %s', (hostname,))
    return {row[0]: row[1] for row in cursor.fetchall()}


def apply_conffile_choices_remote(hostname: str, account: str, keyfile: Path,
                                   timeout: int, choices: dict[str, str]) -> tuple[bool, str]:
    """Apply stored conffile choices on the remote host by moving or removing .dpkg-new files."""
    cmds = []
    for conffile, choice in choices.items():
        dpkg_new = f'{conffile}.dpkg-new'
        if choice == 'new':
            cmds.append(f'[ -f "{dpkg_new}" ] && /usr/bin/sudo mv "{dpkg_new}" "{conffile}" || true')
        else:
            cmds.append(f'/usr/bin/sudo rm -f "{dpkg_new}"')
    result = subprocess.run(
        ['ssh'] + _ssh_opts(keyfile, timeout) + [f'{account}@{hostname}', '; '.join(cmds)],
        capture_output=True, text=True, timeout=60
    )
    if result.returncode != 0:
        return False, result.stderr.strip()
    return True, "ok"


def do_update(conn: psycopg.Connection, account: str, keyfile: Path, timeout: int,
              host_spec:HostSpec):
    from update_tracker.database import report as db_report
    issues = db_report(conn, host_spec)

    never = set(issues.never_updated)
    old = {h: d for h, _, d in issues.update_old}
    hosts = sorted(never | old.keys())

    if not hosts:
        print("No servers with outdated updates.")
        return

    # Prompt all hosts first
    print(f"Found {len(hosts)} server(s) needing updates.\n")
    if host_spec.only_these:
        hosts_to_update = host_spec.only_these
    else:
        hosts_to_update = []
        for hostname in hosts:
            if hostname in never:
                print(f"{hostname}: never updated")
            else:
                print(f"{hostname}: last updated {old[hostname]} days ago")
            answer = input("  Update (N//q/y)? ").strip().lower()
            if answer == 'y':
                hosts_to_update.append(hostname)
            elif answer == 'q':
                break
            else:
                print("  Skipped.")

    if not hosts_to_update:
        return

    # Run all upgrades concurrently, servicing conffile prompts on the main thread
    print(f"\nRunning updates on {len(hosts_to_update)} server(s)...")
    prompt_queue: queue.Queue = queue.Queue()
    futures: dict[concurrent.futures.Future, str] = {}
    results: dict[str, tuple[bool, str]] = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for hostname in hosts_to_update:
            f = executor.submit(run_apt_upgrade, hostname, account, keyfile, timeout,
                                None, prompt_queue)
            futures[f] = hostname

        pending = set(futures.keys())
        while pending:
            # Service any conffile prompts from worker threads
            while True:
                try:
                    hostname, conffile_path, event, holder = prompt_queue.get_nowait()
                    print(f"\n{hostname}: conffile prompt — {conffile_path}")
                    ans = input("  Keep old (O) or install new (N)? [O/n]: ").strip().lower()
                    choice = 'new' if ans == 'n' else 'old'
                    save_conffile_choice(conn, hostname, conffile_path, choice)
                    holder[0] = 'Y' if choice == 'new' else 'N'
                    event.set()
                    print(f"  Stored: {choice}")
                except queue.Empty:
                    break

            # Check for completed futures (short timeout to stay responsive to prompts)
            done, pending = concurrent.futures.wait(pending, timeout=0.5)
            for future in done:
                hostname = futures[future]
                try:
                    success, msg = future.result()
                    results[hostname] = (success, msg)
                    print(f"  {hostname}: {'done' if success else f'FAILED: {msg}'}")
                    if success:
                        update_tracker_logger.info(f"{hostname}: apt upgrade: {msg}")
                    else:
                        update_tracker_logger.error(f"{hostname}: apt upgrade failed: {msg}")
                except Exception as e:
                    results[hostname] = (False, str(e))
                    print(f"  {hostname}: FAILED: {e}")
                    update_tracker_logger.error(f"{hostname}: apt upgrade exception: {e}")


def do_apply(conn: psycopg.Connection, account: str, keyfile: Path, timeout: int):
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT hostname FROM audit.conffile_choices ORDER BY hostname')
    hosts = [row[0] for row in cursor.fetchall()]

    if not hosts:
        print("No stored conffile choices.")
        return

    for hostname in hosts:
        choices = get_conffile_choices(conn, hostname)
        print(f"\n{hostname}: {len(choices)} stored conffile choice(s)")
        for conffile, choice in sorted(choices.items()):
            print(f"  {conffile}: {choice}")

        answer = input("  Apply choices and re-run upgrade (N/y)? ").strip().lower()
        if answer != 'y':
            print("  Skipped.")
            continue

        print("  Applying conffile choices...", end=' ', flush=True)
        ok, msg = apply_conffile_choices_remote(hostname, account, keyfile, timeout, choices)
        print("done." if ok else f"FAILED: {msg}")
        if not ok:
            continue

        print("  Running apt-get upgrade...", end=' ', flush=True)
        try:
            success, msg = run_apt_upgrade(hostname, account, keyfile, timeout, choices)
            print(msg)
            if success:
                update_tracker_logger.info(f"{hostname}: apply upgrade: {msg}")
                cursor.execute('DELETE FROM audit.conffile_choices WHERE hostname = %s', (hostname,))
                conn.commit()
            else:
                update_tracker_logger.error(f"{hostname}: apply upgrade failed: {msg}")
        except subprocess.TimeoutExpired:
            print(f"FAILED: timed out after {APT_UPGRADE_TIMEOUT}s")
            update_tracker_logger.error(f"{hostname}: apply upgrade timed out")
        except Exception as e:
            print(f"FAILED: {e}")
            update_tracker_logger.error(f"{hostname}: apply upgrade exception: {e}")


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('action', choices=['reboot', 'update', 'kernel', 'apply'],
                        help="Action to perform")
    add_common_args(parser)
    parser.add_argument('-s', '--server', action='append', help="limit to just these servers")

    args = parser.parse_args()
    setup_logging(args)
    config = load_config(args)
    a = config['ansible']
    c = config['cutoffs']
    timeout = c['ssh seconds']

    conn = postgres_connect(config)

    if args.action in ('kernel', 'update', 'apply'):
        inv = query_ansible(a['config'], a['inventory'])

    if args.action == 'kernel':
        do_kernel(conn, inv.account, inv.keyfile, timeout)
    elif args.action == 'update':
        host_spec = HostSpec(args.server, build_host_limits(config))
        do_update(conn, inv.account, inv.keyfile, timeout, host_spec)
    elif args.action == 'apply':
        do_apply(conn, inv.account, inv.keyfile, timeout)
    elif args.action == 'reboot':
        print("reboot action not yet implemented")

    conn.close()


if __name__ == "__main__":
    main()
