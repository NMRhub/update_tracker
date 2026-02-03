#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Test script for get_last function."""

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from last_update import get_last

@dataclass
class Sdata:
    account: str
    keyfile: Path

def main():
    """Test the get_last function with command line arguments."""
    parser = argparse.ArgumentParser(
        description='Test get_last function to retrieve apt upgrade and reboot times'
    )
    parser.add_argument(
        'hostname',
        help='Remote hostname to connect to'
    )
    parser.add_argument(
        'account',
        help='SSH account/username'
    )
    parser.add_argument(
        'keyfile',
        type=Path,
        help='Path to SSH private key file'
    )

    args = parser.parse_args()

    # Validate keyfile exists
    if not args.keyfile.exists():
        print(f"Error: SSH key file not found: {args.keyfile}", file=sys.stderr)
        sys.exit(1)

    print(f"Connecting to {args.account}@{args.hostname} with key {args.keyfile}")
    print("-" * 60)

    try:
        sd = Sdata(args.account,Path(args.keyfile))
        result = get_last(args.hostname, sd)
        print(f"Last apt upgrade: {result.update}")
        print(f"Uptime:      {result.uptime}")
        print("-" * 60)
        print("Success!")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
