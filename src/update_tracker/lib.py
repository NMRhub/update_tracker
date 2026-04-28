import argparse
import logging

import yaml

from update_tracker import query_ansible, update_tracker_logger


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add logging and yaml arguments"""
    parser.add_argument('-l', '--loglevel', default='WARN', help="Python logging level")
    parser.add_argument('--yaml', default="/etc/nmrhub.d/update_tracker.yaml",
                        help="YAML configuration file")


def setup_logging(args: argparse.Namespace) -> None:
    """Setup basic logging"""
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True )
    update_tracker_logger.setLevel(getattr(logging, args.loglevel),)


def load_config(args: argparse.Namespace) -> dict:
    """Reed args.yaml"""
    with open(args.yaml) as f:
        return yaml.safe_load(f)


def build_host_limits(config: dict) -> dict[str, int]:
    """Read config for host limits"""
    a = config['ansible']
    c = config['cutoffs']
    host_limits: dict[str, int] = {}
    for inv_name in a['inventory']:
        inv_limits = c[inv_name]
        update_days = inv_limits['update days']
        group_inv = query_ansible(a['config'], [inv_name])
        for host in group_inv.inventory:
            if host in host_limits:
                host_limits[host] = max(host_limits[host], update_days)
            else:
                host_limits[host] = update_days
    return host_limits
