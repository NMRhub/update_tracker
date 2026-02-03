import configparser
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager
from ansible import context
from ansible.module_utils.common.collections import ImmutableDict
from ansible.inventory.host import Host


@dataclass
class AnsibleInfo:
    account: str
    keyfile: Path
    inventory: list[str]

def query_ansible(config: Path, names: Iterable[str]) -> AnsibleInfo:
    """
    List hosts for the given inventory/group name using Ansible's Python API.

    Args:
        config: Full path to ansible.cfg file
        names: The inventory or group name to query
    """
    #os.environ['ANSIBLE_CONFIG'] = str(config)

    # Read the ansible.cfg to get configuration values
    ansible_config = configparser.ConfigParser()
    ansible_config.read(config)

    inventory_path = ansible_config.get('defaults', 'inventory', fallback='/etc/ansible/hosts')

    inventory_path = Path(inventory_path)
    if not inventory_path.is_absolute():
        inventory_path = config.parent / inventory_path

    context.CLIARGS = ImmutableDict(
        connection='local',
        module_path=None,
        forks=10,
        become=None,
        become_method=None,
        become_user=None,
        check=False,
        diff=False,
        verbosity=0
    )

    loader = DataLoader()
    inventory = InventoryManager(loader=loader, sources=str(inventory_path))
    variable_manager = VariableManager(loader=loader, inventory=inventory)

    combined = {}
    for name in names:
        hdata: Host
        for hdata in inventory.get_hosts(pattern=name):

            combined[hdata.name] = hdata

    host_list = list(combined.keys())

    remote_user = ansible_config.get('defaults', 'remote_user', fallback='root')
    private_key_file = ansible_config.get('defaults', 'private_key_file', fallback='')

    if host_list:
        sample = combined[host_list[0]]
        host_vars = variable_manager.get_vars(host=sample)
        remote_user = host_vars.get('ansible_user') or host_vars.get('ansible_ssh_user') or remote_user
        private_key_file = host_vars.get('ansible_ssh_private_key_file') or private_key_file

    if private_key_file:
        private_key_path = Path(private_key_file)
        if not private_key_path.is_absolute():
            private_key_path = config.parent / private_key_file
    else:
        private_key_path = Path()

    return AnsibleInfo(
        account=remote_user,
        keyfile=private_key_path,
        inventory=host_list
    )
