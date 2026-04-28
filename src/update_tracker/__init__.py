import dataclasses
import importlib.metadata
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

update_tracker_logger = logging.getLogger(__name__)

__version__ =  importlib.metadata.version('update_tracker')
from update_tracker.db import postgres_connect

class SshUser(Protocol):
    account: str
    keyfile: Path

from update_tracker.query import query_ansible

HostLimit = dict[str, int]


@dataclass
class HostSpec:
    only_these : list[str] = dataclasses.field(default_factory=list)
    host_limits : HostLimit = dataclasses.field(default_factory=dict)

    def filter(self,hostname:str)->bool:
        return self.only_these is None or len(self.only_these) == 0 or hostname in self.only_these

from update_tracker.lib import add_common_args, setup_logging, load_config, build_host_limits
