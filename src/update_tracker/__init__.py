
import importlib.metadata 
import logging
from pathlib import Path
from typing import Protocol

update_tracker_logger = logging.getLogger(__name__)

__version__ =  importlib.metadata.version('update_tracker')

class SshUser(Protocol):
    account: str
    keyfile: Path
