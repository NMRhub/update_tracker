from pathlib import Path
from unittest import TestCase

from update_tracker.query import query_ansible


class Test(TestCase):
    def test_running_virtual(self):
        c = Path('/etc/ansible/ansible.cfg')
        assert c.is_file()
        assert c.exists()
        inv = query_ansible(c,('production','virtual_running'))
        print(inv)