# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import os
import sys

# project
from checks import AgentCheck

# in python2, use googles subprocess32 library to enable timeouts on
# subprocesses
if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

EVENT_TYPE = SOURCE_TYPE_NAME = 'status_process'

class StatusProcessCheck(AgentCheck):
    HEALTH_CHECK = 'status_process.ok'

    def check(self, instance):
        name = instance.get('check_name', None)
        tags = ["check_name:{}".format(name)] + instance.get('tags', [])
        cmd = instance.get('command', None)
        valid_rc = instance.get('return_codes', [0])

        rc = self._call(cmd)
        if rc in valid_rc:
            # OK
            self.service_check(self.HEALTH_CHECK, AgentCheck.OK,
                    tags=tags)
        elif rc is None:
            # Timeout
            self.service_check(self.HEALTH_CHECK, AgentCheck.WARNING,
                    tags=tags + ['timeout'])
        else:
            # Bad
            self.service_check(self.HEALTH_CHECK, AgentCheck.CRITICAL,
                    tags=tags)

    def _call(self, cmd):
        try:
            return subprocess.call(cmd, timeout=5, shell=True)
        except subprocess.TimeoutExpired:
            return None
        except:
            raise
