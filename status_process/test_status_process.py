# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
from nose.plugins.attrib import attr

# 3p

# project
from tests.checks.common import AgentCheckTest

def merge_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z

MOCK_CONFIG = {
    'init_config': {},
    'instances' : [{
        'check_name': 'true.passes',
        'command': 'true'
    }, {
        'check_name': 'false.fails',
        'command': 'false'
    }, {
        'check_name': 'false.passes.with.rc1',
        'command': 'false',
        'return_codes': [0, 1]
    }, {
        'check_name': 'false.timeout',
        'command': '/bin/sleep 10'
    }
    ]
}

MOCK_AGENT_CONFIG = merge_dicts(AgentCheckTest.DEFAULT_AGENT_CONFIG, {
    'hostname': 'test.local'
})


# NOTE: Feel free to declare multiple test classes if needed

@attr(requires='status_process')
class TestStatusProcess(AgentCheckTest):
    """Basic Test for status_process integration."""
    CHECK_NAME = 'status_process'

    def test_check(self):
        """
        Testing StatusProcess check.
        """
        self.run_check(MOCK_CONFIG, MOCK_AGENT_CONFIG)
        self.assertServiceCheckOK('status_process.ok',
                                  tags=['check_name:true.passes'])
        self.assertServiceCheckCritical('status_process.ok',
                                  tags=['check_name:false.fails'])
        self.assertServiceCheckOK('status_process.ok',
                                  tags=['check_name:false.passes.with.rc1'])
        self.assertServiceCheckWarning('status_process.ok',
                                  tags=['check_name:false.timeout', 'timeout'])
        self.coverage_report()
