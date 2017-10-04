# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
from nose.plugins.attrib import attr
import os

# 3p

# project
from tests.checks.common import AgentCheckTest

CI_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "ci")


def merge_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z

MOCK_CONFIG = {
    'init_config': {},
    'instances': [{
        'name': 'test_success',
        'file': "{}/{}".format(CI_DIR, 'pass.txt'),
        'search_string': 'Success'
    }, {
        'name': 'test_failure',
        'file': "{}/{}".format(CI_DIR, 'fail.txt'),
        'search_string': 'CannotSuccess'
    }, {
        'name': 'test_missing',
        'file': "{}/{}".format(CI_DIR, 'notfound.txt'),
        'search_string': 'Success'
    }
    ]
}

MOCK_AGENT_CONFIG = merge_dicts(AgentCheckTest.DEFAULT_AGENT_CONFIG, {
    'hostname': 'test.local'
})


@attr(requires='file_grep')
class TestFileGrep(AgentCheckTest):
    """Basic Test for file_grep integration."""
    CHECK_NAME = 'file_grep'

    def test_check(self):
        """
        Testing FileGrep check.
        """
        self.run_check(MOCK_CONFIG, MOCK_AGENT_CONFIG)

        self.assertServiceCheckOK('file_grep.match_ok',
                                  tags=['check_name:test_success'])
        self.assertServiceCheckCritical('file_grep.match_ok',
                                  tags=['check_name:test_failure'])
        self.assertServiceCheckUnknown('file_grep.match_ok',
                                  tags=['check_name:test_missing'])

        # Raises when COVERAGE=true and coverage < 100%
        self.coverage_report()
