# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
from nose.plugins.attrib import attr
import xml.etree.ElementTree as ET
import os

# 3p

# project
from tests.checks.common import AgentCheckTest
from utils.containers import hash_mutable

def merge_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z

MOCK_CONFIG = {
    'init_config': {},
    'instances' : [{
        'host': 'localhost',
        'port': 8053,
    }]
}

MOCK_AGENT_CONFIG = merge_dicts(AgentCheckTest.DEFAULT_AGENT_CONFIG, {
    'hostname': 'test.local'
})

# NOTE: Feel free to declare multiple test classes if needed

@attr(requires='bind')
class TestBind(AgentCheckTest):
    """Basic Test for bind integration."""
    CHECK_NAME = 'bind'

    CI_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "ci")

    def _load_stats_xml(self, name="stats.xml"):
        return ET.parse("{}/{}".format(self.CI_DIR, name))

    def mock_get_statistics(self, instance):
        return self._load_stats_xml()

    def mock_get_prev_statistics(self, instance):
        return self._load_stats_xml('stats_prev.xml')

    def _get_bind_mocks(self):
        return {
            '_get_statistics': self.mock_get_statistics
        }

    def test_check(self):
        my_mocks = self._get_bind_mocks()
        my_mocks['_get_statistics'] = self.mock_get_prev_statistics
        self.run_check(MOCK_CONFIG, MOCK_AGENT_CONFIG, mocks=my_mocks)

        my_mocks['_get_statistics'] = self.mock_get_statistics
        self.run_check(MOCK_CONFIG, MOCK_AGENT_CONFIG, mocks=my_mocks)

        self.assertEvent("Zone dc.example.com/IN updated (serial 2017091002 -> 2017091001)",
                event_type = "bind.serial_change")

    def test_extract_serials(self):
        self.load_check(MOCK_CONFIG, MOCK_AGENT_CONFIG)
        instance_hash = hash_mutable(MOCK_CONFIG['instances'][0])
        self.check._instance_states[instance_hash].statistics = self._load_stats_xml()
        serials = self.check.extract_serials(self.check._instance_states[instance_hash])
        self.check.log.debug(serials)
        self.assertEqual(len(serials), 5)
        self.assertIn('corp.example.com/IN', serials.keys())

    def test_zone_change_event(self):
        self.load_check(MOCK_CONFIG, MOCK_AGENT_CONFIG)
        self.check._serial_change_event('corp.example.com/IN', [101, 100])
        self.events = self.check.get_events()
        self.assertEqual(len(self.events), 1)

        event = self.events[0]

        self.assertIn('bind_zone:corp.example.com/IN', event['tags'])
        self.assertEqual(event['event_type'], 'bind.serial_change')
        self.assertEqual(event['msg_text'], 'Zone corp.example.com/IN updated (serial 100 -> 101)')

    def test_zone_add_event(self):
        self.load_check(MOCK_CONFIG, MOCK_AGENT_CONFIG)
        self.check._serial_change_event('corp.example.com/IN', [101])
        self.events = self.check.get_events()
        self.assertEqual(len(self.events), 1)

        event = self.events[0]

        self.assertIn('bind_zone:corp.example.com/IN', event['tags'])
        self.assertEqual(event['event_type'], 'bind.serial_change')
        self.assertEqual(event['msg_text'], 'Zone corp.example.com/IN created or removed (serial 101)')
