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

CI_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "ci")

def _load_stats_xml(name="stats.xml"):
    return ET.parse("{}/{}".format(CI_DIR, name))


@attr(requires='bind')
class TestBind(AgentCheckTest):
    """Basic Test for bind integration."""
    CHECK_NAME = 'bind'

    def mock_get_statistics(self, instance):
        return _load_stats_xml()

    def mock_get_prev_statistics(self, instance):
        return _load_stats_xml('stats_prev.xml')

    def _get_bind_mocks(self):
        return {
            '_get_statistics': self.mock_get_statistics
        }

    def test_extract_serials(self):
        self.load_check(MOCK_CONFIG, MOCK_AGENT_CONFIG)
        instance_hash = hash_mutable(MOCK_CONFIG['instances'][0])
        self.check._instance_states[instance_hash].statistics = _load_stats_xml()
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


@attr(requires='bind')
class TestBindMetrics(AgentCheckTest):
    """Metric Tests for bind integration."""
    CHECK_NAME = 'bind'

    def mock_get_statistics(self, instance):
        return _load_stats_xml()

    def mock_get_prev_statistics(self, instance):
        return _load_stats_xml('stats_prev.xml')

    def _get_bind_mocks(self):
        return {
            '_get_statistics': self.mock_get_statistics
        }

    def setUp(self):
        my_mocks = self._get_bind_mocks()
        my_mocks['_get_statistics'] = self.mock_get_prev_statistics
        self.run_check(MOCK_CONFIG, MOCK_AGENT_CONFIG, mocks=my_mocks)

        my_mocks['_get_statistics'] = self.mock_get_statistics
        self.run_check(MOCK_CONFIG, MOCK_AGENT_CONFIG, mocks=my_mocks)

    def test_event(self):
        self.assertEvent("Zone dc.example.com/IN updated (serial 2017091002 -> 2017091001)",
                event_type = "bind.serial_change")

    def test_service_check(self):
        self.assertServiceCheckOK('bind.can_connect')

    def test_queries_in(self):
        self.assertMetric('bind.queries.in', 10, ['rdtype:A'])
        self.assertMetric('bind.queries.in', 0, ['rdtype:NS'])
        self.assertMetric('bind.queries.in', 0, ['rdtype:SOA'])
        self.assertMetric('bind.queries.in', 0, ['rdtype:PTR'])
        self.assertMetric('bind.queries.in', 0, ['rdtype:MX'])
        self.assertMetric('bind.queries.in', 0, ['rdtype:TXT'])
        self.assertMetric('bind.queries.in', 0, ['rdtype:AAAA'])
        self.assertMetric('bind.queries.in', 0, ['rdtype:SRV'])
        self.assertMetric('bind.queries.in', 0, ['rdtype:DS'])
        self.assertMetric('bind.queries.in', 0, ['rdtype:DNSKEY'])

    def test_network(self):
        self.assertMetric('bind.network.requestv4', 10)
        self.assertMetric('bind.network.requestv6', 0)
        self.assertMetric('bind.network.req_edns0', 0)
        self.assertMetric('bind.network.req_tcp', 0)
        self.assertMetric('bind.network.response', 0)
        self.assertMetric('bind.network.truncated_resp', 0)
        self.assertMetric('bind.network.resp_edns0', 0)
        self.assertMetric('bind.network.qry_success', 0)
        self.assertMetric('bind.network.qry_auth_ans', 0)
        self.assertMetric('bind.network.qry_noauth_ans', 0)
        self.assertMetric('bind.network.qry_referral', 0)
        self.assertMetric('bind.network.qry_nxrrset', 0)
        self.assertMetric('bind.network.qry_servfail', 0)
        self.assertMetric('bind.network.qry_formerr', 0)
        self.assertMetric('bind.network.qry_nxdomain', 0)
        self.assertMetric('bind.network.qry_recursion', 0)
        self.assertMetric('bind.network.qry_duplicate', 0)
        self.assertMetric('bind.network.qry_dropped', 0)
        self.assertMetric('bind.network.qry_failure', 0)

    def test_socket(self):
        self.assertMetric('bind.socket.udp4_open', 0)
        self.assertMetric('bind.socket.udp6_open', 0)
        self.assertMetric('bind.socket.tcp4_open', 0)
        self.assertMetric('bind.socket.tcp6_open', 0)
        self.assertMetric('bind.socket.unix_open', 0)
        self.assertMetric('bind.socket.udp4_open_fail', 0)
        self.assertMetric('bind.socket.udp6_open_fail', 0)
        self.assertMetric('bind.socket.tcp4_open_fail', 0)
        self.assertMetric('bind.socket.tcp6_open_fail', 0)
        self.assertMetric('bind.socket.unix_open_fail', 0)
        self.assertMetric('bind.socket.udp4_close', 0)
        self.assertMetric('bind.socket.udp6_close', 0)
        self.assertMetric('bind.socket.tcp4_close', 0)
        self.assertMetric('bind.socket.tcp6_close', 0)
        self.assertMetric('bind.socket.unix_close', 0)
        self.assertMetric('bind.socket.fd_watch_close', 0)
        self.assertMetric('bind.socket.udp4_bind_fail', 0)
        self.assertMetric('bind.socket.udp6_bind_fail', 0)
        self.assertMetric('bind.socket.tcp4_bind_fail', 0)
        self.assertMetric('bind.socket.tcp6_bind_fail', 0)
        self.assertMetric('bind.socket.unix_bind_fail', 0)
        self.assertMetric('bind.socket.fd_watch_bind_fail', 0)
        self.assertMetric('bind.socket.udp4_conn_fail', 0)
        self.assertMetric('bind.socket.udp6_conn_fail', 0)
        self.assertMetric('bind.socket.tcp4_conn_fail', 0)
        self.assertMetric('bind.socket.tcp6_conn_fail', 0)
        self.assertMetric('bind.socket.unix_conn_fail', 0)
        self.assertMetric('bind.socket.fd_watch_conn_fail', 0)
        self.assertMetric('bind.socket.udp4_conn', 0)
        self.assertMetric('bind.socket.udp6_conn', 0)
        self.assertMetric('bind.socket.tcp4_conn', 0)
        self.assertMetric('bind.socket.tcp6_conn', 0)
        self.assertMetric('bind.socket.unix_conn', 0)
        self.assertMetric('bind.socket.fd_watch_conn', 0)
        self.assertMetric('bind.socket.tcp4_accept_fail', 0)
        self.assertMetric('bind.socket.tcp6_accept_fail', 0)
        self.assertMetric('bind.socket.unix_accept', 0)
        self.assertMetric('bind.socket.udp4_send_err', 0)
        self.assertMetric('bind.socket.udp6_send_err', 0)
        self.assertMetric('bind.socket.tcp4_send_err', 0)
        self.assertMetric('bind.socket.tcp6_send_err', 0)
        self.assertMetric('bind.socket.unix_send_err', 0)
        self.assertMetric('bind.socket.fd_watch_send_err', 0)
        self.assertMetric('bind.socket.udp4_recv_err', 0)
        self.assertMetric('bind.socket.udp6_recv_err', 0)
        self.assertMetric('bind.socket.tcp4_recv_err', 0)
        self.assertMetric('bind.socket.tcp6_recv_err', 0)
        self.assertMetric('bind.socket.unix_recv_err', 0)
        self.assertMetric('bind.socket.fd_watch_recv_err', 0)

    def test_resolver(self):
        self.assertMetric('bind.resolver.edns0_fail', 0)
        self.assertMetric('bind.resolver.formerr', 0)
        self.assertMetric('bind.resolver.glue_fetchv4', 0)
        self.assertMetric('bind.resolver.glue_fetchv4_fail', 0)
        self.assertMetric('bind.resolver.glue_fetchv6', 0)
        self.assertMetric('bind.resolver.glue_fetchv6_fail', 0)
        self.assertMetric('bind.resolver.lame', 0)
        self.assertMetric('bind.resolver.mismatch', 0)
        self.assertMetric('bind.resolver.nxdomain', 0)
        self.assertMetric('bind.resolver.other_error', 0)
        self.assertMetric('bind.resolver.qry_rtt10', 0)
        self.assertMetric('bind.resolver.qry_rtt100', 0)
        self.assertMetric('bind.resolver.qry_rtt1600', 0)
        self.assertMetric('bind.resolver.qry_rtt1600+', 0)
        self.assertMetric('bind.resolver.qry_rtt500', 0)
        self.assertMetric('bind.resolver.qry_rtt800', 0)
        self.assertMetric('bind.resolver.query_abort', 0)
        self.assertMetric('bind.resolver.query_sock_fail', 0)
        self.assertMetric('bind.resolver.query_timeout', 0)
        self.assertMetric('bind.resolver.queryv4', 0)
        self.assertMetric('bind.resolver.queryv6', 0)
        self.assertMetric('bind.resolver.responsev4', 0)
        self.assertMetric('bind.resolver.responsev6', 0)
        self.assertMetric('bind.resolver.retry', 0)
        self.assertMetric('bind.resolver.servfail', 0)
        self.assertMetric('bind.resolver.truncated', 0)
        self.assertMetric('bind.resolver.val_attempt', 0)
        self.assertMetric('bind.resolver.val_fail', 0)
        self.assertMetric('bind.resolver.val_neg_ok', 0)
        self.assertMetric('bind.resolver.val_ok', 0)

    def test_queries_out(self):
        self.assertMetric('bind.queries.out', 0)

    def test_opcode_in(self):
        self.assertMetric('bind.opcode.in', 0)

    def test_cachedb(self):
        pass
