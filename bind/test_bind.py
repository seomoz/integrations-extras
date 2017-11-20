# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
from nose.plugins.attrib import attr
import xml.etree.ElementTree as ET
import os

# 3p
import dns.rdata
import dns.rdataclass
import dns.rdatatype

# project
from tests.checks.common import AgentCheckTest

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

def _load_stats_xml(name="stats_v3_0.xml"):
    return ET.parse("{}/{}".format(CI_DIR, name))

@attr(requires='bind')
class TestBindMetrics(AgentCheckTest):
    """Metric Tests for bind integration."""
    CHECK_NAME = 'bind'

    def mock_get_statistics0(self, instance):
        return _load_stats_xml()

    def mock_get_statistics1(self, instance):
        return _load_stats_xml("stats_v3_1.xml")

    def mock_get_dns(self, q, rr, resolv='127.0.0.1'):
        VALUES = {
            "127.0.0.1": {
                "dal.moz.com": {
                    "SOA" : dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA, 'ns1.dal.moz.com. hostmaster.moz.com. 2016080801 3600 1800 604800 3600')
                },
                "ns1.dal.moz.com.": {
                    "A": dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A, '1.2.3.4')
                },
                "tuk.moz.com": {
                    "SOA" : dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA, 'ns1.tuk.moz.com. hostmaster.moz.com. 2016080801 3600 1800 604800 3600')
                },
                "ns1.tuk.moz.com.": {
                    "A": dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.A, '2.2.3.4')
                }
            },
            "1.2.3.4": {
                "dal.moz.com": {
                    "SOA" : dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA, 'ns1.dal.moz.com. hostmaster.moz.com. 2016080802 3600 1800 604800 3600')
                }
            },
            "2.2.3.4": {
                "tuk.moz.com": {
                    "SOA" : dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.SOA, 'ns1.tuk.moz.com. hostmaster.moz.com. 2016080801 3600 1800 604800 3600')
                }
            }
        }
        return VALUES[resolv][q][rr]

    def _get_bind_mocks(self):
        return {
            '_get_statistics': self.mock_get_statistics0,
            '_get_dns': self.mock_get_dns
        }

    def setUp(self):
        my_mocks = self._get_bind_mocks()
        self.run_check(MOCK_CONFIG, MOCK_AGENT_CONFIG, mocks=my_mocks)
        my_mocks['_get_statistics'] = self.mock_get_statistics1
        self.run_check(MOCK_CONFIG, MOCK_AGENT_CONFIG, mocks=my_mocks)

    def test_service_check(self):
        self.assertServiceCheckOK('bind.can_connect')

    def test_dns_zone_check(self):
        self.assertServiceCheckOK('bind.zone_sync', ['zone:tuk.moz.com'])
        self.assertServiceCheckCritical('bind.zone_sync', ['zone:dal.moz.com'])

    def test_queries_in(self):
        self.assertMetric('bind.queries.in', 3, ['rdtype:soa'])

    def test_queries_out(self):
        self.assertMetric('bind.queries.out', 1, ['rdtype:aaaa'])

    def test_opcode_in(self):
        self.assertMetric('bind.opcode.in', 0)
