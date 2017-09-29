# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
import copy
import logging
import re

# 3rd party
import requests

# project
from checks import AgentCheck
from utils.containers import hash_mutable

EVENT_TYPE = SOURCE_TYPE_NAME = 'bind'
PROCESS_NAME = "named"

STATS_BY_NAME = [
    ('bind.network', 'bind/statistics/server/nsstat'),
    ('bind.resolver', "bind/statistics/views/view[name='_default']/resstat"),
    ('bind.socket', 'bind/statistics/server/sockstat')
]

STATS_BY_TAG = [
    ('bind.queries.in', 'rdtype', 'bind/statistics/server/queries-in/rdtype'),
    ('bind.queries.out', 'rdtype', "bind/statistics/views/view[name='_default']/rdtype"),
    ('bind.opcode.in', 'opcode', 'bind/statistics/server/requests/opcode'),
    ('bind.cachedb', 'rrset', "bind/statistics/views/view[name='_default']/cache[name='_default']/rrset"),
]

RE1 = re.compile('[Ff][Dd][Ww]atch')
RE2 = re.compile('(.)([A-Z][a-z]+)')
RE3 = re.compile('([a-z0-9])([A-Z])')

def convert(name):
    s1 = RE1.sub('FDWatch', name)
    s2 = RE2.sub(r'\1_\2', s1)
    return RE3.sub(r'\1_\2', s2).lower()

class BindCheckInstanceState(object):
    def __init__(self):
        self.statistics = None

class BindCheck(AgentCheck):

    SOURCE_TYPE_NAME = 'bind'
    SERVICE_CHECK_NAME = 'bind.can_connect'

    _log = logging.getLogger("BindCheck")

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

        self._instance_states = defaultdict(lambda: BindCheckInstanceState())

    def check(self, instance):
        self.log.debug('starting check run')

        try:
            # Instance state is mutable, any changes to it will be reflected in self._instance_states
            state = self._instance_states[hash_mutable(instance)]
            prev_state = copy.deepcopy(state)

            state.statistics = self._get_statistics(instance)

            if prev_state.statistics is not None:
                self.log.debug('running serial change detection')
                self._generate_events_serial_change(BindCheck.changed_serials(prev_state, state))

                for i in STATS_BY_TAG:
                    self._generate_counters_by_tags(prev_state, state, i[0], i[1], i[2])

                for i in STATS_BY_NAME:
                    self._generate_counters_by_name(prev_state, state, i[0], i[1])

            # save changed state
            self._instance_states[hash_mutable(instance)] = state
        except requests.exceptions.Timeout:
            self.service_check(
                self.SERVICE_CHECK_NAME,
                AgentCheck.CRITICAL,
                message="Timeout hitting %s." % (self._url(instance))
            )

        except Exception as e:
            self.service_check(
                self.SERVICE_CHECK_NAME,
                AgentCheck.CRITICAL,
                message="Error hitting %s. Error: %s" % (self._url(instance), e.message)
            )

        self.service_check(
            self.SERVICE_CHECK_NAME,
            AgentCheck.OK
        )

        self.log.debug('finishing check run')

    def _url(self, instance):
        return "http://{}:{}/".format(instance.get('host', 'localhost'), instance.get('port', 8053))

    def _get_statistics(self, instance):
        url = self._url(instance)
        try:
            resp = requests.get(url)
        except requests.exceptions.Timeout:
            self.log.exception('BIND/named request to {} timed out.'.format(url))
            raise
        resp.raise_for_status()
        return ET.fromstring(resp.content)

    def _generate_events_serial_change(self, changes):
        self.log.debug('Entering _generate_events_serial_change: {}'.format(changes))
        return map(lambda chg: self._serial_change_event(chg[0], chg[1]), changes.items())

    def _serial_change_event(self, zone, serials):
        self.log.debug('generating serial change event: {}, {}'.format(zone, serials))
        if len(serials) < 1 or len(serials) > 2:
            self.log.warning("Unexpected number of serial numbers {0} for {1}".format(len(serials), zone))
            return

        elif len(serials) == 1:
            txt = "Zone {0} created or removed (serial {1})".format(zone, serials[0])
        else:
            txt = "Zone {0} updated (serial {2} -> {1})".format(zone, serials[0], serials[1])

        self.event({
            "timestamp": int(datetime.now().strftime("%s")),
            "event_type": "bind.serial_change",
            "source_type_name": self.SOURCE_TYPE_NAME,
            "msg_text": txt,
            "aggregation_key": "bind.serial_change",
            "msg_title": "BIND/named zone modification detected",
            "tags": ["bind_zone:{0}".format(zone)]
        })

    def _generate_counters_by_tags(self, before, after, metric_name, tag, xpath):
        counter = self._get_counter_diffs(before, after, xpath)
        for k, v in counter.iteritems():
            self.count(metric_name, v, ['{}:{}'.format(tag, k)])

    def _generate_counters_by_name(self, before, after, prefix, xpath):
        counter = self._get_counter_diffs(before, after, xpath)
        for k, v in counter.iteritems():
            self.count('{}.{}'.format(prefix, convert(k)), v)

    def _get_counter_diffs(self, before, after, xpath):
        result = {}
        c1 = self._get_counters(before, xpath)
        c2 = self._get_counters(after, xpath)
        for k, v in c2.iteritems():
            result[k] = v - c1[k]
        return result

    def _get_counters(self, state, xpath):
        self.log.debug('Entering _get_counters')
        result = {}
        nodes = state.statistics.findall(xpath)
        for node in nodes:
            result[node.findtext('name')] = int(node.findtext('counter'))
        self.log.debug('Exiting _get_counters')
        return result

    @classmethod
    def extract_serials(cls, state):
        cls._log.debug('Entered cls.extract_serials')
        serials = defaultdict(lambda: 0)
        stats = state.statistics
        zones = stats.find(".//views/view[name='_default']/zones")
        for zone in zones:
            serials[zone.findtext('name')] = zone.findtext('serial')
        cls._log.debug('Exited cls.extract_serials: {}'.format(serials))
        return serials

    @classmethod
    def changed_serials(cls, prev_state, new_state):
        cls._log.debug('Entered cls.changed_serials')
        result = defaultdict(lambda: [])
        changes = set(cls.extract_serials(prev_state).items()) ^ set(cls.extract_serials(new_state).items())
        cls._log.debug(changes)
        for zone, serial in changes:
            result[zone].append(serial)
        cls._log.debug('Exited cls.changed_serials: {}'.format(result))
        return result
