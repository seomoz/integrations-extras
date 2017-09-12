# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
import copy
import logging

# 3rd party
import requests

# project
from checks import AgentCheck
from utils.containers import hash_mutable

EVENT_TYPE = SOURCE_TYPE_NAME = 'bind'

class BindCheckInstanceState(object):
    def __init__(self):
        self.statistics = None

class BindCheck(AgentCheck):

    BIND_CHECK = 'bind.up'
    SOURCE_TYPE_NAME = 'bind'

    _log = logging.getLogger("BindCheck")

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

        self._instance_states = defaultdict(lambda: BindCheckInstanceState())

    def check(self, instance):
        self.log.debug('starting check run')
        # Instance state is mutable, any changes to it will be reflected in self._instance_states
        state = self._instance_states[hash_mutable(instance)]
        prev_state = copy.deepcopy(state)

        self.log.debug(prev_state.statistics)

        state.statistics = self._get_statistics(instance)

        if prev_state.statistics is not None:
            self.log.debug('running serial change detection')
            self._generate_events_serial_change(BindCheck.changed_serials(prev_state, state))

        # save changed state
        self._instance_states[hash_mutable(instance)] = state
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
