# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import sys
import os
import xml.etree.ElementTree as ET
import logging
from string import upper, lower

# 3rd party
import requests
import dns.resolver

# project
from checks import AgentCheck

EVENT_TYPE = SOURCE_TYPE_NAME = 'bind'
PROCESS_NAME = "named"

RDTYPES = ['A', 'AAAA', 'ANY', 'AXFR', 'DNSKEY', 'DS', 'MX', 'NAPTR', 'NS', 'PTR', 'SOA', 'SRV', 'TXT']
SOCK_TYPES = ['UDP4', 'UDP6', 'TCP4', 'TCP6', 'Unix']
SOCK_METRIC_TYPES = ['Open','OpenFail','Close','BindFail','ConnFail','Conn','AcceptFail','Accept','SendErr','RecvErr']

resolver = dns.resolver.Resolver(configure=False)


class BindCheck(AgentCheck):

    SOURCE_TYPE_NAME = 'bind'
    SERVICE_CHECK_NAME = 'bind.can_connect'
    SERIAL_CHECK_NAME = 'bind.zone_sync'

    RATE = AgentCheck.rate
    GAUGE = AgentCheck.gauge
    MONOTONIC = AgentCheck.monotonic_count

    _log = logging.getLogger("BindCheck")

    COMMON_METRICS = [
        # Metrics Name, XPATH, tags, type
        ('bind.network.requestv4', "./server/counters[@type='nsstat']/counter[@name='Requestv4']", [], MONOTONIC),
        ('bind.network.requestv6', "./server/counters[@type='nsstat']/counter[@name='Requestv6']", [], MONOTONIC),
        ('bind.network.req_edns0', "./server/counters[@type='nsstat']/counter[@name='ReqEdns0']", [], MONOTONIC),
        ('bind.network.req_bad_edns_ver', "./server/counters[@type='nsstat']/counter[@name='ReqBadEDNSVer']", [], MONOTONIC),
        ('bind.network.req_tsig', "./server/counters[@type='nsstat']/counter[@name='ReqTSIG']", [], MONOTONIC),
        ('bind.network.req_sig0', "./server/counters[@type='nsstat']/counter[@name='ReqSIG0']", [], MONOTONIC),
        ('bind.network.req_bad_sig', "./server/counters[@type='nsstat']/counter[@name='ReqBadSIG']", [], MONOTONIC),
        ('bind.network.req_tcp', "./server/counters[@type='nsstat']/counter[@name='ReqTCP']", [], MONOTONIC),
        ('bind.network.auth_qry_rej', "./server/counters[@type='nsstat']/counter[@name='AuthQryRej']", [], MONOTONIC),
        ('bind.network.rec_qry_rej', "./server/counters[@type='nsstat']/counter[@name='RecQryRej']", [], MONOTONIC),
        ('bind.network.xfr_rej', "./server/counters[@type='nsstat']/counter[@name='XfrRej']", [], MONOTONIC),
        ('bind.network.update_rej', "./server/counters[@type='nsstat']/counter[@name='UpdateRej']", [], MONOTONIC),
        ('bind.network.response', "./server/counters[@type='nsstat']/counter[@name='Response']", [], MONOTONIC),
        ('bind.network.truncated_resp', "./server/counters[@type='nsstat']/counter[@name='TruncatedResp']", [], MONOTONIC),
        ('bind.network.resp_edns0', "./server/counters[@type='nsstat']/counter[@name='RespEDNS0']", [], MONOTONIC),
        ('bind.network.resp_tsig', "./server/counters[@type='nsstat']/counter[@name='RespTSIG']", [], MONOTONIC),
        ('bind.network.resp_sig0', "./server/counters[@type='nsstat']/counter[@name='RespSIG0']", [], MONOTONIC),
        ('bind.network.qry_success', "./server/counters[@type='nsstat']/counter[@name='QrySuccess']", [], MONOTONIC),
        ('bind.network.qry_auth_ans', "./server/counters[@type='nsstat']/counter[@name='QryAuthAns']", [], MONOTONIC),
        ('bind.network.qry_noauth_ans', "./server/counters[@type='nsstat']/counter[@name='QryNoauthAns']", [], MONOTONIC),
        ('bind.network.qry_referral', "./server/counters[@type='nsstat']/counter[@name='QryReferral']", [], MONOTONIC),
        ('bind.network.qry_nxrrset', "./server/counters[@type='nsstat']/counter[@name='QryNxrrset']", [], MONOTONIC),
        ('bind.network.qry_serv_fail', "./server/counters[@type='nsstat']/counter[@name='QrySERVFAIL']", [], MONOTONIC),
        ('bind.network.qry_formerr', "./server/counters[@type='nsstat']/counter[@name='QryFORMERR']", [], MONOTONIC),
        ('bind.network.qry_nxdomain', "./server/counters[@type='nsstat']/counter[@name='QryNXDOMAIN']", [], MONOTONIC),
        ('bind.network.qry_recursion', "./server/counters[@type='nsstat']/counter[@name='QryRecursion']", [], MONOTONIC),
        ('bind.network.qry_duplicate', "./server/counters[@type='nsstat']/counter[@name='QryDuplicate']", [], MONOTONIC),
        ('bind.network.qry_dropped', "./server/counters[@type='nsstat']/counter[@name='QryDropped']", [], MONOTONIC),
        ('bind.network.qry_failure', "./server/counters[@type='nsstat']/counter[@name='QryFailure']", [], MONOTONIC),
        ('bind.network.xfr_req_done', "./server/counters[@type='nsstat']/counter[@name='XfrReqDone']", [], MONOTONIC),
        ('bind.network.update_req_fwd', "./server/counters[@type='nsstat']/counter[@name='UpdateReqFwd']", [], MONOTONIC),
        ('bind.network.update_resp_fwd', "./server/counters[@type='nsstat']/counter[@name='UpdateRespFwd']", [], MONOTONIC),
        ('bind.network.update_fwd_fail', "./server/counters[@type='nsstat']/counter[@name='UpdateFwdFail']", [], MONOTONIC),
        ('bind.network.update_done', "./server/counters[@type='nsstat']/counter[@name='UpdateDone']", [], MONOTONIC),
        ('bind.network.update_fail', "./server/counters[@type='nsstat']/counter[@name='UpdateFail']", [], MONOTONIC),
        ('bind.network.update_bad_prereq', "./server/counters[@type='nsstat']/counter[@name='UpdateBadPrereq']", [], MONOTONIC),
        ('bind.network.recurs_clients', "./server/counters[@type='nsstat']/counter[@name='RecursClients']", [], MONOTONIC),
        ('bind.network.dns64', "./server/counters[@type='nsstat']/counter[@name='DNS64']", [], MONOTONIC),
        ('bind.network.rate_dropped', "./server/counters[@type='nsstat']/counter[@name='RateDropped']", [], MONOTONIC),
        ('bind.network.rate_slipped', "./server/counters[@type='nsstat']/counter[@name='RateSlipped']", [], MONOTONIC),
        ('bind.network.rpz_rewrites', "./server/counters[@type='nsstat']/counter[@name='RPZRewrites']", [], MONOTONIC),
        ('bind.network.qry_udp', "./server/counters[@type='nsstat']/counter[@name='QryUDP']", [], MONOTONIC),
        ('bind.network.qry_tcp', "./server/counters[@type='nsstat']/counter[@name='QryTCP']", [], MONOTONIC),
        ('bind.network.nsid_opt', "./server/counters[@type='nsstat']/counter[@name='NSIDOpt']", [], MONOTONIC),
        ('bind.network.expire_opt', "./server/counters[@type='nsstat']/counter[@name='ExpireOpt']", [], MONOTONIC),
        ('bind.network.other_opt', "./server/counters[@type='nsstat']/counter[@name='OtherOpt']", [], MONOTONIC),
        ('bind.network.sit_opt', "./server/counters[@type='nsstat']/counter[@name='SitOpt']", [], MONOTONIC),
        ('bind.network.sit_new', "./server/counters[@type='nsstat']/counter[@name='SitNew']", [], MONOTONIC),
        ('bind.network.sit_bad_size', "./server/counters[@type='nsstat']/counter[@name='SitBadSize']", [], MONOTONIC),
        ('bind.network.sit_bad_time', "./server/counters[@type='nsstat']/counter[@name='SitBadTime']", [], MONOTONIC),
        ('bind.network.sit_no_match', "./server/counters[@type='nsstat']/counter[@name='SitNoMatch']", [], MONOTONIC),
        ('bind.network.sit_match', "./server/counters[@type='nsstat']/counter[@name='SitMatch']", [], MONOTONIC),
        ('bind.opcode.in', "./server/counters[@type='opcode']/counter[@name='QUERY']", ['opcode:query'], MONOTONIC),
        ('bind.opcode.in', "./server/counters[@type='opcode']/counter[@name='IQUERY']", ['opcode:iquery'], MONOTONIC),
        ('bind.opcode.in', "./server/counters[@type='opcode']/counter[@name='STATUS']", ['opcode:status'], MONOTONIC),
        ('bind.opcode.in', "./server/counters[@type='opcode']/counter[@name='NOTIFY']", ['opcode:notify'], MONOTONIC),
        ('bind.opcode.in', "./server/counters[@type='opcode']/counter[@name='UPDATE']", ['opcode:update'], MONOTONIC),
        ('bind.cache.cache_hits', "./views/view[@name='_default']/counters[@type='cachestats']/counter[@name='CacheHits']", [], MONOTONIC),
        ('bind.cache.cache_misses', "./views/view[@name='_default']/counters[@type='cachestats']/counter[@name='CacheMisses']", [], MONOTONIC),
        ('bind.cache.query_hits', "./views/view[@name='_default']/counters[@type='cachestats']/counter[@name='QueryHits']", [], MONOTONIC),
        ('bind.cache.query_misses', "./views/view[@name='_default']/counters[@type='cachestats']/counter[@name='QueryMisses']", [], MONOTONIC),
        ('bind.cache.delete_lru', "./views/view[@name='_default']/counters[@type='cachestats']/counter[@name='DeleteLRU']", [], MONOTONIC),
        ('bind.cache.delete_ttl', "./views/view[@name='_default']/counters[@type='cachestats']/counter[@name='DeleteTTL']", [], MONOTONIC),
        ('bind.cache.cache_nodes', "./views/view[@name='_default']/counters[@type='cachestats']/counter[@name='CacheNodes']", [], GAUGE),
        ('bind.cache.cache_buckets', "./views/view[@name='_default']/counters[@type='cachestats']/counter[@name='CacheBuckets']", [], GAUGE),
    ]

    SOCKET_METRICS = [('bind.socket.{}'.format(lower(smt)), "./server/counters[@type='sockstat']/counter[@name='{}{}']".format(st, smt),
        ['socket_type:{}'.format(lower(st))], MONOTONIC) for st in SOCK_TYPES for smt in SOCK_METRIC_TYPES]

    QOUT_METRICS = [('bind.queries.out', "./views/view[@name='_default']/counters[@type='resqtype']/counter[@name='{}']".format(upper(rr)),
        ['rdtype:{}'.format(lower(rr))], MONOTONIC) for rr in RDTYPES]

    QIN_METRICS = [('bind.queries.in', "./server/counters[@type='qtype']/counter[@name='{}']".format(upper(rr)),
        ['rdtype:{}'.format(lower(rr))], MONOTONIC) for rr in RDTYPES]

    ALL_METRICS = COMMON_METRICS + SOCKET_METRICS + QOUT_METRICS + QIN_METRICS

    def check(self, instance):
        self.log.debug('starting check run')

        try:
            statxml = self._get_statistics(instance)

            [self._query(statxml, m) for m in self.ALL_METRICS]

            self.log.debug('checking serial for {} zones'.format(len(statxml.findall('./views/view[@name="_default"]/zones/zone'))))
            [self._check_serial_vs_master(z.attrib['name']) for z in statxml.findall('./views/view[@name="_default"]/zones/zone')]

        except requests.exceptions.Timeout:
            self.log.debug('Caught requests.exceptions.Timeout')
            self.service_check(
                self.SERVICE_CHECK_NAME,
                AgentCheck.CRITICAL,
                message="Timeout hitting %s." % (self._url(instance))
            )

        except Exception as e:
            self.log.debug('Caught exception({}): {}'.format(type(e), e))
            self.service_check(
                self.SERVICE_CHECK_NAME,
                AgentCheck.CRITICAL,
                message="Error hitting %s. Error: %s" % (self._url(instance), e.message)
            )
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.log.debug("{}\n{}\n{}".format(exc_type, fname, exc_tb.tb_lineno))
            raise e

        self.service_check(
            self.SERVICE_CHECK_NAME,
            AgentCheck.OK
        )

        self.log.debug('finishing check run')

    def _query(self, xml, metric):
        (metric_name, xpath, tags, metric_func) = metric
        val = xml.find(xpath)
        if val is not None:
            metric_func(self, metric_name, int(val.text), tags)

    def _check_serial_vs_master(self, zone):
        self.log.debug('check serial on {}'.format(zone))
        if self._serial_matches_master(zone):
            status = AgentCheck.OK
        else:
            status = AgentCheck.CRITICAL
        self.service_check(self.SERIAL_CHECK_NAME, status, tags=["zone:{}".format(zone)])

    def _serial_matches_master(self, zone):
        try:
            local = self._get_dns(zone, 'SOA')
            remote_ns = self._get_dns(local.mname.to_text(), 'A')
            remote = self._get_dns(zone, 'SOA', remote_ns.to_text())
        except Exception as e:
            self.log.debug('error in _serial_matches_master: {}'.format(e))
            raise e
        return local.serial == remote.serial

    def _get_dns(self, q, rr, resolv='127.0.0.1'):
        resolver.nameservers = [resolv]
        answers = resolver.query(q, rr)
        return next(iter(answers or []), None)

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
