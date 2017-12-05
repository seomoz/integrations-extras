# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# 3rd party
import rethinkdb as r
# project
from checks import AgentCheck

# Metric types
GAUGE = 'gauge'
COUNTER = 'counter'
GAUGE = 'rate'

EVENT_TYPE = SOURCE_TYPE_NAME = 'rethinkdb'

# Service check
RETHINKDB_SERVICE_CHECK = 'rethinkdb.can_connect'

CLUSTER_METRICS = {
    "queries_per_sec": ('rethinkdb.cluster.queries_per_sec', GAUGE),
    "read_docs_per_sec": ('rethinkdb.cluster.read_docs_per_sec', GAUGE),
    "written_docs_per_sec": ('rethinkdb.cluster.written_docs_per_sec', GAUGE),
    "client_connections": ('rethinkdb.cluster.client_connections',GAUGE),
    "clients_active": ('rethinkdb.cluster.clients_active',GAUGE)}

EXTRA_CLUSTER_METRICS = {
    'server_count':('rethinkdb.cluster.total_server_count', GAUGE),
    'total_issues_count': ('rethinkdb.cluster.total_issues_count', GAUGE),
    'log_write_errors': ('rethinkdb.cluster.log_write_errors', GAUGE),
    'name_collision_errors': ('rethinkdb.cluster.name_collision_errors', GAUGE),
    'outdated_index_errors': ('rethinkdb.cluster.outdated_index_errors', GAUGE),
    'table_availability_errors': ('rethinkdb.cluster.table_availability_errors', GAUGE),
    'memory_errors': ('rethinkdb.cluster.memory_errors', GAUGE),
    'connectivity_issues': ('rethinkdb.cluster.connectivity_issues', GAUGE),
    'total_running_jobs': ('rethinkdb.cluster.total_running_jobs', GAUGE),
    'total_running_queries': ('rethinkdb.cluster.total_running_queries', GAUGE),
    'total_running_disk_compactions': ('rethinkdb.cluster.total_running_disk_compactions', GAUGE),
    'total_running_index_constructions': ('rethinkdb.cluster.total_running_index_constructions', GAUGE),
    'total_running_backfill_jobs': ('rethinkdb.cluster.total_running_backfill_jobs', GAUGE)}

SERVER_METRICS = {
    "queries_per_sec": ('rethinkdb.server.queries_per_sec', GAUGE),
    "queries_total": ('rethinkdb.server.queries_total', GAUGE),
    "read_docs_per_sec": ('rethinkdb.server.read_docs_per_sec', GAUGE),
    "read_docs_total": ('rethinkdb.server.read_docs_total', GAUGE),
    "written_docs_per_sec": ('rethinkdb.server.written_docs_per_sec', GAUGE),
    "written_docs_total": ('rethinkdb.server.written_docs_total', GAUGE),
    "client_connections": ('rethinkdb.server.client_connections', GAUGE),
    "clients_active": ('rethinkdb.server.clients_active', GAUGE)}

REPLICATION_METRICS = {
    "read_docs_per_sec": ('rethinkdb.repl.read_docs_per_sec', GAUGE),
    "read_docs_total": ('rethinkdb.repl.read_docs_total', GAUGE),
    "written_docs_per_sec": ('rethinkdb.repl.written_docs_per_sec', GAUGE),
    "written_docs_total": ('rethinkdb.repl.written_docs_total', GAUGE),
    "in_use_bytes" : ('rethinkdb.repl.cache.in_use_bytes', GAUGE),
    "read_bytes_per_sec": ('rethinkdb.repl.disk.read_bytes_per_sec', GAUGE),
    "read_bytes_total": ('rethinkdb.repl.disk.read_bytes_total', GAUGE),
    "written_bytes_per_sec": ('rethinkdb.repl.disk.written_bytes_per_sec', GAUGE),
    "written_bytes_total": ('rethinkdb.repl.disk.written_bytes_total', GAUGE),
    "metadata_bytes": ('rethinkdb.repl.space_usage.metadata_bytes', GAUGE),
    "data_bytes": ('rethinkdb.repl.space_usage.data_bytes', GAUGE),
    "garbage_bytes": ('rethinkdb.repl.space_usage.garbage_bytes', GAUGE),
    "preallocated_bytes" : ('rethinkdb.repl.space_usage.preallocated_bytes', GAUGE)}

TABLE_CONFIG_METRICS = {
    "shards_count": ('rethinkdb.table.config.shards_count', GAUGE),
    "replicas_count": ('rethinkdb.table.config.replicas_count', GAUGE),
    "index_count": ('rethinkdb.table.config.index_count', GAUGE),
    "durability": ('rethinkdb.table.config.durability.code', GAUGE)}

DURABILITY_CODE = {
    "soft": 0,
    "hard": 1,
    "other": 3}

class RethinkdbCheck(AgentCheck):
    hostname = ''

    def check(self, instance):
        self.log.info('inside rethinkdb check')
        hostname = instance.get('host','localhost')
        self.hostname = hostname.strip()
        port = instance.get('port')
        collect_extra = instance.get('additional_metrics',True)
        if not all([hostname,port]):
            raise Exception('The Rethinkdb hostname and password must be specified in the configuration')

        instance_tags = ["rethinkdb_instance:{0}-{1}".format(hostname, port)]

        #service check
        self.service_check(RETHINKDB_SERVICE_CHECK,
            AgentCheck.OK,
            tags=instance_tags,
            message='Connection to %s was successful' % hostname)

        # get and set metrics
        self._get_rethinkdb_metrics(hostname, port, instance_tags, collect_extra)

    def _get_connection(self, hostname, port, instance_tags):
        '''
        Get connection to rethinkdb
        '''
        try:
            conn = r.connect(host = hostname, port = port)
            return conn

        except Exception as e:
            msg = "Fatal: could not establish database connection to %s:%d: %s,  exiting program." % (hostname, port, e)

            self.service_check(RETHINKDB_SERVICE_CHECK, AgentCheck.CRITICAL, tags = instance_tags, message = msg)
            raise

    def _set_metric(self, metric_name, metric_type, value, instance_tags):
        '''
        Set a metric
        '''
        if metric_type == GAUGE:
            self.gauge(metric_name, value, tags = instance_tags)

        elif metric_type == COUNTER:
            self.count(metric_name, value, tags = instance_tags)

        else:
            self.log.error('Unknown Metric Type: "%s" ' % (metric_type))

    def _get_extra_cluster_stats(self, conn):
        '''
        Get current issues and currently running jobs metrics for the cluster
        '''
        extra_cluster_stats = {'total_issues_count': 0,
                               'log_write_errors': 0,
                               'name_collision_errors': 0,
                               'outdated_index_errors': 0,
                               'table_availability_errors': 0,
                               'memory_errors': 0,
                               'connectivity_issues': 0,
                               'total_running_jobs': 0,
                               'total_running_queries': 0,
                               'total_running_disk_compactions': 0,
                               'total_running_index_constructions': 0,
                               'total_running_backfill_jobs': 0,
                               'server_count': 0}
        issues = {}
        jobs = {}

        if not conn:
            self.log.error('Connection not set, extra cluster stats not calculated')
            return None
        else:
            try:
                extra_cluster_stats['server_count'] = r.db('rethinkdb').table('stats').has_fields('server').pluck('server').distinct().count().run(conn)
                extra_cluster_stats['total_issues_count'] = r.db('rethinkdb').table('current_issues').count().run(conn)
                if extra_cluster_stats['total_issues_count'] > 0:
                    issues = r.db('rethinkdb').table('current_issues').run(conn)
                    for issue in issues:
                        if 'type' in issue and issue['type'] == 'log_write_error':
                            extra_cluster_stats['log_write_errors'] += 1
                        elif 'type' in issue and 'name_collision' in issue['type']:
                            extra_cluster_stats['name_collision_errors'] += 1
                        elif 'type' in issue and issue['type'] == 'outdated_index':
                            extra_cluster_stats['outdated_index_errors'] += 1
                        elif 'type' in issue and issue['type'] == 'table_availability':
                            extra_cluster_stats['table_availability_errors'] += 1
                        elif 'type' in issue and issue['type'] == 'memory_error':
                            extra_cluster_stats['memory_errors'] += 1
                        elif 'type' in issue and issue['type'] == 'non_transitive_error':
                            extra_cluster_stats['connectivity_issues'] += 1

                extra_cluster_stats['total_running_jobs'] = r.db("rethinkdb").table("jobs").count().run(conn)
                if extra_cluster_stats['total_running_jobs'] > 0:
                    jobs = r.db("rethinkdb").table("jobs").run(conn)
                    for job in jobs:
                        if 'type' in job and job['type'] == 'query':
                            extra_cluster_stats['total_running_queries'] += 1
                        if 'type' in job and job['type'] == 'disk_compaction':
                            extra_cluster_stats['total_running_disk_compactions'] += 1
                        if 'type' in job and job['type'] == 'index_construction':
                            extra_cluster_stats['total_running_index_constructions'] += 1
                        if 'type' in job and job['type'] == 'backfill':
                            extra_cluster_stats['total_running_backfill_jobs'] += 1
                return extra_cluster_stats
            except Exception as e:
                msg = 'Error finding extra cluster stats: %s ' % e
                self.log.error(msg)
                return None

    def _parse_metrics(self, metrics, metric_type, instance_tags, collect_extra = True):
        '''
        Parse the metrics according to their type: cluster, server, table_server or table_config
        '''
        if not metric_type:
            return
        if metric_type == 'cluster':
            for metric, (m_name, m_type) in CLUSTER_METRICS.iteritems():
                value = metrics[metric]
                if value is not None:
                    self._set_metric(m_name, m_type, value, instance_tags)
            if collect_extra:
                for metric, (m_name, m_type) in EXTRA_CLUSTER_METRICS.iteritems():
                    value = metrics[metric]
                    if value is not None:
                        self._set_metric(m_name, m_type, value, instance_tags)
        elif metric_type == 'server':
            for server in metrics:
                if 'server' in server and server['server'] == self.hostname:
                    stats = server['query_engine']
                    for metric, (m_name, m_type) in SERVER_METRICS.iteritems():
                        value = stats[metric]
                        if value is not None:
                            self._set_metric(m_name, m_type, value, instance_tags)
                        else:
                            self.log.debug('Value not returned for metric: "%s" ' % (m_name))
        elif metric_type == 'table_server':
            table_server_stats = {}
            for stat in metrics:
                if 'server' in stat and stat['server'] == self.hostname:
                    if 'table' in stat:
                        db_table_tags = list(instance_tags)
                        db_table_tags.append("rethinkdb_table:{0}".format(stat['table']))
                    if 'db' in stat:
                        db_table_tags.append("rethinkdb_db:{0}".format(stat['db']))
                    if 'query_engine' in stat:
                        table_server_stats.update(stat['query_engine'])
                    if 'storage_engine' in stat:
                        table_server_stats.update(stat['storage_engine']['cache'])
                        table_server_stats.update(stat['storage_engine']['disk']['space_usage'])
                        filter_keys = list(stat['storage_engine']['disk'].keys())
                        filter_dict = {k :stat['storage_engine']['disk'][k] for k in filter_keys if k != 'space_usage'}
                        table_server_stats.update(filter_dict)

                    for metric, (m_name, m_type) in REPLICATION_METRICS.iteritems():
                        value = table_server_stats[metric]
                        if value is not None:
                            self._set_metric(m_name, m_type, value, db_table_tags)
                        else:
                            self.log.debug('Value not returned for metric: "%s" ' % (m_name))
        elif metric_type == 'table_config':
            for table in metrics:
                table_tags = list(instance_tags)
                table_tags.append("rethinkdb_table:{0}".format(table['table']))
                table_tags.append("rethinkdb_db:{0}".format(table['db']))

                for metric, (m_name, m_type) in TABLE_CONFIG_METRICS.iteritems():
                        value = table[metric]
                        if value is not None:
                            self._set_metric(m_name, m_type, value, table_tags)


    def _get_rethinkdb_metrics(self, hostname, port, instance_tags, collect_extra):
        '''
        Get RethinkDB metrics from stats or table_config table
        '''
        server_stats = []
        cluster_stats = {}
        table_server_stats = []
        table_config_stats = []
            
        conn = self._get_connection(hostname, port, instance_tags)
        cursor = r.db("rethinkdb").table("stats").run(conn)
        for stats in cursor:
            if 'id' in stats and 'cluster' in stats['id']:
                self.log.info('inside cluster block')
                if 'query_engine' in stats:
                    cluster_stats.update(stats['query_engine'])
                if collect_extra:
                    extra_cluster_stats = self._get_extra_cluster_stats(conn)
                    if extra_cluster_stats:
                        cluster_stats.update(extra_cluster_stats)
                self._parse_metrics(cluster_stats, 'cluster', instance_tags, collect_extra)
            elif 'id' in stats and isinstance(stats['id'],(list,tuple)):
                if 'server' in stats['id']:
                    server_stats.append(stats)
                elif 'table_server' in stats['id']:
                    table_server_stats.append(stats)
        if len(server_stats) > 0:
            self._parse_metrics(server_stats, 'server', instance_tags)
        if len(table_server_stats) > 0:
            self._parse_metrics(table_server_stats, 'table_server', instance_tags)

        # collect table config stats
        cursor = r.db('rethinkdb').table('table_config').run(conn)
        for table in cursor:
            table_stats = {}
            if 'db' in table:
                table_stats['db'] = table['db']
            if 'name' in table:
                table_stats['table'] = table['name']
            if 'indexes' in table:
                table_stats['index_count'] = len(table['indexes'])
            if 'durability' in table:
                durability = table['durability']
                if durability in ['soft', 'hard']:
                    table_stats['durability'] = DURABILITY_CODE[durability]
                else:
                    table_stats['durability'] = DURABILITY_CODE['other']
            if 'shards' in table:
                table_shards = table['shards']
                table_stats['shards_count'] = len(table_shards)
                for shard in table_shards:
                    if 'replicas' in shard:
                        table_stats['replicas_count'] = len(shard['replicas'])
            table_config_stats.append(table_stats)
        if len(table_config_stats) > 0:
            self._parse_metrics(table_config_stats, 'table_config', instance_tags)
        conn.close()
