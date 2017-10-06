# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import mmap
import os

# 3rd party

# project
from checks import AgentCheck

class FileGrepCheck(AgentCheck):

    SOURCE_TYPE_NAME = 'file_grep'
    SERVICE_CHECK_NAME = 'file_grep.match_ok'

    def check(self, instance):
        self.log.debug('starting check run')

        check_name = instance.get('name')
        filename = instance.get('file')
        search_str = instance.get('search_string')

        result = None
        service_check_tags = []
        service_check_tags.append('check_name:{}'.format(check_name))

        try:
            with ctx_fd(filename) as fd:
                size = os.fstat(fd).st_size
                self.gauge('file_grep.size', size, tags=['file:{}'.format(filename)])
                with ctx_mmap(fd) as mm:
                    result = mm.find(search_str)
        except Exception as e:
            self.service_check(
                self.SERVICE_CHECK_NAME, AgentCheck.UNKNOWN, tags=service_check_tags)
            self.warning('Error({}): {}'.format(type(e).__name__ , e))
            return

        if result == -1:
            # NO MATCH
            self.log.debug('NO MATCH')
            self.service_check(
                self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, tags=service_check_tags)
        else:
            self.log.debug('MATCH')
            self.service_check(
                self.SERVICE_CHECK_NAME, AgentCheck.OK, tags=service_check_tags)

class ctx_fd():
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.fd = os.open(self.filename, os.O_RDONLY)
        return self.fd

    def __exit__(self, *args):
        os.close(self.fd)

class ctx_mmap():
    def __init__(self, fd):
        self.fd = fd

    def __enter__(self):
        self.mm = mmap.mmap(self.fd, 0, prot=mmap.PROT_READ)
        return self.mm

    def __exit__(self, *args):
        self.mm.close()
