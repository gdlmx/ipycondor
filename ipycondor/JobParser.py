# Copyright 2019 Lukas Koschmieder
# Copyright 2018 Mingxuan Lin

from __future__ import print_function
from six import string_types

from .ClassAdParser import rule, BaseParser

class JobParser(BaseParser):
    @rule
    def JobUniverse(value):
        Universes={1:'standard', 5:'vanilla', 7:'scheduler', 8:'MPI', 9:'grid', 10:'java', 11:'parallel', 12:'local', 13:'vm'}
        return Universes.get(value, 'unknown')

    @rule
    def JobStatus(value, k, clsad):
        S=['Idle','Running','Removed','Completed','Held','Transferring Output','Suspended']
        value = S[value-1]
        # if value == 'Held':
        #     return '<div class="tooltip">{0}<span class="tooltiptext">{1} [{2}]</span></div>'.format(
        #             value, clsad.get('HoldReason'), clsad.get('ExitStatus')
        #     )
        # elif value == 'Completed':
        #     return '<div class="tooltip">{0}<span class="tooltiptext">{1} [{2}]</span></div>'.format(
        #             value, 'exit code =', clsad.get('ExitStatus')
        #     )
        return value

    @rule
    def DiskUsage(value):
        return '{0} KB'.format(value)

    @rule('\w*(Date|Expiration)$')
    def timestamp2date(value):
        import datetime
        return datetime.datetime.fromtimestamp(value) if isinstance(value, int) else None

    @rule
    def RemoteHost(value, k, clsad):
        return value if value else clsad.get('LastRemoteHost','')

    @rule
    def JobId(value, key, classad):
        return classad.get('GlobalJobId', '').split('#')[1]
