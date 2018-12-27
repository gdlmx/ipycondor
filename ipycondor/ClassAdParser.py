# Copyright 2018 Mingxuan Lin

from __future__ import print_function
from six import string_types

class Rule(object):
    def __init__(self, f):
        self.func=f

def rule(x):
    if callable(x):
        return Rule(x)
    if isinstance(x, string_types):
        def decorater(f1):
            r = Rule(f1)
            r.pattern = x
            return r
        return decorater
    raise TypeError('Unexpected usage of @rule')

class BaseParser(object):
    """
    Sample usage:
        class ClassAdParser(BaseParser):
            # This is a simple rule which will match the attribute name `JobStatus`
            @rule
            def JobStatus(v):
                lookuptable=['Idle','Running','Removed','Completed','Held','Transferring Output']
                return lookuptable[v-1]
        p = ClassAdParser()
        p.parse(jobClassAd, 'JobStatus')
    """
    def __init__(self):
        import re
        super(BaseParser,self).__init__()
        cls = self.__class__
        self.__simple_rules = {}
        self.__re_rules = []
        for n in dir(cls):
            if n.startswith('__'):
                continue
            attr = getattr(cls, n)
            if isinstance(attr, Rule):
                pattern = getattr(attr, 'pattern', None)
                if pattern:
                    pattern = re.compile(pattern, flags=re.IGNORECASE)
                    self.__re_rules.append([pattern, attr.func])
                else:
                    self.__simple_rules[attr.func.__name__.lower()]=attr.func

    def parse(self, clsad, key):
        """
        Parse the value of the named attribute of a ClassAd object `clsad[key]`.

        :param clsad: the ClassAd object to be parsed
        :type  clsad: ClassAd
        :param key: name of the attribute
        :type  key: string
        :rtype: string
        """
        value = clsad.get(key, None)
        srule=self.__simple_rules.get(key.lower(), None)
        if srule:
                value = _safe_call( srule, value , key , clsad)
                if value:
                    return value
        for pt, srule in self.__re_rules:
            if pt.match(key):
                value = _safe_call( srule, value , key , clsad)
        return value if value is not None else 'N/A'

def _safe_call(f, *args):
    import inspect
    n = len(inspect.getargspec(f)[0])
    args = args[:n]
    return f(*args)


class QueryParser(BaseParser):
    """
    Parser class for the ClassAd objects returned by `schedd.query`
    """
    @rule
    def JobUniverse(value):
        Universes={1:'standard', 5:'vanilla', 7:'scheduler', 8:'MPI', 9:'grid', 10:'java', 11:'parallel', 12:'local', 13:'vm'}
        return Universes.get(value, 'unknown')

    @rule
    def JobStatus(value, k, clsad):
        S=['Idle','Running','Removed','Completed','Held','Transferring Output','Suspended']
        return S[value-1]

    @rule
    def DiskUsage(value):
        return '{0} KB'.format(value)

    @rule(r'\w*(Date|Expiration)$')
    def timestamp2date(value):
        import datetime
        if isinstance(value, int) and value > 0:
            return datetime.datetime.fromtimestamp(value)
        else:
            return None

    @rule
    def RemoteHost(value, k, clsad):
        return value if value else clsad.get('LastRemoteHost','')

    @rule
    def JobId(value, key, classad):
        return classad.get('GlobalJobId', '').split('#')[1]

    @rule
    def ExitStatus(value, key, classad):
        if classad.get('CompletionDate','') > 0:
            return value
        else:
            return None

    @rule(r'\w*(Memory)$')
    def mbyte2human(value):
        try:
            import humanize
            return humanize.naturalsize(1024*1024*value, binary=True)
        except ImportError:
            return value

    @rule(r'\w*(Disk)$')
    def kbyte2human(value):
        try:
            import humanize
            return humanize.naturalsize(1024*value, binary=True)
        except ImportError:
            return value
