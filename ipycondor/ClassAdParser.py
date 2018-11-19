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
        value = clsad.get(key, None)
        srule=self.__simple_rules.get(key.lower(), None)
        if srule:
                value = _safe_call( srule, value , key , clsad)
        for pt, srule in self.__re_rules:
            if pt.match(key):
                value = _safe_call( srule, value , key , clsad)
        return value if value is not None else 'N/A'

def _safe_call(f, *args):
    import inspect
    n = len(inspect.getargspec(f)[0])
    args = args[:n]
    return f(*args)
