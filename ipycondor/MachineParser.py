# Copyright 2019 Lukas Koschmieder

from __future__ import print_function
from six import string_types

from .ClassAdParser import rule, BaseParser

class MachineParser(BaseParser):
    @rule('\w*(Memory)$')
    def mbyte2human(value):
        try:
            import humanize
            return humanize.naturalsize(1024*1024*value, binary=True)
        except ImportError:
            return value

    @rule('\w*(Disk)$')
    def kbyte2human(value):
        try:
            import humanize
            return humanize.naturalsize(1024*value, binary=True)
        except ImportError:
            return value
