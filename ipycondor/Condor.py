# Copyright 2019 Lukas Koschmieder
# Copyright 2018 Mingxuan Lin

from __future__ import print_function
import htcondor
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)

from .Qgrid import to_qgrid
from .JobParser import JobParser
from .MachineParser import MachineParser

from subprocess import Popen, PIPE
import os, time

def _load_magic():
    ip = get_ipython()
    ip.register_magics(CondorMagics)


@magics_class
class CondorMagics(Magics):

    @cell_magic
    def CondorJob(self, line, cell):
        "Creation of a condor job"
        username=os.environ.get('JUPYTERHUB_USER', os.environ.get('USER'))
        p=Popen( [ 'condor_submit' ] , stdin=PIPE,stdout=PIPE, stderr=PIPE)
        out,err = p.communicate(cell.encode('utf-8'))
        out=out.decode('utf-8','replace')
        err=err.decode('utf-8','replace')
        print(out, '\n', err)
        if p.poll() == 0:
            ui=Condor()
            return ui.job_table(q='Owner=="{0}" && QDate > {1}'.format(username, int(time.time())-30 ))

class Condor(object):
    def __init__(self, schedd_name=None):
        self.coll = htcondor.Collector()
        # schedd_names =  [ s['Name'] for s in coll.locateAll(htcondor.DaemonTypes.Schedd)]
        if schedd_name:
            schedd_ad = self.coll.locate(htcondor.DaemonTypes.Schedd, schedd_name)
        else:
            schedd_ad = self.coll.locate(htcondor.DaemonTypes.Schedd)
        self.schedd = htcondor.Schedd(schedd_ad)

    def job_table(self, constraint='',
             columns=['ClusterId','ProcId','Owner','JobStatus','QDate',
                      'JobStartDate','JobUniverse', 'RemoteHost','ExitStatus']):
        if not 'ProcId' in columns: columns = ['ProcId'] + list(columns)
        if not 'ClusterId' in columns: columns = ['ClusterId'] + list(columns)
        jobs = self.schedd.query(constraint.encode())
        parser = JobParser()
        data = [[parser.parse(j, c) for c in columns] for j in jobs]
        return to_qgrid(data, columns, ['ClusterId','ProcId'])

    def machine_table(self, constraint='',
             columns=['Machine','SlotID','Activity','CPUs','Memory']):
        if not 'SlotID' in columns: columns = ['SlotID'] + list(columns)
        if not 'Machine' in columns: columns = ['Machine'] + list(columns)
        constraint = 'MyType=="Machine"&&({0})'.format(constraint) if constraint else 'MyType=="Machine"'
        machines = self.coll.query(constraint=constraint.encode())
        parser = MachineParser()
        data = [[parser.parse(m, c) for c in columns] for m in machines]
        return to_qgrid(data, columns, ['Machine','SlotID'])
