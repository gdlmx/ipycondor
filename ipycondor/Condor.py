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
             columns=['ClusterID','ProcID','Owner','JobStatus',
                      'JobStartDate','JobUniverse', 'RemoteHost'],
             index=['ClusterID','ProcID']):
        for i in index:
            if not i in columns: columns = [i] + list(columns)
        jobs = self.schedd.query(constraint.encode())
        parser = JobParser()
        data = [[parser.parse(j, c) for c in columns] for j in jobs]
        return to_qgrid(data, columns, index)

    def machine_table(self, constraint='',
             columns=['Machine','SlotID','Activity','CPUs','Memory'],
             index=['Machine','SlotID']):
        for i in index:
            if not i in columns: columns = [i] + list(columns)
        constraint = 'MyType=="Machine"&&({0})'.format(constraint) if constraint else 'MyType=="Machine"'
        machines = self.coll.query(constraint=constraint.encode())
        parser = MachineParser()
        data = [[parser.parse(m, c) for c in columns] for m in machines]
        return to_qgrid(data, columns, index)

    def tab(self):
        import ipywidgets as widgets
        jobs = self.job_table()
        machines = self.machine_table(constraint='SlotID==1',
            columns=['Machine','TotalSlots','TotalCPUs','TotalMemory'],
            index=['Machine'])
        tab = widgets.Tab(children=[jobs, machines])
        tab.set_title(0, 'Jobs')
        tab.set_title(1, 'Machines')
        return tab

    def dashboard(self):
        import ipywidgets as widgets
        from IPython.display import display, clear_output
        output = widgets.Output()
        def refresh(button):
            with output:
                index = button.tab.selected_index if hasattr(button,'tab') else 0
                button.tab = self.tab()
                button.tab.selected_index = index
                display(button.tab)
                clear_output(wait=True)
        refresh_btn = widgets.Button(description='Refresh', icon='refresh')
        refresh_btn.on_click(refresh)
        refresh(refresh_btn)
        controls = widgets.HBox(children=[refresh_btn],
                             layout=widgets.Layout(justify_content='flex-end'))
        display(controls, output)
