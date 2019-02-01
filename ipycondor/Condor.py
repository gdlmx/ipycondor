# Copyright 2019 Lukas Koschmieder
# Copyright 2018 Mingxuan Lin

from __future__ import print_function
import htcondor
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)

from .ClassAdParser import QueryParser

from IPython.display import display, clear_output
import ipywidgets as widgets

from subprocess import Popen, PIPE
import os, time

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
            return self.condor.dashboard()

    @line_magic
    def CondorMon(self,line):
        "Display the Condor dashboard"
        return self.condor.dashboard()

    @property
    def condor(self):
        c = getattr(self,'_condor', None)
        if isinstance(c, Condor):
            c = Condor()
            self._condor = c
        return c

def to_table(classAds, cols, key_cols = []):
    import pandas as pd
    import qgrid
    columns = tuple(key_cols) + tuple(for c in cols if c not in key_cols)
    # Parse the classAd objects from the query function
    parser = QueryParser()
    data = [[parser.parse(j, c) for c in columns] for j in classAds]
    # Create QGrid table widget
    df = pd.DataFrame(data, columns=columns)
    if key_cols:
        df = df.set_index(key_cols)
        df = df.sort_index()
    widget = qgrid.show_grid(df, show_toolbar=False,
        grid_options={'editable':False,
                      'minVisibleRows':10,
                      'maxVisibleRows':8})
    return widget

class Condor(object):
    def __init__(self, schedd_name=None):
        self.coll = htcondor.Collector()
        # schedd_names =  [ s['Name'] for s in coll.locateAll(htcondor.DaemonTypes.Schedd)]
        if schedd_name:
            schedd_ad = self.coll.locate(htcondor.DaemonTypes.Schedd, schedd_name)
        else:
            schedd_ad = self.coll.locate(htcondor.DaemonTypes.Schedd)
        self.schedd = htcondor.Schedd(schedd_ad)
        self._table_layout = [("Jobs", self.job_table), ("Machines", self.machine_table)]

    def jobs(self, constraint=''):
        return self.schedd.query(constraint.encode())

    def machines(self, constraint=''):
        constraint = 'MyType=="Machine"&&({0})'.format(constraint) if constraint else 'MyType=="Machine"'
        return self.coll.query(constraint=constraint.encode())

    def job_table(self, constraint='',
             columns=['ClusterID','ProcID','Owner','JobStatus',
                      'JobStartDate','JobUniverse', 'RemoteHost'],
             index=['ClusterID','ProcID']):
        return to_table(self.jobs(constraint), columns, index)

    def slot_table(self, constraint='',
             columns=['Machine','SlotID','Activity','CPUs','Memory'],
             index=['Machine','SlotID']):
        return to_table(self.machines(constraint), columns, index)

    def machine_table(self,constraint='SlotID==1||SlotID=="1_1"',
            columns=['Machine','TotalSlots','TotalCPUs','TotalMemory',
                     'TotalDisk','TotalLoadAvg'],
            index=['Machine']):
        return to_table(self.machines(constraint), columns, index)

    def tabs(self):
        tabs = []
        _tabs = self._table_layout
        for title , factory in _tabs:
            tabs.append(factory(self))
        tab = widgets.Tab(children=tabs)
        for i, t_f in enumerate(_tabs):
            tab.set_title(i, t_f[0])
        return tab

    def dashboard(self):
        output = widgets.Output()
        def refresh(button):
            with output:
                index = button.tab.selected_index if hasattr(button, 'tab') else 0
                button.tab = self.tabs()
                button.tab.selected_index = index
                display(button.tab)
                clear_output(wait=True)
        refresh_btn = widgets.Button(description='Refresh', icon='refresh', button_style='')
        refresh_btn.on_click(refresh)
        refresh(refresh_btn)
        controls = widgets.HBox(children=[refresh_btn],
            layout=widgets.Layout(justify_content='flex-end'))
        display(controls, output)
