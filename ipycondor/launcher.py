# Copyright 2019 Mingxuan Lin

import time, json, os, subprocess, socket, io
from ipyparallel.apps.launcher import HTCondorLauncher, BatchClusterAppMixin, ioloop
from traitlets import (Any, Integer, List, Unicode, default)
# CFloat,Dict, Instance, HasTraits, CRegExp, TraitError, validate, observe

class HTCondorEngineSetSshLauncher(HTCondorLauncher, BatchClusterAppMixin):
    """Launch Engines using HTCondor and use condor_ssh_to_job for port forwarding"""

    batch_file_name = Unicode('htcondor_engines.job', config=True)
    batch_template  = Unicode("""
universe             = vanilla
executable           = {exec_cmd}
transfer_executable  = true
transfer_input_files ={to_send}
should_transfer_files= yes

#stream_output = true
stream_error  = true
#output = ipyengine.$(ClusterId).$(ProcId).out.txt
error  = ipyengine.$(ClusterId).$(ProcId).err.txt
log    = ipyengine.$(ClusterId).$(ProcId).log

arguments = "mpiexec --n {n} ipengine --file={name_pre}-engine.json --cluster-id={cluster_id} --mpi --timeout=30 "

{requirements}
{environments}
{proxy}
+ipengine_starter_n={n}

queue
    """ , config=True)

    to_send      = List([], config=True, help="List of local files to send before starting")

    requirements = Unicode('', help='The requirements command in the job description file',config=True)
    environments = Unicode('', help='The environment command in the job description file', config=True)
    exec_cmd     = Unicode('ipengine_launcher', help='Remote launcher script for ipengine(s)',config=True)
    job_timeout  = Integer(30, help='Timeout for job starting in seconds', config=True)

    ssh_to_job_proc = Any()
    context_keys = Unicode('requirements,environments,exec_cmd,name_pre', config=True)

    @default('exec_cmd')
    def _exec_cmd_default(self):
        return os.path.join(self.profile_dir, 'ipengine_launcher')

    @property
    def ipcontroller_info(self):
        return os.path.join(self.profile_dir, 'security', self.name_pre+'-engine.json' )

    @property
    def name_pre(self):
        return 'ipcontroller-%s' % self.cluster_id if self.cluster_id else 'ipcontroller'

    def _update_context(self):
        prefix=lambda a,b: (a+b) if b else ''

        cl_info = self.ipcontroller_info
        to_send = [ cl_info ] + self.to_send
        assert wait_for_new_file(cl_info,3600*24), "File not found or too old %s"%cl_info
        assert all([os.path.exists(x) for x in to_send]),'One or more input file(s) do not exist\n%s'%to_send

        c = {
            'proxy':    prefix( 'x509UserProxy=', os.environ.get('X509_USER_PROXY') ) ,
            'to_send':  ','.join( to_send )
            }
        c.update( {k:getattr(self,k) for k in self.context_keys.split(',')} )
        self.context.update(c)

    poller = None
    job_submit_time = 0
    _last_job_stat  = 0
    def start(self, n):
        self._update_context()
        # call parent method to submit the job
        self.log.debug("Submitting condor job with context %s", self.context)
        ans = super().start(n)
        # setup poller for job status
        self.job_submit_time = time.time()
        self._last_job_stat  = 0
        self.poller          = ioloop.PeriodicCallback(self.poll, 1000)
        self.poller.start()
        self.on_stop(lambda x: self.poller.stop())
        return ans

    def poll(self):
        if not  self.running: return
        old_jstat = self._last_job_stat
        jstat     = self._last_job_stat = self.job_stat
        stat_changed = jstat != old_jstat
        if stat_changed:
            if jstat == 2: #running
                self.poller.callback_time = 20*1000
                if not self.job_is_local and self.ssh_stat() == 'none':
                    try:
                        self.create_tunnel()
                    except: #pylint: disable=W0702
                        self.stop()
            elif jstat in (3, 4, 6): # stopped. No further action on the job is needed
                self.notify_stop(jstat)
        elif jstat != 2 and time.time() > self.job_submit_time + self.job_timeout:
            self.log.error('Condor job %s is under %s for too long', self.job_id, jstat)
            self.stop()
        elif jstat == 2 and self.ssh_stat() == 'exited':
            self.log.error('SSH tunnel is not alive. Exitting ...')
            self.stop()

    @property
    def job_is_local(self):
        remote_host = self.get_job_attr('RemoteHost').split('@')[1]
        local_host  = socket.getfqdn()
        return remote_host.lower().find(local_host.lower())>=0

    @property
    def job_stat(self):
        try:
            return int(self.get_job_attr('JobStatus'))
        except (ValueError, RuntimeError):
            return 0

    def get_job_attr(self,attrname):
        val = subprocess.check_output(['condor_q', '-format','%s', attrname, str(self.job_id)])
        val = val.decode(errors='ignore')
        self.log.debug('Condor job %s: %s=%s ',self.job_id, attrname, val)
        return val

    def create_tunnel(self):
        assert not self.ssh_to_job_proc
        self.ssh_to_job_proc = 'Creating'
        with open(self.ipcontroller_info, 'r') as f:
            stat_engine=json.load( f )

        args=['condor_ssh_to_job', str(self.job_id), '-N', '-v', '-o', "ExitOnForwardFailure yes"]
        for k in ("registration", "control", "mux", "hb_ping", "hb_pong", "task", "iopub" ):
            args += ['-R', 'localhost:{0}:localhost:{0}'.format(stat_engine[k])]

        p=subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.ssh_to_job_proc = p
        try:
            ret_code = p.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass
        else:
            out, err = tuple(x.decode(errors='ignore') for x in p.communicate(timeout=1))
            self.ssh_to_job_proc = None
            self.log.error('condor_ssh_to_job failed too early [%d]\n\t%s\n\t%s', ret_code, out, err)
            raise RuntimeError(err)
        self.on_stop(self.stop_ssh_tunnel)
        #self.ssh_stderr = SubprocPipeBuf(self.loop, p, 'stderr').buf
        SubprocPipeBuf(self.loop, p, 'stderr', lambda l: self.log.debug('[SSH] - %s', l.rstrip()))
        self.log.info('condor_ssh_to_job started successfully (PID=%d)', p.pid)

    def ssh_stat(self):
        p = self.ssh_to_job_proc
        if isinstance(p, subprocess.Popen):
            return 'running' if p.poll() is None else 'exited'
        return 'none' if not p else 'creating'

    def stop_ssh_tunnel(self, cb_data=None): #pylint: disable=W0613
        p = self.ssh_to_job_proc
        if self.ssh_stat() == 'running':
            p.terminate()
            try:
                out, err = tuple(x.decode(errors='ignore') for x in p.communicate(timeout=1))
                self.log.debug('condor_ssh_to_job %s exited with %s: %s\n\t%s', p.pid, p.poll(), out, err)
            except subprocess.TimeoutExpired:
                self.log.error('Fail to kill condor_ssh_to_job with pid=%d, please execute `kill %d` in a console', p.pid, p.pid)


def wait_for_new_file(filename, timeout=20):
    for i in range(timeout): #pylint: disable=W0612
        try:
            fileage = time.time() - os.stat(filename).st_mtime
            if ( fileage<timeout and fileage>0 ):
                return True
        except FileNotFoundError:
            time.sleep(1)
    return False

class SubprocPipeBuf:
    def __init__(self, loop, proc, pipename='stdout', line_callback=None):
        self.pipe = getattr(proc, pipename)
        self.buf  = None
        if line_callback:
            self.line_callback = line_callback
        else:
            self.buf  = io.StringIO()
            self.line_callback = self.buf.write
        self.loop = loop
        self.proc = proc
        loop.add_handler(self.pipe, self._read_handler, loop.READ)

    def _read_handler(self, fd, evt): #pylint: disable=W0613
        l = self.pipe.readline().decode('utf8', 'replace')
        if l:
            self.line_callback(l)
        else:
            if self.proc.poll() is not None:
                self.loop.remove_handler(self.pipe)
