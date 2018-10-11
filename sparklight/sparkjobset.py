import logging
import os
import sys
import re
from subprocess import check_call
from collections import OrderedDict
import sparklight as sl

logger = logging.getLogger(__name__)

##__________________________________________________________________||
class SparkJobSet(object):

    def __init__(self,
                 exe,
                 copy_exe=True,
                 setup_script=None,
                 filename='jobs.spark',
                 out_dir='logs', out_file='tmp.out',
                 err_dir='logs', err_file='tmp.err',
                 log_dir='logs', log_file='tmp.log',                
                 cores=1, memory='1GB', disk='100MB',
                 driver_class_path=None,
                 spark_master=None,
                 certificate=False,
                 dry_run=False,
                 other_args=None):

        super(SparkJobSet, self).__init__()
        self.exe = exe
        self.copy_exe= copy_exe
        self.setup_script = setup_script
        self.filename = os.path.abspath(filename)
        self.out_dir = os.path.realpath(str(out_dir))
        self.out_file = str(out_file)
        self.err_dir = os.path.realpath(str(err_dir))
        self.err_file = str(err_file)
        self.log_dir = os.path.realpath(str(log_dir))
        self.log_file = str(log_file)
        self.cores = cores
        self.memory = memory
        self.disk = disk
        self.driver_class_path = driver_class_path        
        self.spark_master = spark_master
        self.certificate = certificate
        self.other_args = other_args
        self.dry_run = dry_run
        self.init_spark_settings()

        self.jobs = OrderedDict()
        self.cmds = [ ]

    def __getitem__(self, i):
        if isinstance(i, int):
            if i >= len(self):
                raise IndexError()
            return self.jobs.values()[i]
        elif isinstance(i, slice):
            return self.jobs.values()[i]
        else:
            raise TypeError('Invalid argument type - must be int or slice')

    def __len__(self):
        return len(self.jobs)

    def init_spark_settings(self):
        self.master_cmd = ['--master']
        self.driver_class_cmd=['--driver-class-path']
        self.driver_memory_cmd = ['--driver-memory']
        self.executor_cores_cmd = ['--executor-cores']
        self.conf_cmd = ['--conf spark.driver.maxResultSize=2g']
        
    def make_master_cmd(self):
        if not self.master:
            self.master_cmd = None
        else:
            self.master_cmd = generate_cmd(
                self.master_cmd, self.master)
            self.cmds.append(self.master_cmd)
            
    def make_driver_class_cmd(self):
        if not self.driver_class_path:
            self.driver_class_cmd = None
        else:
            self.driver_class_cmd = generate_cmd(
                self.driver_class_cmd, self.driver_class_path)
            self.cmds.append(self.driver_class_cmd)
            
    def make_driver_memory_cmd(self):
        if not self.memory:
            self.driver_memory_cmd = None
        else:
            self.driver_memory_cmd = generate_cmd(
                self.driver_memory_cmd, self.memory)
            self.cmds.append(self.driver_memory_cmd)
            
    def make_cores_cmd(self):
        if not self.cores:
            self.executor_cores_cmd = None
        else:
            self.executor_cores_cmd = generate_cmd(
                self.executor_cores_cmd, self.cores)
            self.cmds.append(self.executor_cores_cmd)
            
    def add_job(self, job):
        if not isinstance(job, sl.SparkJob):
            raise TypeError("Added a job that is not an instance of SparkJob")

        if job.name in self.jobs:
            raise KeyError("Job %s already exists in SparkJobSet" % job.name)

        self.jobs[job.name] = job
        job.manager = self    
    
    def parse_cmd_args(self):
        if not self.exe:
            sys.exit("Unable to submit job - need a executor script")
            
        cmd_str = SparkJobSet.generate_cmd(self.cmds, self.exe)
                
        return cmd_str
        
    @staticmethod
    def generate_cmd(main_cmd, value=None):
        if value:
            if isinstance(value, list):
                value = ' '.join(value)
            cmd = '{0}{1}'.format(' '.join(main_cmd), value)
        else:
            cmd = '{0}'.format(' '.join(main_cmd))
        return cmd
    
    @staticmethod
    def dry_run_cmds(cmds):
        print ' '.join(cmds)
        
    def submit(self):
        cmd_args = self.parse_cmd_args()
        cmd_to_execute = ['spark-submit', cmd_args]

        if self.dry_run:
            SparkJobSet.dry_run_cmds(cmd_to_execute)
            sys.exit("Exiting the service from a dry run")
        
        check_call(cmd_to_execute)
            
##__________________________________________________________________||