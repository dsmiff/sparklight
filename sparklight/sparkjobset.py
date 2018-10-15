import os
import sys
import re
from subprocess import call
from collections import OrderedDict
from sparklight.lightutils import check_directory
import sparklight as sl

##__________________________________________________________________||
class SparkJobSet(object):

    def __init__(self,
                 exe,
                 copy_exe=True,
                 setup_script=None,
                 source_setup=False,
                 filename='jobs.spark',
                 out_dir='logs', out_file='tmp.out',
                 err_dir='logs', err_file='tmp.err',
                 log_dir='logs', log_file='tmp.log',                
                 cores=1, memory='1GB', disk='100MB',
                 driver_class_path=None,
                 spark_master=None,
                 certificate=False,
                 logger=None,
                 dry_run=False,
                 other_args={}):

        super(SparkJobSet, self).__init__()
        self.exe = exe
        self.copy_exe= copy_exe
        self.setup_script = setup_script
        self.source_setup = source_setup
        self.filename = os.path.abspath(filename)
        self.out_dir = os.path.realpath(str(out_dir))
        self.out_file = str(out_file)
        self.err_dir = os.path.realpath(str(err_dir))
        self.err_file = str(err_file)
        self.log_dir = os.path.realpath(str(log_dir))
        self.log_file = str(log_file)
        self.dirs = [self.out_dir, self.err_dir, self.log_dir]
        self.cmds = [ ]        
        self.cores = cores
        self.memory = memory
        self.disk = disk
        self.driver_class_path = driver_class_path        
        self.spark_master = spark_master
        self.certificate = certificate
        self.other_args = other_args
        self.logger = logger
        self.dry_run = dry_run
        self.init_spark_settings()
        self.init_spark_commands()
        self.construct_other_args()
        self.check_dirs(self.dirs)
        
        self.jobs = OrderedDict()

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

    def construct_other_args(self):
        if not self.other_args:
            return self.other_args

        for flag, value in self.other_args.iteritems():
            cmd = '--{flag} {value}'.format(flag=flag, value=value)
            self.cmds.append(cmd)        
    
    def check_dirs(self, dirs):
        for directory in dirs:
            if directory: check_directory(directory)
    
    def init_spark_settings(self):
        self.master_cmd = ['--master']
        self.driver_class_cmd=['--driver-class-path']
        self.driver_memory_cmd = ['--driver-memory']
        self.executor_cores_cmd = ['--executor-cores']

    def init_spark_commands(self):
        self.make_master_cmd()
        self.make_driver_class_cmd()
        self.make_driver_memory_cmd()
        self.make_cores_cmd()
        
    def make_master_cmd(self):
        if not self.spark_master:
            self.master_cmd = None
        else:
            self.master_cmd = SparkJobSet.generate_cmd(
                self.master_cmd, self.spark_master)
            self.cmds.append(self.master_cmd)
            
    def make_driver_class_cmd(self):
        if not self.driver_class_path:
            self.driver_class_cmd = None
        else:
            self.driver_class_cmd = SparkJobSet.generate_cmd(
                self.driver_class_cmd, self.driver_class_path)
            self.cmds.append(self.driver_class_cmd)
            
    def make_driver_memory_cmd(self):
        if not self.memory:
            self.driver_memory_cmd = None
        else:
            self.driver_memory_cmd = SparkJobSet.generate_cmd(
                self.driver_memory_cmd, self.memory)
            self.cmds.append(self.driver_memory_cmd)
            
    def make_cores_cmd(self):
        if not self.cores:
            self.executor_cores_cmd = None
        else:
            self.executor_cores_cmd = SparkJobSet.generate_cmd(
                self.executor_cores_cmd, self.cores)
            self.cmds.append(self.executor_cores_cmd)

    def generate_metadata(self):
        self.logger.info("Master: {master} ".format(master=self.spark_master))
        self.logger.info("Cores: {cores} ".format(cores=self.cores))
        self.logger.info("Memory: {mem} ".format(mem=self.memory))
        self.logger.info("Executable: {exe} ".format(exe=self.exe))
            
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
            cmd = '{0} {1}'.format(' '.join(main_cmd), value)
        else:
            cmd = '{0}'.format(' '.join(main_cmd))
        return cmd
    
    @staticmethod
    def dry_run_cmds(cmds):
        print "Command to run: ", cmds
        
    def submit(self):

        self.generate_metadata()
        cmd_args = self.parse_cmd_args()
        spark_execute_command = 'spark-submit'
        cmd_to_execute = '{0} {1}'.format(spark_execute_command, cmd_args)
        
        if self.dry_run:
            SparkJobSet.dry_run_cmds(cmd_to_execute)
            self.logger.info("Exiting the service for a dry run")
            sys.exit("Exiting the service from a dry run")
        
        call(cmd_to_execute, shell=True)

        self.logger.info("Done")
            
##__________________________________________________________________||
