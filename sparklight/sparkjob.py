import logging
import os
import sparklight as sl

logger = logging.getLogger(__name__)

##__________________________________________________________________||
class SparkJob(object):

    def __init__(self, name, args=None,
                 input_files=None, output_files=None):
        super(SparkJob, self).__init__()
        self._manager = None
        self.name = name
        if not args:
            args = [ ]
        self.args = args[:]
        if isinstance(args, str):
            self.args = args.split()
        if not input_files:
            input_files = [ ]
        self.input_files = input_files[:]
        if not output_files:
            output_files = [ ]
        self.output_files = output_files[:]

    def __eq__(self, other):
        return self.name == other.name

    @property
    def manager(self):
        return self._manager

    @manager.setter
    def manager(self, manager):
        if not isinstance(manager, sl.JobSet):
            raise TypeError("Instance type incorrect")
        self._manager = manager
    
##__________________________________________________________________||
