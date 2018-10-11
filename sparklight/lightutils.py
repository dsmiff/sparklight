import logging
import os

logger = logging.getLogger(__name__)

##__________________________________________________________________||
def check_directory(directory):
    if not os.path.isdir(directory):
        if os.path.isfile(directory):
            raise IOError('%s is already a file, cannot make dir' % directory)
        logger.info("Making directory %s", directory)
        if os.path.abspath(directory).startswith('/hdfs'):
            check_call(['hadoop', 'fs', '-mkdir', '-p', os.path.abspath(directory).replace('/hdfs', '')])
        else:
            os.makedirs(directory)

##__________________________________________________________________||            
