from sparklight.sparkjobset import SparkJobSet
from sparklight.sparkjob import SparkJob

import logging
try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())
