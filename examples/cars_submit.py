import os
import sys
import logging
import argparse
import datetime
try:
    import sparklight as sl
except ImportError:
    raise "Unable to import spark-light"

##__________________________________________________________________||
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_file = 'job_cars_{:%Y%m%d}.log'.format(datetime.datetime.now())
handler = logging.FileHandler(log_file, 'w')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

##__________________________________________________________________||
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--data-path', help='Path to the directory containing the data', default=None)
parser.add_argument('-c', '--convert', help="Convert training data in from PySpark pickle format to numpy 'npz' format.", default=False)
parser.add_argument('-t', '--testsetids', help="List of test set session ids in RDD", default=None)
parser.add_argument('-o', '--outdir', help='Output directory', default=None)
args = parser.parse_args()

##__________________________________________________________________||
log_stem = 'cars'
LOG_STORE = '/tmp/cars_spark/logs'

##__________________________________________________________________||
other_args = {'input' : args.data_path,
              'output': args.outdir}

##__________________________________________________________________||
# Declare SparkJobSet
job_set = sl.SparkJobSet(
    exe = 'test_spark.py',
    copy_exe = False,
    filename = os.path.join('./', 'cars_job.spark'),
    out_dir = LOG_STORE, out_file = log_stem + '.out',
    err_dir = LOG_STORE, err_file = log_stem + '.err',
    log_dir = LOG_STORE, log_file = log_stem + '.log',
    cores = 1,
    memory = '1g',
    disk = '10000',
    certificate = True,
    spark_master='local',
    logger=logger,
    other_args=other_args,
    dry_run=True,
)

job = sl.SparkJob(
    name='cars',
    args=None,
    input_files=[],
    output_files=[]
)

job_set.add_job(job)

##__________________________________________________________________||
for job in job_set:
    logger.info("Job has name: {name}".format(name=job.name))
    print("Job has name: ", job.name)

##__________________________________________________________________||    
job_set.submit()

##__________________________________________________________________||
