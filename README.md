A simple wrapper around Apache Spark spark-submit command.

#### Description
sparklight is a library for submitting Spark jobs either locally or to a cluster.

It is designed to provide easy access for setting up and submitting Spark jobs, removing the complexity of the command-line-interface.

#### Requirements
* Apache Spark
* pyspark

#### Installation
Clone this repository then run the setup.sh.
```
git clone git@github.com:dsmiff/sparklight.git
./setup.sh
```

#### Examples
* examples/cars_submit.py: Submits a simple spark job to perform a groupBy on the cars.csv dataset

#### TODO
* Submit to cluster functionality
* HDFS interface
* DAG jobs
