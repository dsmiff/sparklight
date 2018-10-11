import os
import argparse
import time
from operator import add
import arrow
from pyspark.sql import SparkSession

if __name__ == '__main__':
    

    spark = SparkSession \
        .builder \
        .appName("Testing my first SparkSession") \
        .getOrCreate()

    cars = spark\
           .read\
           .option("inferSchema", "true")\
           .option("header", "true")\
           .csv("cars.csv")

    cars\
        .groupBy("year")\
        .count()\
        .show()
    

    print cars
