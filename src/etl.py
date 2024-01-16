import pyspark
from pyspark.sql import SparkSession

#create SparkSession
def initialize_Spark():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("first etl") \
        .getOrCreate()

    return spark

#read CSV
