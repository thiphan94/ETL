import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType,FloatType, TimestampType, ShortType, DateType, BooleanType

#create SparkSession
def initialize_Spark():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("first etl") \
        .getOrCreate()

    return spark

#Reading CSV without using Schema
def loadDFWithoutSchema(spark):

    df = spark.read.format("csv").option("header", "true").load("../data/london.csv")

    return df

#Reading CSV using user-defined Schema
def loadDFWithSchema(spark):

    schema = StructType([
        StructField("timestamp", DateType(), True),
        StructField("cnt", LongType(), True),
        StructField("t1", FloatType(), True),
        StructField("t2", FloatType(), True),
        StructField("hum", FloatType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("weather_code", LongType(), True),
        StructField("is_holiday", BooleanType(), True),
        StructField("season", LongType(), True)
    ])

    df = spark \
        .read \
        .format("csv") \
        .schema(schema)         \
        .option("header", "true") \
        .load("../data/london.csv")

    return df