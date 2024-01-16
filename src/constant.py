from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    BooleanType,
    StringType,
)


# create SparkSession
def initialize_Spark():
    spark = SparkSession.builder.master("local[*]").appName("first etl").getOrCreate()

    return spark


# Define the schema
SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("Gender", StringType(), True),
        StructField("Age", LongType(), True),
        StructField("Driving_License", BooleanType(), True),
        StructField("Region_Code", LongType(), True),
        StructField("Previously_Insured", BooleanType(), True),
        StructField("Vehicle_Age", LongType(), True),
        StructField("Vehicle_Damage", BooleanType(), True),
        StructField("Annual_Premium", LongType(), True),
        StructField("Policy_Sales_Channel", LongType(), True),
        StructField("Vintage", LongType(), True),
        StructField("Response", BooleanType(), True),
    ]
)
