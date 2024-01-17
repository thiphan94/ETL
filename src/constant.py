from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    FloatType,
    StringType,
)


# create SparkSession
def initialize_Spark():
    spark = SparkSession.builder.master("local[*]").appName("first etl").getOrCreate()

    return spark


# Define the schema for csv
SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("Gender", StringType(), True),
        StructField("Age", LongType(), True),
        StructField("Driving_License", LongType(), True),
        StructField("Region_Code", FloatType(), True),
        StructField("Previously_Insured", LongType(), True),
        StructField("Vehicle_Age", StringType(), True),
        StructField("Vehicle_Damage", StringType(), True),
        StructField("Annual_Premium", FloatType(), True),
        StructField("Policy_Sales_Channel", FloatType(), True),
        StructField("Vintage", LongType(), True),
        StructField("Response", LongType(), True),
    ]
)

# name for columns
INSURANCE_COLS = {
    "id": "id",
    "Gender": "gender",
    "Age": "age",
    "Driving_License": "driving_license",
    "Region_Code": "region_code",
    "Previously_Insured": "previously_insured",
    "Vehicle_Age": "vehicle_age",
    "Vehicle_Damage": "vehicle_damage",
    "Annual_Premium": "annual_premium",
    "Policy_Sales_Channel": "policy_sales_channel",
    "Vintage": "vintage",
    "Response": "response",
}
