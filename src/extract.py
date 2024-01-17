from constant import SCHEMA


# Reading CSV without using Schema
def loadDFWithoutSchema(spark):
    df = spark.read.format("csv").option("header", "true").load("../ETL/data/train.csv")

    return df


# Reading CSV with using Schema
def loadDFWithSchema(spark):
    df = (
        spark.read.format("csv")
        .schema(SCHEMA)
        .option("header", "true")
        .load("../ETL/data//train.csv")
    )

    return df
