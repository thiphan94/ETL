from constant import SCHEMA


# Reading CSV without using Schema
def loadDFWithoutSchema(spark):
    df = spark.read.format("csv").option("header", "true").load("../data/train.csv")

    return df


def loadDFWithSchema(spark):
    df = (
        spark.read.format("csv")
        .schema(SCHEMA)
        .option("header", "true")
        .load("../data/train.csv")
    )

    return df
