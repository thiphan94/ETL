from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, BooleanType


def rename_cols(df: DataFrame, mapping_dict: dict) -> DataFrame:
    """rename name all columns

    :param df: input dataframe
    :param mapping_dict: dict of columns names
    :return: ouput dataframe
    """
    for key in mapping_dict.keys():
        df = df.withColumnRenamed(key, mapping_dict.get(key))
    return df


def clean_data(df: DataFrame) -> DataFrame:
    """drop column and transform column
    :param df: input dataframe
    :return: ouput dataframe
    """
    df_dropped = df.drop("id")

    df_dropped = df_dropped.withColumn(
        "gender",
        F.dense_rank().over(Window.orderBy("gender")),
    )

    df_dropped = df_dropped.withColumn(
        "vehicle_age", F.dense_rank().over(Window.orderBy("vehicle_age"))
    )

    df_dropped = df_dropped.withColumn(
        "vehicle_damage", F.dense_rank().over(Window.orderBy("vehicle_damage"))
    )

    # Convert type of columns
    df_dropped = df_dropped.withColumn(
        "gender", df_dropped["gender"].cast(IntegerType())
    )
    df_dropped = df_dropped.withColumn(
        "driving_license", df_dropped["driving_license"].cast(BooleanType())
    )

    df_dropped = df_dropped.withColumn(
        "region_code", df_dropped["region_code"].cast(IntegerType())
    )
    df_dropped = df_dropped.withColumn(
        "previously_insured", df_dropped["previously_insured"].cast(BooleanType())
    )
    df_dropped = df_dropped.withColumn(
        "vehicle_age", df_dropped["vehicle_age"].cast(IntegerType())
    )
    df_dropped = df_dropped.withColumn(
        "vehicle_damage", df_dropped["vehicle_damage"].cast(BooleanType())
    )
    df_dropped = df_dropped.withColumn(
        "annual_premium", df_dropped["annual_premium"].cast(IntegerType())
    )
    df_dropped = df_dropped.withColumn(
        "policy_sales_channel", df_dropped["policy_sales_channel"].cast(IntegerType())
    )
    df_dropped = df_dropped.withColumn(
        "response", df_dropped["response"].cast(BooleanType())
    )

    df_final = df_dropped
    return df_final
