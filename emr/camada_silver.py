"""
EMR Serverless - Silver Layer
Reads raw CSV from S3, applies transformations and validations,
writes partitioned Parquet and registers in Glue Catalog.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

BUCKET = "[YOUR_BUCKET]"
INPUT_PATH = f"s3://{BUCKET}/data/fedex.csv"
OUTPUT_PATH = f"s3://{BUCKET}/output/fedex_silver/"
GLUE_DATABASE = "dbt_landing"
GLUE_TABLE = "fedex_silver"


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .enableHiveSupport()
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .csv(path)
    )


def filter_invalid_times(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("Actual_Shipment_Time").isNotNull()) &
        (col("Planned_Shipment_Time").isNotNull()) &
        (col("Planned_Delivery_Time").isNotNull()) &
        (col("Actual_Shipment_Time") > 0) &
        (col("Planned_Shipment_Time") > 0)
    )


def convert_timestamps(df: DataFrame) -> DataFrame:
    for col_name in ["Actual_Shipment_Time", "Planned_Shipment_Time", "Planned_Delivery_Time"]:
        df = df.withColumn(col_name,
            to_timestamp(
                concat_ws("-", col("Year"), col("Month"), col("DayofMonth"),
                    lpad(col(col_name).cast("string"), 4, "0")),
                "yyyy-M-d-HHmm"))
    return df


def add_travel_time(df: DataFrame) -> DataFrame:
    return df.withColumn("Travel_Time_Real",
        ((unix_timestamp(col("Actual_Shipment_Time")) -
          unix_timestamp(col("Planned_Shipment_Time"))) / 60).cast("integer"))


def add_route_id(df: DataFrame) -> DataFrame:
    return df.withColumn("Route_ID", concat_ws("-", col("Source"), col("Destination")))


def add_delay_category(df: DataFrame) -> DataFrame:
    return df.withColumn("Delay_Category",
        when(col("Shipment_Delay") <= 0, "OnTime")
        .when(col("Shipment_Delay") <= 15, "Minor")
        .when(col("Shipment_Delay") <= 60, "Moderate")
        .otherwise("Severe"))


def write_parquet_to_glue(df: DataFrame, spark: SparkSession):
    """Write Parquet to S3 and register in Glue Catalog"""
    (
        df.write
        .mode("overwrite")
        .partitionBy("Year", "Month")
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .saveAsTable(f"{GLUE_DATABASE}.{GLUE_TABLE}")
    )


def main():
    spark = create_spark_session("FedEx-Silver-Pipeline")

    df = read_csv(spark, INPUT_PATH)
    df = filter_invalid_times(df)
    df = convert_timestamps(df)
    df = add_travel_time(df)
    df = add_route_id(df)
    df = add_delay_category(df)

    write_parquet_to_glue(df, spark)

    spark.stop()


if __name__ == "__main__":
    main()
