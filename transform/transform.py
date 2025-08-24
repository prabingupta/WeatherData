import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time


def create_spark_session():
    """Create SparkSession with proper memory and partition settings."""
    return SparkSession.builder \
        .appName("USWeatherTransform") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .getOrCreate()


def load_and_transform(spark, input_dir, output_dir, logger):
    """
    Load CSV(s), clean, handle nulls, transform, and save stage1 & stage2 Parquet files.
    """

    # Define schema for dataset
    weather_schema = T.StructType([
        T.StructField("EventId", T.StringType(), True),
        T.StructField("Type", T.StringType(), True),
        T.StructField("Severity", T.StringType(), True),
        T.StructField("StartTime(UTC)", T.StringType(), True),
        T.StructField("EndTime(UTC)", T.StringType(), True),
        T.StructField("Precipitation(in)", T.FloatType(), True),
        T.StructField("TimeZone", T.StringType(), True),
        T.StructField("AirportCode", T.StringType(), True),
        T.StructField("LocationLat", T.FloatType(), True),
        T.StructField("LocationLng", T.FloatType(), True),
        T.StructField("City", T.StringType(), True),
        T.StructField("County", T.StringType(), True),
        T.StructField("State", T.StringType(), True),
        T.StructField("ZipCode", T.StringType(), True)
    ])

    logger.info("Loading weather CSV files into Spark DataFrame...")
    csv_path = os.path.join(input_dir, "*.csv")
    df = spark.read.csv(csv_path, header=True, schema=weather_schema)

    # Data Cleaning & Handling Nulls
    df = df.withColumn("StartTime(UTC)", F.to_timestamp("StartTime(UTC)", "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("EndTime(UTC)", F.to_timestamp("EndTime(UTC)", "yyyy-MM-dd HH:mm:ss"))

    df = df.withColumn("StartTime(UTC)", F.coalesce(F.col("StartTime(UTC)"), F.lit("2000-01-01 00:00:00"))) \
           .withColumn("EndTime(UTC)", F.coalesce(F.col("EndTime(UTC)"), F.lit("2000-01-01 00:00:00")))

    # Filter rows with missing critical info
    df = df.filter(F.col("AirportCode").isNotNull() & F.col("State").isNotNull())

    # Filling numeric nulls
    numeric_cols = ["Precipitation(in)", "LocationLat", "LocationLng"]
    for col in numeric_cols:
        df = df.withColumn(col, F.coalesce(F.col(col).cast(T.FloatType()), F.lit(0.0)))

    # Filling string nulls
    string_cols = ["Type", "Severity", "City", "County", "TimeZone", "ZipCode"]
    for col in string_cols:
        df = df.withColumn(col, F.coalesce(F.col(col), F.lit("Unknown")))

    # Ensure EventId is unique
    df = df.withColumn("EventId", F.when(F.col("EventId").isNull(),
                                         F.monotonically_increasing_id().cast(T.StringType()))
                                     .otherwise(F.col("EventId")))

    # Remove duplicates based on EventId
    df = df.dropDuplicates(["EventId"])
    logger.info(f"Stage 1: Cleaned & null-handled data count = {df.count()}")

    # Stage 1: Save cleaned data
    stage1_path = os.path.join(output_dir, "stage1", "weather_cleaned")
    os.makedirs(os.path.dirname(stage1_path), exist_ok=True)
    df = df.repartition(8).persist()
    df.write.mode("overwrite").partitionBy("State").parquet(stage1_path)
    logger.info(f"Stage 1 cleaned data saved at {stage1_path}")

    # Stage 2: Aggregations
    agg_df = df.groupBy("State", "AirportCode", "Type").agg(
        F.count("*").alias("event_count"),
        F.avg("Precipitation(in)").alias("avg_precipitation"),
        F.max("Precipitation(in)").alias("max_precipitation"),
        F.min("Precipitation(in)").alias("min_precipitation")
    )
    logger.info(f"Stage 2: Aggregated data count = {agg_df.count()}")

    # Stage 2: Save aggregated data
    stage2_path = os.path.join(output_dir, "stage2", "aggregated_weather")
    os.makedirs(os.path.dirname(stage2_path), exist_ok=True)
    agg_df.write.mode("overwrite").partitionBy("State").parquet(stage2_path)
    logger.info(f"Stage 2 aggregated data saved at {stage2_path}")

    return df, agg_df


if __name__ == "__main__":
    try:
        logger = setup_logging("weather_transform.log")

        if len(sys.argv) != 3:
            print("Usage: python transform.py <input_dir> <output_dir>")
            sys.exit(1)

        input_dir = sys.argv[1]
        output_dir = sys.argv[2]

        if not os.path.exists(input_dir):
            logger.error(f"Input directory {input_dir} does not exist")
            sys.exit(1)

        os.makedirs(output_dir, exist_ok=True)

        logger.info("Weather Transformation stage started")
        start = time.time()

        spark = create_spark_session()
        load_and_transform(spark, input_dir, output_dir, logger)
        spark.stop()

        end = time.time()
        logger.info("Weather Transformation stage completed")
        logger.info(f"Total time taken {format_time(end - start)}")

    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
