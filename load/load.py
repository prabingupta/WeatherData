import os
import sys
import time
import psycopg2
from pyspark.sql import SparkSession
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time



# Spark Session with JDBC driver
def create_spark_session():
    """Create and return a SparkSession with PostgreSQL driver and clean logging."""
    spark = SparkSession.builder \
        .appName("USWeatherLoad") \
        .config("spark.jars", "file:///Users/prabinkumargupta/Downloads/postgresql-42.7.7.jar") \
        .config("spark.driver.extraClassPath", "/Users/prabinkumargupta/Downloads/postgresql-42.7.7.jar") \
        .getOrCreate()
    
    # Suppress internal Spark INFO logs, show only WARN or ERROR
    spark.sparkContext.setLogLevel("WARN")
    return spark



# Creating PostgreSQL tables

def create_postgres_tables(logger, db_user, db_password, db_host, db_name, db_port):
    """Create PostgreSQL tables if they don't exist."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cursor = conn.cursor()
        logger.debug("Successfully connected to PostgreSQL database")

        # Stage 1: Raw weather data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_raw (
                id VARCHAR(50),
                source TEXT,
                event_type TEXT,
                severity TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                tz TEXT,
                airport_code VARCHAR(10),
                city TEXT,
                state VARCHAR(10),
                lat FLOAT,
                lng FLOAT,
                location TEXT,
                distance FLOAT,
                direction TEXT,
                magnitude FLOAT,
                magnitude_type TEXT,
                flood_cause TEXT,
                category TEXT,
                county TEXT,
                begin_range TEXT,
                end_range TEXT,
                episode_id VARCHAR(50),
                event_id VARCHAR(50) PRIMARY KEY
            );
        """)

        # Stage 2: Aggregated weather data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_aggregates (
                state VARCHAR(10),
                airport_code VARCHAR(10),
                event_type TEXT,
                event_count INT,
                avg_magnitude FLOAT,
                max_magnitude FLOAT,
                min_magnitude FLOAT
            );
        """)

        conn.commit()
        logger.info("PostgreSQL tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



# Loading Parquet data into PostgreSQL

def load_to_postgres(logger, spark, input_dir, db_user, db_password, db_host, db_name, db_port):
    """Load Parquet files into PostgreSQL tables."""
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    try:
        # Stage 1: Cleaned weather data
        stage1_path = os.path.join(input_dir, "stage1", "weather_cleaned")
        if not os.path.exists(stage1_path):
            logger.warning(f"Stage1 path not found: {stage1_path}")
        else:
            df_stage1 = spark.read.parquet(stage1_path)
            df_stage1.write.mode("overwrite").jdbc(
                url=jdbc_url,
                table="weather_raw",
                properties=connection_properties
            )
            logger.info(f"Loaded {df_stage1.count()} records into weather_raw table")

        # Stage 2: Aggregated weather data
        stage2_path = os.path.join(input_dir, "stage2", "aggregated_weather")
        if not os.path.exists(stage2_path):
            logger.warning(f"Stage2 path not found: {stage2_path}")
        else:
            df_stage2 = spark.read.parquet(stage2_path)
            df_stage2.write.mode("overwrite").jdbc(
                url=jdbc_url,
                table="weather_aggregates",
                properties=connection_properties
            )
            logger.info(f"Loaded {df_stage2.count()} records into weather_aggregates table")

    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")




if __name__ == "__main__":
    try:
        logger = setup_logging("weather_load.log")

        if len(sys.argv) != 7:
            print("Usage: python load.py <input_dir> <db_user> <db_password> <db_host> <db_name> <db_port>")
            sys.exit(1)

        input_dir, db_user, db_password, db_host, db_name, db_port = sys.argv[1:7]

        if not os.path.exists(input_dir):
            print(f"Error: Input directory {input_dir} does not exist")
            sys.exit(1)

        logger.info("Load stage started")
        start_time = time.time()

        spark = create_spark_session()
        create_postgres_tables(logger, db_user, db_password, db_host, db_name, db_port)
        load_to_postgres(logger, spark, input_dir, db_user, db_password, db_host, db_name, db_port)

        spark.stop()

        end_time = time.time()
        logger.info("Load stage completed")
        logger.info(f"Total time taken: {format_time(end_time - start_time)}")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
