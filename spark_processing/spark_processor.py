from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
HDFS_INPUT_PATH = "hdfs://localhost:9000/user/sunbeam/air_quality"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/user/sunbeam/air_quality_processed"

def create_spark_session():
    """Create Spark session with proper configuration"""
    try:
        spark = SparkSession.builder \
            .appName("AirQualityETLProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")  # Reduce verbose logging
        logger.info("✅ Spark session created successfully")
        return spark
        
    except Exception as e:
        logger.error(f"❌ Failed to create Spark session: {e}")
        return None

def define_schema():
    """Define schema for air quality data"""
    return StructType([
        StructField("location", StringType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("localtime", StringType(), True),
        StructField("temp_c", FloatType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("condition", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("co", FloatType(), True),
        StructField("no2", FloatType(), True),
        StructField("o3", FloatType(), True),
        StructField("so2", FloatType(), True),
        StructField("pm2_5", FloatType(), True),
        StructField("pm10", FloatType(), True),
        StructField("processed_timestamp", StringType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_partition", IntegerType(), True)
    ])

def read_data(spark, schema):
    """Read data from HDFS"""
    try:
        logger.info(f"📖 Reading data from: {HDFS_INPUT_PATH}")
        
        df = spark.read \
            .schema(schema) \
            .option("multiline", "true") \
            .json(HDFS_INPUT_PATH)
            
        count = df.count()
        logger.info(f"📊 Successfully read {count} records")
        
        if count == 0:
            logger.warning("⚠️ No data found in HDFS path")
            return None
            
        return df
        
    except Exception as e:
        logger.error(f"❌ Error reading data: {e}")
        return None

def clean_and_transform_data(df):
    """Clean and transform the data"""
    try:
        logger.info("🧹 Starting data cleaning and transformation")
        
        # Remove duplicates and null values for critical fields
        cleaned_df = df.dropDuplicates() \
                      .filter(col("location").isNotNull()) \
                      .filter(col("temp_c").isNotNull()) \
                      .filter(col("timestamp").isNotNull())
        
        # Add derived columns
        transformed_df = cleaned_df.withColumn(
            "air_quality_index", 
            when(col("pm2_5") <= 12, "Good")
            .when(col("pm2_5") <= 35, "Moderate")
            .when(col("pm2_5") <= 55, "Unhealthy for Sensitive Groups")
            .when(col("pm2_5") <= 150, "Unhealthy")
            .when(col("pm2_5") <= 250, "Very Unhealthy")
            .otherwise("Hazardous")
        ).withColumn(
            "temperature_category",
            when(col("temp_c") < 0, "Freezing")
            .when(col("temp_c") < 10, "Cold")
            .when(col("temp_c") < 20, "Cool")
            .when(col("temp_c") < 30, "Warm")
            .otherwise("Hot")
        ).withColumn(
            "processing_date",
            current_date()
        ).withColumn(
            "year",
            year(to_timestamp(col("timestamp")))
        ).withColumn(
            "month",
            month(to_timestamp(col("timestamp")))
        ).withColumn(
            "day",
            dayofmonth(to_timestamp(col("timestamp")))
        ).withColumn(
            "hour",
            hour(to_timestamp(col("timestamp")))
        )
        
        # Calculate pollution score (weighted average of pollutants)
        transformed_df = transformed_df.withColumn(
            "pollution_score",
            round(
                (col("pm2_5") * 0.3 + 
                 col("pm10") * 0.25 + 
                 col("no2") * 0.2 + 
                 col("o3") * 0.15 + 
                 col("co") * 0.05 + 
                 col("so2") * 0.05), 2
            )
        )
        
        logger.info("✅ Data cleaning and transformation completed")
        return transformed_df
        
    except Exception as e:
        logger.error(f"❌ Error in cleaning and transformation: {e}")
        return None

def analyze_data(df):
    """Perform data analysis and show results"""
    try:
        logger.info("📊 Starting data analysis")
        
        # Basic statistics
        print("\n" + "="*50)
        print("📈 DATA ANALYSIS RESULTS")
        print("="*50)
        
        # Show sample data
        print("\n🔍 Sample Data:")
        df.select("location", "temp_c", "humidity", "pm2_5", "air_quality_index", "pollution_score").show(10, truncate=False)
        
        # Temperature statistics by location
        print("\n🌡️ Temperature Statistics by Location:")
        df.groupBy("location") \
          .agg(
              round(avg("temp_c"), 2).alias("avg_temp"),
              round(min("temp_c"), 2).alias("min_temp"),
              round(max("temp_c"), 2).alias("max_temp"),
              count("*").alias("record_count")
          ).show()
        
        # Air quality distribution
        print("\n🌬️ Air Quality Index Distribution:")
        df.groupBy("air_quality_index") \
          .count() \
          .orderBy(desc("count")) \
          .show()
        
        # Pollution statistics
        print("\n☠️ Pollution Statistics:")
        df.select(
            round(avg("pm2_5"), 2).alias("avg_pm2_5"),
            round(avg("pm10"), 2).alias("avg_pm10"),
            round(avg("no2"), 2).alias("avg_no2"),
            round(avg("o3"), 2).alias("avg_o3"),
            round(avg("pollution_score"), 2).alias("avg_pollution_score")
        ).show()
        
        # Hourly patterns
        print("\n⏰ Hourly Data Distribution:")
        df.groupBy("hour") \
          .agg(
              count("*").alias("record_count"),
              round(avg("temp_c"), 2).alias("avg_temp"),
              round(avg("pm2_5"), 2).alias("avg_pm2_5")
          ).orderBy("hour").show()
        
        logger.info("✅ Data analysis completed")
        
    except Exception as e:
        logger.error(f"❌ Error in data analysis: {e}")

def save_processed_data(df):
    """Save processed data back to HDFS"""
    try:
        logger.info(f"💾 Saving processed data to: {HDFS_OUTPUT_PATH}")
        
        # Save as parquet for better performance
        df.write \
          .mode("overwrite") \
          .partitionBy("location", "year", "month") \
          .parquet(HDFS_OUTPUT_PATH)
        
        logger.info("✅ Processed data saved successfully")
        
        # Also save a summary CSV for easy access
        summary_df = df.groupBy("location", "air_quality_index") \
                      .agg(
                          count("*").alias("record_count"),
                          round(avg("temp_c"), 2).alias("avg_temperature"),
                          round(avg("humidity"), 2).alias("avg_humidity"),
                          round(avg("pm2_5"), 2).alias("avg_pm2_5"),
                          round(avg("pollution_score"), 2).alias("avg_pollution_score")
                      )
        
        summary_path = f"{HDFS_OUTPUT_PATH}_summary"
        summary_df.coalesce(1) \
                  .write \
                  .mode("overwrite") \
                  .option("header", "true") \
                  .csv(summary_path)
        
        logger.info(f"✅ Summary data saved to: {summary_path}")
        
    except Exception as e:
        logger.error(f"❌ Error saving processed data: {e}")

def main():
    """Main function to run the Spark processor"""
    logger.info("🚀 Starting Spark Air Quality Data Processor")
    
    # Create Spark session
    spark = create_spark_session()
    if not spark:
        return
    
    try:
        # Define schema
        schema = define_schema()
        
        # Read data
        df = read_data(spark, schema)
        if df is None:
            return
        
        # Clean and transform data
        transformed_df = clean_and_transform_data(df)
        if transformed_df is None:
            return
        
        # Cache for multiple operations
        transformed_df.cache()
        
        # Analyze data
        analyze_data(transformed_df)
        
        # Save processed data
        save_processed_data(transformed_df)
        
        logger.info("✅ Spark processing completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error in Spark processing: {e}")
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("🔒 Spark session stopped")

if __name__ == "__main__":
    main()
