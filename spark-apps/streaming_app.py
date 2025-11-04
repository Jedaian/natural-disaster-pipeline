from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, from_json, explode, length, split, size
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, DateType, ArrayType, LongType, IntegerType
)
from datetime import datetime

#Spark session
spark = SparkSession.builder.appName("NaturalEventsPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartititions.enabled", "true") \
    .config("spark.sql.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#Define schema for the fires data
fires_schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("bright_ti4", DoubleType(), True),
    StructField("scan", DoubleType(), True),
    StructField("track", DoubleType(), True),
    StructField("acq_date", DateType(), True),
    StructField("acq_time", StringType(), True),
    StructField("satellite", StringType(), True),
    StructField("instrument", StringType(), True),
    StructField("confidence", StringType(), True),
    StructField("version", StringType(), True),
    StructField("bright_ti5", DoubleType(), True),
    StructField("frp", DoubleType(), True),
    StructField("daynight", StringType(), True)
])

earthquakes_schema = StructType([
    StructField("type", StringType(), True),
    StructField("features", ArrayType(StructType([
        StructField("properties", StructType([
            StructField("mag", DoubleType(), True),
            StructField("place", StringType(), True),
            StructField("time", LongType(), True),
            StructField("updated", LongType(), True),
            StructField("tz", IntegerType(), True),
            StructField("url", StringType(), True),
            StructField("detail", StringType(), True),
            StructField("felt", IntegerType(), True),
            StructField("cdi", DoubleType(), True),
            StructField("mmi", DoubleType(), True),
            StructField("alert", StringType(), True),
            StructField("status", StringType(), True),
            StructField("tsunami", IntegerType(), True),
            StructField("sig", IntegerType(), True),
            StructField("net", StringType(), True),
            StructField("code", StringType(), True),
            StructField("ids", StringType(), True),
            StructField("sources", StringType(), True),
            StructField("types", StringType(), True),
            StructField("nst", IntegerType(), True),
            StructField("dmin", DoubleType(), True),
            StructField("rms", DoubleType(), True),
            StructField("gap", DoubleType(), True),
            StructField("magType", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True)
        ]), True),
        StructField("geometry", StructType([
            StructField("type", StringType(), True),
            StructField("coordinates", ArrayType(DoubleType()), True),
        ]), True),
        StructField("id", StringType(), True)
    ])), True)
])

fires_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'redpanda:9092') \
    .option("subscribe", "fires-topic") \
    .option("startingOffsets", "earliest") \
    .load()

fires_df = fires_stream \
    .selectExpr("CAST(value AS STRING) as value") \
    .filter(col("value").isNotNull()) \
    .filter(~col("value").startswith("latitude")) \
    .filter(length(col("value")) > 10) \
    .select(split(col("value"), ",").alias("fields")) \
    .filter(size(col("fields")) == 13) \
    .select(
        col("fields")[0].cast(DoubleType()).alias("latitude"),
        col("fields")[1].cast(DoubleType()).alias("longitude"),
        col("fields")[2].cast(DoubleType()).alias("brightness"),
        col("fields")[3].cast(DoubleType()).alias("scan"),
        col("fields")[4].cast(DoubleType()).alias("track"),
        col("fields")[5].alias("acq_date"),
        col("fields")[6].alias("acq_time"),
        col("fields")[7].alias("satellite"),
        col("fields")[8].alias("confidence"),
        col("fields")[9].alias("version"),
        col("fields")[10].cast(DoubleType()).alias("bright_t31"),
        col("fields")[11].cast(DoubleType()).alias("frp"),
        col("fields")[12].alias("daynight")
    ) \
    .filter(col("latitude").isNotNull() & col("longitude").isNotNull()) \
    .withColumn("event_type", lit("fire")) \
    .withColumn("timestamp", current_timestamp()) \
    .withWatermark("timestamp", "15 minutes")

earthquakes_stream = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'redpanda:9092') \
    .option("subscribe", "earthquakes-topic") \
    .option("startingOffsets", "earliest") \
    .load()

earthquakes_df = earthquakes_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), earthquakes_schema).alias("data")) \
    .select(explode("data.features").alias("feature")) \
    .select(
        col("feature.properties.mag").alias("magnitude"),
        col("feature.geometry.coordinates")[0].alias("longitude"),
        col("feature.geometry.coordinates")[1].alias("latitude"),
        col("feature.geometry.coordinates")[2].alias('depth'),
        lit("earthquake").alias("event_type")
    ) \
    .withColumn("timestamp", current_timestamp()) \
    .withWatermark("timestamp", "1 minute")

def print_batch_info(df, batch_id):
    count = df.count()
    print(f"Batch {batch_id}: {count} records processed at {datetime.now()}")
    if count > 0:
        df.show(5, truncate=False)

def writes_fire_batch(df, batch_id):
    print_batch_info(df, batch_id)
    df.write \
        .mode("append") \
        .partitionBy("acq_date") \
        .parquet("/opt/spark-data/fires") \

fires_query = fires_df \
    .writeStream \
    .foreachBatch(writes_fire_batch) \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/fires") \
    .trigger(processingTime='30 seconds') \
    .start()

def writes_fire_batch(df, batch_id):
    print_batch_info(df, batch_id)
    df.write \
        .mode("append") \
        .option("compression", "none") \
        .parquet("/opt/spark-data/earthquakes") \

earthquakes_query = earthquakes_df \
    .writeStream \
    .foreachBatch(writes_fire_batch) \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/earthquakes") \
    .trigger(processingTime='30 seconds') \
    .start()

print("Streaming queries started...")
print("Fires data will be written to: /opt/spark-data/fires")
print("Earthquakes data will be written to: /opt/spark-data/earthquakes")

spark.streams.awaitAnyTermination()