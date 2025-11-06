from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, from_json, explode, length, from_csv, to_date, regexp_replace, trim, split, from_unixtime, to_timestamp, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, ArrayType, LongType, IntegerType
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
    StructField("acq_date", StringType(), True),
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
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "fires-topic") \
    .option("startingOffsets", "earliest") \
    .option("kafka.group.id", "fire_stream_consumer") \
    .option("failOnDataLoss", "false") \
    .load()

fires_df = fires_stream \
    .selectExpr("CAST(value AS STRING) as value") \
    .filter(col("value").isNotNull()) \
    .withColumn("clean_value", trim(regexp_replace(col("value"), r'[\r"]', ""))) \
    .withColumn("lines", split(col("clean_value"), "\n")) \
    .withColumn("line", explode(col("lines"))) \
    .filter(~col("line").startswith("latitude")) \
    .filter(length(col("line")) > 10) \
    .withColumn("parsed", from_csv(col("line"), fires_schema.simpleString())) \
    .select("parsed.*") \
    .withColumn("acq_date", to_date(col("acq_date"), "yyyy-MM-dd")) \
    .withColumn("event_type", lit("fire")) \
    .withColumn("timestamp", current_timestamp()) \
    .withWatermark("timestamp", "1 hour")

earthquakes_stream = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'redpanda:9092') \
    .option("subscribe", "earthquakes-topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

earthquakes_df = earthquakes_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), earthquakes_schema).alias("data")) \
    .select(explode("data.features").alias("feature")) \
    .select(
        col("feature.properties.mag").alias("magnitude"),
        col("feature.properties.place").alias("place"),
        col("feature.properties.time").alias("time"),
        col("feature.geometry.coordinates")[0].alias("longitude"),
        col("feature.geometry.coordinates")[1].alias("latitude"),
        col("feature.geometry.coordinates")[2].alias("depth"),
        lit("earthquake").alias("event_type")
    ) \
    .filter(col("time").isNotNull()) \
    .filter(col("time") > 0) \
    .withColumn("event_timestamp", from_unixtime(col("time") / 1000).cast("timestamp")) \
    .filter(col("event_timestamp").isNotNull()) \
    .withColumn("event_date", to_date(col("event_timestamp"))) \
    .withColumn("processed_at", current_timestamp()) \
    .withWatermark("event_timestamp", "10 minutes")

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
        .option("compression", "none") \
        .parquet("/opt/spark-data/fires") \

fires_query = fires_df \
    .writeStream \
    .foreachBatch(writes_fire_batch) \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/fires") \
    .trigger(processingTime='30 seconds') \
    .start()

def writes_earthquake_batch(df, batch_id):
    print_batch_info(df, batch_id)
    df.write \
        .mode("append") \
        .partitionBy("event_date") \
        .option("compression", "none") \
        .parquet("/opt/spark-data/earthquakes") \

earthquakes_query = earthquakes_df \
    .writeStream \
    .foreachBatch(writes_earthquake_batch) \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/earthquakes") \
    .trigger(processingTime='30 seconds') \
    .start()

print("Streaming queries started...")
print("Fires data will be written to: /opt/spark-data/fires")
print("Earthquakes data will be written to: /opt/spark-data/earthquakes")

spark.streams.awaitAnyTermination()