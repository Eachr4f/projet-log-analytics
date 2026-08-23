from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import window, count, avg, sum as spark_sum, when

spark = SparkSession.builder \
    .appName("LogProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma correspondant exactement aux champs du générateur.py
log_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("service", StringType(), True),
    StructField("level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("http_code", IntegerType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("ip", StringType(), True)
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs-raw") \
    .option("startingOffsets", "latest") \
    .load()

# Parsing du JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), log_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

# Calcul des métriques par fenêtre de 1 minute, glissante toutes les 10 secondes
metrics = df_parsed \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "1 minute", "10 seconds"),
        col("service")
    ) \
    .agg(
        count("*").alias("total_logs"),
        avg("latency_ms").alias("avg_latency_ms"),
        spark_sum(when(col("level") == "ERROR", 1).otherwise(0)).alias("error_count")
    ) \
    .withColumn("error_rate", col("error_count") / col("total_logs"))

query = metrics.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
