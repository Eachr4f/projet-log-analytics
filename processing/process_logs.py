from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, sum as spark_sum, when, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("LogProcessor") \
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Récupération des credentials AWS depuis l'environnement (voir étape 15) ---
import os
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-west-3.amazonaws.com")  # adapte ta région

log_schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("service", StringType(), True),
    StructField("level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("http_code", IntegerType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("ip", StringType(), True)
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:30092") \
    .option("subscribe", "logs-raw") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), log_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# --- Calcul des métriques par fenêtre ---
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

# --- Détection d'anomalie : seuil simple sur le taux d'erreur ---
ANOMALY_THRESHOLD = 0.30  # 30% d'erreurs sur la fenêtre = anomalie

anomalies = metrics.filter(col("error_rate") > ANOMALY_THRESHOLD) \
    .withColumn("anomaly_type", lit("high_error_rate")) \
    .withColumn("window_start", col("window.start").cast(StringType())) \
    .withColumn("window_end", col("window.end").cast(StringType())) \
    .select("service", "window_start", "window_end", "total_logs",
            "error_count", "error_rate", "anomaly_type")

# --- Sortie 1 : afficher les métriques dans la console (comme Jour 2) ---
query_console = metrics.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

# --- Sortie 2 : publier les anomalies vers Kafka (topic logs-anomalies) ---
anomalies_kafka = anomalies.select(
    to_json(struct("*")).alias("value")
)

query_anomalies = anomalies_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:30092") \
    .option("topic", "logs-anomalies") \
    .option("checkpointLocation", "/tmp/checkpoints/anomalies") \
    .outputMode("update") \
    .start()

# --- Sortie 3 : archiver les logs bruts parsés vers S3 en Parquet ---
BUCKET_NAME = "log-analytics-achraf-2026"

query_s3 = df_parsed.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{BUCKET_NAME}/logs-archive/") \
    .option("checkpointLocation", "/tmp/checkpoints/s3-archive") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()

spark.streams.awaitAnyTermination()
