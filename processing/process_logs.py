import joblib
import boto3
import pandas as pd
import numpy as np
from pyspark.sql.functions import when
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BooleanType
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

# --- Chargement du modèle ML depuis S3 ---
BUCKET_NAME = "log-analytics-achraf-2026"
s3 = boto3.client("s3")
s3.download_file(BUCKET_NAME, "models/isolation_forest_v1.joblib", "/tmp/isolation_forest_v1.joblib")
s3.download_file(BUCKET_NAME, "models/scaler_v1.joblib", "/tmp/scaler_v1.joblib")

ml_model = joblib.load("/tmp/isolation_forest_v1.joblib")
ml_scaler = joblib.load("/tmp/scaler_v1.joblib")

@pandas_udf(BooleanType())
def is_ml_anomaly(total_logs: pd.Series, avg_latency_ms: pd.Series, error_rate: pd.Series) -> pd.Series:
    X = pd.DataFrame({
        "total_logs": total_logs.fillna(0),
        "avg_latency_ms": avg_latency_ms.fillna(0),
        "error_rate": error_rate.fillna(0)
    })
    X_scaled = ml_scaler.transform(X)
    predictions = ml_model.predict(X_scaled)  # -1 = anomalie, 1 = normal
    return pd.Series(predictions == -1)

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

# --- Détection combinée : seuil fixe OU modèle ML ---
ANOMALY_THRESHOLD = 0.30
metrics_with_ml = metrics.withColumn(
    "ml_anomaly",
    is_ml_anomaly(col("total_logs"), col("avg_latency_ms"), col("error_rate"))
)

anomalies = metrics_with_ml.filter(
    (col("error_rate") > ANOMALY_THRESHOLD) | (col("ml_anomaly") == True)
).withColumn(
    "anomaly_type",
    when(col("error_rate") > ANOMALY_THRESHOLD, "high_error_rate")
    .otherwise("ml_detected_anomaly")
).withColumn("window_start", col("window.start").cast(StringType())) \
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
