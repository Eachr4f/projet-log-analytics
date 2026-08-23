from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaRawConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs-raw") \
    .option("startingOffsets", "latest") \
    .load()

# La valeur du message Kafka arrive en binaire, on la convertit en texte
df_text = df.selectExpr("CAST(value AS STRING) as raw_log")

query = df_text.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
