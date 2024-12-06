import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

# Додаємо шлях до кореневої директорії (для доступу до configs.py)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Тепер імпортуємо конфігурацію
from configs import kafka_config

# Налаштування PySpark пакету
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Ініціалізація SparkSession
spark = (SparkSession.builder
         .appName("KafkaSparkConsumer")
         .master("local[*]")
         .getOrCreate())

# Схема для даних
schema = StructType([
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("noc_country", StringType(), True),
    StructField("avg_height", StringType(), True),
    StructField("avg_weight", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Читання даних з Kafka
kafka_streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]) \
    .option("subscribe", "lesia_soloviova_enriched_athlete_avg") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Перетворення Kafka value у таблицю
formatted_df = kafka_streaming_df \
    .selectExpr("CAST(value AS STRING)") \
    .withColumn("value", regexp_replace(col("value"), "\\\\", "")) \
    .withColumn("value", regexp_replace(col("value"), "^\"|\"$", "")) \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Виведення результатів у консоль
query = formatted_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
