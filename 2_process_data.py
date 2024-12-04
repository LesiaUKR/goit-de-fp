from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StructField, StructType
from configs import kafka_config
from pyspark.sql.types import StringType
import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# # Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_event_results"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# # Створення Spark сесії
spark = SparkSession.builder \2
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .appName("JDBCToKafka") \
    .master("local[*]") \
    .getOrCreate()
# Перевірка версій бібліотек
print("Spark version:", spark.version)

# Читання даних з SQL бази даних athlete_event_results
jdbc_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password,
    partitionColumn='result_id',
    lowerBound=1,
    upperBound=1000000,
    numPartitions='10'
).load()

# Print intermediate data from MySQL
print("=== Data from MySQL: athlete_event_results ===")
jdbc_df.show(5, truncate=False)

# Читання даних з SQL бази даних athlete_bio
athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password,
    partitionColumn='athlete_id',  # Колонка для розділення даних на партиції
    lowerBound=1,  # Нижня межа значень колонки
    upperBound=1000000,  # Верхня межа значень колонки
    numPartitions='10'  # Кількість партицій
).load()

# Print intermediate data from MySQL
print("=== Data from MySQL: athlete_bio ===")
athlete_bio_df.show(5, truncate=False)

# Відфільтрування даних, де показники зросту та ваги є порожніми або не є числами
athlete_bio_df = athlete_bio_df.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull()) &
    (col("height").cast("double").isNotNull()) & (col("weight").cast("double").isNotNull())
)
# Print filtered athlete_bio data
print("=== Filtered Data from MySQL: athlete_bio ===")
athlete_bio_df.show(5, truncate=False)

# Відправка даних до Kafka
jdbc_df .selectExpr("CAST(result_id AS STRING) AS key", "to_json(struct(*)) AS value") \
     .write \
     .format("kafka") \
     .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
     .option("kafka.security.protocol", "SASL_PLAINTEXT") \
     .option("kafka.sasl.mechanism", "PLAIN") \
     .option("kafka.sasl.jaas.config",
             'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
     .option("topic", "lesia_topic_athlete_event_results") \
     .save()

# Визначення схеми для JSON-даних
schema = StructType([
     StructField("athlete_id", IntegerType(), True),
     StructField("sport", StringType(), True),
     StructField("medal", StringType(), True),
     StructField("timestamp", StringType(), True)
])
# Читання даних з Kafka у стрімінговий DataFrame
kafka_streaming_df = spark.readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
     .option("kafka.security.protocol", "SASL_PLAINTEXT") \
     .option("kafka.sasl.mechanism", "PLAIN") \
     .option("kafka.sasl.jaas.config",
             'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
     .option("subscribe", "lesia_topic_athlete_event_results") \
     .option("startingOffsets", "earliest") \
     .option("maxOffsetsPerTrigger", "5") \
     .option('failOnDataLoss', 'false') \
     .load() \
     .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")) \
     .withColumn("value", regexp_replace(col("value"), "^\"|\"$", "")) \
     .selectExpr("CAST(value AS STRING)") \
     .select(from_json(col("value"), schema).alias("data")) \
     .select("data.athlete_id", "data.sport", "data.medal")

# Виведення отриманих даних на екран
kafka_streaming_df.writeStream \
     .trigger(availableNow=True) \
     .outputMode("append") \
     .format("console") \
     .option("truncate", "false") \
     .start() \
     .awaitTermination()

# Об'єднання даних з результатами змагань з біологічними даними
joined_df = kafka_streaming_df.join(athlete_bio_df, "athlete_id")

print("=== Joined Data (athlete_bio + athlete_event_results) ===")
joined_df.show(5, truncate=False)

# Обчислення середнього зросту і ваги атлетів
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
     .agg(
     avg("height").alias("avg_height"),
     avg("weight").alias("avg_weight"),
     current_timestamp().alias("timestamp")
 )

# Print aggregated results
print("=== Aggregated Data: Average Height and Weight by Sport, Medal, Gender, and Country ===")
aggregated_df.show(5, truncate=False)

# Функція для запису даних у Kafka та базу даних
def foreach_batch_function(df, epoch_id):
     # Запис даних до Kafka
     df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value") \
         .write \
         .format("kafka") \
         .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
         .option("kafka.security.protocol", "SASL_PLAINTEXT") \
         .option("kafka.sasl.mechanism", "PLAIN") \
         .option("kafka.sasl.jaas.config",
                 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
         .option("topic", "lesia_topic_enriched_athlete_avg") \
         .save()

     # Запис даних до бази даних
     df.write.format('jdbc').options(
         url="jdbc:mysql://217.61.57.46:3306/neo_data",
         driver='com.mysql.cj.jdbc.Driver',
         dbtable="lesia_soloviova_enriched_athlete_avg",
         user=jdbc_user,
         password=jdbc_password).mode('append').save()


# Запуск стрімінгу
aggregated_df.writeStream \
     .outputMode("complete") \
     .foreachBatch(foreach_batch_function) \
     .option("checkpointLocation", "/path/to/checkpoint/dir") \
     .start() \
     .awaitTermination()