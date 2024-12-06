import sys
import os
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType, StringType
from topics import athlete_event_results, enriched_athlete_avg


os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Додаємо шлях до кореневої директорії (для доступу до configs.py)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Імпортуємо конфігурацію Kafka
from configs import kafka_config

# Пункт 1: Зчитування даних фізичних показників атлетів з MySQL
# Налаштування SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_event_results"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
jdbc_driver = "com.mysql.cj.jdbc.Driver"

# Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .appName("JDBCToKafka") \
    .master("local[*]") \
    .getOrCreate()

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
print("=== Data from MySQL: athlete_event_results ===")
jdbc_df.show(5, truncate=False)

# Читання даних з SQL бази даних athlete_bio
athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password,
    partitionColumn='athlete_id',
    lowerBound=1,
    upperBound=1000000,
    numPartitions='10'
).load()
print("=== Raw Athlete Bio Data ===")
athlete_bio_df.show(5, truncate=50)

# Пункт 2: Фільтрація даних за зрістом і вагою
athlete_bio_df = athlete_bio_df.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull()) &
    (col("height").cast("double").isNotNull()) & (col("weight").cast("double").isNotNull())
)
print("=== Filtered Athlete Bio Data ===")
athlete_bio_df.show(5, truncate=50)

# Відправка даних до Kafka
jdbc_df.selectExpr("CAST(result_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]) \
    .option("topic", athlete_event_results) \
    .save()
print("Data sent to Kafka topic:", athlete_event_results)

# Пункт 3: Зчитування даних результатів змагань з Kafka
# Визначення схеми для JSON-даних
schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Читання даних з Kafka
kafka_streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]) \
    .option("subscribe", athlete_event_results) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .option('failOnDataLoss', 'false') \
    .load() \
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")) \
    .withColumn("value", regexp_replace(col("value"), "^\"|\"$", "")) \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.athlete_id", "data.sport", "data.medal")

print("=== Kafka Streaming Data ===")
kafka_streaming_df.printSchema()

# Виведення деяких даних із Kafka у вигляді таблиці
formatted_kafka_df = kafka_streaming_df.select("athlete_id", "sport", "medal")
formatted_kafka_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Пункт 4: Об'єднання з біологічними даними за athlete_id
# Об'єднання стрімінгового DataFrame з широкою трансляцією
joined_df = kafka_streaming_df.join(broadcast(athlete_bio_df), "athlete_id")

print("=== Joined Data ===")
joined_df.printSchema()

# Виведення об'єднаних даних у вигляді таблиці
formatted_joined_df = joined_df.select(
    "athlete_id", "sport", "medal", "name", "sex", "born", "height", "weight",
    "country", "country_noc", "description", "special_notes"
)
formatted_joined_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Пункт 5: Розрахунок середніх значень
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

# Пункт 6: Функція для запису даних у Kafka та базу даних
def foreach_batch_function(df, epoch_id):
    print(f"=== Foreach Batch Function: Epoch {epoch_id} ===")

    # Запис у Kafka
    df.selectExpr(
        "CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_config["bootstrap_servers"]
    ).option(
        "kafka.security.protocol", kafka_config["security_protocol"]
    ).option(
        "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
    ).option(
        "kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]
    ).option(
        "topic", enriched_athlete_avg
    ).save()

    print(f"=== Data successfully sent to Kafka topic: {enriched_athlete_avg} ===")
    df.show(20, truncate=False)

    # Запис у MySQL
    df.write.format("jdbc").options(
        url="jdbc:mysql://217.61.57.46:3306/neo_data",
        driver=jdbc_driver,
        dbtable="lesia_soloviova_enriched_athlete_avg",
        user=jdbc_user,
        password=jdbc_password,
    ).mode("append").save()

aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(foreach_batch_function) \
    .option("checkpointLocation", "checkpoint_dir") \
    .start() \
    .awaitTermination()
