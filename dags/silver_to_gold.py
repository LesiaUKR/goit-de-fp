from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os

# Створення сесії Spark для обробки даних
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

# Завантаження таблиць із silver-шару
athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

# Перейменування колонок для уникнення неоднозначності при об'єднанні
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

# Об'єднання таблиць за колонкою "athlete_id"
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Обчислення середніх значень для кожної групи
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),  # Середній зріст
        avg("weight").alias("avg_weight"),  # Середня вага
        current_timestamp().alias("timestamp")  # Час виконання агрегації
    )

# Створення директорії для збереження результатів у gold-шар
output_path = "/tmp/gold/avg_stats"
os.makedirs(output_path, exist_ok=True)  # Переконуємося, що директорія існує

# Збереження оброблених даних у форматі parquet
aggregated_df.write.mode("overwrite").parquet(output_path)

# Виведення повідомлення про успішне збереження
print(f"Data saved to {output_path}")

# Повторне читання parquet-файлу для перевірки даних
df = spark.read.parquet(output_path)
df.show(truncate=False)  # Виведення вмісту DataFrame без обрізання рядків

# Завершення роботи Spark-сесії
spark.stop()
