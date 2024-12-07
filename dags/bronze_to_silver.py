from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType
import os

# Створюємо сесію Spark з назвою "BronzeToSilver".
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

def clean_text(df):
    """
    Очищає текстові дані у всіх колонках DataFrame.

    Ця функція перевіряє тип кожної колонки у DataFrame. Якщо тип колонки є StringType,
    то значення у цій колонці будуть очищені шляхом:
    - Обрізання зайвих пробілів з початку і кінця рядка.
    - Переведення тексту у нижній регістр.

    Параметри:
    df (pyspark.sql.DataFrame): Вхідний DataFrame для очищення.

    Повертає:
    pyspark.sql.DataFrame: DataFrame з очищеними текстовими колонками.

    """
    for column in df.columns:
        if df.schema[column].dataType == StringType():  # Перевірка, чи тип даних - StringType
            df = df.withColumn(column, trim(lower(col(column))))  # Очищення та нормалізація тексту
    return df

# Список таблиць, які потрібно обробити (bronze-таблиці).
tables = ["athlete_bio", "athlete_event_results"]

# Ітерація по кожній таблиці з метою очищення і збереження у silver-шар.
for table in tables:
    # Читання parquet-файлів із bronze-шару для поточної таблиці.
    df = spark.read.parquet(f"/tmp/bronze/{table}")

    # Очищення тексту за допомогою функції `clean_text`.
    df = clean_text(df)
    # Видалення дублікатів у DataFrame.
    df = df.dropDuplicates()

    # Створення шляху для збереження оброблених даних у silver-шар.
    output_path = f"/tmp/silver/{table}"
    os.makedirs(output_path, exist_ok=True)  # Переконуємося, що папка існує

    # Запис оброблених даних у parquet-файл у silver-шар із режимом "overwrite".
    df.write.mode("overwrite").parquet(output_path)

    # Виведення повідомлення про успішне збереження оброблених даних.
    print(f"Data saved to {output_path}")

    # Повторне читання збережених даних для перевірки та їх відображення.
    df = spark.read.parquet(output_path)
    df.show(truncate=False)  # Відображення вмісту DataFrame без обрізання тексту

# Завершення роботи Spark-сесії.
spark.stop()
