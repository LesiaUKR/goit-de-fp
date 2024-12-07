from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

# Визначення базового шляху до проекту (вказано в docker-compose.yaml)
BASE_PATH = os.getenv('BASE_PATH', '/opt/airflow/dags')

# Опис DAG (Directed Acyclic Graph)
"""
Цей DAG визначає багатоступеневий пайплайн для обробки даних у Data Lake.

Кроки пайплайна:
1. Landing to Bronze: Завантаження та попередня обробка даних.
2. Bronze to Silver: Очищення та підготовка даних для аналітики.
3. Silver to Gold: Агрегація та запис кінцевих даних у форматі Gold Layer.

Використовується:
- `BashOperator` для виконання Python-скриптів на кожному етапі.

"""

# Визначення базових аргументів для DAG
default_args = {
    'owner': 'airflow',  # Власник DAG
    'start_date': days_ago(1),  # Початок роботи DAG (вчора для зручності тестування)
}

# Ініціалізація DAG
dag = DAG(
    'etl_pipeline_data_lake',  # Ідентифікатор DAG
    default_args=default_args,  # Базові аргументи
    description='ETL pipeline for transforming data across Bronze, Silver, and Gold layers in the Data Lake.',  # Опис
    schedule_interval=None,  # DAG запускається вручну (немає розкладу)
    tags=['lesia_soloviova'],  # Теги для пошуку
)

# Завдання для запуску landing_to_bronze.py
landing_to_bronze = BashOperator(
    task_id='landing_to_bronze',  # Унікальний ідентифікатор завдання
    bash_command=f'python {BASE_PATH}/landing_to_bronze.py',  # Команда Bash для виконання
    dag=dag,  # DAG, до якого належить завдання
)

# Завдання для запуску bronze_to_silver.py
bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',  # Унікальний ідентифікатор завдання
    bash_command=f'python {BASE_PATH}/bronze_to_silver.py',  # Команда Bash для виконання
    dag=dag,  # DAG, до якого належить завдання
)

# Завдання для запуску silver_to_gold.py
silver_to_gold = BashOperator(
    task_id='silver_to_gold',  # Унікальний ідентифікатор завдання
    bash_command=f'python {BASE_PATH}/silver_to_gold.py',  # Команда Bash для виконання
    dag=dag,  # DAG, до якого належить завдання
)

# Визначення послідовності виконання завдань
"""
landing_to_bronze >> bronze_to_silver >> silver_to_gold

Цей порядок визначає, що:
1. Спочатку виконується `landing_to_bronze` (завантаження даних у Bronze Layer).
2. Після цього виконується `bronze_to_silver` (очищення даних у Silver Layer).
3. Завершальним етапом є `silver_to_gold` (агрегація даних у Gold Layer).
"""
landing_to_bronze >> bronze_to_silver >> silver_to_gold
