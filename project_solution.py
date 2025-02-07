from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Налаштування DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
    'retries': 1,  # Повторити завдання у випадку невдачі
}

# Визначаємо DAG
with DAG(
        'olena_dag',  # Ім'я DAG
        default_args=default_args,
        schedule_interval=None,  # Запуск тільки вручну
        catchup=False,
        tags=["olena"]
) as dag:

    # Завдання 1: Завантаження даних (Landing -> Bronze)
    landing_to_bronze = SparkSubmitOperator(
        application='dags/olena_b/landing_to_bronze.py',
        task_id='landing_to_bronze',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    # Завдання 2: Очистка даних (Bronze -> Silver)
    bronze_to_silver = SparkSubmitOperator(
        application='dags/olena_b/bronze_to_silver.py',
        task_id='bronze_to_silver',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    # Завдання 3: Агрегація даних (Silver -> Gold)
    silver_to_gold = SparkSubmitOperator(
        application='dags/olena_b/silver_to_gold.py',
        task_id='silver_to_gold',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

# Послідовність виконання завдань

landing_to_bronze >> bronze_to_silver >> silver_to_gold

