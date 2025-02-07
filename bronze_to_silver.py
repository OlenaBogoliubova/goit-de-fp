from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re
import os
import sys

# Явно вказуємо Python для Spark (важливо для Windows та Mac)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# Функція очищення тексту (видаляє всі символи, крім літер, цифр, ком, крапок, лапок)
def clean_text(text):
    if text is None:  # Перевірка на None
        return ""
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))
# Spark UDF для очищення тексту


clean_text_udf = udf(clean_text, StringType())


def process_table(spark, table):
    """Зчитуємо дані із bronze, чистимо, видаляємо дублікатні рядки та записуємо у silver."""
    input_path = f"/tmp/bronze/{table}"  # Звідки зчитуємо
    output_path = f"/tmp/silver/{table}"  # Куди зберігаємо

    # Зчитуємо Parquet у Spark DataFrame
    df = spark.read.parquet(input_path)

    # ✅ Чистимо тільки текстові колонки
    for col_name in df.columns:
        if isinstance(df.schema[col_name].dataType, StringType):
            print(f"Cleaning column: {col_name}")  # Логування перед обробкою
            df = df.withColumn(col_name, clean_text_udf(col(col_name)))

    # Видаляємо дублікатні рядки
    df = df.dropDuplicates()

    # Записуємо очищені дані у silver-зону
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")

    # ✅ Вивід фінального DataFrame (перевіряємо 5 рядків)
    print(f"Preview of cleaned {table} data:")
    df.show(5, truncate=False)


def bronze_to_silver():
    """Обробка всіх таблиць з bronze у silver."""
    tables = ["athlete_bio", "athlete_event_results"]

    # Створюємо одну Spark-сесію для всіх таблиць
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    for table in tables:
        process_table(spark, table)  # Виконуємо очищення + дедублікацію + запис у silver

    spark.stop()


if __name__ == "__main__":
    bronze_to_silver()
