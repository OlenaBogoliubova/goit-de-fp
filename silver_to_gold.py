from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp


def process_gold(spark):
    """Об'єднує дані, агрегує середні значення та записує у gold-зону."""

    # Зчитуємо дані з silver-таблиці
    athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
    athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

    # Перейменовуємо country_noc у athlete_bio, щоб уникнути конфлікту
    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

    # Об'єднуємо таблиці за athlete_id
    joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

    # Виправлення: залишаємо одну колонку country_noc
    joined_df = joined_df.drop("bio_country_noc")

    # Агрегуємо середні значення по sport, medal, sex, country_noc
    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")  # Часова мітка виконання
    )

    # Шлях до збереження у gold-зоні
    output_path = "/tmp/gold/avg_stats"

    # Запис у gold-зону
    aggregated_df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")

    # ✅ Вивід фінального DataFrame
    print(f"Preview of aggregated avg_stats data:")
    aggregated_df.show(5, truncate=False)  # Показати перші 5 рядків


def silver_to_gold():
    """Обробка даних silver -> gold."""
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    process_gold(spark)  # Запускаємо процес перетворення

    spark.stop()


if __name__ == "__main__":
    silver_to_gold()