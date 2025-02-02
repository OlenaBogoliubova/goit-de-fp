# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, current_timestamp, coalesce, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# 1. Створення Spark сесії
spark = SparkSession.builder \
    .appName("OlympicStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoint_dir") \
    .config("spark.driver.extraClassPath", "C:/Users/Elena/Desktop/MasterIT/DE/FP/mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

# 2. Конфігурація Kafka та MySQL
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "athlete_event_results"
kafka_output_topic = "aggregated_athlete_data"
mysql_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
mysql_user = "neo_data_admin"
mysql_password = "Proyahaxuqithab9oplp"

# 3. Схема для Kafka
schema = StructType([
    StructField("athlete_id", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("country_noc", StringType(), True),
])

# 4. Читання біологічних даних атлетів з MySQL
athlete_bio_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://217.61.57.46:3306/olympic_dataset") \
    .option("dbtable", "athlete_bio") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("user", "neo_data_admin") \
    .option("password", "Proyahaxuqithab9oplp") \
    .load()

# 5. Відображення перших рядків
athlete_bio_df.show()

# 6. Фільтрація та приведення до числового типу
filtered_bio_df = athlete_bio_df.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull()) &
    (col("height").cast("double").isNotNull()) & (col("weight").cast("double").isNotNull())
).withColumnRenamed("country_noc", "bio_country_noc")


# 7. Читання результатів змагань з MySQL
athlete_event_results_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://217.61.57.46:3306/olympic_dataset") \
    .option("dbtable", "athlete_event_results") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("user", "neo_data_admin") \
    .option("password", "Proyahaxuqithab9oplp") \
    .load()

# 8. Приведення athlete_id до числового типу
athlete_event_results_df = athlete_event_results_df.withColumn("athlete_id", col("athlete_id").cast("int"))
athlete_event_results_df.show()

# 9. Запис результатів у Kafka
athlete_event_results_df.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", kafka_topic) \
    .mode("append") \
    .save()


# 10. Читання потоку з Kafka
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

parsed_df = streaming_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*") \
    .withColumn("athlete_id", col("athlete_id").cast("int"))  # Приведення типу


# 11. Об’єднання з біологічними даними
joined_df = parsed_df.alias("p").join(
    broadcast(filtered_bio_df.alias("b")),
    col("p.athlete_id") == col("b.athlete_id"),
    "inner"
)

# 12. Використовуємо `coalesce`, щоб вибрати одну з колонок `country_noc`
joined_df = joined_df.withColumn(
    "country_noc", coalesce(col("p.country_noc"), col("b.bio_country_noc"))
).drop("p.country_noc", "b.bio_country_noc")  # Видаляємо оригінальні колонки


# 13. Обчислення середніх значень
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight")) \
    .withColumn("timestamp", current_timestamp())

# 14. Функція для запису у Kafka та MySQL


def write_to_kafka_and_db(df, epoch_id):
    print(f"Processing batch {epoch_id}")  # Додаємо лог для використання epoch_id
    df.show(10)

    df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_output_topic) \
        .save()

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://217.61.57.46:3306/olympic_dataset") \
        .option("dbtable", "athlete_enrichted_agg_obogol") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", "neo_data_admin") \
        .option("password", "Proyahaxuqithab9oplp") \
        .mode("append") \
        .save()


# 15. Запуск стримінгу


query = aggregated_df.writeStream \
    .foreachBatch(write_to_kafka_and_db) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/aggregated_checkpoint") \
    .start()

query.awaitTermination()
