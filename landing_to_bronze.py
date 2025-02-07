from pyspark.sql import SparkSession
import requests


def download_data(file):
    """Завантажуємо дані"""
    url = f"https://ftp.goit.study/neoversity/{file}.csv"
    local_path = f"{file}.csv"

    print(f"Downloading from {url}")
    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(response.content)
        print(f"File {local_path} downloaded successfully.")
    else:
        raise Exception(f"Failed to download {file}. Status code: {response.status_code}")


def process_table(spark, table):
    """Обробка таблиці CSV, конвертація у Parquet, збереження."""
    local_path = f"{table}.csv"
    output_path = f"/tmp/bronze/{table}"

    print("Processing {table}...")

    df = spark.read.csv(local_path, header=True, inferSchema=True)

    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")

    print(f"Preview of {table} data:")
    df.show(5, truncate=False)


def landing_to_bronze():
   tables = ["athlete_bio", "athlete_event_results"]

    # Create a single Spark session
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    for table in tables:
        download_data(table)
        process_table(spark, table)

    spark.stop()


if __name__ == "__main__":
    landing_to_bronze()
    
