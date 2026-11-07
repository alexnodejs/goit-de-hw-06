# Етапи 2-5: середні за ковзним вікном -> перевірка за умовами з CSV -> алерти в Kafka.
import os
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from configs import ALERTS_TOPIC, SENSORS_TOPIC, kafka_config

BASE_DIR = Path(__file__).parent
CONDITIONS_CSV = BASE_DIR / "alerts_conditions.csv"
CHECKPOINT_DIR = BASE_DIR / "checkpoints"

KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
SENSOR_SCHEMA = "id INT, temperature DOUBLE, humidity DOUBLE, timestamp TIMESTAMP"
CONDITIONS_SCHEMA = (
    "id INT, humidity_min DOUBLE, humidity_max DOUBLE, "
    "temperature_min DOUBLE, temperature_max DOUBLE, code STRING, message STRING"
)
NOT_USED = -999  # така межа в CSV не застосовується до цього алерту
TRIGGER_INTERVAL = "10 seconds"


def create_spark_session():
    # Воркери мають брати той самий Python, що й драйвер, а не перший python3 з PATH.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    return (
        SparkSession.builder.appName("SensorAlerts")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # Типові 200 партицій на один локальний потік дають лише зайві stateful-задачі.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_conditions(spark):
    # cache(): умови читаються один раз на старті, а не в кожному мікробатчі.
    # Нові умови застосовуються перезапуском, код змінювати не треба.
    return spark.read.csv(str(CONDITIONS_CSV), header=True, schema=CONDITIONS_SCHEMA).cache()


def read_sensor_stream(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("subscribe", SENSORS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        # Некоректний JSON дає null-поля: у вікна він не потрапить і потік не зупинить.
        .select(F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("reading"))
        .select("reading.*")
    )


def window_averages(readings):
    return (
        readings.withWatermark("timestamp", "10 seconds")
        .groupBy(F.window("timestamp", "1 minute", "30 seconds"))
        .agg(F.avg("temperature").alias("t_avg"), F.avg("humidity").alias("h_avg"))
    )


def in_range(value, low, high):
    return ((low == NOT_USED) | (value >= low)) & ((high == NOT_USED) | (value <= high))


def find_alerts(averages, conditions):
    return averages.crossJoin(conditions).where(
        in_range(F.col("t_avg"), F.col("temperature_min"), F.col("temperature_max"))
        & in_range(F.col("h_avg"), F.col("humidity_min"), F.col("humidity_max"))
    )


def to_kafka_messages(alerts):
    return alerts.select(
        F.to_json(
            F.struct(
                "window",
                F.round("t_avg", 2).alias("t_avg"),
                F.round("h_avg", 2).alias("h_avg"),
                "code",
                "message",
                F.current_timestamp().cast("string").alias("timestamp"),
            )
        ).alias("value")
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    conditions = load_conditions(spark)
    print("Умови алертів:")
    conditions.show(truncate=False)

    averages = window_averages(read_sensor_stream(spark))

    # Режим append: вікно виходить один раз, коли watermark його закрив. Тому середнє
    # пораховане за повним вікном, а алерт не дублюється на кожному мікробатчі.
    (
        averages.writeStream.format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("checkpointLocation", str(CHECKPOINT_DIR / "averages"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    (
        to_kafka_messages(find_alerts(averages, conditions))
        .writeStream.format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("topic", ALERTS_TOPIC)
        .option("checkpointLocation", str(CHECKPOINT_DIR / "alerts"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"Читаю {SENSORS_TOPIC}, алерти пишу в {ALERTS_TOPIC}. Ctrl+C — зупинити.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
