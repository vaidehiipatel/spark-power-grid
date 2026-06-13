from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window

spark = SparkSession.builder \
    .appName("SmartPowerGrid") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

zone_map = spark.read.csv("static/zone_mapping.csv", header=True)

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
schema = StructType([
    StructField("timestamp",   TimestampType(), True),
    StructField("meter_id",    StringType(),    True),
    StructField("consumption", DoubleType(),    True),
])

raw_stream = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", "3") \
    .csv("stream_input/")

watermarked = raw_stream.withWatermark("timestamp", "1 hours")

windowed_avg = watermarked.groupBy(
    window(col("timestamp"), "5 minutes", "1 minute"),
    col("meter_id")
).agg(avg("consumption").alias("avg_consumption"))

enriched = windowed_avg.join(zone_map, on="meter_id", how="left")

query = enriched.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("power_data") \
    .trigger(processingTime="10 seconds") \
    .start()

import time

print("\n Smart Power Grid Monitor running...")
print("   Watching stream_input/ for new files\n")

batch_num = 0
while True:
    time.sleep(15)
    batch_num += 1

    df = spark.sql("SELECT * FROM power_data")
    total = df.count()
    print(f"\n[Check {batch_num}] Records in window table: {total}")

    if total == 0:
        print("   No data yet, waiting...")
        continue

    industrial = df.filter(col("zone_type") == "industrial") \
        .groupBy("window") \
        .agg(avg("avg_consumption").alias("industrial_avg")) \
        .withColumnRenamed("window", "ind_window")

    residential = df.filter(col("zone_type") == "residential")

    final = residential.join(
        industrial,
        residential["window"] == industrial["ind_window"],
        how="left"
    )

    alerts = final.filter(
        col("avg_consumption") > col("industrial_avg")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("meter_id").alias("zone_id"),
        col("zone_type"),
        col("avg_consumption"),
        col("industrial_avg")
    ).orderBy("window_start")

    alert_count = alerts.count()
    print(f"   Alerts firing: {alert_count}")

    if alert_count > 0:
        print("\n===== GRID ANOMALY ALERTS =====")
        alerts.show(20, truncate=False)
        print("================================\n")
        break

query.awaitTermination()
