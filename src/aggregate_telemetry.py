"""
PySpark batch job: aggregate fleet GPS events into idle-zone / distance metrics.

Mirrors the analytical pattern of keeping heavy scans off an OLTP path by
partition-pruned batch aggregation over telemetry.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window, functions as F


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run(input_path: str, output_path: str, idle_speed_kmh: float) -> None:
    spark = build_spark("fleet-telemetry-aggregator")
    spark.sparkContext.setLogLevel("WARN")

    # Partition discovery on date=... folders
    events = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
        .withColumn("event_ts", F.to_timestamp("ts"))
        .withColumn("date", F.to_date("event_ts"))
        .withColumn("is_idle", F.col("speed_kmh") < F.lit(idle_speed_kmh))
        .withColumn("window_col", F.window(F.col("event_ts"), "1 hour"))
        .withColumn("window_start", F.col("window_col.start"))
        .drop("window_col")
    )

    # Approximate consecutive-ping distance per vehicle (shuffle by vehicle_id)
    w = Window.partitionBy("vehicle_id").orderBy("event_ts")
    with_prev = (
        events.withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn(
            "segment_km",
            F.when(
                F.col("prev_lat").isNull(),
                F.lit(0.0),
            ).otherwise(
                # Haversine in Spark SQL (km)
                F.lit(2 * 6371.0)
                * F.asin(
                    F.sqrt(
                        F.pow(F.sin(F.radians(F.col("lat") - F.col("prev_lat")) / 2), 2)
                        + F.cos(F.radians(F.col("prev_lat")))
                        * F.cos(F.radians(F.col("lat")))
                        * F.pow(F.sin(F.radians(F.col("lon") - F.col("prev_lon")) / 2), 2)
                    )
                )
            ),
        )
    )

    aggregates = (
        with_prev.groupBy("vehicle_id", "geohash", "window_start", "date")
        .agg(
            F.count("*").alias("ping_count"),
            F.sum(F.col("is_idle").cast("int")).alias("idle_pings"),
            F.round(F.sum("segment_km"), 3).alias("distance_km"),
            F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
        )
        .withColumn(
            "idle_ratio",
            F.round(F.col("idle_pings") / F.col("ping_count"), 3),
        )
    )

    (
        aggregates.repartition("date")
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(output_path)
    )

    print(f"Wrote aggregates to {output_path}")
    aggregates.orderBy(F.desc("idle_ratio")).show(15, truncate=False)
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw")
    parser.add_argument("--output", default="output/aggregates")
    parser.add_argument("--idle-speed-kmh", type=float, default=3.0)
    args = parser.parse_args()
    run(args.input, args.output, args.idle_speed_kmh)


if __name__ == "__main__":
    main()
