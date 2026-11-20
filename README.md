# Fleet Telemetry Aggregator (Apache Spark)

PySpark batch pipeline that aggregates vehicle GPS telemetry into **geohash / time-window idle-zone and distance metrics**, then writes partitioned Parquet summaries.

**Live demo (job output):** https://gargv837.github.io/fleet-telemetry-spark/

Built to mirror production patterns used on a live logistics telemetry estate — partition-pruned reads, shuffle-aware distance calculation, and idempotent batch outputs that keep heavy analytics off OLTP PostgreSQL.

## Demo results (sample run)

| Metric | Value |
|---|---|
| Input GPS events | 37,500 |
| Vehicles | 50 |
| Days / partitions | 5 |
| Aggregate rows | 25,930 |
| Distinct geohash buckets | 5,157 |
| Total distance | 45,730 km |
| Avg idle ratio | 0.109 |

Browse the rendered output table and CSV on the [demo page](https://gargv837.github.io/fleet-telemetry-spark/).

## Stack

- Python 3.10+
- Apache Spark 3.5 / PySpark
- Parquet I/O

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Generate sample partitioned GPS events
python src/generate_sample_data.py --vehicles 50 --days 5 --pings-per-vehicle-day 150

# Run the aggregation job (local Spark)
python src/aggregate_telemetry.py \
  --input data/raw \
  --output output/aggregates \
  --idle-speed-kmh 3.0
```

## What the job does

1. Reads partitioned GPS events (`date=YYYY-MM-DD/...`).
2. Derives geohash buckets and 1-hour time windows.
3. Flags idle points (speed below threshold).
4. Computes consecutive-ping distance per vehicle (window lag + haversine).
5. Aggregates per `(vehicle_id, geohash, window)` and writes partitioned Parquet.

## Interview talking points

- Why partition by date before geohash aggregation
- Shuffle cost of joining consecutive pings for distance
- How DLQ / recompute maps to Spark lineage + recomputation
- How this relates to keeping heavy analytics off an OLTP PostgreSQL path
