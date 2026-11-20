# Fleet Telemetry Aggregator (Apache Spark)

PySpark batch pipeline that aggregates vehicle GPS telemetry into geohash / time-window idle-zone and distance metrics, then writes Parquet summaries.

Built to mirror production patterns used on a live logistics telemetry estate (partitioned reads, shuffle-aware joins, idempotent batch outputs).

## Stack

- Python 3.10+
- Apache Spark 3.5 / PySpark
- Parquet I/O

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Generate sample partitioned GPS events
python src/generate_sample_data.py

# Run the aggregation job (local Spark)
python src/aggregate_telemetry.py \
  --input data/raw \
  --output output/aggregates \
  --idle-speed-kmh 3.0
```

## What the job does

1. Reads partitioned GPS events (`date=YYYY-MM-DD/...`).
2. Derives geohash buckets and time windows.
3. Flags idle points (speed below threshold).
4. Aggregates per `(vehicle_id, geohash, window)`: ping count, idle ratio, approx distance.
5. Writes partitioned Parquet under `output/aggregates`.

## Interview talking points

- Why partition by date before geohash aggregation
- Shuffle cost of joining consecutive pings for distance
- How DLQ / recompute maps to Spark lineage + recomputation
- How this relates to keeping heavy analytics off an OLTP PostgreSQL path
