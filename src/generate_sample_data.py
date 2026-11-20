"""Generate partitioned sample GPS telemetry for the Spark job."""

from __future__ import annotations

import argparse
import csv
import math
import random
from datetime import datetime, timedelta
from pathlib import Path


def geohash_approx(lat: float, lon: float, precision: int = 6) -> str:
    """Lightweight base32 geohash (enough for demos / bucketing talks)."""
    base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    bits = []
    even = True
    while len(bits) < precision * 5:
        if even:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon >= mid:
                bits.append(1)
                lon_range[0] = mid
            else:
                bits.append(0)
                lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                bits.append(1)
                lat_range[0] = mid
            else:
                bits.append(0)
                lat_range[1] = mid
        even = not even

    chars = []
    for i in range(0, len(bits), 5):
        idx = 0
        for b in bits[i : i + 5]:
            idx = (idx << 1) | b
        chars.append(base32[idx])
    return "".join(chars)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def generate(output_dir: Path, vehicles: int, days: int, pings_per_vehicle_day: int, seed: int) -> None:
    rng = random.Random(seed)
    start = datetime(2026, 1, 1, 0, 0, 0)
    fieldnames = [
        "event_id",
        "vehicle_id",
        "ts",
        "lat",
        "lon",
        "speed_kmh",
        "geohash",
    ]

    for day in range(days):
        day_start = start + timedelta(days=day)
        partition = output_dir / f"date={day_start.date().isoformat()}"
        partition.mkdir(parents=True, exist_ok=True)
        out_path = partition / "events.csv"

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for v in range(vehicles):
                vehicle_id = f"V{v:04d}"
                # Seed each vehicle near Gurgaon / NCR corridor
                lat = 28.40 + rng.uniform(-0.15, 0.15)
                lon = 77.05 + rng.uniform(-0.20, 0.20)
                ts = day_start + timedelta(minutes=rng.randint(0, 120))

                for i in range(pings_per_vehicle_day):
                    # Mostly moving, occasional idle clusters
                    idle = rng.random() < 0.18
                    if idle:
                        speed = rng.uniform(0.0, 2.5)
                        dlat, dlon = rng.uniform(-0.0002, 0.0002), rng.uniform(-0.0002, 0.0002)
                    else:
                        speed = rng.uniform(15.0, 65.0)
                        # ~speed toward a soft heading
                        heading = rng.uniform(0, 2 * math.pi)
                        # ~1 ping / 2 minutes => convert speed to approx degree step
                        step_km = speed * (2 / 60.0)
                        dlat = (step_km / 111.0) * math.cos(heading)
                        dlon = (step_km / (111.0 * math.cos(math.radians(lat)))) * math.sin(heading)

                    prev_lat, prev_lon = lat, lon
                    lat = max(27.8, min(29.0, lat + dlat))
                    lon = max(76.5, min(77.8, lon + dlon))
                    # Keep sampled speed consistent with displacement for realism talks
                    dist = haversine_km(prev_lat, prev_lon, lat, lon)
                    if not idle and dist > 0:
                        speed = max(speed, dist / (2 / 60.0) * 0.7)

                    ts = ts + timedelta(minutes=2)
                    writer.writerow(
                        {
                            "event_id": f"{vehicle_id}-{day_start.date()}-{i:04d}",
                            "vehicle_id": vehicle_id,
                            "ts": ts.isoformat(sep=" "),
                            "lat": f"{lat:.6f}",
                            "lon": f"{lon:.6f}",
                            "speed_kmh": f"{speed:.2f}",
                            "geohash": geohash_approx(lat, lon, precision=6),
                        }
                    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/raw")
    parser.add_argument("--vehicles", type=int, default=40)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--pings-per-vehicle-day", type=int, default=120)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    out = Path(args.output)
    generate(out, args.vehicles, args.days, args.pings_per_vehicle_day, args.seed)
    print(f"Wrote sample telemetry under {out.resolve()}")


if __name__ == "__main__":
    main()
