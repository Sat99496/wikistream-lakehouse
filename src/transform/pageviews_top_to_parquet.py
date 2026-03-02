import argparse
import json
from pathlib import Path

import pandas as pd

from src.ingest.storage import current_partition, bronze_dir, ensure_dir

# If you added silver_dir() to storage.py, import it; otherwise define here.
try:
    from src.ingest.storage import silver_dir
except ImportError:
    def silver_dir(dataset, part):
        return Path("data") / "silver" / dataset / f"dt={part.dt}" / f"hour={part.hour}"


def main(dt: str | None, hour: str | None, out: str | None) -> None:
    part = current_partition()
    if dt is not None:
        part = type(part)(dt=dt, hour=part.hour)
    if hour is not None:
        part = type(part)(dt=part.dt, hour=hour)

    in_path = bronze_dir("pageviews_top", part) / "pageviews_top.json"
    if not in_path.exists():
        raise FileNotFoundError(f"Missing bronze input: {in_path}")

    out_path = Path(out) if out else (silver_dir("pageviews_top", part) / "pageviews_top.parquet")
    ensure_dir(out_path.parent)

    if out_path.exists():
        print(f"Skip (already exists): {out_path}")
        return

    with open(in_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for day in data.get("items", []):
        for a in day.get("articles", []):
            rows.append({
                "project": day.get("project"),
                "access": day.get("access"),
                "year": day.get("year"),
                "month": day.get("month"),
                "day": day.get("day"),
                "rank": a.get("rank"),
                "article": a.get("article"),
                "views": a.get("views"),
            })

    df = pd.DataFrame(rows)
    df.to_parquet(out_path, index=False)

    print(f"Read:  {in_path}")
    print(f"Wrote: {out_path} ({len(df)} rows)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", type=str, default=None)
    ap.add_argument("--hour", type=str, default=None)
    ap.add_argument("--out", type=str, default=None)
    args = ap.parse_args()
    main(args.dt, args.hour, args.out)
