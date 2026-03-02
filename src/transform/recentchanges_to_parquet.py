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


def load_jsonl(path: Path) -> list[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def main(dt: str | None, hour: str | None, out: str | None) -> None:
    part = current_partition()
    if dt is not None:
        part = type(part)(dt=dt, hour=part.hour)
    if hour is not None:
        part = type(part)(dt=part.dt, hour=hour)

    in_path = bronze_dir("recentchanges", part) / "recentchanges.jsonl"
    if not in_path.exists():
        raise FileNotFoundError(f"Missing bronze input: {in_path}")

    out_path = Path(out) if out else (silver_dir("recentchanges", part) / "recentchanges.parquet")
    ensure_dir(out_path.parent)

    if out_path.exists():
        print(f"Skip (already exists): {out_path}")
        return

    rows = load_jsonl(in_path)

    def pick(o: dict) -> dict:
        rev = o.get("revision") or {}
        return {
            "timestamp": o.get("timestamp"),
            "type": o.get("type"),
            "wiki": o.get("wiki"),
            "namespace": o.get("namespace"),
            "title": o.get("title"),
            "user": o.get("user"),
            "bot": bool(o.get("bot", False)),
            "minor": bool(o.get("minor", False)),
            "comment": o.get("comment"),
            "revision_new": rev.get("new"),
            "revision_old": rev.get("old"),
            "server_name": o.get("server_name"),
            "raw_json": json.dumps(o, ensure_ascii=False),
        }

    df = pd.DataFrame([pick(r) for r in rows])
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
