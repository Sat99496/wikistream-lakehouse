import pandas as pd
import json
import time
from pathlib import Path
import argparse
from src.ingest.storage import Partition, bronze_dir, ensure_dir, current_partition

def silver_dir(dataset: str, part: Partition) -> Path:
    return Path("data") / "silver" / dataset / f"dt={part.dt}" / f"hour={part.hour}"

def transform_recentchanges(part: Partition):
    b_dir = bronze_dir("recentchanges", part)
    s_dir = silver_dir("recentchanges", part)
    src_file = b_dir / "recentchanges.jsonl"
    
    if not src_file.exists():
        print(f"No bronze data found for {src_file}")
        return

    df = pd.read_json(src_file, lines=True)
    
    # Selecting core columns for our Silver layer
    cols = ['id', 'type', 'title', 'user', 'bot', 'timestamp', 'wiki']
    df_clean = df[cols] if all(c in df.columns for c in cols) else df

    ensure_dir(s_dir)
    out_path = s_dir / "recentchanges.parquet"
    df_clean.to_parquet(out_path, index=False)
    print(f"Refined to Silver: {out_path}")

def transform_pageviews(part: Partition):
    b_dir = bronze_dir("pageviews_top", part)
    s_dir = silver_dir("pageviews_top", part)
    src_file = b_dir / "pageviews_top.json"

    if not src_file.exists():
        return

    with open(src_file, 'r') as f:
        data = json.load(f)
    
    articles = data['items'][0]['articles']
    df = pd.DataFrame(articles)
    
    ensure_dir(s_dir)
    out_path = s_dir / "pageviews_top.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Refined to Silver: {out_path}")

if __name__ == "__main__":
    part = current_partition()
    print(f"Transforming partition: {part.dt} T{part.hour}")
    transform_recentchanges(part)
    transform_pageviews(part)
