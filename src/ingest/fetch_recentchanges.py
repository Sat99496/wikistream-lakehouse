import argparse
import json
import time
from pathlib import Path
import requests

from src.ingest.storage import current_partition, bronze_dir, ensure_dir

URL = "https://stream.wikimedia.org/v2/stream/recentchange"

def iter_sse_data(resp):
    buf = []
    for raw in resp.iter_lines(decode_unicode=True):
        if raw is None:
            continue
        line = raw.strip()
        if line == "":
            if buf:
                yield "\n".join(buf)
                buf = []
            continue
        if line.startswith("data:"):
            buf.append(line[len("data:"):].lstrip())

def main(limit: int, timeout_sec: int, out: str | None) -> None:
    headers = {
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "User-Agent": "wikistream-lakehouse (contact: your-email@gmail.com)",
    }

    part = current_partition()
    out_path = Path(out) if out else (bronze_dir("recentchanges", part) / "recentchanges.jsonl")
    ensure_dir(out_path.parent)

    if out_path.exists():
        print(f"Skip (already exists): {out_path}")
        return

    start = time.time()
    count = 0

    with requests.get(URL, stream=True, timeout=30, headers=headers) as resp:
        resp.raise_for_status()
        with open(out_path, "w", encoding="utf-8") as f:
            for payload in iter_sse_data(resp):
                try:
                    obj = json.loads(payload)
                except json.JSONDecodeError:
                    continue

                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                count += 1

                if count >= limit:
                    break
                if (time.time() - start) > timeout_sec:
                    break

    print(f"Wrote {count} events to {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--timeout-sec", type=int, default=30)
    ap.add_argument("--out", type=str, default=None)
    args = ap.parse_args()
    main(args.limit, args.timeout_sec, args.out)
