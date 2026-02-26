import argparse
import json
import time
import requests

URL = "https://stream.wikimedia.org/v2/stream/recentchange"

def iter_sse_data(resp):
    """
    Minimal SSE parser: yields full payloads from `data:` lines (joined until blank line).
    """
    buf = []
    for raw in resp.iter_lines(decode_unicode=True):
        if raw is None:
            continue
        line = raw.strip()

        # blank line => end of event
        if line == "":
            if buf:
                yield "\n".join(buf)
                buf = []
            continue

        if line.startswith("data:"):
            buf.append(line[len("data:"):].lstrip())

def main(limit: int, out: str, timeout_sec: int) -> None:
    headers = {
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "User-Agent": "wikistream-lakehouse (contact: your-email@gmail.com)",
    }

    start = time.time()
    count = 0

    with requests.get(URL, stream=True, timeout=30, headers=headers) as resp:
        resp.raise_for_status()

        with open(out, "w", encoding="utf-8") as f:
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

    print(f"Wrote {count} events to {out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--out", type=str, default="data_samples/recentchanges_sample.jsonl")
    ap.add_argument("--timeout-sec", type=int, default=30)
    args = ap.parse_args()
    main(args.limit, args.out, args.timeout_sec)
