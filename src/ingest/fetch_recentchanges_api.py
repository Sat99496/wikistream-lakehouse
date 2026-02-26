import argparse
import json
import requests

API = "https://en.wikipedia.org/w/api.php"

def main(limit: int, out: str) -> None:
    params = {
        "action": "query",
        "list": "recentchanges",
        "rcprop": "title|ids|timestamp|user|comment|flags|sizes|tags",
        "rclimit": limit,
        "format": "json",
    }
    r = requests.get(API, params=params, timeout=30, headers={
        "User-Agent": "wikistream-lakehouse (contact: your-real-email)"
    })
    r.raise_for_status()
    data = r.json()

    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Wrote {limit} recent changes to {out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--out", type=str, default="data_samples/recentchanges_api_sample.json")
    args = ap.parse_args()
    main(args.limit, args.out)
