import argparse
import json
import requests

BASE = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top"

def main(project: str, access: str, year: int, month: int, day: int, out: str) -> None:
    url = f"{BASE}/{project}/{access}/{year}/{month:02d}/{day:02d}"
    r = requests.get(url, timeout=30, headers={
        "User-Agent": "wikistream-lakehouse (contact: your-email@gmail.com)"
    })
    r.raise_for_status()
    data = r.json()

    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Fetched: {url}")
    print(f"Wrote to: {out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", type=str, default="en.wikipedia")
    ap.add_argument("--access", type=str, default="all-access")
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--month", type=int, required=True)
    ap.add_argument("--day", type=int, required=True)
    ap.add_argument("--out", type=str, default="data_samples/pageviews_top_sample.json")
    args = ap.parse_args()
    main(args.project, args.access, args.year, args.month, args.day, args.out)
