import argparse
from datetime import date, timedelta

from src.ingest.fetch_pageviews import main as fetch_pageviews_main


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main(start: str, end: str, project: str, access: str) -> None:
    y1, m1, d1 = map(int, start.split("-"))
    y2, m2, d2 = map(int, end.split("-"))
    start_d = date(y1, m1, d1)
    end_d = date(y2, m2, d2)

    for d in daterange(start_d, end_d):
        # out=None => writes into partitioned bronze path for "now"
        fetch_pageviews_main(project, access, d.year, d.month, d.day, out=None)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--project", default="en.wikipedia")
    ap.add_argument("--access", default="all-access")
    args = ap.parse_args()
    main(args.start, args.end, args.project, args.access)
