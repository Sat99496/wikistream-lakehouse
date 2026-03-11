from pathlib import Path
import subprocess
import sys


def latest_partition_file(glob_path: str) -> Path:
    files = sorted(Path(".").glob(glob_path))
    if not files:
        raise SystemExit(f"No files found for: {glob_path}")
    return files[-1]


def parse_dt_hour(p: Path):
    parts = p.parts
    dt = [x for x in parts if x.startswith("dt=")][-1].split("=", 1)[1]
    hour = [x for x in parts if x.startswith("hour=")][-1].split("=", 1)[1]
    return dt, hour


def run(cmd):
    print("+", " ".join(cmd))
    subprocess.check_call(cmd)


def main():
    rc = latest_partition_file("data/bronze/recentchanges/dt=*/hour=*/recentchanges.jsonl")
    pv = latest_partition_file("data/bronze/pageviews_top/dt=*/hour=*/pageviews_top.json")

    rc_dt, rc_hour = parse_dt_hour(rc)
    pv_dt, pv_hour = parse_dt_hour(pv)

    run([sys.executable, "-m", "src.transform.recentchanges_to_parquet", "--dt", rc_dt, "--hour", rc_hour])
    run([sys.executable, "-m", "src.transform.pageviews_top_to_parquet", "--dt", pv_dt, "--hour", pv_hour])


if __name__ == "__main__":
    main()
