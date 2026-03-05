import glob
from pathlib import Path

import duckdb

OUT = Path("data/gold/pageviews_daily_top.parquet")


def main():
    files = glob.glob("data/silver/pageviews_top/*/*/pageviews_top.parquet")
    if not files:
        raise SystemExit("No silver pageviews parquet found.")

    OUT.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute("CREATE VIEW pv AS SELECT * FROM read_parquet('data/silver/pageviews_top/*/*/pageviews_top.parquet')")

    # For each (year,month,day): top by views
    con.execute(f"""
        COPY (
            SELECT
                year, month, day,
                article,
                views,
                rank
            FROM pv
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY year, month, day
                ORDER BY views DESC
            ) <= 100
        ) TO '{OUT.as_posix()}' (FORMAT PARQUET);
    """)

    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
