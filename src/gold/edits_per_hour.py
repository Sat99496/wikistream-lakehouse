from pathlib import Path
import duckdb

OUT = Path("data/gold/edits_per_hour.parquet")


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")

    con.execute("""
        CREATE VIEW rc AS
        SELECT * FROM read_parquet('data/silver/recentchanges/*/*/recentchanges.parquet')
    """)

    con.execute(f"""
        COPY (
            SELECT
                strftime(to_timestamp(timestamp), '%Y-%m-%d %H:00:00') AS hour_bucket,
                COUNT(*) AS edits
            FROM rc
            GROUP BY 1
            ORDER BY 1
        ) TO '{OUT.as_posix()}' (FORMAT PARQUET);
    """)

    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
