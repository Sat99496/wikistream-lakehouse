from pathlib import Path
import duckdb

OUT = Path("data/gold/bot_vs_human.parquet")


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
                CASE WHEN bot THEN 'bot' ELSE 'human' END AS editor_type,
                COUNT(*) AS edits
            FROM rc
            GROUP BY 1
        ) TO '{OUT.as_posix()}' (FORMAT PARQUET);
    """)

    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
