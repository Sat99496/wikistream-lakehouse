import glob
import duckdb

RECENTCHANGES = "data/silver/recentchanges/*/*/recentchanges.parquet"
PAGEVIEWS = "data/silver/pageviews_top/*/*/pageviews_top.parquet"

def main():
    rc_files = sorted(glob.glob(RECENTCHANGES))
    pv_files = sorted(glob.glob(PAGEVIEWS))

    if not rc_files:
        raise SystemExit("No recentchanges parquet found under data/silver/recentchanges/")
    if not pv_files:
        raise SystemExit("No pageviews parquet found under data/silver/pageviews_top/")

    con = duckdb.connect(database=":memory:")

    con.execute(f"CREATE VIEW recentchanges AS SELECT * FROM read_parquet('{RECENTCHANGES}')")
    con.execute(f"CREATE VIEW pageviews_top AS SELECT * FROM read_parquet('{PAGEVIEWS}')")

    print("\nTop 10 users by edits (recentchanges):")
    print(con.execute("""
        SELECT user, COUNT(*) AS edits
        FROM recentchanges
        WHERE user IS NOT NULL AND user <> ''
        GROUP BY user
        ORDER BY edits DESC
        LIMIT 10
    """).fetchdf())

    print("\nTop 10 pages by views (pageviews_top):")
    print(con.execute("""
        SELECT article, MAX(views) AS views
        FROM pageviews_top
        GROUP BY article
        ORDER BY views DESC
        LIMIT 10
    """).fetchdf())

    print("\nBot vs Human edits:")
    print(con.execute("""
        SELECT bot, COUNT(*) AS edits
        FROM recentchanges
        GROUP BY bot
        ORDER BY edits DESC
    """).fetchdf())

if __name__ == "__main__":
    main()
