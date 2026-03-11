# WikiStream Lakehouse

WikiStream Lakehouse is a small data engineering project that ingests Wikimedia data and organizes it using a lakehouse-style architecture.

Current data sources:
- Wikipedia **RecentChanges** event stream
- Wikimedia **Pageviews Top** REST API

The pipeline stores raw data in a Bronze layer and transforms it into structured parquet datasets in a Silver layer.

## Project Goal

The goal of this project is to practice and demonstrate:

- stream and API ingestion
- partitioned data lake storage
- idempotent pipeline design
- Bronze to Silver transformations
- parquet-based analytics workflows

## Architecture

The project follows a layered data design:

## Architecture

The project follows a layered data design:

- **Bronze**: raw ingested JSON/JSONL data from Wikimedia APIs
- **Silver**: cleaned and structured Parquet datasets
- **Gold**: analytics-ready datasets built with DuckDB

## Architecture Diagram
```

                        +--------------------+      +--------------------+
            |   RecentChanges    |      |    Pageviews Top   |
            |     SSE Stream     |      |      REST API      |
            +--------------------+      +--------------------+
                     |                          |
                     +-----------+--------------+
                                 |
                                 v
                        +------------------+
                        |      Bronze      |
                        |   Raw JSON/JSONL |
                        | Partitioned Data |
                        +------------------+
                                 |
                                 v
                        +------------------+
                        |      Silver      |
                        |   Clean Parquet  |
                        |  Structured Data |
                        +------------------+
                                 |
                                 v
                        +------------------+
                        |       Gold       |
                        |  Analytics Tables|
                        |  DuckDB Queries  |
                        +------------------+
                                 |
                                 v
                        +------------------+
                        |   Orchestration  |
                        |     Prefect      |
                        |   Pipeline Flow  |
                        +------------------+


```
## Gold Datasets

The pipeline produces the following analytics tables:

| Dataset | Description |
|------|------|
| `bot_vs_human.parquet` | Comparison of bot edits vs human edits |
| `edits_per_hour.parquet` | Wikipedia edits aggregated by hour |
| `pageviews_daily_top.parquet` | Top viewed Wikipedia pages per day |


## Project Structure

```
wikistream-lakehouse/
├── src/
│   ├── ingest/
│   │   ├── storage.py
│   │   ├── fetch_recentchanges.py
│   │   ├── fetch_recentchanges_api.py
│   │   ├── fetch_pageviews.py
│   │   └── backfill_pageviews.py
│   ├── transform/
│   │   ├── recentchanges_to_parquet.py
│   │   ├── pageviews_top_to_parquet.py
│   │   └── bronze_to_silver_latest.py
│   ├── gold/
│   │   ├── pageviews_daily_top.py
│   │   ├── edits_per_hour.py
│   │   ├── bot_vs_human.py
│   │   └── run_gold.py
│   ├── pipeline/
│   │   ├── run_pipeline.py
│   │   └── prefect_flow.py
│   └── query/
│       └── duckdb_queries.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── requirements.txt
├── .gitignore
└── README.md
