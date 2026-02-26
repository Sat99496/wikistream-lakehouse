# WikiStream Lakehouse (Day 1)

Day 1 delivers two ingestion sources:
- Wikipedia RecentChanges (SSE stream) -> JSONL
- Wikimedia Pageviews Top (REST API) -> JSON

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

