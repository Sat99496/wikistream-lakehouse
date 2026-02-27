from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

@dataclass(frozen=True)
class Partition:
    dt: str   # YYYY-MM-DD
    hour: str # HH

def current_partition() -> Partition:
    now = datetime.now(timezone.utc)
    return Partition(dt=now.strftime("%Y-%m-%d"), hour=now.strftime("%H"))

def bronze_dir(dataset: str, part: Partition) -> Path:
    return Path("data") / "bronze" / dataset / f"dt={part.dt}" / f"hour={part.hour}"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)
