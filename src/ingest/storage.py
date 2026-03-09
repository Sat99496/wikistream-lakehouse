from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class Partition:
    dt: str   # YYYY-MM-DD
    hour: str # HH


def current_partition(now: datetime | None = None) -> Partition:
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)

    return Partition(dt=now.strftime("%Y-%m-%d"), hour=now.strftime("%H"))


def partition_for_date(dt: str, hour: str = "00") -> Partition:
    return Partition(dt=dt, hour=hour)


def bronze_dir(dataset: str, part: Partition) -> Path:
    return Path("data") / "bronze" / dataset / f"dt={part.dt}" / f"hour={part.hour}"


def silver_dir(dataset: str, part: Partition) -> Path:
    return Path("data") / "silver" / dataset / f"dt={part.dt}" / f"hour={part.hour}"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)
