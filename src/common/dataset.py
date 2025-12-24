from __future__ import annotations
import csv
from typing import Iterator, Tuple, Dict, Any

def iter_movies_csv(path: str) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """
    Yields (title, attributes_dict) for TMDB movies CSV.
    Minimal payload; extend as needed.
    """
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            title = row.get("title")
            if not title:
                continue
            value = {
                "id": row.get("id"),
                "popularity": _to_float(row.get("popularity")),
                "vote_average": _to_float(row.get("vote_average")),
                "vote_count": _to_int(row.get("vote_count")),
                "release_date": row.get("release_date"),
            }
            yield title, value

def _to_float(x):
    try:
        return float(x) if x is not None and x != "" else None
    except Exception:
        return None

def _to_int(x):
    try:
        return int(float(x)) if x is not None and x != "" else None
    except Exception:
        return None
