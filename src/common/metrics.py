from __future__ import annotations
from dataclasses import dataclass, field
from collections import defaultdict
from typing import DefaultDict, List, Dict, Any

@dataclass
class Metrics:
    hops: DefaultDict[str, List[int]] = field(default_factory=lambda: defaultdict(list))

    def record(self, op: str, hop_count: int) -> None:
        self.hops[op].append(int(hop_count))

    def summary(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for op, values in self.hops.items():
            if not values:
                continue
            values_sorted = sorted(values)
            n = len(values_sorted)
            out[op] = {
                "count": n,
                "mean": sum(values_sorted) / n,
                "median": values_sorted[n // 2],
                "p95": values_sorted[int(0.95 * (n - 1))],
            }
        return out
