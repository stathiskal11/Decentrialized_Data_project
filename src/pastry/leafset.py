from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from src.common.ids import circ_dist

@dataclass
class LeafSet:
    L: int
    nodes: List[int] = field(default_factory=list)

    def rebuild(self, self_id: int, all_node_ids: List[int]) -> None:
        ids = sorted(set(all_node_ids))
        if self_id not in ids:
            self.nodes = []
            return

        idx = ids.index(self_id)
        half = self.L // 2

        smaller = [ids[(idx - k) % len(ids)] for k in range(1, half + 1)]
        larger  = [ids[(idx + k) % len(ids)] for k in range(1, half + 1)]

        out: List[int] = []
        for x in (smaller + larger):
            if x != self_id and x not in out:
                out.append(x)
        self.nodes = out[: self.L]

    def candidates_with_self(self, self_id: int) -> List[int]:
        return [self_id] + list(self.nodes)

    def closest_to(self, self_id: int, target_id: int) -> int:
        cands = self.candidates_with_self(self_id)
        return min(cands, key=lambda nid: circ_dist(nid, target_id))
