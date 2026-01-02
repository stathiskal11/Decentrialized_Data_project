from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set

from src.common.ids import circ_dist, common_prefix_len_hex, to_hex128
from .leafset import LeafSet
from .routing_table import RoutingTable

@dataclass
class PastryNode:
    node_id: int
    leaf_set: LeafSet
    routing_table: RoutingTable
    kv_store: Dict[str, dict] = field(default_factory=dict)

    def next_hop(self, target_id: int, neighborhood: List[int], visited: Set[int]) -> int:
        self_id = self.node_id


        # 1) leaf-set improvement
        best_leaf = self.leaf_set.closest_to(self_id, target_id)
        if best_leaf != self_id and circ_dist(best_leaf, target_id) < circ_dist(self_id, target_id):
            return best_leaf

        # 2) routing table entry
        p = common_prefix_len_hex(self_id, target_id)
        th = to_hex128(target_id)
        if p < len(th):
            col = int(th[p], 16)
            rt = self.routing_table.entry(p, col)
            if rt is not None and rt not in visited and circ_dist(rt, target_id) < circ_dist(self_id, target_id):
                return rt

        # 3) fallback: any neighborhood node with >=prefix and closer
        self_hex = to_hex128(self_id)
        best = self_id
        best_d = circ_dist(self_id, target_id)

        for nid in neighborhood:
            if nid == self_id or nid in visited:
                continue
            nh = to_hex128(nid)
            q = 0
            while q < len(self_hex) and self_hex[q] == nh[q]:
                q += 1
            if q < p:
                continue
            d = circ_dist(nid, target_id)
            if d < best_d:
                best = nid
                best_d = d

        return best
