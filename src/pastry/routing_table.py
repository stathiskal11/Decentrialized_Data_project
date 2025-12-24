from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from src.common.ids import to_hex128, ID_HEX_LEN

@dataclass
class RoutingTable:
    base: int = 16
    table: List[Dict[int, int]] = field(default_factory=lambda: [dict() for _ in range(ID_HEX_LEN)])

    def rebuild(self, self_id: int, all_node_ids: List[int]) -> None:
        self.table = [dict() for _ in range(ID_HEX_LEN)]
        self_hex = to_hex128(self_id)

        for nid in all_node_ids:
            if nid == self_id:
                continue
            nh = to_hex128(nid)

            p = 0
            while p < ID_HEX_LEN and self_hex[p] == nh[p]:
                p += 1
            if p >= ID_HEX_LEN:
                continue

            col = int(nh[p], 16)
            if col not in self.table[p]:
                self.table[p][col] = nid

    def entry(self, row: int, col: int) -> Optional[int]:
        if row < 0 or row >= len(self.table):
            return None
        return self.table[row].get(col)

    def row_candidates(self, row: int) -> List[int]:
        if row < 0 or row >= len(self.table):
            return []
        return list(self.table[row].values())
