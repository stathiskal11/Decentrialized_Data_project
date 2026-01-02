from __future__ import annotations

from typing import Dict, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from src.pastry.node import PastryNode


def snapshot_state(nodes: Dict[int, PastryNode]) -> Dict[int, Tuple[...]]:
    snap = {}
    for nid, node in nodes.items():
        leaf = tuple(sorted(node.leaf_set.nodes))
        rt_items = []
        for r, rowmap in enumerate(node.routing_table.table):
            for c, dest in rowmap.items():
                rt_items.append((r, c, dest))
        snap[nid] = (leaf, tuple(sorted(rt_items)))
    return snap

def count_changed(before: dict, after: dict) -> int:
    changed = 0
    for nid, b in before.items():
        a = after.get(nid)
        if a is None:
            continue
        if a != b:
            changed += 1
    return changed
