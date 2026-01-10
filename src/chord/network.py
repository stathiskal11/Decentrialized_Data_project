from __future__ import annotations

import random
import threading
from bisect import bisect_left
from typing import Dict, Optional, Tuple, Set

from src.common.metrics import Metrics
from src.common.ids import hash_128, ID_BITS

from .chord_node import ChordNode


def snapshot_state(nodes: Dict[int, ChordNode]) -> Dict[int, Tuple[int, int, Tuple[int, ...]]]:
    """
    Snapshot for join/leave cost proxy:
    per node: (successor_id, predecessor_id, finger_ids_tuple)
    """
    snap = {}
    for nid, node in nodes.items():
        succ = node.successor.id if getattr(node, "successor", None) is not None else nid
        pred = node.predecessor.id if getattr(node, "predecessor", None) is not None else nid
        fingers = tuple(f.id for f in getattr(node, "finger", []))
        snap[nid] = (succ, pred, fingers)
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


class ChordNetwork:
    """
    In-process Chord simulator exposing the required API (compatible with experiments/run_experiments.py).
    Hop count = number of overlay forwarding steps between distinct nodes.

    Implementation approach:
    - Maintain a sorted ring of node IDs.
    - Build successor/predecessor pointers and finger tables deterministically.
    - Route each operation starting from a random node (for realistic hop measurements).
    """

    def __init__(self, m_bits: int = ID_BITS):
        self.m_bits = m_bits
        self.mod = 1 << m_bits

        self.nodes: Dict[int, ChordNode] = {}
        self.sorted_ids: list[int] = []

        self.metrics = Metrics()
        self.rng = random.Random(0)
        self._lock = threading.Lock()

    # ---------- internal ----------
    def _random_existing_node_id(self) -> int:
        return self.rng.choice(self.sorted_ids)

    def _successor_id_of(self, x: int) -> int:
        """Return the node id of the successor of x on the ring."""
        if not self.sorted_ids:
            raise RuntimeError("Chord network is empty")
        i = bisect_left(self.sorted_ids, x)
        if i == len(self.sorted_ids):
            return self.sorted_ids[0]
        return self.sorted_ids[i]

    def _rebuild_ring_and_fingers(self) -> None:
        """Recompute successor/predecessor pointers and full finger tables for all nodes."""
        self.sorted_ids = sorted(self.nodes.keys())
        if not self.sorted_ids:
            return

        # successor / predecessor
        n = len(self.sorted_ids)
        for idx, nid in enumerate(self.sorted_ids):
            node = self.nodes[nid]
            succ_id = self.sorted_ids[(idx + 1) % n]
            pred_id = self.sorted_ids[(idx - 1) % n]
            node.successor = self.nodes[succ_id]
            node.predecessor = self.nodes[pred_id]

        # finger tables
        for nid in self.sorted_ids:
            node = self.nodes[nid]
            node.finger = [node] * self.m_bits
            for i in range(self.m_bits):
                start = (nid + (1 << i)) % self.mod
                finger_succ_id = self._successor_id_of(start)
                node.finger[i] = self.nodes[finger_succ_id]

    def _route(self, start_id: int, target_id: int) -> Tuple[int, int]:
        """
        Chord routing using closest preceding finger, with safety guards.
        Returns (destination_node_id, hops).
        """
        if not self.nodes:
            raise RuntimeError("Chord network is empty")

        current = self.nodes[start_id]
        hops = 0
        visited: Set[int] = set()

        while True:
            # Single-node case
            if current.successor.id == current.id:
                return current.id, 0

            succ = current.successor

            # If target is in (current, successor] then successor is responsible
            if current.in_interval(target_id, current.id, succ.id, inclusive_right=True):
                dest = succ
                # forwarding step only if destination is different node
                return dest.id, (hops + (1 if dest.id != current.id else 0))

            # Otherwise advance via finger table
            nxt = current.closest_preceding_finger(target_id)

            # safety: avoid loops / no-progress
            if nxt.id == current.id or nxt.id in visited:
                nxt = succ

            visited.add(current.id)
            current = nxt
            hops += 1

            # hard safety bound
            if hops > len(self.nodes) + 5:
                return current.id, hops
    def _rebalance_all_keys(self) -> int:
        """
        Rebalance all key_id -> values after topology change.
        Returns total migration hops (sum of routing hops per key_id).
        """
        if not self.nodes:
            return 0

        # 1) collect
        all_data: Dict[int, list] = {}
        with self._lock:
            for node in self.nodes.values():
                for key_id, vals in node.data.items():
                    all_data.setdefault(key_id, []).extend(list(vals))
                node.data.clear()

        # 2) reinsert
        migration_hops = 0
        for key_id, vals in all_data.items():
            start = self._random_existing_node_id()
            dest_id, hops = self._route(start, key_id)
            with self._lock:
                # keep same structure: list of dicts
                self.nodes[dest_id].data[key_id] = list(vals)
            migration_hops += hops

        return migration_hops

    # ---------- contract API ----------
    def build(self, n_nodes: int, seed: int = 0) -> None:
        self.rng = random.Random(seed)
        self.nodes.clear()
        self.metrics = Metrics()

        # unique node IDs in [0, 2^m_bits)
        while len(self.nodes) < n_nodes:
            nid = self.rng.getrandbits(self.m_bits)
            if nid in self.nodes:
                continue
            self.nodes[nid] = ChordNode(nid, self.m_bits)

        self._rebuild_ring_and_fingers()

    def insert(self, key: str, value: dict) -> int:
        key_id = hash_128(key) % self.mod
        start = self._random_existing_node_id()
        dest_id, hops = self._route(start, key_id)
        dest = self.nodes[dest_id]

        # store single value per key_id (overwrite semantics like Pastry kv_ops.update_at)
        with self._lock:
            dest.data[key_id] = [value]
            self.metrics.record("insert", hops)

        return hops

    def lookup(self, key: str) -> Tuple[Optional[dict], int]:
        key_id = hash_128(key) % self.mod
        start = self._random_existing_node_id()
        dest_id, hops = self._route(start, key_id)
        dest = self.nodes[dest_id]

        with self._lock:
            recs = dest.data.get(key_id)
            val = recs[0] if recs else None
            self.metrics.record("lookup", hops)

        return val, hops

    def update(self, key: str, value: dict) -> int:
        key_id = hash_128(key) % self.mod
        start = self._random_existing_node_id()
        dest_id, hops = self._route(start, key_id)
        dest = self.nodes[dest_id]

        with self._lock:
            dest.data[key_id] = [value]
            self.metrics.record("update", hops)

        return hops

    def delete(self, key: str) -> int:
        key_id = hash_128(key) % self.mod
        start = self._random_existing_node_id()
        dest_id, hops = self._route(start, key_id)
        dest = self.nodes[dest_id]

        with self._lock:
            dest.data.pop(key_id, None)
            self.metrics.record("delete", hops)

        return hops

    def join(self) -> int:
        """
        Add one node.
        Cost proxy = bootstrap route hops to its ID + number of nodes whose pointers/fingers changed.
        """
        if not self.nodes:
            nid = self.rng.getrandbits(self.m_bits)
            self.nodes[nid] = ChordNode(nid, self.m_bits)
            self._rebuild_ring_and_fingers()
            self.metrics.record("join", 0)
            return 0

        before = snapshot_state(self.nodes)

        new_id = self.rng.getrandbits(self.m_bits)
        while new_id in self.nodes:
            new_id = self.rng.getrandbits(self.m_bits)

        bootstrap = self._random_existing_node_id()
        _dest, route_hops = self._route(bootstrap, new_id)

        self.nodes[new_id] = ChordNode(new_id, self.m_bits)
        self._rebuild_ring_and_fingers()

        migration_hops = self._rebalance_all_keys()

        after = snapshot_state(self.nodes)
        update_cost = count_changed(before, after)

        total = route_hops + update_cost + migration_hops

        self.metrics.record("join", total)
        return total

    def leave(self, node_id=None) -> int:
        """
        Remove one node.
        Cost proxy = nodes changed + migration_hops.
        """
        if not self.nodes:
            self.metrics.record("leave", 0)
            return 0

        if node_id is None:
            node_id = self._random_existing_node_id()

        if node_id not in self.nodes:
            self.metrics.record("leave", 0)
            return 0

        # collect all keys BEFORE removing node
        all_data: Dict[int, list] = {}
        with self._lock:
            for node in self.nodes.values():
                for key_id, vals in node.data.items():
                    all_data.setdefault(key_id, []).extend(list(vals))

        before = snapshot_state(self.nodes)
        self.nodes.pop(node_id)

        migration_hops = 0
        if self.nodes:
            self._rebuild_ring_and_fingers()

            # clear stores then reinsert into new topology
            with self._lock:
                for node in self.nodes.values():
                    node.data.clear()

            for key_id, vals in all_data.items():
                start = self._random_existing_node_id()
                dest_id, hops = self._route(start, key_id)
                with self._lock:
                    self.nodes[dest_id].data[key_id] = list(vals)
                migration_hops += hops

        after = snapshot_state(self.nodes)
        update_cost = count_changed(before, after)

        total = update_cost + migration_hops
        self.metrics.record("leave", total)
        return total


# alias for contract friendliness (optional)
DHTNetwork = ChordNetwork
