from __future__ import annotations
import random
from typing import Dict, Optional, Tuple, List, Set

from src.common.metrics import Metrics
from src.common.ids import hash_128

from .leafset import LeafSet
from .routing_table import RoutingTable
from .node import PastryNode
from .join_leave import snapshot_state, count_changed
from .kv_ops import insert_at, lookup_at, update_at, delete_at

ID_BITS = 128

class PastryNetwork:
    """
    In-process Pastry simulator exposing the required API.
    Hop count is the number of overlay forwarding steps.
    """
    def __init__(self, leaf_L: int = 16, base: int = 16):
        self.leaf_L = leaf_L
        self.base = base
        self.nodes: Dict[int, PastryNode] = {}
        self.metrics = Metrics()
        self.rng = random.Random(0)

    # ---------- internal ----------
    def _rebuild_structures(self) -> None:
        ids = sorted(self.nodes.keys())
        for nid, node in self.nodes.items():
            node.leaf_set.rebuild(nid, ids)
            node.routing_table.rebuild(nid, ids)

    def _random_existing_node_id(self) -> int:
        return self.rng.choice(list(self.nodes.keys()))

    def _route(self, start_id: int, target_id: int) -> Tuple[int, int]:
        if not self.nodes:
            raise RuntimeError("Network is empty")

        current = start_id
        hops = 0
        visited: Set[int] = set()

        while True:
            visited.add(current)
            node = self.nodes[current]

            # neighborhood = leaf set + routing row candidates
            # row = common prefix with target; used for realistic fallback neighborhood
            from src.common.ids import common_prefix_len_hex
            p = common_prefix_len_hex(node.node_id, target_id)

            neighborhood = node.leaf_set.candidates_with_self(node.node_id) + node.routing_table.row_candidates(p)
            nxt = node.next_hop(target_id, neighborhood=neighborhood, visited=visited)

            if nxt == current:
                return current, hops
            if nxt not in self.nodes:
                return current, hops

            current = nxt
            hops += 1
    def _rebalance_all_keys(self) -> int:
        """
        Rebalance all (key,value) pairs after topology change.
        Returns total migration hops (sum of routing hops per reinserted key).
        """
        if not self.nodes:
            return 0

        # 1) collect
        all_items: List[Tuple[str, dict]] = []
        for node in self.nodes.values():
            all_items.extend(list(node.kv_store.items()))
            node.kv_store.clear()

        # 2) reinsert
        migration_hops = 0
        for key, value in all_items:
            key_id = hash_128(key)
            start = self._random_existing_node_id()
            dest, hops = self._route(start, key_id)
            insert_at(self.nodes[dest], key, value)
            migration_hops += hops

        return migration_hops
    
    # ---------- contract API ----------
    def build(self, n_nodes: int, seed: int = 0) -> None:
        self.rng = random.Random(seed)
        self.nodes.clear()
        self.metrics = Metrics()

        # create unique node ids
        while len(self.nodes) < n_nodes:
            nid = self.rng.getrandbits(ID_BITS)
            if nid in self.nodes:
                continue
            self.nodes[nid] = PastryNode(
                node_id=nid,
                leaf_set=LeafSet(self.leaf_L),
                routing_table=RoutingTable(self.base),
            )

        self._rebuild_structures()

    def insert(self, key: str, value: dict) -> int:
        key_id = hash_128(key)
        start = self._random_existing_node_id()
        dest, hops = self._route(start, key_id)
        insert_at(self.nodes[dest], key, value)
        self.metrics.record("insert", hops)
        return hops

    def lookup(self, key: str) -> Tuple[Optional[dict], int]:
        key_id = hash_128(key)
        start = self._random_existing_node_id()
        dest, hops = self._route(start, key_id)
        val = lookup_at(self.nodes[dest], key)
        self.metrics.record("lookup", hops)
        return val, hops

    def update(self, key: str, value: dict) -> int:
        key_id = hash_128(key)
        start = self._random_existing_node_id()
        dest, hops = self._route(start, key_id)
        update_at(self.nodes[dest], key, value)
        self.metrics.record("update", hops)
        return hops

    def delete(self, key: str) -> int:
        key_id = hash_128(key)
        start = self._random_existing_node_id()
        dest, hops = self._route(start, key_id)
        delete_at(self.nodes[dest], key)
        self.metrics.record("delete", hops)
        return hops

    def join(self) -> int:
        """
        Add one node.
        Hop-cost = route hops to its ID (bootstrap) + number of nodes whose tables changed (sim cost proxy).
        """
        if not self.nodes:
            nid = self.rng.getrandbits(ID_BITS)
            self.nodes[nid] = PastryNode(nid, LeafSet(self.leaf_L), RoutingTable(self.base))
            self._rebuild_structures()
            self.metrics.record("join", 0)
            return 0

        before = snapshot_state(self.nodes)

        new_id = self.rng.getrandbits(ID_BITS)
        while new_id in self.nodes:
            new_id = self.rng.getrandbits(ID_BITS)

        bootstrap = self._random_existing_node_id()
        _, route_hops = self._route(bootstrap, new_id)

        self.nodes[new_id] = PastryNode(new_id, LeafSet(self.leaf_L), RoutingTable(self.base))
        self._rebuild_structures()

        migration_hops = self._rebalance_all_keys()

        after = snapshot_state(self.nodes)
        update_cost = count_changed(before, after)

        total = route_hops + update_cost + migration_hops

        self.metrics.record("join", total)
        return total

    def leave(self, node_id=None) -> int:
        """
        Remove one node.
        Hop-cost = number of nodes whose tables changed (sim cost proxy) + migration_hops.
        """
        if not self.nodes:
            self.metrics.record("leave", 0)
            return 0

        if node_id is None:
            node_id = self._random_existing_node_id()

        if node_id not in self.nodes:
            self.metrics.record("leave", 0)
            return 0

        # collect all keys BEFORE removing node (so we don't lose departing node's keys)
        all_items: List[Tuple[str, dict]] = []
        for node in self.nodes.values():
            all_items.extend(list(node.kv_store.items()))

        before = snapshot_state(self.nodes)
        self.nodes.pop(node_id)

        migration_hops = 0
        if self.nodes:
            self._rebuild_structures()

            # clear and reinsert all items into the new topology
            for node in self.nodes.values():
                node.kv_store.clear()

            for key, value in all_items:
                key_id = hash_128(key)
                start = self._random_existing_node_id()
                dest, hops = self._route(start, key_id)
                insert_at(self.nodes[dest], key, value)
                migration_hops += hops

        after = snapshot_state(self.nodes)
        update_cost = count_changed(before, after)

        total = update_cost + migration_hops
        self.metrics.record("leave", total)
        return total


# alias for contract friendliness
DHTNetwork = PastryNetwork
