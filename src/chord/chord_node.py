# chord_node.py
import hashlib
import random
from typing import Optional, Dict, Tuple, List, Any


class ChordNode:
    """
    Full Chord node implementation (C1 + C2) with safe multi-record handling:
    - Node state: id, successor, predecessor, finger[]
    - Methods: join, leave, stabilize, notify, fix_fingers
    - DHT ops: put, get, delete, update (store lists of records per key_id)
    - Utility: hash_key, in_interval, lookup_with_hops (for measuring hops)
    """

    def __init__(self, node_id: int, m_bits: int):
        self.id: int = node_id
        self.m: int = m_bits
        self.mod: int = 2 ** m_bits

        # Pointers
        self.successor: "ChordNode" = self
        self.predecessor: "ChordNode" = self

        # Finger table: length m
        self.finger: list["ChordNode"] = [self] * m_bits

        # Local storage: mapping hashed_key -> list_of_values
        # Each value is expected to be a dict of attributes (e.g. movie metadata)
        self.data: Dict[int, List[Any]] = {}

    def __repr__(self):
        return f"Node({self.id})"

    # ---------------------------
    # Interval utilities
    # ---------------------------
    def in_interval(self, x: int, a: int, b: int, inclusive_right: bool = True) -> bool:
        """
        Check whether x is in (a, b] if inclusive_right==True,
        else (a, b) — with wrap-around modulo space.
        """
        if a < b:
            return a < x <= b if inclusive_right else a < x < b
        else:  # wrap-around
            if inclusive_right:
                return x > a or x <= b
            else:
                return x > a or x < b

    # ---------------------------
    # Basic routing (C1)
    # ---------------------------
    def closest_preceding_finger(self, key: int) -> "ChordNode":
        """
        Return closest preceding finger for key
        (scans fingers from high to low).
        """
        for i in reversed(range(self.m)):
            f = self.finger[i]
            if f is None:
                continue
            if self.in_interval(f.id, self.id, key, inclusive_right=False):
                return f
        return self

    def find_predecessor(self, key: int) -> "ChordNode":
        """
        Starting from this node, find predecessor of key.
        """
        node = self
        # Loop until key ∈ (node.id, node.successor.id]
        while not self.in_interval(key, node.id, node.successor.id, inclusive_right=True):
            # move to closest preceding finger (or self)
            next_node = node.closest_preceding_finger(key)
            if next_node == node:
                # can't improve routing; break to avoid infinite loop (safety)
                break
            node = next_node
        return node

    def find_successor(self, key: int) -> "ChordNode":
        pred = self.find_predecessor(key)
        return pred.successor

    # Lookup function that returns (successor_node, hops) for instrumentation
    def lookup_with_hops(self, key: int) -> Tuple["ChordNode", int]:
        node = self
        hops = 0
        while True:
            if self.in_interval(key, node.id, node.successor.id, inclusive_right=True):
                return node.successor, hops + 1  # final hop to successor
            next_node = node.closest_preceding_finger(key)
            # if closest_preceding_finger returns self, we still hop to successor
            if next_node == node:
                node = node.successor
                hops += 1
            else:
                node = next_node
                hops += 1

    # ---------------------------
    # Join / Stabilization (C2)
    # ---------------------------
    def join(self, existing_node: Optional["ChordNode"]) -> None:
        """
        Join the ring. If existing_node is None, this node becomes the only node.
        Otherwise, initialize successor via existing_node.find_successor(self.id).
        Note: stabilize/fix_fingers should be run periodically (simulator) to complete setup.
        """
        if existing_node is None:
            # first node in ring
            self.successor = self
            self.predecessor = self
            self.finger = [self] * self.m
        else:
            # set successor using existing node
            self.predecessor = None
            self.successor = existing_node.find_successor(self.id)
        # we don't immediately steal keys here; simulator/stabilize will eventually move keys.
        # Optionally, try to acquire keys right away if successor has them:
        try:
            self.acquire_keys_from_successor()
        except Exception:
            pass

    def notify(self, node: "ChordNode") -> None:
        """
        Called when node thinks it might be our predecessor.
        """
        if self.predecessor is None or self.in_interval(node.id, self.predecessor.id, self.id, inclusive_right=False):
            self.predecessor = node

    def stabilize(self) -> None:
        """
        Stabilize operation:
        x = successor.predecessor
        if x in (self, successor) then set successor = x
        successor.notify(self)
        """
        x = self.successor.predecessor
        # If x exists and is between self and successor, update successor
        if x is not None and self.in_interval(x.id, self.id, self.successor.id, inclusive_right=False):
            self.successor = x
        # Notify successor that self might be its predecessor
        try:
            self.successor.notify(self)
        except Exception:
            # In simple simulator, this should not fail
            pass

    def fix_fingers(self) -> None:
        """
        Recompute the entire finger table.
        (Simpler to implement than doing one finger per invocation.)
        """
        for i in range(self.m):
            start = (self.id + 2 ** i) % self.mod
            self.finger[i] = self.find_successor(start)

    # ---------------------------
    # Key transfer utilities
    # ---------------------------
    def acquire_keys_from_successor(self) -> None:
        """
        Ask successor to transfer keys for which self is now responsible.
        This is a best-effort transfer; exact correctness depends on predecessor knowledge
        being accurate (stabilize updates predecessor pointers).
        Keys moved are merged (lists appended) rather than overwritten.
        """
        succ = self.successor
        if succ is None or succ is self:
            return

        keys_to_move = []
        for k in list(succ.data.keys()):
            # If key k should be handled by self (i.e., k ∈ (predecessor.id, self.id])
            pred_id = self.predecessor.id if self.predecessor is not None else None
            if pred_id is None:
                # If predecessor unknown, be conservative: move keys k <= self.id
                if k <= self.id:
                    keys_to_move.append(k)
            else:
                if self.in_interval(k, pred_id, self.id, inclusive_right=True):
                    keys_to_move.append(k)

        for k in keys_to_move:
            src = succ.data.pop(k)
            # ensure src is a list
            src_list = src if isinstance(src, list) else [src]
            if k in self.data:
                # extend existing list
                self.data[k].extend(src_list)
            else:
                # take ownership (store list)
                self.data[k] = list(src_list)

    # ---------------------------
    # DHT operations (put/get/delete/update)
    # ---------------------------
    def hash_key(self, key: object) -> int:
        """
        Hash a key (e.g., movie title) into the ID space using SHA-1.
        """
        digest = hashlib.sha1(str(key).encode("utf-8")).hexdigest()
        return int(digest, 16) % self.mod

    def put(self, key: object, value: object) -> None:
        """
        Insert key/value into DHT: hash the key, find successor, store locally there.
        Stores multiple records per numeric key_id as a list (appends).
        """
        key_id = self.hash_key(key)
        node = self.find_successor(key_id)
        # ensure a list exists for this key id
        if key_id not in node.data:
            node.data[key_id] = []
        node.data[key_id].append(value)

    def get(self, key: object) -> Optional[List[Any]]:
        """
        Retrieve value(s) for key from DHT. Returns a list of matching records or None.
        We perform an exact-title filter if records are dicts and contain 'title'.
        """
        key_id = self.hash_key(key)
        node = self.find_successor(key_id)
        records = node.data.get(key_id)
        if records is None:
            return None
        # If values are dicts that contain 'title', filter by exact title to be precise
        if isinstance(records, list) and len(records) > 0 and isinstance(records[0], dict) and 'title' in records[0]:
            return [r for r in records if r.get('title') == key]
        # otherwise return full list
        return list(records)

    def delete(self, key: object, match_criteria: dict = None) -> bool:
        """
        Remove key from DHT. If match_criteria is None -> delete all records for that key_id.
        Else remove records that match the criteria (e.g., {'year': 1933}).
        Returns True if something was removed.
        """
        key_id = self.hash_key(key)
        node = self.find_successor(key_id)
        if key_id not in node.data:
            return False
        if match_criteria is None:
            del node.data[key_id]
            return True
        before = len(node.data[key_id])
        node.data[key_id] = [r for r in node.data[key_id] if not all(r.get(k) == v for k, v in match_criteria.items())]
        after = len(node.data.get(key_id, []))
        if after == 0 and key_id in node.data:
            node.data.pop(key_id, None)
        return (before != after)

    def update(self, key: object, update_fn) -> None:
        """
        Update key in DHT: apply update_fn(record) to each record under the key_id.
        update_fn should return the updated record or None to delete the record.
        """
        key_id = self.hash_key(key)
        node = self.find_successor(key_id)
        recs = node.data.get(key_id)
        if not recs:
            return
        new_recs = []
        for r in recs:
            new = update_fn(r)
            if new is not None:
                new_recs.append(new)
        if new_recs:
            node.data[key_id] = new_recs
        else:
            node.data.pop(key_id, None)

    # ---------------------------
    # Leave (graceful)
    # ---------------------------
    def leave(self) -> None:
        """
        Graceful leave:
        - transfer all local keys to successor (merge lists)
        - link predecessor <-> successor
        """
        # transfer keys (merge into successor)
        for k, v in list(self.data.items()):
            src_list = v if isinstance(v, list) else [v]
            if k in self.successor.data:
                self.successor.data[k].extend(src_list)
            else:
                self.successor.data[k] = list(src_list)
        self.data.clear()

        # fix neighbors
        if self.predecessor is not None:
            self.predecessor.successor = self.successor
        if self.successor is not None:
            self.successor.predecessor = self.predecessor

        # optional cleanup
        self.predecessor = self
        self.successor = self
        self.finger = [self] * self.m

    # ---------------------------
    # Helper: iterate local storage (for debugging)
    # ---------------------------
    def dump(self) -> None:
        print(f"{self}: pred={self.predecessor.id if self.predecessor else None} succ={self.successor.id if self.successor else None}")
        print("  data keys:", sorted(self.data.keys()))
        print("  fingers:", [f.id for f in self.finger])
