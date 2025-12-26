# chord_simulator.py
import time
import random
from typing import List
from chord_node import ChordNode


def run_periodic_maintenance(nodes: List[ChordNode], rounds: int = 3, delay: float = 0.0) -> None:
    """
    Run stabilize() and fix_fingers() for all nodes for a number of rounds.
    Optionally randomize order each round to simulate asynchronous behavior.
    delay: optional sleep per round (0 by default).
    """
    for r in range(rounds):
        order = nodes[:]
        random.shuffle(order)
        for node in order:
            try:
                node.stabilize()
            except Exception:
                pass
        for node in order:
            try:
                node.fix_fingers()
            except Exception:
                pass
        # After stabilization, let nodes try to acquire keys they should own
        for node in order:
            try:
                node.acquire_keys_from_successor()
            except Exception:
                pass
        if delay > 0:
            time.sleep(delay)
