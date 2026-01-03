import sys
import random
from pathlib import Path
import pandas as pd
import pytest

# ------------------------------------------------------------------
# Ensure src/chord is importable when pytest runs from project root
# ------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_CHORD = PROJECT_ROOT / "src" / "chord"
sys.path.insert(0, str(SRC_CHORD))

from chord_node import ChordNode
from chord_simulator import run_periodic_maintenance


# ------------------------------------------------------------------
# Helper
# ------------------------------------------------------------------
def build_chord_network(m=20, k=5, seed=42):
    mod = 2 ** m
    random.seed(seed)

    node_ids = random.sample(range(0, mod), k)
    node_ids.sort()

    nodes = []
    n0 = ChordNode(node_ids[0], m)
    n0.join(None)
    nodes.append(n0)

    for nid in node_ids[1:]:
        n = ChordNode(nid, m)
        n.join(n0)
        nodes.append(n)
        run_periodic_maintenance(nodes, rounds=3)

    return nodes


# ------------------------------------------------------------------
# TESTS
# ------------------------------------------------------------------

def test_chord_join_creates_valid_ring():
    nodes = build_chord_network()

    for n in nodes:
        assert n.successor is not None
        assert n.predecessor is not None
        assert n.successor.id != n.id
        assert n.predecessor.id != n.id


def test_insert_and_lookup_movies():
    nodes = build_chord_network()

    data_path = PROJECT_ROOT / "data" / "data_movies_clean.xlsx"
    df = pd.read_excel(data_path)

    K = 5
    titles = []

    for _, row in df.head(K).iterrows():
        value = {
            "title": row["title"],
            "popularity": row.get("popularity"),
            "vote_average": row.get("vote_average"),
        }
        nodes[0].put(row["title"], value)
        titles.append(row["title"])

    run_periodic_maintenance(nodes, rounds=2)

    for title in titles:
        value = nodes[0].get(title)
        assert value is not None

        if isinstance(value, list):
            assert len(value) >= 1
            assert "vote_average" in value[0]
        else:
            assert "vote_average" in value


def test_lookup_returns_hops():
    nodes = build_chord_network()

    key = "ChordTestKey"
    nodes[0].put(key, {"x": 1})

    key_id = nodes[0].hash_key(key)
    node, hops = nodes[0].lookup_with_hops(key_id)

    assert node is not None
    assert isinstance(hops, int)
    assert hops >= 0


def test_data_is_distributed_across_nodes():
    nodes = build_chord_network()

    for i in range(20):
        nodes[0].put(f"key-{i}", {"i": i})

    run_periodic_maintenance(nodes, rounds=3)

    total_keys = sum(len(n.data) for n in nodes)

    assert total_keys >= 20
    assert any(len(n.data) > 0 for n in nodes)


def test_node_leave_preserves_ring_and_data():
    nodes = build_chord_network()

    for i in range(10):
        nodes[0].put(f"leave-key-{i}", {"i": i})

    run_periodic_maintenance(nodes, rounds=2)

    leaving = nodes[2]
    leaving.leave()
    nodes.remove(leaving)

    run_periodic_maintenance(nodes, rounds=3)

    for n in nodes:
        assert n.successor is not None
        assert n.predecessor is not None

    # Data should still exist somewhere
    found = False
    for n in nodes:
        if n.data:
            found = True
            break

    assert found