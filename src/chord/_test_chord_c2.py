# test_chord_c2.py (balanced node ids for m = 20)
import random
import pandas as pd
from chord_node import ChordNode
from chord_simulator import run_periodic_maintenance

def interval_size(a: int, b: int, mod: int) -> int:
    # size of (a, b] modulo mod
    if a < b:
        return b - a
    else:
        return (mod - a) + b

def main():
    # -------------------------------
    # CONFIG
    # -------------------------------
    m = 20  # ID space = 2^20 = 1,048,576  (good for ~946k movies)
    mod = 2 ** m
    k = 5   # number of nodes

    # deterministic seed so experiments are reproducible
    random.seed(42)

    # pick k unique node IDs uniformly across the full ID space
    node_ids = random.sample(range(0, mod), k)
    node_ids.sort()
    print("m =", m, "mod =", mod)
    print("node_ids =", node_ids)

    # -------------------------------
    # 1. CREATE CHORD NETWORK
    # -------------------------------
    nodes = []
    n0 = ChordNode(node_ids[0], m)
    n0.join(None)
    nodes.append(n0)

    for nid in node_ids[1:]:
        n = ChordNode(nid, m)
        n.join(n0)
        nodes.append(n)
        run_periodic_maintenance(nodes, rounds=4)

    print("\n=== NETWORK STATE AFTER JOIN ===")
    for n in nodes:
        print(f"Node({n.id}) -> succ={n.successor.id}, pred={n.predecessor.id}")

    # print interval sizes (what fraction of ID space each node owns)
    sorted_nodes = sorted(nodes, key=lambda n: n.id)
    print("\n=== INTERVAL SIZES (ids owned by each node) ===")
    for i, n in enumerate(sorted_nodes):
        pred = sorted_nodes[i - 1]
        size = interval_size(pred.id, n.id, mod)
        print(f"Node {n.id} owns {size} ids ({size/mod*100:.4f}%)")

    # -------------------------------
    # 2. LOAD DATASET
    # -------------------------------
    DATA_PATH = "../../data/data_movies_clean.xlsx"
    df = pd.read_excel(DATA_PATH)
    print("\nDataset loaded:", len(df), "rows")

    # -------------------------------
    # 3. INSERT MOVIES INTO DHT
    # -------------------------------
    K = 10   # number of movies to insert (start small for debug)
    print(f"\nInserting first {K} movies into DHT...\n")

    for _, row in df.head(K).iterrows():
        value = {
            "title": row["title"],
            "popularity": row.get("popularity"),
            "vote_average": row.get("vote_average"),
            "release_date": row.get("release_date")
        }
        nodes[0].put(row["title"], value)

    run_periodic_maintenance(nodes, rounds=3)

    # -------------------------------
    # 4. LOOKUP + HOP COUNT
    # -------------------------------
    print("\n=== LOOKUPS ===")
    for title in df["title"].head(K):
        key_id = nodes[0].hash_key(title)
        node, hops = nodes[0].lookup_with_hops(key_id)
        value = nodes[0].get(title)

        # value may be None, a list of records, or a single dict (depending on implementation)
        rating = None
        if value:
            if isinstance(value, list):
                # choose first matching record (or change to print all)
                rec = value[0] if len(value) > 0 else None
                rating = rec.get("vote_average") if rec else None
            elif isinstance(value, dict):
                rating = value.get("vote_average")
        print(f"'{title}' → Node {node.id} | hops={hops} | rating={rating}")


    # -------------------------------
    # 5. SHOW DATA DISTRIBUTION
    # -------------------------------
    print("\n=== DATA PER NODE ===")
    for n in nodes:
        print(f"Node {n.id}: {len(n.data)} keys")

    # -------------------------------
    # 6. NODE LEAVE TEST
    # -------------------------------
    leaving = nodes[2]
    print(f"\nNode {leaving.id} leaving the network...\n")
    leaving.leave()
    nodes.remove(leaving)

    run_periodic_maintenance(nodes, rounds=4)

    print("=== NETWORK AFTER LEAVE ===")
    for n in nodes:
        print(f"Node({n.id}) -> succ={n.successor.id}, pred={n.predecessor.id}")

    print("\n=== DATA PER NODE AFTER LEAVE ===")
    for n in nodes:
        print(f"Node {n.id}: {len(n.data)} keys")


            # 1) How many exact duplicate titles in the dataset?
    dupes = df['title'].duplicated().sum()
    print("Exact duplicate titles in dataset:", dupes)

    # 2) For the K inserted movies, see if any share the same numeric key_id
    K = 100
    seen = {}
    collisions = []
    for _, row in df.head(K).iterrows():
        title = row['title']
        key_id = nodes[0].hash_key(title)
        if key_id in seen and seen[key_id] != title:
            collisions.append((key_id, seen[key_id], title))
        else:
            seen[key_id] = title

    print("Collisions (different titles same key_id) among first K:", collisions[:10])

    # 3) For full dataset: count numeric key_id duplicates (collisions)
    from collections import Counter
    key_ids = df['title'].apply(lambda t: nodes[0].hash_key(t))
    cnt = Counter(key_ids)
    coll_count = sum(1 for v in cnt.values() if v > 1)
    print("Number of numeric key_ids with >1 titles (collisions) in dataset:", coll_count)

    #SIMANTIKO POLI, EXOUME PERIPOU 145K IDIOUS TITLOUS KAI SINOLIKA 211K COLLISIONS
    #ARA EXOUME 65K COLLISIONS APO OXI IDIOUS TITLOUS (pou isos thelei allagi?)



if __name__ == "__main__":
    main()
