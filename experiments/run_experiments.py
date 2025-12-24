from __future__ import annotations
import argparse
import json
import random
from concurrent.futures import ThreadPoolExecutor

from src.pastry.network import PastryNetwork
from src.chord.network import ChordNetwork
from src.common.dataset import iter_movies_csv

def run_workload(net, csv_path: str, inserts: int, lookups: int, updates: int, deletes: int, join_leave: int, K: int, seed: int = 0):
    rng = random.Random(seed)

    # load titles/values for a bounded prefix
    items = []
    for i, (title, value) in enumerate(iter_movies_csv(csv_path)):
        items.append((title, value))
        if len(items) >= max(inserts, lookups, updates, deletes, K) + 100:
            break

    inserted_titles = []

    # Inserts
    for title, value in items[:inserts]:
        net.insert(title, value)
        inserted_titles.append(title)

    # Lookups
    for _ in range(lookups):
        t = rng.choice(inserted_titles) if inserted_titles else items[0][0]
        net.lookup(t)

    # Updates
    for _ in range(updates):
        t = rng.choice(inserted_titles) if inserted_titles else items[0][0]
        net.update(t, {"popularity": rng.random() * 100})

    # Deletes
    for _ in range(deletes):
        t = rng.choice(inserted_titles) if inserted_titles else items[0][0]
        net.delete(t)

    # Join/Leave cycles
    for _ in range(join_leave):
        net.join()
        net.leave()

    # Concurrent K-title popularity lookup
    titles_k = [t for (t, _) in items[:K]]
    with ThreadPoolExecutor(max_workers=min(32, max(1, K))) as ex:
        futs = [ex.submit(net.lookup, t) for t in titles_k]
        results = [f.result() for f in futs]  # list of (value, hops)

    found = sum(1 for v, _h in results if v is not None and v.get("popularity") is not None)
    total_hops = sum(_h for _v, _h in results)

    return {
        "metrics": net.metrics.summary(),
        "k_query": {
            "K": K,
            "found_count": found,
            "total_hops": total_hops,
            "mean_hops": (total_hops / K) if K else 0.0,
        },
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to movies_dataset.csv")
    ap.add_argument("--N", type=int, default=100)
    ap.add_argument("--inserts", type=int, default=2000)
    ap.add_argument("--lookups", type=int, default=2000)
    ap.add_argument("--updates", type=int, default=300)
    ap.add_argument("--deletes", type=int, default=300)
    ap.add_argument("--join_leave", type=int, default=30)
    ap.add_argument("--K", type=int, default=50)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    pastry = PastryNetwork()
    pastry.build(args.N, seed=args.seed)

    chord = ChordNetwork()
    chord.build(args.N, seed=args.seed)

    out = {
        "pastry": run_workload(pastry, args.csv, args.inserts, args.lookups, args.updates, args.deletes, args.join_leave, args.K, seed=args.seed),
        "chord":  run_workload(chord,  args.csv, args.inserts, args.lookups, args.updates, args.deletes, args.join_leave, args.K, seed=args.seed),
        "params": vars(args),
    }

    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print("Wrote results.json")

if __name__ == "__main__":
    main()
