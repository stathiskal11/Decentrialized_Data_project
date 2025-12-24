from __future__ import annotations
import argparse
import json
import os
import matplotlib.pyplot as plt

OPS = ["insert", "lookup", "update", "delete", "join", "leave"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results.json")
    ap.add_argument("--outdir", default="plots")
    args = ap.parse_args()

    with open(args.results, "r", encoding="utf-8") as f:
        data = json.load(f)

    os.makedirs(args.outdir, exist_ok=True)

    for op in OPS:
        p = data["pastry"]["metrics"].get(op)
        c = data["chord"]["metrics"].get(op)
        if not p and not c:
            continue

        labels = ["pastry", "chord"]
        means = [ (p["mean"] if p else 0.0), (c["mean"] if c else 0.0) ]
        med   = [ (p["median"] if p else 0.0), (c["median"] if c else 0.0) ]
        p95   = [ (p["p95"] if p else 0.0), (c["p95"] if c else 0.0) ]

        # mean plot
        plt.figure()
        plt.bar(labels, means)
        plt.title(f"{op} — mean hops")
        plt.ylabel("hops")
        plt.savefig(os.path.join(args.outdir, f"{op}_mean.png"), dpi=200)
        plt.close()

        # median plot
        plt.figure()
        plt.bar(labels, med)
        plt.title(f"{op} — median hops")
        plt.ylabel("hops")
        plt.savefig(os.path.join(args.outdir, f"{op}_median.png"), dpi=200)
        plt.close()

        # p95 plot
        plt.figure()
        plt.bar(labels, p95)
        plt.title(f"{op} — p95 hops")
        plt.ylabel("hops")
        plt.savefig(os.path.join(args.outdir, f"{op}_p95.png"), dpi=200)
        plt.close()

    print(f"Plots written to {args.outdir}/")

if __name__ == "__main__":
    main()
