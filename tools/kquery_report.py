import glob
import json
import os
import re
from dataclasses import dataclass
from typing import List, Tuple

import matplotlib.pyplot as plt


@dataclass(frozen=True)
class Row:
    path: str
    N: int
    JL: int
    K: int
    seed: int
    pastry_found: int
    pastry_K: int
    pastry_mean_hops: float
    chord_found: int
    chord_K: int
    chord_mean_hops: float


def parse_filename(path: str) -> Tuple[int, int, int, int]:
    """
    Expect: results/res_N{N}_JL{JL}_K{K}_S{seed}.json
    """
    base = os.path.basename(path)
    m = re.match(r"res_N(\d+)_JL(\d+)_K(\d+)_S(\d+)\.json$", base)
    if not m:
        raise ValueError(f"Unexpected filename format: {base}")
    return int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))


def load_rows(pattern: str = "results/res_*.json") -> List[Row]:
    rows: List[Row] = []
    for p in sorted(glob.glob(pattern)):
        N, JL, K, seed = parse_filename(p)
        with open(p, "r", encoding="utf-8") as f:
            d = json.load(f)

        pk = d["pastry"]["k_query"]["K"]
        ck = d["chord"]["k_query"]["K"]
        # sanity
        if pk != K or ck != K:
            raise ValueError(f"K mismatch in {p}: filename K={K}, pastry K={pk}, chord K={ck}")

        rows.append(
            Row(
                path=p,
                N=N,
                JL=JL,
                K=K,
                seed=seed,
                pastry_found=d["pastry"]["k_query"]["found_count"],
                pastry_K=pk,
                pastry_mean_hops=float(d["pastry"]["k_query"]["mean_hops"]),
                chord_found=d["chord"]["k_query"]["found_count"],
                chord_K=ck,
                chord_mean_hops=float(d["chord"]["k_query"]["mean_hops"]),
            )
        )
    if not rows:
        raise RuntimeError("No results found. Expected files like results/res_N*_JL*_K*_S*.json")
    return rows


def write_csv(rows: List[Row], out_csv: str = "results/kquery_summary.csv") -> None:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    with open(out_csv, "w", encoding="utf-8") as f:
        f.write("run,N,join_leave,K,seed,pastry_found,pastry_mean_hops,chord_found,chord_mean_hops\n")
        for r in rows:
            f.write(
                f"{os.path.basename(r.path)},{r.N},{r.JL},{r.K},{r.seed},"
                f"{r.pastry_found}/{r.pastry_K},{r.pastry_mean_hops},"
                f"{r.chord_found}/{r.chord_K},{r.chord_mean_hops}\n"
            )


def write_markdown(rows: List[Row], out_md: str = "results/kquery_summary.md") -> None:
    lines = []
    lines.append("| run | N | join_leave | K | seed | pastry found | pastry mean_hops | chord found | chord mean_hops |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        lines.append(
            f"| {os.path.basename(r.path)} | {r.N} | {r.JL} | {r.K} | {r.seed} | "
            f"{r.pastry_found}/{r.pastry_K} | {r.pastry_mean_hops:.3f} | "
            f"{r.chord_found}/{r.chord_K} | {r.chord_mean_hops:.3f} |"
        )
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def plot_grouped_bars(labels: List[str], a: List[float], b: List[float], ylabel: str, title: str, out_png: str) -> None:
    import numpy as np

    x = np.arange(len(labels))
    width = 0.38

    plt.figure()
    plt.bar(x - width / 2, a, width, label="Pastry")
    plt.bar(x + width / 2, b, width, label="Chord")
    plt.xticks(x, labels, rotation=35, ha="right")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    os.makedirs(os.path.dirname(out_png), exist_ok=True)
    plt.savefig(out_png, dpi=200)
    plt.close()


def plot_trend(rows: List[Row], fixed_field: str, fixed_value: int, x_field: str, out_png: str, title: str) -> None:
    """
    Produce a line plot of mean_hops vs x_field for both overlays,
    filtering rows where fixed_field == fixed_value.
    """
    filtered = [r for r in rows if getattr(r, fixed_field) == fixed_value]
    if not filtered:
        raise RuntimeError(f"No rows for {fixed_field}={fixed_value}")

    # sort by x
    filtered.sort(key=lambda r: getattr(r, x_field))
    xs = [getattr(r, x_field) for r in filtered]
    pastry = [r.pastry_mean_hops for r in filtered]
    chord = [r.chord_mean_hops for r in filtered]

    plt.figure()
    plt.plot(xs, pastry, marker="o", label="Pastry")
    plt.plot(xs, chord, marker="o", label="Chord")
    plt.xlabel(x_field)
    plt.ylabel("mean_hops (K-query)")
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    os.makedirs(os.path.dirname(out_png), exist_ok=True)
    plt.savefig(out_png, dpi=200)
    plt.close()


def main() -> None:
    rows = load_rows()

    # table outputs
    write_csv(rows)
    write_markdown(rows)

    # per-run labels (compact)
    labels = [f"N{r.N}-JL{r.JL}" for r in rows]

    # plot: mean hops by run (grouped bars)
    plot_grouped_bars(
        labels,
        [r.pastry_mean_hops for r in rows],
        [r.chord_mean_hops for r in rows],
        ylabel="mean_hops (K-query)",
        title="K-query mean hops per run (Pastry vs Chord)",
        out_png="report_figures/kquery_mean_hops_by_run.png",
    )

    # plot: found rate by run (found_count/K)
    plot_grouped_bars(
        labels,
        [r.pastry_found / r.pastry_K for r in rows],
        [r.chord_found / r.chord_K for r in rows],
        ylabel="found_rate (found_count/K)",
        title="K-query correctness per run (Pastry vs Chord)",
        out_png="report_figures/kquery_found_rate_by_run.png",
    )

    # trend plots aligned with your matrix:
    # - vs N for JL=20
    plot_trend(
        rows,
        fixed_field="JL",
        fixed_value=20,
        x_field="N",
        out_png="report_figures/kquery_mean_hops_vs_N_JL20.png",
        title="K-query mean hops vs N (join_leave=20)",
    )

    # - vs join_leave for N=100
    plot_trend(
        rows,
        fixed_field="N",
        fixed_value=100,
        x_field="JL",
        out_png="report_figures/kquery_mean_hops_vs_JL_N100.png",
        title="K-query mean hops vs join_leave (N=100)",
    )

    print("Wrote: results/kquery_summary.csv, results/kquery_summary.md")
    print("Wrote plots in: report_figures/")


if __name__ == "__main__":
    main()
