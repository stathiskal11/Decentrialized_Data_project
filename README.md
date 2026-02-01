# Decentralized Data  
## Project-1: DHTs (Chord & Pastry) — Implementation and Experimental Evaluation

**Course:** Decentralized Data  
**Academic Year:** 2025–2026  
**Project:** Project-1 (DHTs)  

---

## 1. Project Description

This project implements and experimentally evaluates two structured overlay networks (Distributed Hash Tables):

- **Chord**
- **Pastry**

The implementation supports the required DHT functionality (routing and key–value storage) and provides an evaluation pipeline that measures routing performance in terms of **hops** for multiple operations and workloads.

The deliverables include:
- Full source code (Chord + Pastry)
- Experiment runner (baseline + optional grid runs)
- Plot generation scripts
- Technical report (methods + experimental results)

---

## 2. Implemented Protocols

### 2.1 Chord
- Identifier space and consistent hashing
- Successor/predecessor maintenance
- Finger table routing
- Key responsibility based on ring intervals

### 2.2 Pastry
- Prefix-based routing
- Routing table and leaf set maintenance
- Key responsibility based on nodeId proximity

---

## 3. Supported Operations (Workload)

Both overlays support:

- **Insert (put)**: store a key–value pair
- **Lookup (get)**: retrieve a value by key
- **Update**: update the value of an existing key
- **Delete**: delete a key–value pair
- **Join**: node joins the overlay
- **Leave**: node leaves the overlay

### Metrics
For each operation, the evaluation reports:
- **count**
- **mean hops**
- **median hops**
- **p95 hops**

---

## 4. Dataset

The project uses a CSV dataset of movies.

Example path used in this repository:
- `data/archive/movies_dataset_cleaned/data_movies_clean.csv`

The workload inserts keys derived from movie records and uses (a subset of) attributes for values (e.g., popularity, vote_average, vote_count, release_date).

> Note: The dataset file may not be included in the submission ZIP due to size. If not included, download it separately and place it at the expected path, or adjust the `--csv` argument accordingly.

---

## 5. Experimental Evaluation

### 5.1 Baseline Run
A baseline run executes a complete workload for both protocols with parameters:
- number of peers **N**
- operation counts (insert/lookup/update/delete)
- churn events (**join_leave**)
- K-query parameter (**K**) and randomness seed (**seed**)

The baseline run produces a `results.json` file.

### 5.2 K-query (Concurrent Lookups)
The project supports K concurrent lookups to fetch movie popularities (or derived attributes), using concurrency to evaluate routing under concurrent query load.

Optional grid runs across multiple values of:
- N ∈ {20, 50, 100}
- join_leave ∈ {0, 20, 50}
- K = 20
- seed = 1

Results are aggregated into:
- `results/kquery_summary.csv`
- `report_figures/*.png` (summary figures)

---

## 6. Output Artifacts

After running experiments and plotting:

- `results.json`  
  Baseline metrics for both protocols.

- `plots/`  
  Operation plots (mean/median/p95) per protocol and per operation.

- `results/`  
  Optional grid-run JSON outputs (e.g., `res_N100_JL20_K20_S1.json`).

- `report_figures/`  
  K-query summary figures (for the report).

> Important: Running experiments typically overwrites `results.json`. Rename it first if you want to keep an older version.

---

## 7. How to Run

### 7.1 Install dependencies
If `requirements.txt` exists:

pip install -r requirements.txt


7.2 FULL execution (used to generate results/plots for the technical report)
7.2.1 Run baseline workload (produces results.json)

python -m experiments.run_experiments --csv "data/archive/movies_dataset_cleaned/data_movies_clean.csv" --N 100 --inserts 200 --lookups 200 --updates 50 --deletes 50 --join_leave 20 --K 20 --seed 1

7.2.2 Generate plots from results.json
python -m experiments.plot_results --results "results.json" --outdir "plots"


7.3 DEMO execution (quick run for live presentation)

7.3.1 Quick baseline demo (fast results.json)
python -m experiments.run_experiments --csv "data/archive/movies_dataset_cleaned/data_movies_clean.csv" --N 50 --inserts 50 --lookups 50 --updates 10 --deletes 10 --join_leave 5 --K 20 --seed 1

7.3.2 Demo plots (avoid overwriting report plots)
python -m experiments.plot_results --results "results.json" --outdir "plots_demo"

