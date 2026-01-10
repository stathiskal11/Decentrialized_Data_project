import json

with open("results.json", encoding="utf-8") as f:
    d = json.load(f)

ops = ["insert", "lookup", "update", "delete", "join", "leave"]

print("op\tproto\tcount\tmean\tmedian\tp95")

for op in ops:
    for proto in ["chord", "pastry"]:
        m = d[proto]["metrics"].get(op)
        if m:
            print(f"{op}\t{proto}\t{m['count']}\t{m['mean']:.3f}\t{m['median']}\t{m['p95']}")
