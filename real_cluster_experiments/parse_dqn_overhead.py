#!/usr/bin/env python3
"""Parse DQN scheduling overhead logs and report mean/P95 latency metrics.

Example:
  kubectl -n volcano-system logs deploy/volcano-scheduler > scheduler.log
  kubectl -n volcano-system logs deploy/dqn-scheduler-grpc > dqn.log
  python3 real_cluster_experiments/parse_dqn_overhead.py scheduler.log dqn.log \
      --csv real_cluster_experiments/dqn_overhead_summary.csv
"""

import argparse
import csv
import re
from pathlib import Path
from statistics import mean

FLOAT_RE = re.compile(r"([A-Za-z0-9_]+)=(-?\d+(?:\.\d+)?)")
FIELD_RE = re.compile(r"([A-Za-z0-9_]+)=([^\s]+)")

METRIC_KEYS = [
    "total_ms",
    "rpc_ms",
    "schedule_ms",
    "graph_build_ms",
    "action_select_ms",
    "helper_select_ms",
    "fallback_select_ms",
    "allocate_ms",
    "list_pods_ms",
    "request_build_ms",
    "process_ms",
    "scheduler_side_ms",
]


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100.0
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def parse_line(line):
    if "DQNOverhead" not in line and "[DQN-overhead]" not in line:
        return None
    fields = dict(FIELD_RE.findall(line))
    numeric = {k: float(v) for k, v in FLOAT_RE.findall(line)}
    source = "dqn-service" if "[DQN-overhead]" in line else "volcano-scheduler"
    policy = fields.get("policy", "dqn-service")
    method = fields.get("method", fields.get("path", "unknown"))
    return source, policy, method, numeric


def collect(paths):
    rows = []
    for path in paths:
        for line in Path(path).read_text(errors="replace").splitlines():
            parsed = parse_line(line)
            if parsed is not None:
                rows.append(parsed)
    return rows


def summarize(rows):
    grouped = {}
    for source, policy, method, numeric in rows:
        group = grouped.setdefault((source, policy, method), {k: [] for k in METRIC_KEYS})
        for key in METRIC_KEYS:
            if key in numeric:
                group[key].append(numeric[key])

    summary = []
    for (source, policy, method), metrics in sorted(grouped.items()):
        for key, values in metrics.items():
            if not values:
                continue
            summary.append(
                {
                    "source": source,
                    "policy": policy,
                    "method": method,
                    "metric": key,
                    "count": len(values),
                    "mean_ms": mean(values),
                    "p95_ms": percentile(values, 95),
                }
            )
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("logs", nargs="+", help="Scheduler and/or DQN service log files")
    parser.add_argument("--csv", default="", help="Optional CSV output path")
    args = parser.parse_args()

    rows = collect(args.logs)
    summary = summarize(rows)

    if args.csv:
        out = Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["source", "policy", "method", "metric", "count", "mean_ms", "p95_ms"],
            )
            writer.writeheader()
            writer.writerows(summary)

    for row in summary:
        print(
            f"{row['source']} {row['policy']} {row['method']} {row['metric']} "
            f"count={row['count']} mean_ms={row['mean_ms']:.3f} p95_ms={row['p95_ms']:.3f}"
        )


if __name__ == "__main__":
    main()
