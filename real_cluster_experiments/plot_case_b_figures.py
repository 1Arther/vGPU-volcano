#!/usr/bin/env python3
"""Generate real-cluster case-b paper figures from aggregate.csv."""

from pathlib import Path
import csv
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
RESULT_DIR = ROOT / 'results_case_b_repeats'
FIG_DIR = RESULT_DIR / 'figures'
FIG_DIR.mkdir(parents=True, exist_ok=True)
ORDER = ['dqn-job', 'spread', 'binpack', 'original', 'random']
COLORS = {'dqn-job': '#2563eb'}


def load_rows():
    with open(RESULT_DIR / 'aggregate.csv', newline='') as f:
        rows = list(csv.DictReader(f))
    return sorted(rows, key=lambda r: ORDER.index(r['policy']) if r['policy'] in ORDER else 99)


def values(rows, key):
    return [float(r[key]) for r in rows]


def barplot(rows, mean_key, std_key, ylabel, title, out_name, ymin=None):
    policies = [r['policy'] for r in rows]
    vals = values(rows, mean_key)
    errs = values(rows, std_key)
    x = np.arange(len(policies))
    colors = [COLORS.get(p, '#9ca3af') for p in policies]
    fig, ax = plt.subplots(figsize=(6.2, 3.4))
    ax.bar(x, vals, yerr=errs, capsize=4, color=colors, edgecolor='#374151', linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(policies, rotation=20, ha='right')
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if ymin is not None:
        ymax = max(v + e for v, e in zip(vals, errs)) * 1.08
        ax.set_ylim(ymin, ymax)
    ax.grid(axis='y', alpha=0.25, linestyle='--')
    fig.tight_layout()
    fig.savefig(FIG_DIR / f'{out_name}.pdf', bbox_inches='tight')
    fig.savefig(FIG_DIR / f'{out_name}.svg', bbox_inches='tight')
    plt.close(fig)


def main():
    rows = load_rows()
    barplot(rows, 'balance_score_mean', 'balance_score_std',
            'Balance score (lower is better)',
            'Real-cluster case-b balance score',
            'case_b_balance_score', ymin=0.75)
    barplot(rows, 'intra_gap_mean', 'intra_gap_std',
            'Intra-GPU gap (lower is better)',
            'Real-cluster case-b intra-GPU gap',
            'case_b_intra_gap', ymin=0.0)
    barplot(rows, 'core_range_mean', 'core_range_std',
            'Core range (lower is better)',
            'Real-cluster case-b core range',
            'case_b_core_range', ymin=0.0)
    print('Generated case-b figures in', FIG_DIR)


if __name__ == '__main__':
    main()
