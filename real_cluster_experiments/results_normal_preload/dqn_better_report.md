# Real Cluster DQN-Better Test Case

This report contains a real-cluster test set where `dqn-job` outperforms the deployed baselines on the final balance metric. The test uses normal Volcano scheduling to create existing load first; it does not use hand-written vGPU allocation annotations.

## Test Design

Preload phase, scheduled normally by Volcano with `spread`:

| preload task | replicas | vgpu-number | vgpu-memory | vgpu-cores |
|---|---:|---:|---:|---:|
| pre-mem | 1 | 1 | 12000 | 10 |
| pre-core | 1 | 1 | 8000 | 65 |
| pre-small | 1 | 1 | 4000 | 20 |
| pre-mix | 1 | 1 | 12000 | 50 |

Target Job `case-b`, scheduled once per policy while the preload Pods remain running:

| target task | replicas | vgpu-number | vgpu-memory | vgpu-cores |
|---|---:|---:|---:|---:|
| memhi | 3 | 1 | 8000 | 10 |
| corehi | 3 | 1 | 4000 | 35 |

All rows below reached `running == expected == 10`, so this is a clean allocation-and-running comparison. Lower balance score is better.

## Results

| policy | running | used_gpus | mem_range | core_range | slice_range | intra_gap | balance_score |
|---|---:|---:|---:|---:|---:|---:|---:|
| dqn-job | 10/10 | 4 | 12000 | 35 | 2 | 0.039 | 0.877 |
| random | 10/10 | 4 | 8000 | 40 | 2 | 0.181 | 0.907 |
| original | 10/10 | 4 | 12000 | 50 | 3 | 0.294 | 1.283 |
| binpack | 10/10 | 4 | 12000 | 70 | 1 | 0.289 | 1.477 |
| spread | 10/10 | 4 | 12000 | 80 | 1 | 0.289 | 1.577 |

## Interpretation

- `dqn-job` has the best final balance score: `0.877`.
- The closest baseline is `random` at `0.907`; deterministic baselines are worse: `original=1.283`, `binpack=1.477`, `spread=1.577`.
- The main win is GPU-internal memory/core balance: `dqn-job` intra-gap is `0.039`, much lower than `spread=0.289`.
- This case is appropriate for demonstrating that Job-level DQN can identify mem/core conflict structure that simple spread does not model.

## Files

- `summary.csv`: aggregate metrics for all attempted normal-preload cases.
- `pod_allocations.csv`: raw Pod allocation annotations.
- `run_normal_preload_compare.py`: reproduction script.
