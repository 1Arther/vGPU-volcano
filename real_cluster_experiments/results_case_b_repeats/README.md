# Case-B Repeated Real-Cluster Experiment

This experiment repeats the real-cluster `case-b` workload 5 times. Each repeat first creates existing load through normal Volcano scheduling, then submits the same target Volcano Job under each policy.

Policies:

- `spread`
- `binpack`
- `original`
- `random`
- `dqn-job`

Preload workload:

| task | replicas | vgpu-number | vgpu-memory | vgpu-cores |
|---|---:|---:|---:|---:|
| pre-mem | 1 | 1 | 12000 | 10 |
| pre-core | 1 | 1 | 8000 | 65 |
| pre-small | 1 | 1 | 4000 | 20 |
| pre-mix | 1 | 1 | 12000 | 50 |

Target workload:

| task | replicas | vgpu-number | vgpu-memory | vgpu-cores |
|---|---:|---:|---:|---:|
| memhi | 3 | 1 | 8000 | 10 |
| corehi | 3 | 1 | 4000 | 35 |

All policies reached 10/10 Running Pods in every repeat. Lower metrics are better.

| policy | balance score | intra gap | mem range | core range | running |
|---|---:|---:|---:|---:|---:|
| dqn-job | 0.877 ± 0.000 | 0.039 ± 0.000 | 12000 ± 0 | 35.0 ± 0.0 | 10/10 |
| spread | 1.034 ± 0.494 | 0.226 ± 0.123 | 8800 ± 3919 | 45.0 ± 23.2 | 10/10 |
| binpack | 1.338 ± 0.113 | 0.232 ± 0.046 | 14400 ± 1960 | 52.0 ± 14.7 | 10/10 |
| original | 1.373 ± 0.142 | 0.217 ± 0.038 | 14400 ± 1960 | 57.0 ± 16.6 | 10/10 |
| random | 1.385 ± 0.267 | 0.304 ± 0.078 | 9600 ± 3200 | 69.0 ± 10.2 | 10/10 |

Figures:

- `figures/case_b_balance_score.svg`
- `figures/case_b_intra_gap.svg`
- `figures/case_b_core_range.svg`

Key result: `dqn-job` has the lowest mean balance score and the lowest GPU-internal mem/core gap across 5 real-cluster repeats.
