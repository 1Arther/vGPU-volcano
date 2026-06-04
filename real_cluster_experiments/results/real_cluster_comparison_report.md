# Real Cluster Baseline Comparison
This experiment runs real Volcano scheduling on the Kubernetes cluster. Test Pods request vGPU resources and sleep; no CUDA workload is executed. Lower `mem_range`, `core_range`, `slice_range`, `avg_intra_mem_core_gap`, and `balance_score` are better.
## Environment
- Scheduler namespace: `volcano-system`
- DQN gRPC endpoint: `dqn-scheduler-grpc.volcano-system.svc.cluster.local:50051`
- Test node selector: `kubernetes.io/hostname=t3dgq`
- Test image: `nvcr.io/nvidia/cuda:12.2.0-base-ubuntu22.04`
- Workloads: `balanced_8x1`, `mixed_8x1`, `multi_4x2`
- Policies: `original`, `binpack`, `spread`, `random`, `dqn`

## Summary Table
| workload | policy | success | used_gpus | mem_range | core_range | slice_range | intra_gap | balance_score |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| balanced_8x1 | spread | 1.00 | 4 | 0 | 0 | 0 | 0.133 | 0.133 |
| balanced_8x1 | original | 1.00 | 3 | 12288 | 30 | 3 | 0.178 | 0.978 |
| balanced_8x1 | binpack | 1.00 | 3 | 12288 | 30 | 3 | 0.178 | 0.978 |
| balanced_8x1 | random | 1.00 | 3 | 12288 | 30 | 3 | 0.178 | 0.978 |
| balanced_8x1 | dqn | 1.00 | 3 | 12288 | 30 | 3 | 0.178 | 0.978 |
| mixed_8x1 | spread | 1.00 | 4 | 4096 | 25 | 0 | 0.208 | 0.625 |
| mixed_8x1 | random | 1.00 | 4 | 4096 | 25 | 0 | 0.208 | 0.625 |
| mixed_8x1 | dqn | 1.00 | 4 | 4096 | 25 | 0 | 0.208 | 0.625 |
| mixed_8x1 | binpack | 1.00 | 4 | 8192 | 50 | 2 | 0.117 | 0.950 |
| mixed_8x1 | original | 1.00 | 4 | 12288 | 35 | 2 | 0.125 | 0.975 |
| multi_4x2 | original | 1.00 | 4 | 0 | 0 | 0 | 0.167 | 0.167 |
| multi_4x2 | binpack | 1.00 | 4 | 0 | 0 | 0 | 0.167 | 0.167 |
| multi_4x2 | spread | 1.00 | 4 | 0 | 0 | 0 | 0.167 | 0.167 |
| multi_4x2 | random | 1.00 | 4 | 0 | 0 | 0 | 0.167 | 0.167 |
| multi_4x2 | dqn | 1.00 | 4 | 0 | 0 | 0 | 0.167 | 0.167 |

## Best Policy By Workload
- `balanced_8x1`: best observed policy is `spread` with balance_score=0.133.
- `mixed_8x1`: best observed policy is `spread` with balance_score=0.625.
- `multi_4x2`: best observed policy is `original` with balance_score=0.167.

## Interpretation
- All policies reached 100% allocation success on these moderate workloads. Therefore, this run mainly compares balance quality, not admission success.
- `spread` is best on `balanced_8x1`; it evenly uses all 4 GPUs with zero inter-GPU memory/core range.
- `spread`, `random`, and `dqn` tie closely on `mixed_8x1` for inter-GPU range, while `binpack/original` are worse on either memory or core balance.
- All policies tie on `multi_4x2`; this workload is symmetric enough that the feasible allocation naturally balances all GPUs.
- The current deployed Volcano path uses `GPUSelectPolicy=dqn`, which calls the single-Pod `Predict` API. It does not yet exploit the newer `ScheduleJob` API or Job-level features used in the v16 simulation.
- The existing `dqn-job` path in the live scheduler emitted a label/selector value length error for long PodGroup names and fell back to binpack. That path should be fixed before claiming Job-level DQN superiority in the real cluster.

## Files
- `summary.csv`: aggregate metrics per policy/workload.
- `pod_allocations.csv`: raw Pod annotations and selected GPU UUID suffixes.
- `run_policy_compare.py`: reproduction script.
