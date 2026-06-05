# vGPU-Volcano DQN Scheduler Integration

This repository contains the Volcano/HAMi scheduler-side integration for a DQN-based vGPU placement policy. It is based on Volcano v1.13.1 and adds configurable GPU ordering policies plus a gRPC client that can call a Python DQN inference service.

The current paper-oriented branch supports two DQN modes:

- `dqn`: single-Pod `Predict` API. This is backward compatible but does not use Job-level features.
- `dqn-job`: Job-level `ScheduleJob` API. This is the preferred policy for experiments because it lets the model observe all Pods in the same Volcano Job / PodGroup.

## Policies

Supported `deviceshare.GPUSelectPolicy` values:

| policy | meaning |
|---|---|
| `original` | Volcano/HAMi source-order style GPU selection |
| `binpack` | prefer already-used GPUs |
| `spread` | prefer less-used GPUs |
| `random` | deterministic per-Pod randomized order |
| `dqn` | call gRPC `Predict` for single-Pod GPU ordering |
| `dqn-job` | call gRPC `ScheduleJob` for Job-level allocation guidance |

If `dqn-job` fails, the scheduler falls back to `dqn`; if that also fails, it falls back to `binpack` so the scheduler remains usable.

## Cluster Deployment

The DQN gRPC service is deployed from the companion repository `1Arther/vGPU-DQN` branch `v8`.

Current in-cluster endpoint:

```text
dqn-scheduler-grpc.volcano-system.svc.cluster.local:50051
```

Scheduler ConfigMap snippet:

```yaml
deviceshare.VGPUEnable: true
deviceshare.GPUSelectPolicy: dqn-job
deviceshare.DQNGRPCEndpoint: dqn-scheduler-grpc.volcano-system.svc.cluster.local:50051
```

On the current 10.33 cluster the scheduler is deployed by hostPath-mounting the locally built binary:

```text
/home/zbs/volcano-v1.13.1-dqn/_output/bin/vc-scheduler
```

The deployment currently uses:

```text
volcano-scheduler hostPath: /home/zbs/volcano-v1.13.1-dqn/_output/bin/vc-scheduler
GPUSelectPolicy: dqn-job
```

## Build

This repository is a patch subset, not a complete Volcano checkout. To build the scheduler binary, overlay this `pkg/` directory onto a full Volcano v1.13.1 tree.

```bash
cd /home/zbs/vGPU-volcano
IMAGE=vgpu-volcano-scheduler:dqn-v8 hack/patch_full_volcano_and_build_scheduler.sh
```

For the current cluster, the binary was built in:

```bash
cd /home/zbs/volcano-v1.13.1-dqn
make vc-scheduler
```

Package-level validation after overlay:

```bash
go test ./pkg/scheduler/api/devices/nvidia/vgpu ./pkg/scheduler/plugins/deviceshare
```

## Real-Cluster Experiments

Experiments are under `real_cluster_experiments/`.

### 1. Baseline Comparison

Path:

```text
real_cluster_experiments/results/
```

This compares `original`, `binpack`, `spread`, `random`, and single-Pod `dqn` on independent Pods. It shows that single-Pod `dqn` is not enough to exploit Job-level features.

### 2. ScheduleJob Validation

Path:

```text
real_cluster_experiments/results_vcjob/
```

This uses real Volcano Jobs so `dqn-job` can call `ScheduleJob`.

Key validation:

- `balanced-job-8x1`: `dqn-job` matches `spread`; 8 Pods are allocated as 2 slices on each of 4 GPUs.
- `multi-job-4x2`: `dqn-job` matches `spread`; 4 Pods each request 2 vGPUs and all GPUs receive 2 slices.

### 3. DQN-Better Case

Path:

```text
real_cluster_experiments/results_normal_preload/
```

This creates existing load through normal Volcano scheduling, then submits the same target Job under each policy. The clean `case-b` result shows `dqn-job` outperforming all baselines on the final balance score.

### 4. Repeated DQN-Better Case

Path:

```text
real_cluster_experiments/results_case_b_repeats/
```

This repeats `case-b` 5 times. All policies reach 10/10 Running Pods in every repeat.

Mean result over 5 real-cluster repeats:

| policy | balance score | intra gap | mem range | core range | running |
|---|---:|---:|---:|---:|---:|
| dqn-job | 0.877 ± 0.000 | 0.039 ± 0.000 | 12000 ± 0 | 35.0 ± 0.0 | 10/10 |
| spread | 1.034 ± 0.494 | 0.226 ± 0.123 | 8800 ± 3919 | 45.0 ± 23.2 | 10/10 |
| binpack | 1.338 ± 0.113 | 0.232 ± 0.046 | 14400 ± 1960 | 52.0 ± 14.7 | 10/10 |
| original | 1.373 ± 0.142 | 0.217 ± 0.038 | 14400 ± 1960 | 57.0 ± 16.6 | 10/10 |
| random | 1.385 ± 0.267 | 0.304 ± 0.078 | 9600 ± 3200 | 69.0 ± 10.2 | 10/10 |

Figures:

```text
real_cluster_experiments/results_case_b_repeats/figures/case_b_balance_score.svg
real_cluster_experiments/results_case_b_repeats/figures/case_b_intra_gap.svg
real_cluster_experiments/results_case_b_repeats/figures/case_b_core_range.svg
```

## Reproduce The Repeated Real-Cluster Case

Run as root on `t3dgq` because the kubeconfig is under root:

```bash
cd /home/zbs/vGPU-volcano
python3 real_cluster_experiments/run_case_b_repeats.py
```

The script restores the scheduler policy to `dqn-job` and cleans test Pods/Jobs at the end.

## Simulation Results

Large-scale simulation and paper figures are in the companion repository:

```text
/home/zbs/vGPU-DQN
```

Relevant artifacts include baseline, per-load, ablation, and multi-seed results generated by the v16 Job-feature DQN experiments.

## Paper Claim Boundary

A fair paper statement is:

- Simulation demonstrates large-scale trends and ablations for the Job-feature DQN model.
- Real-cluster experiments validate the end-to-end Volcano integration and show a repeated case where `dqn-job` improves final balance under existing load and mem/core-conflict target Jobs.
- Single-Pod `dqn` should not be used as the main paper method; `dqn-job` is the correct Volcano integration for the Job-level model.
