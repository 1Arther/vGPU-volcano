# Volcano Job ScheduleJob Validation

This run uses real Volcano Jobs instead of independent Pods, so `GPUSelectPolicy=dqn-job` can list all Pods in the same Volcano Job and call the gRPC `ScheduleJob` API.

Reliable result:

- `balanced-job-8x1`: `dqn-job` matches `spread`; 8 Pods are allocated as 2 slices on each of 4 GPUs.
- `multi-job-4x2`: `dqn-job` matches `spread`; 4 Pods each request 2 vGPUs and all 4 GPUs receive 2 slices.

This validates the Job-level integration and fixes the earlier independent-Pod issue where `dqn-job` could not see the whole Job.
