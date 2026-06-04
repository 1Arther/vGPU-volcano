# Volcano DQN gRPC Deployment Notes

This repository is a Volcano v1.13.1 patch subset, not a complete Volcano source tree. Build the scheduler image by overlaying `pkg/` onto a full Volcano v1.13.1 checkout:

```bash
cd /home/zbs/vGPU-volcano
IMAGE=vgpu-volcano-scheduler:dqn-v8 hack/patch_full_volcano_and_build_scheduler.sh
```

Current DQN endpoint default in source:

```text
dqn-scheduler-grpc.volcano-system.svc.cluster.local:50051
```

Deploy order:

```bash
# 1. Build and deploy DQN gRPC service.
cd /home/zbs/vGPU-DQN
IMAGE=vgpu-dqn-grpc:v8 DQN2/grpc/scripts/build_grpc_image.sh
kubectl apply -k DQN2/grpc/k8s
kubectl rollout status deployment/dqn-scheduler-grpc -n volcano-system

# 2. Build/publish the modified Volcano scheduler image, then point volcano-scheduler to it.
cd /home/zbs/vGPU-volcano
IMAGE=vgpu-volcano-scheduler:dqn-v8 hack/patch_full_volcano_and_build_scheduler.sh
kubectl -n volcano-system set image deployment/volcano-scheduler volcano-scheduler=vgpu-volcano-scheduler:dqn-v8
kubectl apply -f deploy/scheduler-configmap-dqn.yaml
kubectl rollout restart deployment/volcano-scheduler -n volcano-system
kubectl rollout status deployment/volcano-scheduler -n volcano-system

# 3. Resource-only scheduling tests. These occupy vGPU resources but do not run CUDA workload.
kubectl apply -f deploy/volcano-vgpu-resource-only-pod.yaml
kubectl apply -f deploy/volcano-vgpu-4pod-2vgpu-job.yaml
```

On 10.33, `kubectl` currently has no kubeconfig and falls back to `localhost:8080`. Copy a valid kubeconfig to `/home/zbs/.kube/config` or set `KUBECONFIG` before applying the manifests.

The resource-only manifests use `nvcr.io/nvidia/cuda:12.2.0-base-ubuntu22.04` and only run `sleep`. `busybox` is intentionally avoided because the NVIDIA runtime injection can fail against its minimal libc layout. The manifests use `nodeSelector` rather than `nodeName`; setting `nodeName` bypasses Volcano binding and can make the vGPU device plugin reject the Pod with `device request not found`.
