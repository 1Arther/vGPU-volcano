# vGPU-Volcano: DQN-based vGPU Scheduling for Volcano/HAMi

本仓库基于 Volcano v1.13.1，对 Volcano/HAMi 的 vGPU 调度链路进行了实验性修改，主要目标是支持**节点内 GPU 选择策略可配置**，并进一步通过 gRPC 接入外部 Python DQN 推理服务，实现基于强化学习的 vGPU 选卡策略。

> 注意：本仓库不是官方 Volcano 仓库，而是用于 vGPU 调度实验的修改版。

---

## 1. 背景与目标

Volcano/HAMi 支持将一张物理 GPU 切分为多个 vGPU slice，并通过 `volcano.sh/vgpu-number`、`volcano.sh/vgpu-memory`、`volcano.sh/vgpu-cores` 等资源字段进行调度。

原始 Volcano/HAMi 的设备选择逻辑主要由内部固定扫描顺序和已有调度策略控制。为了研究不同节点内 GPU 选择策略对 vGPU 分配、资源利用率和任务性能的影响，本仓库增加了可配置的 GPU 选择策略：

- `original`：保留原始扫描倾向；
- `binpack`：优先把任务集中放到已使用 GPU 上；
- `spread`：优先把任务均匀分散到多张 GPU 上；
- `random`：随机选择 GPU 顺序；
- `dqn`：通过 gRPC 调用外部 Python GNN-DQN 服务返回 GPU 排序。

---

## 2. 主要改动

### 2.1 新增节点内 GPU 选择策略

在 `deviceshare` 插件中新增参数：

```yaml
deviceshare.GPUSelectPolicy: dqn

支持以下取值：

original
binpack
spread
random
dqn

相关修改文件：

pkg/scheduler/plugins/deviceshare/deviceshare.go
pkg/scheduler/api/devices/nvidia/vgpu/type.go
pkg/scheduler/api/devices/nvidia/vgpu/utils.go
2.2 新增 DQN gRPC endpoint 配置

新增参数：

deviceshare.DQNGRPCEndpoint: 172.16.20.32:50051

该参数用于指定外部 Python DQN 推理服务地址。

示例 Volcano scheduler ConfigMap：

apiVersion: v1
kind: ConfigMap
metadata:
  name: volcano-scheduler-configmap
  namespace: volcano-system
data:
  volcano-scheduler.conf: |
    actions: "enqueue, allocate, backfill"
    tiers:
    - plugins:
      - name: priority
      - name: gang
        enablePreemptable: false
      - name: conformance
    - plugins:
      - name: overcommit
      - name: drf
        enablePreemptable: false
      - name: deviceshare
        arguments:
          deviceshare.VGPUEnable: true
          deviceshare.GPUSelectPolicy: dqn
          deviceshare.DQNGRPCEndpoint: 172.16.20.32:50051
      - name: predicates
      - name: proportion
      - name: nodeorder
      - name: binpack
2.3 新增 DQN gRPC client

新增 Go gRPC client：

pkg/scheduler/api/devices/nvidia/vgpu/dqn_client.go

功能：

收集当前节点内 GPU 状态；
将 Pod 的 vGPU 请求和 GPU 状态发送给外部 DQN 服务；
接收 DQN 返回的 GPU index 排序；
Volcano/HAMi 按该排序尝试分配 vGPU；
如果 DQN 服务不可用，则 fallback 到 binpack，避免 scheduler 不可用。
2.4 新增 gRPC proto

新增 proto 文件及 Go 生成代码：

pkg/scheduler/api/devices/nvidia/vgpu/dqngrpc/dqn_scheduler.proto
pkg/scheduler/api/devices/nvidia/vgpu/dqngrpc/dqn_scheduler.pb.go
pkg/scheduler/api/devices/nvidia/vgpu/dqngrpc/dqn_scheduler_grpc.pb.go

gRPC 服务接口：

service DQNScheduler {
  rpc Predict(PredictRequest) returns (PredictResponse);
}

其中 PredictRequest 包含：

节点名称；
Pod namespace/name；
Pod 请求的 vGPU memory/core/number；
当前节点内所有 GPU 的 used memory、used core、used number、total memory 等状态。

PredictResponse 返回：

ordered_indexes：DQN 推荐的 GPU 排序；
selected_index：首选 GPU；
scores：每张 GPU 的 DQN 分数；
fallback：是否为服务端 fallback；
reason：返回原因。
3. 外部 DQN 服务

DQN 推理服务不在本仓库中实现，而是在外部 Python 项目中运行。

当前实验中，DQN 服务运行在：

R5300: 172.16.20.32:50051

Python 服务加载 v5 训练得到的 GNN-DQN 模型，并通过 gRPC 接收 Volcano scheduler 的请求。

Volcano 侧调用链路：

Volcano scheduler
  -> deviceshare plugin
  -> vgpu orderedGPUIndexes()
  -> DQN gRPC client
  -> Python GNN-DQN service
  -> 返回 GPU 排序
  -> Volcano/HAMi 按排序分配 vGPU
4. 编译方式

在仓库根目录执行：

make vc-scheduler

编译成功后生成：

_output/bin/vc-scheduler

当前实验中，Volcano scheduler Pod 通过 hostPath 挂载本地编译出的 vc-scheduler 二进制。因此修改代码并重新编译后，只需要重启 scheduler：

kubectl rollout restart deployment volcano-scheduler -n volcano-system
kubectl rollout status deployment volcano-scheduler -n volcano-system
5. 实验环境
5.1 Kubernetes/Volcano 环境

集群包含两个节点：

t3dgq: control-plane, 4 × NVIDIA GeForce RTX 4090
r5300: worker,        4 × NVIDIA A40

实验中主要使用 t3dgq 作为单节点多 GPU vGPU 调度测试节点。

5.2 vGPU 资源示例

每个测试 Pod 请求：

resources:
  limits:
    volcano.sh/vgpu-number: 1
    volcano.sh/vgpu-memory: 4096
    volcano.sh/vgpu-cores: 10
5.3 测试 Pod 示例
apiVersion: v1
kind: Pod
metadata:
  name: vgpu-test-1
spec:
  runtimeClassName: nvidia
  schedulerName: volcano
  nodeSelector:
    bszh.ai/vgpu-dqn-node: "true"
  restartPolicy: Never
  containers:
  - name: test
    image: pytorch/pytorch:2.2.2-cuda12.1-cudnn8-runtime
    imagePullPolicy: IfNotPresent
    command: ["/bin/bash", "-lc"]
    args:
    - |
      python3 -u - <<'PY'
      import torch, time
      print("cuda:", torch.cuda.is_available(), flush=True)
      print("device_count:", torch.cuda.device_count(), flush=True)
      time.sleep(600)
      PY
    resources:
      limits:
        volcano.sh/vgpu-number: 1
        volcano.sh/vgpu-memory: 4096
        volcano.sh/vgpu-cores: 10
6. 实验结果
6.1 策略切换验证

通过修改：

deviceshare.GPUSelectPolicy: <policy>

已经验证 Volcano/HAMi 可以切换不同节点内 GPU 选择策略。

测试策略包括：

original
binpack
spread
random
dqn
6.2 单节点 8 Pod 分配结果

在单节点 4 × RTX 4090 上，连续创建 8 个 vGPU Pod，每个 Pod 请求 1 个 vGPU、4096 MiB 显存、10% core。

已观察到的 baseline 分配结果如下：

Policy	GPU0	GPU1	GPU2	GPU3	行为特点
original	0	1	3	4	保留原始高 index 优先倾向
binpack	0	0	4	4	集中使用少数 GPU
spread	2	2	2	2	均匀分散到所有 GPU
random	4	2	1	1	随机偏斜分布

这些结果说明新增的 GPUSelectPolicy 能够实际改变 Volcano/HAMi 节点内 GPU 分配行为。

6.3 DQN gRPC 接入结果

在 deviceshare.GPUSelectPolicy=dqn 下，Volcano scheduler 能够成功调用外部 Python GNN-DQN 服务。

典型日志如下：

DQNPolicy grpc endpoint=172.16.20.32:50051 pod=default/vgpu-test-3 selected=3 ordered=[3 2 1 0] fallback=false reason=dqn
GPUSelectPolicy=dqn reqMem=4096 reqCore=10 ordered indexes=[3 2 1 0]

当 GPU3 不可继续放置时，DQN 服务会返回避开 GPU3 的排序，例如：

DQNPolicy grpc endpoint=172.16.20.32:50051 pod=default/vgpu-test-4 selected=2 ordered=[2 1 0 3] fallback=false reason=dqn
GPUSelectPolicy=dqn reqMem=4096 reqCore=10 ordered indexes=[2 1 0 3]

这说明：

Volcano scheduler 已经成功接入外部 DQN 推理服务；
DQN 返回结果不是本地 fallback；
fallback=false reason=dqn 表明排序结果来自 DQN 服务；
当某些 GPU fit=false 时，DQN 排序能够将不可用 GPU 放到后面。
6.4 gRPC 长连接优化结果

初始版本中，Go client 每次排序都会重新 Dial gRPC 服务，在 Volcano 高频 predicate/order 调用下偶尔出现：

context deadline exceeded

后续将 gRPC client 改为长连接复用：

scheduler process 内复用 grpc.ClientConn
endpoint 改变或请求失败时 reset connection

优化后，scheduler 日志中持续出现：

fallback=false reason=dqn

并且未再持续出现 deadline exceeded，说明 gRPC 推理链路已经稳定。

7. 当前结论

本仓库完成了以下工作：

在 Volcano/HAMi vGPU 调度链路中新增节点内 GPU 选择策略；
支持 original、binpack、spread、random 四种规则策略；
支持通过 deviceshare.GPUSelectPolicy=dqn 调用外部 DQN 服务；
实现 Go scheduler 到 Python DQN 服务的 gRPC 推理链路；
通过真实 Kubernetes/Volcano 环境验证 DQN 推理链路可用；
通过日志确认 DQN 返回的 GPU 排序已实际进入 Volcano/HAMi 的 GPU 分配逻辑。
8. 当前限制

当前实验仍有以下限制：

DQN 服务部署在外部 Python 进程中，尚未容器化；
当前 DQN 服务依赖预训练模型，模型训练代码不在本仓库；
当前 README 中的 DQN 结果主要验证调度链路和 GPU 排序接入，完整 JCT/吞吐性能对比仍需进一步实验；
如果宿主机上已有非 HAMI 管理的 GPU 进程，Volcano/HAMi 账本不一定能感知该真实负载，因此真实性能实验前应保证 GPU 初始状态干净；
当前 DQN 策略是单节点 GPU 排序层面的接入，多节点联合调度策略仍需进一步扩展。
9. 后续计划

后续可以继续做：

将 Python DQN gRPC 服务容器化，并以 Kubernetes Service 方式部署；
增加 DQN 与 original/binpack/spread/random 的 JCT、吞吐、GPU 利用率对比；
增加多随机种子实验和置信区间；
进一步减少 scheduler 与外部模型服务交互的延迟；
扩展到多节点 vGPU 调度；
将任务类型、模型类型、训练/推理负载等特征纳入 DQN 状态输入。
10. 快速使用
10.1 启动外部 DQN 服务

在 DQN 服务所在机器上启动 Python gRPC 服务，例如：

python DQN2/grpc/dqn_scheduler_server.py \
  --model DQN2/outputs_mixed_load_fixed_v5/vgpu_dqn_mixed_best.pth \
  --host 0.0.0.0 \
  --port 50051 \
  --device cpu \
  --hidden-dim 256 \
  --fallback binpack
10.2 配置 Volcano scheduler

修改 volcano-scheduler-configmap：

deviceshare.VGPUEnable: true
deviceshare.GPUSelectPolicy: dqn
deviceshare.DQNGRPCEndpoint: 172.16.20.32:50051

重启 scheduler：

kubectl rollout restart deployment volcano-scheduler -n volcano-system
kubectl rollout status deployment volcano-scheduler -n volcano-system
10.3 验证 DQN 是否生效

查看 scheduler 日志：

kubectl logs -n volcano-system deploy/volcano-scheduler \
  | grep -E "DQNPolicy|GPUSelectPolicy=dqn" \
  | tail -100

如果看到：

fallback=false reason=dqn

说明 DQN 推理链路已经生效。