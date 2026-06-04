#!/usr/bin/env python3
import csv
import json
import math
import os
import re
import subprocess
import sys
import time
from pathlib import Path

OUT = Path('/home/zbs/vGPU-volcano/real_cluster_experiments/results_schedulejob')
OUT.mkdir(parents=True, exist_ok=True)

POLICIES = ['spread', 'dqn', 'dqn-job']
IMAGE = 'nvcr.io/nvidia/cuda:12.2.0-base-ubuntu22.04'
NODE = 't3dgq'
NS = 'default'
PREFIX = 'cmp-vgpu'
GPU_TOTAL_MEM = 24564.0
GPU_TOTAL_CORE = 100.0

WORKLOADS = {
    'balanced_8x1': [(1, 4096, 10)] * 8,
    'mixed_8x1': [
        (1, 8192, 10), (1, 8192, 10),
        (1, 4096, 35), (1, 4096, 35),
        (1, 4096, 15), (1, 4096, 15), (1, 4096, 15), (1, 4096, 15),
    ],
    'multi_4x2': [(2, 8192, 25)] * 4,
}


def run(cmd, check=True, capture=True):
    p = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE if capture else None, stderr=subprocess.STDOUT)
    if check and p.returncode != 0:
        print(p.stdout)
        raise RuntimeError(f'command failed: {cmd}')
    return p.stdout or ''


def cleanup():
    run(f"kubectl delete pod -n {NS} -l experiment={PREFIX} --ignore-not-found=true --force --grace-period=0", check=False)
    time.sleep(3)


def set_policy(policy):
    conf = f'''apiVersion: v1
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
          deviceshare.GPUSelectPolicy: {policy}
          deviceshare.DQNGRPCEndpoint: dqn-scheduler-grpc.volcano-system.svc.cluster.local:50051
      - name: predicates
      - name: proportion
      - name: nodeorder
      - name: binpack
'''
    path = OUT / f'config-{policy}.yaml'
    path.write_text(conf)
    run(f'kubectl apply -f {path}')
    run('kubectl rollout restart deployment/volcano-scheduler -n volcano-system')
    run('kubectl rollout status deployment/volcano-scheduler -n volcano-system --timeout=180s')
    time.sleep(3)


def pod_yaml(name, nums, mem, core):
    return f'''apiVersion: v1
kind: Pod
metadata:
  name: {name}
  namespace: {NS}
  labels:
    experiment: {PREFIX}
spec:
  runtimeClassName: nvidia
  schedulerName: volcano
  nodeSelector:
    kubernetes.io/hostname: {NODE}
  restartPolicy: Never
  containers:
    - name: hold-resource
      image: {IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["/bin/bash", "-lc"]
      args:
        - |
          echo "resource allocated; no GPU workload is executed"
          sleep 600
      resources:
        limits:
          volcano.sh/vgpu-number: {nums}
          volcano.sh/vgpu-memory: {mem}
          volcano.sh/vgpu-cores: {core}
'''


def submit(workload_name, specs):
    manifest = ''
    for i, (nums, mem, core) in enumerate(specs):
        manifest += pod_yaml(f'{PREFIX}-{workload_name.replace("_", "-")}-{i}', nums, mem, core) + '---\n'
    path = OUT / f'{workload_name}.yaml'
    path.write_text(manifest)
    run(f'kubectl apply -f {path}')


def collect(workload_name, specs, policy):
    expected = len(specs)
    deadline = time.time() + 90
    while time.time() < deadline:
        out = run(f"kubectl get pod -n {NS} -l experiment={PREFIX} -o json", check=False)
        data = json.loads(out) if out.strip() else {'items': []}
        items = data.get('items', [])
        running = sum(1 for x in items if x.get('status', {}).get('phase') == 'Running')
        annotated = sum(1 for x in items if x.get('metadata', {}).get('annotations', {}).get('volcano.sh/vgpu-ids-new'))
        if len(items) >= expected and annotated >= expected and running >= expected:
            break
        time.sleep(3)
    out = run(f"kubectl get pod -n {NS} -l experiment={PREFIX} -o json", check=False)
    data = json.loads(out) if out.strip() else {'items': []}
    rows = []
    gpu_usage = {}
    allocated_pods = 0
    running_pods = 0
    for item in sorted(data.get('items', []), key=lambda x: x['metadata']['name']):
        name = item['metadata']['name']
        phase = item.get('status', {}).get('phase', '')
        if phase == 'Running':
            running_pods += 1
        ann = item.get('metadata', {}).get('annotations', {})
        ids = ann.get('volcano.sh/vgpu-ids-new', '') or ''
        if ids:
            allocated_pods += 1
        entries = [x for x in ids.split(':') if x.strip()]
        gpus = []
        for e in entries:
            parts = e.split(',')
            if len(parts) >= 4:
                uuid, typ, mem_s, core_s = parts[:4]
                mem = float(mem_s)
                core = float(core_s)
                g = gpu_usage.setdefault(uuid, {'mem': 0.0, 'core': 0.0, 'slices': 0})
                g['mem'] += mem
                g['core'] += core
                g['slices'] += 1
                gpus.append(uuid[-8:])
        rows.append({'policy': policy, 'workload': workload_name, 'pod': name, 'phase': phase, 'annotation': ids, 'gpus': '|'.join(gpus)})
    # Include zero GPUs? For range fairness we infer only used GPUs; but all workloads target 4 GPUs.
    # Fill to 4 slots for a comparable empty-GPU penalty when policy packs too hard.
    vals = list(gpu_usage.values())
    while len(vals) < 4:
        vals.append({'mem': 0.0, 'core': 0.0, 'slices': 0})
    mems = [v['mem'] for v in vals]
    cores = [v['core'] for v in vals]
    slices = [v['slices'] for v in vals]
    def stdev(xs):
        m = sum(xs)/len(xs) if xs else 0.0
        return math.sqrt(sum((x-m)**2 for x in xs)/len(xs)) if xs else 0.0
    intra = []
    for v in vals:
        if v['mem'] > 0 or v['core'] > 0:
            intra.append(abs(v['mem']/GPU_TOTAL_MEM - v['core']/GPU_TOTAL_CORE))
    summary = {
        'policy': policy,
        'workload': workload_name,
        'expected_pods': expected,
        'allocated_pods': allocated_pods,
        'running_pods': running_pods,
        'success_rate': allocated_pods / expected if expected else 0,
        'mem_range': max(mems)-min(mems),
        'core_range': max(cores)-min(cores),
        'slice_range': max(slices)-min(slices),
        'mem_std': stdev(mems),
        'core_std': stdev(cores),
        'avg_intra_mem_core_gap': sum(intra)/len(intra) if intra else 0.0,
        'used_gpu_count': len(gpu_usage),
        'gpu_usage': json.dumps(gpu_usage, ensure_ascii=False, sort_keys=True),
    }
    return summary, rows


def main():
    all_summary = []
    all_rows = []
    cleanup()
    for policy in POLICIES:
        print(f'== policy {policy} ==', flush=True)
        set_policy(policy)
        for workload, specs in WORKLOADS.items():
            print(f'-- workload {workload}', flush=True)
            cleanup()
            submit(workload, specs)
            summary, rows = collect(workload, specs, policy)
            print(json.dumps(summary, ensure_ascii=False), flush=True)
            all_summary.append(summary)
            all_rows.extend(rows)
            cleanup()
    # Restore dqn for the cluster.
    set_policy('dqn')
    with open(OUT / 'summary.csv', 'w', newline='') as f:
        fields = ['policy','workload','expected_pods','allocated_pods','running_pods','success_rate','mem_range','core_range','slice_range','mem_std','core_std','avg_intra_mem_core_gap','used_gpu_count','gpu_usage']
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(all_summary)
    with open(OUT / 'pod_allocations.csv', 'w', newline='') as f:
        fields = ['policy','workload','pod','phase','annotation','gpus']
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(all_rows)
    print('RESULT', OUT / 'summary.csv')
    print('RESULT', OUT / 'pod_allocations.csv')

if __name__ == '__main__':
    main()
