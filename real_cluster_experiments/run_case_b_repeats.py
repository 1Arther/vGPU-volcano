#!/usr/bin/env python3
import csv
import json
import math
import statistics
import subprocess
import time
from pathlib import Path

OUT = Path('/home/zbs/vGPU-volcano/real_cluster_experiments/results_case_b_repeats')
OUT.mkdir(parents=True, exist_ok=True)
IMAGE = 'nvcr.io/nvidia/cuda:12.2.0-base-ubuntu22.04'
NODE = 't3dgq'
TOTAL_MEM = 24564.0
TOTAL_CORE = 100.0
PREFIX = 'caseb-repeat'
POLICIES = ['spread', 'binpack', 'original', 'random', 'dqn-job']
REPEATS = 5
PRELOAD = [
    ('pre-mem', 1, 1, 12000, 10),
    ('pre-core', 1, 1, 8000, 65),
    ('pre-small', 1, 1, 4000, 20),
    ('pre-mix', 1, 1, 12000, 50),
]
TARGET = [
    ('memhi', 3, 1, 8000, 10),
    ('corehi', 3, 1, 4000, 35),
]


def run(cmd, check=True):
    p = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and p.returncode:
        print(p.stdout)
        raise RuntimeError(cmd)
    return p.stdout


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
    time.sleep(4)


def delete_job(name):
    run(f'kubectl delete vcjob {name} --ignore-not-found=true', False)


def cleanup_all():
    delete_job('caseb-repeat-preload')
    delete_job('caseb-repeat-target')
    run(f'kubectl delete pod -l experiment={PREFIX} --ignore-not-found=true --force --grace-period=0', False)
    time.sleep(8)


def job_yaml(name, specs, role, repeat, policy=None):
    min_available = sum(reps for _, reps, _, _, _ in specs)
    tasks = []
    for idx, (task_name, reps, nums, mem, core) in enumerate(specs):
        labels = f'''            experiment: {PREFIX}
            role: {role}
            repeat: "{repeat}"'''
        if policy:
            labels += f'\n            policy: {policy}'
        tasks.append(f'''    - name: {task_name}-{idx}
      replicas: {reps}
      template:
        metadata:
          labels:
{labels}
        spec:
          runtimeClassName: nvidia
          schedulerName: volcano
          nodeSelector:
            kubernetes.io/hostname: {NODE}
          restartPolicy: Never
          containers:
          - name: hold
            image: {IMAGE}
            imagePullPolicy: IfNotPresent
            command: ["/bin/bash", "-lc"]
            args: ["echo {role}; sleep 420"]
            resources:
              limits:
                volcano.sh/vgpu-number: {nums}
                volcano.sh/vgpu-memory: {mem}
                volcano.sh/vgpu-cores: {core}
''')
    return f'''apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: {name}
spec:
  schedulerName: volcano
  minAvailable: {min_available}
  tasks:
{''.join(tasks)}'''


def wait_role(role, expected):
    deadline = time.time() + 120
    data = {'items': []}
    while time.time() < deadline:
        raw = run(f'kubectl get pod -l experiment={PREFIX},role={role} -o json', False)
        data = json.loads(raw) if raw.strip() else {'items': []}
        items = data.get('items', [])
        running = sum(1 for item in items if item.get('status', {}).get('phase') == 'Running')
        allocated = sum(1 for item in items if item.get('metadata', {}).get('annotations', {}).get('volcano.sh/vgpu-ids-new'))
        if len(items) >= expected and running >= expected and allocated >= expected:
            return data
        time.sleep(3)
    return data


def collect(repeat, policy):
    raw = run(f'kubectl get pod -l experiment={PREFIX} -o json', False)
    data = json.loads(raw) if raw.strip() else {'items': []}
    gpu = {}
    pod_rows = []
    for item in sorted(data.get('items', []), key=lambda x: x['metadata']['name']):
        labels = item['metadata'].get('labels', {})
        ann = item['metadata'].get('annotations', {}).get('volcano.sh/vgpu-ids-new', '') or ''
        phase = item.get('status', {}).get('phase', '')
        pod_rows.append({
            'repeat': repeat,
            'policy': policy,
            'pod': item['metadata']['name'],
            'role': labels.get('role', ''),
            'phase': phase,
            'annotation': ann,
        })
        for entry in [x for x in ann.split(':') if x.strip()]:
            parts = entry.split(',')
            if len(parts) >= 4:
                g = gpu.setdefault(parts[0], {'mem': 0.0, 'core': 0.0, 'slices': 0})
                g['mem'] += float(parts[2])
                g['core'] += float(parts[3])
                g['slices'] += 1
    vals = list(gpu.values())
    while len(vals) < 4:
        vals.append({'mem': 0.0, 'core': 0.0, 'slices': 0})
    mem = [v['mem'] for v in vals]
    core = [v['core'] for v in vals]
    slices = [v['slices'] for v in vals]
    intra = [abs(v['mem'] / TOTAL_MEM - v['core'] / TOTAL_CORE) for v in vals if v['mem'] or v['core']]
    balance_score = (max(mem) - min(mem)) / TOTAL_MEM + (max(core) - min(core)) / TOTAL_CORE + (sum(intra) / len(intra) if intra else 0)
    summary = {
        'repeat': repeat,
        'policy': policy,
        'expected': sum(x[1] for x in PRELOAD) + sum(x[1] for x in TARGET),
        'allocated': sum(1 for row in pod_rows if row['annotation']),
        'running': sum(1 for row in pod_rows if row['phase'] == 'Running'),
        'used_gpus': len(gpu),
        'mem_range': max(mem) - min(mem),
        'core_range': max(core) - min(core),
        'slice_range': max(slices) - min(slices),
        'intra_gap': sum(intra) / len(intra) if intra else 0,
        'balance_score': balance_score,
        'gpu_usage': json.dumps(gpu, sort_keys=True),
    }
    return summary, pod_rows

all_summary = []
all_pods = []
try:
    for repeat in range(1, REPEATS + 1):
        print(f'REPEAT {repeat}', flush=True)
        cleanup_all()
        set_policy('spread')
        preload_path = OUT / f'repeat-{repeat}-preload.yaml'
        preload_path.write_text(job_yaml('caseb-repeat-preload', PRELOAD, 'preload', repeat))
        run(f'kubectl apply -f {preload_path}')
        wait_role('preload', sum(x[1] for x in PRELOAD))
        for policy in POLICIES:
            print(f'POLICY {policy}', flush=True)
            delete_job('caseb-repeat-target')
            run(f'kubectl delete pod -l experiment={PREFIX},role=target --ignore-not-found=true --force --grace-period=0', False)
            time.sleep(5)
            set_policy(policy)
            target_path = OUT / f'repeat-{repeat}-{policy}-target.yaml'
            target_path.write_text(job_yaml('caseb-repeat-target', TARGET, 'target', repeat, policy))
            run(f'kubectl apply -f {target_path}')
            wait_role('target', sum(x[1] for x in TARGET))
            summary, pods = collect(repeat, policy)
            print(summary, flush=True)
            all_summary.append(summary)
            all_pods.extend(pods)
finally:
    cleanup_all()
    set_policy('dqn-job')
    with open(OUT / 'summary.csv', 'w', newline='') as f:
        fields = ['repeat', 'policy', 'expected', 'allocated', 'running', 'used_gpus', 'mem_range', 'core_range', 'slice_range', 'intra_gap', 'balance_score', 'gpu_usage']
        writer = csv.DictWriter(f, fields)
        writer.writeheader()
        writer.writerows(all_summary)
    with open(OUT / 'pod_allocations.csv', 'w', newline='') as f:
        fields = ['repeat', 'policy', 'pod', 'role', 'phase', 'annotation']
        writer = csv.DictWriter(f, fields)
        writer.writeheader()
        writer.writerows(all_pods)
    grouped = {}
    for row in all_summary:
        grouped.setdefault(row['policy'], []).append(row)
    agg_rows = []
    for policy, rows in grouped.items():
        agg = {'policy': policy, 'n': len(rows)}
        for key in ['balance_score', 'intra_gap', 'mem_range', 'core_range', 'slice_range', 'running', 'allocated']:
            vals = [float(r[key]) for r in rows]
            agg[f'{key}_mean'] = statistics.mean(vals)
            agg[f'{key}_std'] = statistics.pstdev(vals) if len(vals) > 1 else 0.0
        agg_rows.append(agg)
    agg_rows.sort(key=lambda r: r['balance_score_mean'])
    with open(OUT / 'aggregate.csv', 'w', newline='') as f:
        fields = ['policy', 'n', 'balance_score_mean', 'balance_score_std', 'intra_gap_mean', 'intra_gap_std', 'mem_range_mean', 'mem_range_std', 'core_range_mean', 'core_range_std', 'slice_range_mean', 'slice_range_std', 'running_mean', 'running_std', 'allocated_mean', 'allocated_std']
        writer = csv.DictWriter(f, fields)
        writer.writeheader()
        writer.writerows(agg_rows)
    print('RESULT', OUT / 'aggregate.csv')
