#!/usr/bin/env python3
import csv
import itertools
import json
import random
import subprocess
import time
from pathlib import Path

OUT = Path('/home/zbs/vGPU-volcano/real_cluster_experiments/results_dqn_better_search')
OUT.mkdir(parents=True, exist_ok=True)
IMAGE = 'nvcr.io/nvidia/cuda:12.2.0-base-ubuntu22.04'
NODE = 't3dgq'
TOTAL_MEM = 24564.0
TOTAL_CORE = 100.0
PREFIX = 'dqn-better'
POLICIES = ['spread', 'binpack', 'original', 'random', 'dqn-job']

# Hand-picked candidate families plus deterministic random mixes.
CANDIDATES = [
    [('memhi', 2, 1, 8000, 10), ('corehi', 2, 1, 4000, 35), ('normal', 4, 1, 4000, 15)],
    [('pair', 2, 2, 4000, 15), ('memhi', 2, 1, 8000, 10), ('corehi', 2, 1, 4000, 35)],
    [('pair', 2, 2, 4000, 15), ('memhi', 1, 1, 8000, 10), ('corehi', 3, 1, 4000, 35)],
    [('pair', 3, 2, 4000, 15), ('memhi', 1, 1, 8000, 10), ('corehi', 1, 1, 4000, 35)],
    [('fatpair', 2, 2, 8000, 20), ('corehi', 2, 1, 4000, 35)],
    [('memhi', 3, 1, 8000, 10), ('corehi', 3, 1, 4000, 35)],
]

random.seed(11)
SHAPES = [
    ('memhi', 1, 8000, 10),
    ('corehi', 1, 4000, 35),
    ('normal', 1, 4000, 15),
    ('pair', 2, 4000, 15),
    ('fatpair', 2, 8000, 20),
]
for ci in range(16):
    specs = []
    for name, nums, mem, core in random.sample(SHAPES, random.randint(3, 5)):
        reps = random.choice([1, 2, 3])
        specs.append((f'{name}{ci}', reps, nums, mem, core))
    total_slices = sum(reps * nums for _, reps, nums, _, _ in specs)
    total_core = sum(reps * nums * core for _, reps, nums, _, core in specs)
    total_mem = sum(reps * nums * mem for _, reps, nums, mem, _ in specs)
    if total_slices <= 12 and total_core <= 360 and total_mem <= 80000:
        CANDIDATES.append(specs)


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
    time.sleep(3)


def cleanup(job_name=None):
    if job_name:
        run(f'kubectl delete vcjob {job_name} --ignore-not-found=true', False)
    run(f'kubectl delete pod -l experiment={PREFIX} --ignore-not-found=true --force --grace-period=0', False)
    time.sleep(5)


def job_yaml(job_name, specs):
    min_avail = sum(reps for _, reps, _, _, _ in specs)
    tasks = []
    for idx, (name, reps, nums, mem, core) in enumerate(specs):
        task = f'''    - name: {name}-{idx}
      replicas: {reps}
      template:
        metadata:
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
                  sleep 480
              resources:
                limits:
                  volcano.sh/vgpu-number: {nums}
                  volcano.sh/vgpu-memory: {mem}
                  volcano.sh/vgpu-cores: {core}
'''
        tasks.append(task)
    return f'''apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: {job_name}
spec:
  schedulerName: volcano
  minAvailable: {min_avail}
  tasks:
{''.join(tasks)}'''


def wait_and_collect(expected):
    deadline = time.time() + 90
    data = {'items': []}
    while time.time() < deadline:
        raw = run(f'kubectl get pod -l experiment={PREFIX} -o json', False)
        data = json.loads(raw) if raw.strip() else {'items': []}
        items = data.get('items', [])
        running = sum(1 for i in items if i.get('status', {}).get('phase') == 'Running')
        allocated = sum(1 for i in items if i.get('metadata', {}).get('annotations', {}).get('volcano.sh/vgpu-ids-new'))
        if len(items) >= expected and running >= expected and allocated >= expected:
            break
        time.sleep(3)
    return data


def metrics(data):
    gpu = {}
    pod_rows = []
    for item in sorted(data.get('items', []), key=lambda x: x['metadata']['name']):
        ann = item['metadata'].get('annotations', {}).get('volcano.sh/vgpu-ids-new', '') or ''
        phase = item.get('status', {}).get('phase', '')
        pod_rows.append({'pod': item['metadata']['name'], 'phase': phase, 'annotation': ann})
        for e in [x for x in ann.split(':') if x.strip()]:
            parts = e.split(',')
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
    score = (max(mem) - min(mem)) / TOTAL_MEM + (max(core) - min(core)) / TOTAL_CORE + (sum(intra) / len(intra) if intra else 0)
    return {
        'running': sum(1 for r in pod_rows if r['phase'] == 'Running'),
        'allocated': sum(1 for r in pod_rows if r['annotation']),
        'used_gpus': len(gpu),
        'mem_range': max(mem) - min(mem),
        'core_range': max(core) - min(core),
        'slice_range': max(slices) - min(slices),
        'intra_gap': sum(intra) / len(intra) if intra else 0,
        'balance_score': score,
        'gpu_usage': json.dumps(gpu, sort_keys=True),
    }, pod_rows


def run_case(case_id, specs, policy):
    expected = sum(reps for _, reps, _, _, _ in specs)
    job_name = f'{PREFIX}-{case_id}-{policy}'.replace('_', '-')
    cleanup(job_name)
    path = OUT / f'{job_name}.yaml'
    path.write_text(job_yaml(job_name, specs))
    run(f'kubectl apply -f {path}')
    data = wait_and_collect(expected)
    m, pods = metrics(data)
    cleanup(job_name)
    m.update({'case_id': case_id, 'policy': policy, 'expected': expected, 'specs': json.dumps(specs)})
    for p in pods:
        p.update({'case_id': case_id, 'policy': policy})
    return m, pods

all_rows = []
all_pods = []
best = None
try:
    for cid, specs in enumerate(CANDIDATES):
        print('CASE', cid, specs, flush=True)
        case_rows = []
        for policy in POLICIES:
            set_policy(policy)
            m, pods = run_case(cid, specs, policy)
            print(m, flush=True)
            case_rows.append(m)
            all_rows.append(m)
            all_pods.extend(pods)
        valid = [r for r in case_rows if r['running'] == r['expected'] and r['allocated'] == r['expected']]
        dqn = next((r for r in valid if r['policy'] == 'dqn-job'), None)
        bases = [r for r in valid if r['policy'] != 'dqn-job']
        if dqn and bases:
            best_base = min(bases, key=lambda r: r['balance_score'])
            gain = best_base['balance_score'] - dqn['balance_score']
            if best is None or gain > best[0]:
                best = (gain, cid, specs, dqn, best_base, case_rows)
            if gain > 0.08:
                print('FOUND', gain, cid, flush=True)
                break
finally:
    set_policy('dqn-job')
    with open(OUT / 'summary.csv', 'w', newline='') as f:
        fields = ['case_id','policy','expected','allocated','running','used_gpus','mem_range','core_range','slice_range','intra_gap','balance_score','gpu_usage','specs']
        w = csv.DictWriter(f, fields); w.writeheader(); w.writerows(all_rows)
    with open(OUT / 'pod_allocations.csv', 'w', newline='') as f:
        fields = ['case_id','policy','pod','phase','annotation']
        w = csv.DictWriter(f, fields); w.writeheader(); w.writerows(all_pods)
    with open(OUT / 'best.json', 'w') as f:
        json.dump(best, f, indent=2)
    print('BEST', json.dumps(best, indent=2), flush=True)
    print('RESULT', OUT / 'summary.csv')
