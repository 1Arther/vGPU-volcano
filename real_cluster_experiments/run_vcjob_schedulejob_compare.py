#!/usr/bin/env python3
import csv, json, math, subprocess, time
from pathlib import Path
OUT=Path('/home/zbs/vGPU-volcano/real_cluster_experiments/results_vcjob'); OUT.mkdir(parents=True, exist_ok=True)
POLICIES=['spread','dqn-job']
IMAGE='nvcr.io/nvidia/cuda:12.2.0-base-ubuntu22.04'
NODE='t3dgq'
GPU_TOTAL_MEM=24564.0
GPU_TOTAL_CORE=100.0
WORKLOADS={
  'balanced-job-8x1': {'replicas':8,'nums':1,'mem':4096,'core':10},
  'multi-job-4x2': {'replicas':4,'nums':2,'mem':8192,'core':25},
}

def run(cmd, check=True):
    p=subprocess.run(cmd,shell=True,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if check and p.returncode:
        print(p.stdout); raise RuntimeError(cmd)
    return p.stdout

def set_policy(policy):
    y=f'''apiVersion: v1
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
    p=OUT/f'config-{policy}.yaml'; p.write_text(y); run(f'kubectl apply -f {p}'); run('kubectl rollout restart deployment/volcano-scheduler -n volcano-system'); run('kubectl rollout status deployment/volcano-scheduler -n volcano-system --timeout=180s'); time.sleep(3)

def vcjob_yaml(name, spec):
    return f'''apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: {name}
spec:
  schedulerName: volcano
  minAvailable: {spec['replicas']}
  tasks:
    - name: worker
      replicas: {spec['replicas']}
      template:
        metadata:
          labels:
            experiment: cmp-vcjob
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
                  volcano.sh/vgpu-number: {spec['nums']}
                  volcano.sh/vgpu-memory: {spec['mem']}
                  volcano.sh/vgpu-cores: {spec['core']}
'''

def cleanup(name=None):
    if name: run(f'kubectl delete vcjob {name} --ignore-not-found=true', False)
    run("kubectl delete pod -l experiment=cmp-vcjob --ignore-not-found=true --force --grace-period=0", False)
    time.sleep(5)

def collect(name, expected):
    deadline=time.time()+90
    while time.time()<deadline:
        data=json.loads(run("kubectl get pod -l experiment=cmp-vcjob -o json", False) or '{"items":[]}')
        items=data.get('items',[])
        if len(items)>=expected and all(i.get('status',{}).get('phase')=='Running' for i in items) and all(i.get('metadata',{}).get('annotations',{}).get('volcano.sh/vgpu-ids-new') for i in items): break
        time.sleep(3)
    data=json.loads(run("kubectl get pod -l experiment=cmp-vcjob -o json", False) or '{"items":[]}')
    gpu={}; pods=[]
    for item in sorted(data.get('items',[]), key=lambda x:x['metadata']['name']):
        ann=item['metadata'].get('annotations',{}).get('volcano.sh/vgpu-ids-new','') or ''
        pods.append((item['metadata']['name'], item.get('status',{}).get('phase',''), ann))
        for e in [x for x in ann.split(':') if x.strip()]:
            parts=e.split(',')
            if len(parts)>=4:
                g=gpu.setdefault(parts[0], {'mem':0.0,'core':0.0,'slices':0})
                g['mem']+=float(parts[2]); g['core']+=float(parts[3]); g['slices']+=1
    vals=list(gpu.values())
    while len(vals)<4: vals.append({'mem':0.0,'core':0.0,'slices':0})
    mem=[v['mem'] for v in vals]; core=[v['core'] for v in vals]; sl=[v['slices'] for v in vals]
    intra=[abs(v['mem']/GPU_TOTAL_MEM-v['core']/GPU_TOTAL_CORE) for v in vals if v['mem'] or v['core']]
    return {'running':sum(1 for _,ph,_ in pods if ph=='Running'), 'allocated':sum(1 for _,_,a in pods if a), 'used_gpus':len(gpu), 'mem_range':max(mem)-min(mem), 'core_range':max(core)-min(core), 'slice_range':max(sl)-min(sl), 'intra_gap':sum(intra)/len(intra) if intra else 0, 'gpu_usage':json.dumps(gpu,sort_keys=True)}, pods

rows=[]; podrows=[]
for pol in POLICIES:
    print('POLICY',pol,flush=True); set_policy(pol)
    for wl,spec in WORKLOADS.items():
        name=f'cmp-{wl}-{pol}'.replace('_','-')
        cleanup(name)
        p=OUT/f'{name}.yaml'; p.write_text(vcjob_yaml(name,spec)); run(f'kubectl apply -f {p}')
        summ,pods=collect(name,spec['replicas']); summ.update({'policy':pol,'workload':wl,'expected':spec['replicas']}); rows.append(summ)
        print(summ, flush=True)
        for pod in pods: podrows.append({'policy':pol,'workload':wl,'pod':pod[0],'phase':pod[1],'annotation':pod[2]})
        cleanup(name)
# restore dqn-job
set_policy('dqn-job')
with open(OUT/'summary.csv','w',newline='') as f:
    fields=['policy','workload','expected','allocated','running','used_gpus','mem_range','core_range','slice_range','intra_gap','gpu_usage']; w=csv.DictWriter(f,fields); w.writeheader(); w.writerows(rows)
with open(OUT/'pod_allocations.csv','w',newline='') as f:
    fields=['policy','workload','pod','phase','annotation']; w=csv.DictWriter(f,fields); w.writeheader(); w.writerows(podrows)
print('RESULT', OUT/'summary.csv')
