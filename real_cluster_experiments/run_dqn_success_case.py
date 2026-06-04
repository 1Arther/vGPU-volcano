#!/usr/bin/env python3
import csv,json,subprocess,time
from pathlib import Path
OUT=Path('/home/zbs/vGPU-volcano/real_cluster_experiments/results_dqn_success'); OUT.mkdir(parents=True,exist_ok=True)
IMAGE='nvcr.io/nvidia/cuda:12.2.0-base-ubuntu22.04'; NODE='t3dgq'; TOTAL_MEM=24564.0
POLICIES=['spread','dqn-job']
PRELOADS=[
 ('preload-a','GPU-7b94f673-ce00-719b-2c94-706baa032dae',12000,10),
 ('preload-b','GPU-fb6aaeaf-9ca4-3c9a-7b3b-f56714c1f7cd',8000,65),
 ('preload-c','GPU-28cf7186-0bdd-be9f-92e5-3c59b4e92c1e',4000,20),
 ('preload-d','GPU-27be0024-0498-4114-e6b9-b2d0db148309',12000,50),
]

def run(cmd,check=True):
 p=subprocess.run(cmd,shell=True,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 if check and p.returncode: print(p.stdout); raise RuntimeError(cmd)
 return p.stdout

def set_policy(pol):
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
          deviceshare.GPUSelectPolicy: {pol}
          deviceshare.DQNGRPCEndpoint: dqn-scheduler-grpc.volcano-system.svc.cluster.local:50051
      - name: predicates
      - name: proportion
      - name: nodeorder
      - name: binpack
'''
 p=OUT/f'config-{pol}.yaml'; p.write_text(y); run(f'kubectl apply -f {p}'); run('kubectl rollout restart deployment/volcano-scheduler -n volcano-system'); run('kubectl rollout status deployment/volcano-scheduler -n volcano-system --timeout=180s'); time.sleep(5)

def cleanup():
 run('kubectl delete vcjob dqn-success-job --ignore-not-found=true',False)
 run('kubectl delete pod -l experiment=dqn-success --ignore-not-found=true --force --grace-period=0',False)
 time.sleep(8)

def preload_yaml():
 docs=[]
 for name,uuid,mem,core in PRELOADS:
  docs.append(f'''apiVersion: v1
kind: Pod
metadata:
  name: dqn-success-{name}
  labels:
    experiment: dqn-success
    role: preload
  annotations:
    volcano.sh/bind-phase: "success"
    volcano.sh/vgpu-node: "{NODE}"
    volcano.sh/devices-to-allocate: "{uuid},NVIDIA,{mem},{core}:"
    volcano.sh/vgpu-ids-new: "{uuid},NVIDIA,{mem},{core}:"
spec:
  runtimeClassName: nvidia
  nodeName: {NODE}
  restartPolicy: Never
  containers:
  - name: hold
    image: {IMAGE}
    imagePullPolicy: IfNotPresent
    command: ["/bin/bash", "-lc"]
    args: ["echo preload; sleep 600"]
    resources:
      limits:
        volcano.sh/vgpu-number: 1
        volcano.sh/vgpu-memory: {mem}
        volcano.sh/vgpu-cores: {core}
''')
 return '---\n'.join(docs)

def job_yaml():
 return f'''apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: dqn-success-job
spec:
  schedulerName: volcano
  minAvailable: 4
  tasks:
  - name: pair
    replicas: 2
    template:
      metadata:
        labels:
          experiment: dqn-success
          role: target
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
          args: ["echo pair; sleep 600"]
          resources:
            limits:
              volcano.sh/vgpu-number: 2
              volcano.sh/vgpu-memory: 4000
              volcano.sh/vgpu-cores: 15
  - name: mem
    replicas: 1
    template:
      metadata:
        labels:
          experiment: dqn-success
          role: target
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
          args: ["echo mem; sleep 600"]
          resources:
            limits:
              volcano.sh/vgpu-number: 1
              volcano.sh/vgpu-memory: 8000
              volcano.sh/vgpu-cores: 10
  - name: core
    replicas: 1
    template:
      metadata:
        labels:
          experiment: dqn-success
          role: target
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
          args: ["echo core; sleep 600"]
          resources:
            limits:
              volcano.sh/vgpu-number: 1
              volcano.sh/vgpu-memory: 4000
              volcano.sh/vgpu-cores: 35
'''

def wait_all():
 deadline=time.time()+90
 while time.time()<deadline:
  data=json.loads(run('kubectl get pod -l experiment=dqn-success -o json',False) or '{"items":[]}')
  items=data.get('items',[])
  if len(items)>=8 and all(i.get('status',{}).get('phase')=='Running' for i in items) and all(i['metadata'].get('annotations',{}).get('volcano.sh/vgpu-ids-new') for i in items): return data
  time.sleep(3)
 return json.loads(run('kubectl get pod -l experiment=dqn-success -o json',False) or '{"items":[]}')

def collect(data):
 gpu={}; pods=[]
 for item in sorted(data.get('items',[]), key=lambda x:x['metadata']['name']):
  ann=item['metadata'].get('annotations',{}).get('volcano.sh/vgpu-ids-new','') or ''
  pods.append({'pod':item['metadata']['name'],'phase':item.get('status',{}).get('phase',''),'role':item['metadata'].get('labels',{}).get('role',''),'annotation':ann})
  for e in [x for x in ann.split(':') if x.strip()]:
   parts=e.split(',')
   if len(parts)>=4:
    g=gpu.setdefault(parts[0],{'mem':0.0,'core':0.0,'slices':0})
    g['mem']+=float(parts[2]); g['core']+=float(parts[3]); g['slices']+=1
 vals=list(gpu.values())
 while len(vals)<4: vals.append({'mem':0.0,'core':0.0,'slices':0})
 mem=[v['mem'] for v in vals]; core=[v['core'] for v in vals]; sl=[v['slices'] for v in vals]
 intra=[abs(v['mem']/TOTAL_MEM-v['core']/100.0) for v in vals if v['mem'] or v['core']]
 score=(max(mem)-min(mem))/TOTAL_MEM+(max(core)-min(core))/100.0+(sum(intra)/len(intra) if intra else 0)
 return {'running':sum(1 for p in pods if p['phase']=='Running'),'allocated':sum(1 for p in pods if p['annotation']),'used_gpus':len(gpu),'mem_range':max(mem)-min(mem),'core_range':max(core)-min(core),'slice_range':max(sl)-min(sl),'intra_gap':sum(intra)/len(intra) if intra else 0,'balance_score':score,'gpu_usage':json.dumps(gpu,sort_keys=True)},pods

rows=[]; podrows=[]
for pol in POLICIES:
 print('POLICY',pol,flush=True); cleanup(); set_policy(pol)
 (OUT/'preload.yaml').write_text(preload_yaml()); run(f'kubectl apply -f {OUT}/preload.yaml'); time.sleep(12)
 (OUT/f'job-{pol}.yaml').write_text(job_yaml()); run(f'kubectl apply -f {OUT}/job-{pol}.yaml')
 data=wait_all(); summ,pods=collect(data); summ['policy']=pol; rows.append(summ); print(summ,flush=True)
 for p in pods: p['policy']=pol; podrows.append(p)
 cleanup()
set_policy('dqn-job')
with open(OUT/'summary.csv','w',newline='') as f:
 fields=['policy','allocated','running','used_gpus','mem_range','core_range','slice_range','intra_gap','balance_score','gpu_usage']; w=csv.DictWriter(f,fields); w.writeheader(); w.writerows(rows)
with open(OUT/'pod_allocations.csv','w',newline='') as f:
 fields=['policy','pod','role','phase','annotation']; w=csv.DictWriter(f,fields); w.writeheader(); w.writerows(podrows)
print('RESULT',OUT/'summary.csv')
