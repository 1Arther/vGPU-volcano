#!/usr/bin/env python3
import csv,json,subprocess,time
from pathlib import Path
OUT=Path('/home/zbs/vGPU-volcano/real_cluster_experiments/results_normal_preload'); OUT.mkdir(parents=True,exist_ok=True)
IMAGE='nvcr.io/nvidia/cuda:12.2.0-base-ubuntu22.04'; NODE='t3dgq'; TOTAL_MEM=24564.0; TOTAL_CORE=100.0
PREFIX='normal-preload'
POLICIES=['spread','binpack','original','random','dqn-job']
# These preloads are scheduled normally by Volcano spread; they create uneven mem/core ratios across GPUs.
PRELOAD=[('pre-mem',1,1,12000,10),('pre-core',1,1,8000,65),('pre-small',1,1,4000,20),('pre-mix',1,1,12000,50)]
TARGETS=[
 ('case-a',[('pair',2,2,4000,15),('mem',1,1,8000,10),('core',1,1,4000,35)]),
 ('case-b',[('memhi',3,1,8000,10),('corehi',3,1,4000,35)]),
 ('case-c',[('pair',2,2,4000,15),('corehi',3,1,4000,35),('normal',2,1,4000,15)]),
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
 p=OUT/f'config-{pol}.yaml'; p.write_text(y); run(f'kubectl apply -f {p}'); run('kubectl rollout restart deployment/volcano-scheduler -n volcano-system'); run('kubectl rollout status deployment/volcano-scheduler -n volcano-system --timeout=180s'); time.sleep(4)

def cleanup():
 run('kubectl delete vcjob normal-preload-pre normal-preload-target --ignore-not-found=true',False)
 run(f'kubectl delete pod -l experiment={PREFIX} --ignore-not-found=true --force --grace-period=0',False)
 time.sleep(6)

def job_yaml(name,specs,role):
 minavail=sum(reps for _,reps,_,_,_ in specs); tasks=[]
 for idx,(nm,reps,nums,mem,core) in enumerate(specs):
  tasks.append(f'''    - name: {nm}-{idx}
      replicas: {reps}
      template:
        metadata:
          labels:
            experiment: {PREFIX}
            role: {role}
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
            args: ["echo {role}; sleep 600"]
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
  minAvailable: {minavail}
  tasks:
{''.join(tasks)}'''

def wait_count(role,expected):
 deadline=time.time()+90
 while time.time()<deadline:
  data=json.loads(run(f'kubectl get pod -l experiment={PREFIX},role={role} -o json',False) or '{"items":[]}')
  items=data.get('items',[])
  if len(items)>=expected and all(i.get('status',{}).get('phase')=='Running' for i in items) and all(i['metadata'].get('annotations',{}).get('volcano.sh/vgpu-ids-new') for i in items): return True
  time.sleep(3)
 return False

def collect():
 data=json.loads(run(f'kubectl get pod -l experiment={PREFIX} -o json',False) or '{"items":[]}')
 gpu={}; pods=[]
 for item in sorted(data.get('items',[]),key=lambda x:x['metadata']['name']):
  ann=item['metadata'].get('annotations',{}).get('volcano.sh/vgpu-ids-new','') or ''
  role=item['metadata'].get('labels',{}).get('role','')
  pods.append({'pod':item['metadata']['name'],'role':role,'phase':item.get('status',{}).get('phase',''),'annotation':ann})
  for e in [x for x in ann.split(':') if x.strip()]:
   parts=e.split(',')
   if len(parts)>=4:
    g=gpu.setdefault(parts[0],{'mem':0.0,'core':0.0,'slices':0})
    g['mem']+=float(parts[2]); g['core']+=float(parts[3]); g['slices']+=1
 vals=list(gpu.values())
 while len(vals)<4: vals.append({'mem':0.0,'core':0.0,'slices':0})
 mem=[v['mem'] for v in vals]; core=[v['core'] for v in vals]; sl=[v['slices'] for v in vals]
 intra=[abs(v['mem']/TOTAL_MEM-v['core']/TOTAL_CORE) for v in vals if v['mem'] or v['core']]
 score=(max(mem)-min(mem))/TOTAL_MEM+(max(core)-min(core))/TOTAL_CORE+(sum(intra)/len(intra) if intra else 0)
 return {'running':sum(1 for p in pods if p['phase']=='Running'),'allocated':sum(1 for p in pods if p['annotation']),'used_gpus':len(gpu),'mem_range':max(mem)-min(mem),'core_range':max(core)-min(core),'slice_range':max(sl)-min(sl),'intra_gap':sum(intra)/len(intra) if intra else 0,'balance_score':score,'gpu_usage':json.dumps(gpu,sort_keys=True)},pods

rows=[]; podrows=[]
try:
 for case,specs in TARGETS:
  print('CASE',case,flush=True)
  for pol in POLICIES:
   cleanup()
   set_policy('spread')
   (OUT/f'{case}-{pol}-preload.yaml').write_text(job_yaml('normal-preload-pre',PRELOAD,'preload'))
   run(f'kubectl apply -f {OUT}/{case}-{pol}-preload.yaml')
   if not wait_count('preload',sum(x[1] for x in PRELOAD)): print('preload not fully running',flush=True)
   set_policy(pol)
   (OUT/f'{case}-{pol}-target.yaml').write_text(job_yaml('normal-preload-target',specs,'target'))
   run(f'kubectl apply -f {OUT}/{case}-{pol}-target.yaml')
   wait_count('target',sum(x[1] for x in specs))
   m,pods=collect(); m.update({'case':case,'policy':pol,'expected':sum(x[1] for x in PRELOAD)+sum(x[1] for x in specs),'target_specs':json.dumps(specs)})
   print(m,flush=True)
   rows.append(m)
   for p in pods: p.update({'case':case,'policy':pol}); podrows.append(p)
finally:
 cleanup(); set_policy('dqn-job')
 with open(OUT/'summary.csv','w',newline='') as f:
  fields=['case','policy','expected','allocated','running','used_gpus','mem_range','core_range','slice_range','intra_gap','balance_score','gpu_usage','target_specs']; w=csv.DictWriter(f,fields); w.writeheader(); w.writerows(rows)
 with open(OUT/'pod_allocations.csv','w',newline='') as f:
  fields=['case','policy','pod','role','phase','annotation']; w=csv.DictWriter(f,fields); w.writeheader(); w.writerows(podrows)
 print('RESULT',OUT/'summary.csv')
