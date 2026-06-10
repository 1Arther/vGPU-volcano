package vgpu

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dqngrpc "volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/vgpu/dqngrpc"
)

var (
	dqnClientMu       sync.Mutex
	dqnClientEndpoint string
	dqnClientConn     *grpc.ClientConn
	dqnClient         dqngrpc.DQNSchedulerClient

	dqnKubeClientMu sync.Mutex
	dqnKubeClient   kubernetes.Interface

	dqnJobCacheMu sync.Mutex
	dqnJobCache   = map[string]dqnJobCacheEntry{}
)

type dqnJobCacheEntry struct {
	created     time.Time
	allocations map[string][]int
}

func dqnDurationMs(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func getDQNClient() (dqngrpc.DQNSchedulerClient, error) {
	if DQNGRPCEndpoint == "" {
		return nil, fmt.Errorf("empty DQNGRPCEndpoint")
	}

	dqnClientMu.Lock()
	defer dqnClientMu.Unlock()

	if dqnClient != nil && dqnClientConn != nil && dqnClientEndpoint == DQNGRPCEndpoint {
		return dqnClient, nil
	}

	if dqnClientConn != nil {
		_ = dqnClientConn.Close()
		dqnClientConn = nil
		dqnClient = nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		DQNGRPCEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("connect dqn grpc %s failed: %w", DQNGRPCEndpoint, err)
	}

	dqnClientConn = conn
	dqnClientEndpoint = DQNGRPCEndpoint
	dqnClient = dqngrpc.NewDQNSchedulerClient(conn)

	klog.Infof("DQNPolicy connected grpc endpoint=%s", DQNGRPCEndpoint)

	return dqnClient, nil
}

func resetDQNClient() {
	dqnClientMu.Lock()
	defer dqnClientMu.Unlock()

	if dqnClientConn != nil {
		_ = dqnClientConn.Close()
	}

	dqnClientEndpoint = ""
	dqnClientConn = nil
	dqnClient = nil
}

func gpuStatesForDQN(gs *GPUDevices) []*dqngrpc.GPUState {
	gpuStates := make([]*dqngrpc.GPUState, 0, len(gs.Device))

	for idx, gpu := range gs.Device {
		if gpu == nil {
			continue
		}

		gpuStates = append(gpuStates, &dqngrpc.GPUState{
			Index:      int32(idx),
			Uuid:       gpu.UUID,
			TotalMem:   uint32(gpu.Memory),
			UsedMem:    uint32(gpu.UsedMem),
			UsedCore:   uint32(gpu.UsedCore),
			UsedNum:    uint32(gpu.UsedNum),
			Number:     uint32(gpu.Number),
			DeviceType: gpu.Type,
		})
	}

	sort.SliceStable(gpuStates, func(i, j int) bool {
		return gpuStates[i].Index < gpuStates[j].Index
	})

	return gpuStates
}

func queryDQNOrderedGPUIndexes(gs *GPUDevices, pod *v1.Pod, req ContainerDeviceRequest) ([]int, error) {
	totalStart := time.Now()
	client, err := getDQNClient()
	if err != nil {
		return nil, err
	}

	rpcReq := &dqngrpc.PredictRequest{
		NodeName:     gs.Name,
		PodNamespace: pod.Namespace,
		PodName:      pod.Name,
		MemReq:       uint32(req.Memreq),
		CoreReq:      uint32(req.Coresreq),
		Nums:         req.Nums,
		Gpus:         gpuStatesForDQN(gs),
	}
	requestBuildMS := dqnDurationMs(time.Since(totalStart))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rpcStart := time.Now()
	resp, err := client.Predict(ctx, rpcReq)
	rpcMS := dqnDurationMs(time.Since(rpcStart))
	if err != nil {
		resetDQNClient()
		return nil, fmt.Errorf("dqn predict failed: %w", err)
	}

	processStart := time.Now()
	indexes := sanitizeDQNIndexes(resp.OrderedIndexes, gs)
	if len(indexes) == 0 {
		return nil, fmt.Errorf("dqn returned empty ordered indexes, fallback=%v, reason=%s", resp.Fallback, resp.Reason)
	}
	processMS := dqnDurationMs(time.Since(processStart))
	totalMS := dqnDurationMs(time.Since(totalStart))
	klog.Infof(
		"DQNOverhead policy=dqn method=Predict total_ms=%.3f rpc_ms=%.3f request_build_ms=%.3f process_ms=%.3f scheduler_side_ms=%.3f pod=%s/%s node=%s gpus=%d fallback=%v",
		totalMS,
		rpcMS,
		requestBuildMS,
		processMS,
		totalMS-rpcMS,
		pod.Namespace,
		pod.Name,
		gs.Name,
		len(rpcReq.Gpus),
		resp.Fallback,
	)

	klog.Infof(
		"DQNPolicy grpc endpoint=%s pod=%s/%s selected=%d ordered=%v fallback=%v reason=%s",
		DQNGRPCEndpoint,
		pod.Namespace,
		pod.Name,
		resp.SelectedIndex,
		indexes,
		resp.Fallback,
		resp.Reason,
	)

	for _, s := range resp.Scores {
		klog.Infof(
			"DQNPolicy score pod=%s/%s gpu=%d score=%.6f fit=%v",
			pod.Namespace,
			pod.Name,
			s.Index,
			s.Score,
			s.Fit,
		)
	}

	return indexes, nil
}

func getDQNKubeClient() (kubernetes.Interface, error) {
	dqnKubeClientMu.Lock()
	defer dqnKubeClientMu.Unlock()

	if dqnKubeClient != nil {
		return dqnKubeClient, nil
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config failed: %w", err)
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kube client failed: %w", err)
	}

	dqnKubeClient = client
	return dqnKubeClient, nil
}

func dqnPodGroupKey(pod *v1.Pod) string {
	if pod == nil {
		return ""
	}
	if jobName := pod.Labels["volcano.sh/job-name"]; jobName != "" {
		return "volcano-job/" + pod.Namespace + "/" + jobName
	}
	if groupName := pod.Annotations["scheduling.k8s.io/group-name"]; groupName != "" {
		return "podgroup/" + pod.Namespace + "/" + groupName
	}
	return "pod/" + pod.Namespace + "/" + pod.Name
}

func listDQNJobPods(ctx context.Context, pod *v1.Pod) ([]v1.Pod, error) {
	if pod == nil {
		return nil, fmt.Errorf("nil pod")
	}

	client, err := getDQNKubeClient()
	if err != nil {
		return nil, err
	}

	list, err := client.CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list namespace pods failed: %w", err)
	}

	jobName := pod.Labels["volcano.sh/job-name"]
	groupName := pod.Annotations["scheduling.k8s.io/group-name"]
	pods := make([]v1.Pod, 0)

	for _, item := range list.Items {
		if item.DeletionTimestamp != nil {
			continue
		}
		if jobName != "" && item.Labels["volcano.sh/job-name"] == jobName {
			pods = append(pods, item)
			continue
		}
		if groupName != "" && item.Annotations["scheduling.k8s.io/group-name"] == groupName {
			pods = append(pods, item)
		}
	}

	found := false
	for _, item := range pods {
		if item.Namespace == pod.Namespace && item.Name == pod.Name {
			found = true
			break
		}
	}
	if !found {
		pods = append(pods, *pod)
	}

	sort.SliceStable(pods, func(i, j int) bool {
		li := pods[i].Labels["volcano.sh/task-index"]
		lj := pods[j].Labels["volcano.sh/task-index"]
		if li != lj {
			return li < lj
		}
		return pods[i].Name < pods[j].Name
	})

	return pods, nil
}

func podToDQNRequest(pod v1.Pod) (*dqngrpc.PodRequest, bool) {
	reqs := resourcereqs(&pod)
	if len(reqs) == 0 {
		return nil, false
	}

	out := &dqngrpc.PodRequest{PodNamespace: pod.Namespace, PodName: pod.Name}
	for _, req := range reqs {
		out.MemReq += uint32(req.Memreq)
		out.CoreReq += uint32(req.Coresreq)
		out.Nums += req.Nums
	}
	if out.Nums <= 0 {
		out.Nums = 1
	}
	return out, true
}

func reorderDQNPreferredIndexes(preferred []int, gs *GPUDevices) []int {
	seen := map[int]bool{}
	result := make([]int, 0, len(gs.Device))
	for _, idx := range preferred {
		if idx < 0 || idx >= len(gs.Device) || gs.Device[idx] == nil || seen[idx] {
			continue
		}
		seen[idx] = true
		result = append(result, idx)
	}
	for idx := range gs.Device {
		if gs.Device[idx] == nil || seen[idx] {
			continue
		}
		seen[idx] = true
		result = append(result, idx)
	}
	return result
}

func queryDQNJobOrderedGPUIndexes(gs *GPUDevices, pod *v1.Pod, req ContainerDeviceRequest) ([]int, error) {
	totalStart := time.Now()
	jobKey := dqnPodGroupKey(pod)
	podKey := pod.Namespace + "/" + pod.Name
	cacheKey := DQNGRPCEndpoint + "|" + gs.Name + "|" + jobKey

	dqnJobCacheMu.Lock()
	if cached, ok := dqnJobCache[cacheKey]; ok && time.Since(cached.created) < 10*time.Second {
		if preferred, ok := cached.allocations[podKey]; ok && len(preferred) > 0 {
			dqnJobCacheMu.Unlock()
			klog.Infof(
				"DQNOverhead policy=dqn-job path=cache total_ms=%.3f job=%s pod=%s node=%s",
				dqnDurationMs(time.Since(totalStart)),
				jobKey,
				podKey,
				gs.Name,
			)
			return reorderDQNPreferredIndexes(preferred, gs), nil
		}
	}
	dqnJobCacheMu.Unlock()

	client, err := getDQNClient()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	listStart := time.Now()
	pods, err := listDQNJobPods(ctx, pod)
	listPodsMS := dqnDurationMs(time.Since(listStart))
	if err != nil {
		return nil, err
	}

	podReqs := make([]*dqngrpc.PodRequest, 0, len(pods))
	for _, item := range pods {
		podReq, ok := podToDQNRequest(item)
		if !ok {
			continue
		}
		podReqs = append(podReqs, podReq)
	}
	if len(podReqs) == 0 {
		return nil, fmt.Errorf("no vgpu pod requests found for %s", jobKey)
	}

	rpcReq := &dqngrpc.ScheduleJobRequest{
		NodeName: gs.Name,
		Gpus:     gpuStatesForDQN(gs),
		Pods:     podReqs,
	}
	requestBuildMS := dqnDurationMs(time.Since(totalStart)) - listPodsMS

	rpcStart := time.Now()
	resp, err := client.ScheduleJob(ctx, rpcReq)
	rpcMS := dqnDurationMs(time.Since(rpcStart))
	if err != nil {
		resetDQNClient()
		return nil, fmt.Errorf("dqn schedule job failed: %w", err)
	}

	processStart := time.Now()
	allocations := map[string][]int{}
	for _, alloc := range resp.Allocations {
		key := alloc.PodNamespace + "/" + alloc.PodName
		if !alloc.Success || len(alloc.SelectedIndexes) == 0 {
			klog.Infof("DQNJobPolicy allocation pod=%s success=%v reason=%s", key, alloc.Success, alloc.Reason)
			continue
		}
		idxs := make([]int, 0, len(alloc.SelectedIndexes))
		for _, idx := range alloc.SelectedIndexes {
			idxs = append(idxs, int(idx))
		}
		allocations[key] = idxs
	}

	if len(allocations) == 0 {
		return nil, fmt.Errorf("dqn schedule job returned no successful allocations, fallback=%v reason=%s", resp.Fallback, resp.Reason)
	}

	dqnJobCacheMu.Lock()
	for key, value := range dqnJobCache {
		if time.Since(value.created) > time.Minute {
			delete(dqnJobCache, key)
		}
	}
	dqnJobCache[cacheKey] = dqnJobCacheEntry{created: time.Now(), allocations: allocations}
	dqnJobCacheMu.Unlock()

	preferred, ok := allocations[podKey]
	if !ok || len(preferred) == 0 {
		known := make([]string, 0, len(allocations))
		for key := range allocations {
			known = append(known, key)
		}
		sort.Strings(known)
		return nil, fmt.Errorf("dqn schedule job has no allocation for pod %s, known=%s", podKey, strings.Join(known, ","))
	}

	processMS := dqnDurationMs(time.Since(processStart))
	totalMS := dqnDurationMs(time.Since(totalStart))
	klog.Infof(
		"DQNOverhead policy=dqn-job method=ScheduleJob total_ms=%.3f rpc_ms=%.3f list_pods_ms=%.3f request_build_ms=%.3f process_ms=%.3f scheduler_side_ms=%.3f job=%s pod=%s node=%s pods=%d gpus=%d fallback=%v",
		totalMS,
		rpcMS,
		listPodsMS,
		requestBuildMS,
		processMS,
		totalMS-rpcMS,
		jobKey,
		podKey,
		gs.Name,
		len(podReqs),
		len(rpcReq.Gpus),
		resp.Fallback,
	)

	klog.Infof(
		"DQNJobPolicy grpc endpoint=%s job=%s pod=%s selected=%v fallback=%v reason=%s pods=%d",
		DQNGRPCEndpoint,
		jobKey,
		podKey,
		preferred,
		resp.Fallback,
		resp.Reason,
		len(podReqs),
	)

	return reorderDQNPreferredIndexes(preferred, gs), nil
}

func sanitizeDQNIndexes(raw []int32, gs *GPUDevices) []int {
	seen := map[int]bool{}
	result := make([]int, 0, len(gs.Device))

	for _, x := range raw {
		idx := int(x)
		if idx < 0 || idx >= len(gs.Device) {
			continue
		}
		if gs.Device[idx] == nil {
			continue
		}
		if seen[idx] {
			continue
		}

		seen[idx] = true
		result = append(result, idx)
	}

	for idx := range gs.Device {
		if gs.Device[idx] == nil {
			continue
		}
		if seen[idx] {
			continue
		}

		seen[idx] = true
		result = append(result, idx)
	}

	return result
}
