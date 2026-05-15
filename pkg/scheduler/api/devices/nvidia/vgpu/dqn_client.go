package vgpu

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
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
)

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

func queryDQNOrderedGPUIndexes(gs *GPUDevices, pod *v1.Pod, req ContainerDeviceRequest) ([]int, error) {
	client, err := getDQNClient()
	if err != nil {
		return nil, err
	}

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

	rpcReq := &dqngrpc.PredictRequest{
		NodeName:     gs.Name,
		PodNamespace: pod.Namespace,
		PodName:      pod.Name,
		MemReq:       uint32(req.Memreq),
		CoreReq:      uint32(req.Coresreq),
		Nums:         req.Nums,
		Gpus:         gpuStates,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.Predict(ctx, rpcReq)
	if err != nil {
		resetDQNClient()
		return nil, fmt.Errorf("dqn predict failed: %w", err)
	}

	indexes := sanitizeDQNIndexes(resp.OrderedIndexes, gs)
	if len(indexes) == 0 {
		return nil, fmt.Errorf("dqn returned empty ordered indexes, fallback=%v, reason=%s", resp.Fallback, resp.Reason)
	}

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
