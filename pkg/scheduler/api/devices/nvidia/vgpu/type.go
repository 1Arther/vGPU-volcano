/*
Copyright 2023 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vgpu

const (
	// DeviceName used to indicate this device
	DeviceName = "hamivgpu"

	GPUInUse                         = "nvidia.com/use-gputype"
	GPUNoUse                         = "nvidia.com/nouse-gputype"
	AssignedTimeAnnotations          = "volcano.sh/vgpu-time"
	AssignedIDsAnnotations           = "volcano.sh/vgpu-ids-new"
	AssignedIDsToAllocateAnnotations = "volcano.sh/devices-to-allocate"
	AssignedNodeAnnotations          = "volcano.sh/vgpu-node"
	BindTimeAnnotations              = "volcano.sh/bind-time"
	DeviceBindPhase                  = "volcano.sh/bind-phase"

	NvidiaGPUDevice = "NVIDIA"

	// PredicateTime is the key of predicate time
	PredicateTime = "volcano.sh/predicate-time"
	// GPUIndex is the key of gpu index
	GPUIndex = "volcano.sh/gpu-index"

	// UnhealthyGPUIDs list of unhealthy gpu ids
	UnhealthyGPUIDs = "volcano.sh/gpu-unhealthy-ids"

	OriginalPolicy = "original"
	BinpackPolicy  = "binpack"
	SpreadPolicy   = "spread"
	RandomPolicy   = "random"
	DQNPolicy      = "dqn"
	DQNJobPolicy   = "dqn-job"

	// Keep old internal aliases for existing node scoring logic.
	binpackPolicy = BinpackPolicy
	spreadPolicy  = SpreadPolicy

	DefaultMemPercentage = 101
	binpackMultiplier    = 100
	spreadMultiplier     = 100

	GPUModeAnnotation      = "volcano.sh/vgpu-mode"
	vGPUControllerHAMICore = "hami-core"
	vGPUControllerMIG      = "mig"
	vGPUControllerMPS      = "mps"
)

var (
	VGPUEnable      bool
	NodeLockEnable  bool
	GPUSelectPolicy = OriginalPolicy

	// R5300 DQN gRPC service
	DQNGRPCEndpoint = "dqn-scheduler-grpc.volcano-system.svc.cluster.local:50051"
)

type ContainerDeviceRequest struct {
	Nums int32
	// device type, like NVIDIA, MLU
	Type             string
	Memreq           uint
	MemPercentagereq int32
	Coresreq         uint
}

type ContainerDevice struct {
	UUID string
	// device type, like NVIDIA, MLU
	Type      string
	Usedmem   uint
	Usedcores uint
}

type ContainerDevices []ContainerDevice
