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

// vgpu 包：Volcano 调度器中和 NVIDIA vGPU/HAMI/MIG 设备解析、资源检查、设备分配相关的逻辑。
package vgpu

import (
	"context"       // 用于 Kubernetes API 调用时传递上下文
	"encoding/json" // 用于把 patch 对象序列化成 JSON
	"errors"        // 用于构造 error
	"fmt"           // 用于格式化错误信息
	"hash/fnv"
	"sort"
	"strconv" // 用于字符串和数字互转
	"strings" // 用于字符串匹配、切分、大小写处理

	v1 "k8s.io/api/core/v1"                       // Kubernetes core/v1 API 类型，例如 Pod、Node、ResourceName
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1" // Kubernetes 元信息类型，例如 PatchOptions
	k8stypes "k8s.io/apimachinery/pkg/types"      // Kubernetes patch 类型，例如 StrategicMergePatchType
	"k8s.io/client-go/kubernetes"                 // Kubernetes client interface
	"k8s.io/klog/v2"                              // Kubernetes 日志库

	"volcano.sh/volcano/pkg/scheduler/api/devices"        // Volcano 设备相关 API，主要用于获取 Kubernetes client
	"volcano.sh/volcano/pkg/scheduler/api/devices/config" // Volcano 设备配置，包含 NVIDIA/MIG 配置
)

// extractGeometryFromType 根据 GPU 型号字符串 t，查找该型号对应的 MIG 几何切分模板。
//
// 例如：
// - t 可能是 "NVIDIA A100-SXM4-40GB"
// - 配置里可能声明 A100 支持哪些 MIG profile/geometry
//
// 返回：
// - 匹配到的 MIG 几何配置列表
// - 如果找不到，则返回错误
func extractGeometryFromType(t string) ([]config.Geometry, error) {
	// 如果全局配置存在，则从配置中读取 NVIDIA MIG 几何配置列表。
	if config.GetConfig() != nil {
		// 遍历所有 MIG 几何配置项。
		for _, val := range config.GetConfig().NvidiaConfig.MigGeometriesList {
			found := false

			// 一个 geometry 配置可能适配多个 GPU 型号。
			for _, migDevType := range val.Models {
				// 只要当前设备类型字符串 t 包含配置中的型号关键字，就认为匹配。
				if strings.Contains(t, migDevType) {
					found = true
				}
			}

			// 找到匹配型号后，返回对应 MIG 几何模板。
			if found {
				return val.Geometries, nil
			}
		}
	}

	// 没有配置或没有匹配到型号，返回空列表和错误。
	return []config.Geometry{}, errors.New("mig type not found")
}

// decodeNodeDevices 将节点 annotation 中保存的 GPU 设备字符串反序列化为 GPUDevices 结构。
//
// name: 节点名
// str:  节点上报的 GPU 字符串，通常来自 node annotation
//
// 该字符串大致格式类似：
//
//	uuid,count,memory,type,health,sharingMode:uuid,count,memory,type,health,sharingMode:
//
// 返回：
// - *GPUDevices：该节点上的设备信息
// - sharingMode：该节点的共享模式，例如 HAMICore 或 MIG
func decodeNodeDevices(name, str string) (*GPUDevices, string) {
	// 没有冒号说明格式不对，或者没有设备列表。
	if !strings.Contains(str, ":") {
		return nil, ""
	}

	// 按冒号切分出每张 GPU 的信息。
	tmp := strings.Split(str, ":")

	// 初始化该节点的 GPUDevices 对象。
	retval := &GPUDevices{
		Name:   name,                     // 节点名
		Device: make(map[int]*GPUDevice), // key 是设备 index，value 是设备指针
		Score:  float64(0),               // 节点或设备累计分数，初始为 0
	}

	// 默认共享模式为 HAMI core。
	sharingMode := vGPUControllerHAMICore

	// 遍历每张 GPU 的字符串描述。
	for index, val := range tmp {
		// 只有包含逗号的片段才认为是有效设备信息。
		if strings.Contains(val, ",") {
			items := strings.Split(val, ",")

			// 至少需要 6 个字段：uuid、count、memory、type、health、sharingMode。
			if len(items) < 6 {
				klog.Error("wrong Node GPU info: ", val)
				return nil, ""
			}

			// items[1]：该物理 GPU 可切分/可分配的 vGPU 数量。
			count, _ := strconv.Atoi(items[1])

			// items[2]：该 GPU 的总显存，单位通常是 MB。
			devmem, _ := strconv.Atoi(items[2])

			// items[4]：设备健康状态，字符串转 bool。
			health, _ := strconv.ParseBool(items[4])

			// 构造单张 GPU 设备对象。
			i := GPUDevice{
				ID:          index,                      // 设备编号
				Node:        name,                       // 所属节点
				UUID:        items[0],                   // 物理 GPU UUID
				Number:      uint(count),                // 最大可分配 vGPU 数量
				Memory:      uint(devmem),               // 总显存
				Type:        items[3],                   // GPU 型号，例如 NVIDIA A100
				PodMap:      make(map[string]*GPUUsage), // 记录该 GPU 上已经分配的 Pod 使用情况
				Health:      health,                     // 是否健康
				MigTemplate: []config.Geometry{},        // MIG 几何模板，默认空
				MigUsage: config.MigInUse{ // MIG 使用情况，默认 Index=-1 表示未选择模板
					Index: -1},
			}

			// items[5]：共享模式。只有 MIG 明确指定才返回 MIG，否则默认 HAMICore。
			sharingMode = getSharingMode(items[5])

			// 如果节点设备声明使用 MIG 模式，则需要找到对应 GPU 型号的 MIG 几何模板。
			if sharingMode == vGPUControllerMIG {
				var err error
				i.MigTemplate, err = extractGeometryFromType(i.Type)
				if err != nil {
					// 如果找不到 MIG 模板，则降级到 HAMI core 模式。
					sharingMode = vGPUControllerHAMICore
					klog.ErrorS(err, "extract mig geometry error and fall back to hamicore mode")
				}
			}

			// 保存到设备 map 中。
			retval.Device[index] = &i
		}
	}

	// 记录该节点最终使用的共享模式。
	retval.Mode = sharingMode
	return retval, sharingMode
}

// encodeContainerDevices 将一个 container 被分配到的设备列表编码成字符串。
//
// 每个 ContainerDevice 编码为：
//
//	UUID,Type,Usedmem,Usedcores:
//
// 多个设备之间用冒号分隔。
func encodeContainerDevices(cd []ContainerDevice) string {
	tmp := ""
	for _, val := range cd {
		// 拼接单个设备的 UUID、类型、占用显存、占用 core。
		tmp += val.UUID + "," + val.Type + "," + strconv.Itoa(int(val.Usedmem)) + "," + strconv.Itoa(int(val.Usedcores)) + ":"
	}
	klog.V(4).Infoln("Encoded container Devices=", tmp)
	return tmp
	// return strings.Join(cd, ",") // 旧思路：这里 cd 不是 []string，所以不能直接 Join。
}

// encodePodDevices 将一个 Pod 中所有 container 的设备分配结果编码成字符串。
//
// 一个 Pod 可能有多个 container；
// 每个 container 的设备列表用 encodeContainerDevices 编码；
// 多个 container 之间用分号 ; 分隔。
func encodePodDevices(pd []ContainerDevices) string {
	var ss []string
	for _, cd := range pd {
		ss = append(ss, encodeContainerDevices(cd))
	}
	return strings.Join(ss, ";")
}

// decodeContainerDevices 将单个 container 的设备分配字符串反序列化为 ContainerDevices。
//
// 输入格式大致是：
//
//	UUID,Type,Usedmem,Usedcores:UUID,Type,Usedmem,Usedcores:
func decodeContainerDevices(str string) ContainerDevices {
	// 空字符串表示没有设备分配。
	if len(str) == 0 {
		return ContainerDevices{}
	}

	// 每个设备用冒号分隔。
	cd := strings.Split(str, ":")
	contdev := ContainerDevices{}
	tmpdev := ContainerDevice{}

	// 这里是重复的空判断，前面已经判断过一次。
	if len(str) == 0 {
		return contdev
	}

	// 遍历每个设备字符串。
	for _, val := range cd {
		// 只有包含逗号的片段才是有效设备描述。
		if strings.Contains(val, ",") {
			tmpstr := strings.Split(val, ",")

			// 解析 UUID 和类型。
			tmpdev.UUID = tmpstr[0]
			tmpdev.Type = tmpstr[1]

			// 解析占用显存。
			devmem, _ := strconv.ParseInt(tmpstr[2], 10, 32)
			tmpdev.Usedmem = uint(devmem)

			// 解析占用 core。
			devcores, _ := strconv.ParseInt(tmpstr[3], 10, 32)
			tmpdev.Usedcores = uint(devcores)

			// 添加到当前 container 的设备列表。
			contdev = append(contdev, tmpdev)
		}
	}
	return contdev
}

// decodePodDevices 将 Pod 级别的设备分配字符串反序列化。
//
// 多个 container 之间用分号 ; 分隔。
func decodePodDevices(str string) []ContainerDevices {
	// 空字符串表示该 Pod 没有设备分配。
	if len(str) == 0 {
		return []ContainerDevices{}
	}

	var pd []ContainerDevices
	for _, s := range strings.Split(str, ";") {
		cd := decodeContainerDevices(s)
		pd = append(pd, cd)
	}
	return pd
}

// checkVGPUResourcesInPod 检查 Pod 中是否存在 vGPU 资源请求。
//
// 只要任意 container 的 limits 中声明了：
// - vgpu-memory
// - vgpu-number
// 就认为该 Pod 是 vGPU Pod。
func checkVGPUResourcesInPod(pod *v1.Pod) bool {
	// 遍历 Pod 内所有 container。
	for _, container := range pod.Spec.Containers {
		// 判断是否声明了 vGPU 显存资源。
		_, ok := container.Resources.Limits[v1.ResourceName(getConfig().ResourceMemoryName)]
		if ok {
			return true
		}

		// 判断是否声明了 vGPU 数量资源。
		_, ok = container.Resources.Limits[v1.ResourceName(getConfig().ResourceCountName)]
		if ok {
			return true
		}
	}

	// 所有 container 都没有 vGPU 资源声明，则不是 vGPU Pod。
	return false
}

// resourcereqs 解析 Pod 中每个 container 的 vGPU 请求，转成内部统一的 ContainerDeviceRequest。
//
// 返回值中每一个 ContainerDeviceRequest 对应一个有 vGPU 请求的 container。
func resourcereqs(pod *v1.Pod) []ContainerDeviceRequest {
	// vGPU 数量资源名，例如 volcano.sh/vgpu-number。
	resourceName := v1.ResourceName(getConfig().ResourceCountName)

	// vGPU 显存资源名，例如 volcano.sh/vgpu-memory。
	resourceMem := v1.ResourceName(getConfig().ResourceMemoryName)

	// vGPU 显存百分比资源名，例如 volcano.sh/vgpu-memory-percentage。
	resourceMemPercentage := v1.ResourceName(getConfig().ResourceMemoryPercentageName)

	// vGPU core 资源名，例如 volcano.sh/vgpu-cores。
	resourceCores := v1.ResourceName(getConfig().ResourceCoreName)

	// 保存所有 container 的 vGPU 请求。
	counts := []ContainerDeviceRequest{}

	// Count Nvidia GPU：遍历 Pod 内每个 container，解析 NVIDIA vGPU 请求。
	for i := 0; i < len(pod.Spec.Containers); i++ {
		// singledevice 表示：没有显式声明 vgpu-number，但声明了 vgpu-memory。
		// 这种情况下默认认为请求 1 张/1 个 vGPU。
		singledevice := false

		// 优先从 limits 中读取 vgpu-number。
		v, ok := pod.Spec.Containers[i].Resources.Limits[resourceName]

		// 如果没有声明 vgpu-number，则尝试用 vgpu-memory 作为是否请求 GPU 的判断依据。
		if !ok {
			v, ok = pod.Spec.Containers[i].Resources.Limits[resourceMem]
			singledevice = true
		}

		// 只有声明了 vgpu-number 或 vgpu-memory，才认为这个 container 有 vGPU 请求。
		if ok {
			// 默认请求数量为 1。
			n := int64(1)

			// 如果不是 singledevice，说明 v 来自 vgpu-number，此时解析实际请求数量。
			if !singledevice {
				n, _ = v.AsInt64()
			}

			// 解析显存请求，默认 0。
			memnum := uint(0)
			mem, ok := pod.Spec.Containers[i].Resources.Limits[resourceMem]
			if !ok {
				// limits 没有时，尝试从 requests 读取。
				mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMem]
			}
			if ok {
				memnums, ok := mem.AsInt64()
				if ok {
					memnum = uint(memnums)
				}
			}

			// 解析显存百分比请求。
			// 101 是哨兵值，表示用户没有显式声明显存百分比。
			mempnum := int32(101)
			mem, ok = pod.Spec.Containers[i].Resources.Limits[resourceMemPercentage]
			if !ok {
				// limits 没有时，尝试从 requests 读取。
				mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMemPercentage]
			}
			if ok {
				mempnums, ok := mem.AsInt64()
				if ok {
					mempnum = int32(mempnums)
				}
			}

			// 如果用户既没有声明显存大小，也没有声明显存百分比，默认按 100% 显存处理。
			if mempnum == 101 && memnum == 0 {
				mempnum = 100
			}

			// 解析 vgpu-cores，默认 0。
			corenum := uint(0)
			core, ok := pod.Spec.Containers[i].Resources.Limits[resourceCores]
			if !ok {
				// limits 没有时，尝试从 requests 读取。
				core, ok = pod.Spec.Containers[i].Resources.Requests[resourceCores]
			}
			if ok {
				corenums, ok := core.AsInt64()
				if ok {
					corenum = uint(corenums)
				}
			}

			// 将当前 container 的请求标准化为 ContainerDeviceRequest。
			counts = append(counts, ContainerDeviceRequest{
				Nums:             int32(n),       // 请求 vGPU 数量
				Type:             "NVIDIA",       // 当前逻辑固定按 NVIDIA 处理
				Memreq:           memnum,         // 显存大小请求
				MemPercentagereq: int32(mempnum), // 显存百分比请求
				Coresreq:         corenum,        // core/算力份额请求
			})
		}
	}

	// 打印解析结果。
	klog.V(3).Infoln("counts=", counts)
	return counts
}

// checkGPUtype 根据 Pod annotations 中的 GPU 白名单/黑名单规则，判断某张 GPU 卡型是否可用。
//
// annos:    Pod 的 annotations
// cardtype: 当前候选 GPU 的类型，例如 "NVIDIA A100-PCIE-40GB"
//
// GPUInUse：白名单，只允许使用匹配的 GPU 类型。
// GPUNoUse：黑名单，禁止使用匹配的 GPU 类型。
//
// 优先级：GPUInUse > GPUNoUse。
func checkGPUtype(annos map[string]string, cardtype string) bool {
	// 先读取白名单 annotation。
	inuse, ok := annos[GPUInUse]
	if ok {
		// 白名单只有一个值，不包含逗号。
		if !strings.Contains(inuse, ",") {
			// 大小写不敏感地判断 cardtype 是否包含白名单关键字。
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(inuse)) {
				return true
			}
		} else {
			// 白名单有多个值，使用逗号分隔。
			for _, val := range strings.Split(inuse, ",") {
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return true
				}
			}
		}

		// 如果设置了白名单，但当前卡型没有命中任何白名单项，则不可用。
		return false
	}

	// 没有白名单时，再读取黑名单 annotation。
	nouse, ok := annos[GPUNoUse]
	if ok {
		// 黑名单只有一个值。
		if !strings.Contains(nouse, ",") {
			// 如果 cardtype 命中黑名单，则不可用。
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(nouse)) {
				return false
			}
		} else {
			// 黑名单有多个值，逐个匹配。
			for _, val := range strings.Split(nouse, ",") {
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return false
				}
			}
		}

		// 设置了黑名单但没有命中，说明当前卡型可用。
		return true
	}

	// 没有白名单也没有黑名单，则默认所有 GPU 类型都可用。
	return true
}

// checkType 判断某个候选 GPUDevice 是否满足 container 的设备类型请求。
//
// 第一层：检查大类，例如 NVIDIA 请求必须匹配 NVIDIA 设备。
// 第二层：如果是 NVIDIA，则继续根据 Pod annotations 做具体卡型白名单/黑名单过滤。
func checkType(annos map[string]string, d GPUDevice, n ContainerDeviceRequest) bool {
	// 通用设备类型检查，例如：
	// d.Type = "NVIDIA A100"，n.Type = "NVIDIA"，则通过。
	// d.Type = "MLU"，n.Type = "NVIDIA"，则失败。
	if !strings.Contains(d.Type, n.Type) {
		return false
	}

	// 如果请求的是 NVIDIA GPU，则继续检查具体卡型是否符合 Pod 注解限制。
	if n.Type == NvidiaGPUDevice {
		return checkGPUtype(annos, d.Type)
	}

	// 其他设备类型当前未识别，直接失败。
	klog.Errorf("Unrecognized device %v", n.Type)
	return false
}

// getGPUDeviceSnapShot 获取 GPUDevices 的快照。
//
// 注意：原注释说明这不是严格意义上的深拷贝，因为部分指针字段仍可能与原对象共享。
// 但这里会为 GPUDevice 对象创建新结构，并对 MigUsage 做深拷贝。
func getGPUDeviceSnapShot(snap *GPUDevices) *GPUDevices {
	// 初始化返回对象。
	ret := GPUDevices{
		Name:    snap.Name,
		Device:  make(map[int]*GPUDevice),
		Score:   float64(0),
		Sharing: snap.Sharing,
	}

	// 遍历原始设备 map。
	for index, val := range snap.Device {
		if val != nil {
			// 复制 GPUDevice 的主要字段。
			ret.Device[index] = &GPUDevice{
				ID:          val.ID,
				Node:        val.Node,
				UUID:        val.UUID,
				PodMap:      val.PodMap, // 注意：这里仍然共享原 PodMap 指针/引用
				Memory:      val.Memory,
				Number:      val.Number,
				Type:        val.Type,
				Health:      val.Health,
				UsedNum:     val.UsedNum,
				UsedMem:     val.UsedMem,
				UsedCore:    val.UsedCore,
				MigTemplate: val.MigTemplate,
				MigUsage:    val.MigUsage,
			}

			// 对 MIG 使用状态做深拷贝，避免快照和原对象互相影响。
			ret.Device[index].MigUsage = deepCopyMigInUse(val.MigUsage)

			klog.V(4).Infoln("getGPUDeviceSnapShot:", ret.Device[index].UsedMem, val.UsedMem, ret.Device[index].MigUsage, val.MigUsage)
		}
	}
	return &ret
}

// deepCopyMigInUse 深拷贝 MIG 使用状态。
//
// MIG 使用状态里包含切片列表和 UsedIndex 切片，必须手动 copy，避免共享底层数组。
func deepCopyMigInUse(src config.MigInUse) config.MigInUse {
	// 先复制简单字段。
	dst := config.MigInUse{
		Index: src.Index,
	}

	// 为 UsageList 新建切片。
	dst.UsageList = make(config.MIGS, len(src.UsageList))

	// 逐项复制 MIG 模板使用情况。
	for i, usage := range src.UsageList {
		dst.UsageList[i] = config.MigTemplateUsage{
			Name:      usage.Name,
			Memory:    usage.Memory,
			InUse:     usage.InUse,
			UsedIndex: make([]int, len(usage.UsedIndex)), // UsedIndex 也要新建底层数组
		}

		// 拷贝 UsedIndex 内容。
		copy(dst.UsageList[i].UsedIndex, usage.UsedIndex)
	}

	return dst
}

// getSharingMode 解析共享模式。
//
// 默认使用 HAMI core 模式；只有 mode 明确等于 vGPUControllerMIG 时才使用 MIG。
func getSharingMode(mode string) string {
	switch mode {
	case vGPUControllerMIG:
		return vGPUControllerMIG
	default:
		return vGPUControllerHAMICore
	}
}

func normalizeGPUSelectPolicy(policy string) string {
	switch policy {
	case OriginalPolicy, BinpackPolicy, SpreadPolicy, RandomPolicy, DQNPolicy:
		return policy
	default:
		return OriginalPolicy
	}
}

func stableRandomKey(pod *v1.Pod, gpuID int) uint32 {
	h := fnv.New32a()
	key := pod.Namespace + "/" + pod.Name + "/" + string(pod.UID) + "/" + strconv.Itoa(gpuID)
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

// 新加
func canFitForOrder(g *GPUDevice, req ContainerDeviceRequest) bool {
	if g == nil {
		return false
	}
	if g.Number <= uint(g.UsedNum) {
		return false
	}
	if int(g.Memory)-int(g.UsedMem) < int(req.Memreq) {
		return false
	}
	if g.UsedCore+req.Coresreq > 100 {
		return false
	}
	if req.Coresreq == 100 && g.UsedNum > 0 {
		return false
	}
	if g.UsedCore == 100 && req.Coresreq == 0 {
		return false
	}
	return true
}

func orderedGPUIndexes(gs *GPUDevices, pod *v1.Pod, req ContainerDeviceRequest) []int {
	indexes := make([]int, 0, len(gs.Device))
	for idx := range gs.Device {
		if gs.Device[idx] != nil {
			indexes = append(indexes, idx)
		}
	}

	policy := normalizeGPUSelectPolicy(GPUSelectPolicy)
	if policy == DQNPolicy {
		dqnIndexes, err := queryDQNOrderedGPUIndexes(gs, pod, req)
		if err == nil && len(dqnIndexes) > 0 {
			klog.Infof("GPUSelectPolicy=dqn reqMem=%d reqCore=%d ordered indexes=%v", req.Memreq, req.Coresreq, dqnIndexes)

			for _, idx := range dqnIndexes {
				g := gs.Device[idx]
				klog.Infof(
					"GPUSelectPolicy=dqn idx=%d id=%d uuid=%s fit=%v usedMem=%d totalMem=%d usedCore=%d usedNum=%d",
					idx,
					g.ID,
					g.UUID,
					canFitForOrder(g, req),
					g.UsedMem,
					g.Memory,
					g.UsedCore,
					g.UsedNum,
				)
			}

			return dqnIndexes
		}

		klog.Warningf(
			"GPUSelectPolicy=dqn failed for pod %s/%s on node %s: %v, fallback to binpack",
			pod.Namespace,
			pod.Name,
			gs.Name,
			err,
		)

		// DQN 服务异常时兜底到 binpack，避免调度器不可用。
		policy = BinpackPolicy
	}

	sort.SliceStable(indexes, func(a, b int) bool {
		ga := gs.Device[indexes[a]]
		gb := gs.Device[indexes[b]]

		afit := canFitForOrder(ga, req)
		bfit := canFitForOrder(gb, req)

		// 可放的 GPU 永远排在不可放 GPU 前面
		if afit != bfit {
			return afit
		}

		switch policy {
		case BinpackPolicy:
			// binpack：在可放 GPU 里，优先选择已经使用更多的 GPU
			if ga.UsedMem != gb.UsedMem {
				return ga.UsedMem > gb.UsedMem
			}
			if ga.UsedCore != gb.UsedCore {
				return ga.UsedCore > gb.UsedCore
			}
			if ga.UsedNum != gb.UsedNum {
				return ga.UsedNum > gb.UsedNum
			}
			return ga.ID > gb.ID

		case SpreadPolicy:
			// spread：在可放 GPU 里，优先选择使用更少的 GPU
			if ga.UsedNum != gb.UsedNum {
				return ga.UsedNum < gb.UsedNum
			}
			if ga.UsedMem != gb.UsedMem {
				return ga.UsedMem < gb.UsedMem
			}
			if ga.UsedCore != gb.UsedCore {
				return ga.UsedCore < gb.UsedCore
			}
			return ga.ID > gb.ID

		case RandomPolicy:
			ka := stableRandomKey(pod, ga.ID)
			kb := stableRandomKey(pod, gb.ID)
			if ka != kb {
				return ka < kb
			}
			return ga.ID > gb.ID

		case OriginalPolicy:
			fallthrough
		default:
			return ga.ID > gb.ID
		}
	})

	klog.Infof(
		"GPUSelectPolicy=%s reqMem=%d reqCore=%d ordered indexes=%v",
		policy,
		req.Memreq,
		req.Coresreq,
		indexes,
	)

	for _, idx := range indexes {
		g := gs.Device[idx]
		klog.Infof(
			"GPUSelectPolicy=%s idx=%d id=%d uuid=%s fit=%v usedMem=%d totalMem=%d usedCore=%d usedNum=%d",
			policy,
			idx,
			g.ID,
			g.UUID,
			canFitForOrder(g, req),
			g.UsedMem,
			g.Memory,
			g.UsedCore,
			g.UsedNum,
		)
	}

	return indexes
}

// checkNodeGPUSharingPredicateAndScore 检查一个有 GPU 需求的 Pod 是否可以调度到某个节点，
// 同时尝试分配设备并计算调度分数。
//
// 参数：
// - pod: 待调度 Pod
// - gssnap: 节点 GPU 设备状态快照/对象
// - replicate: 是否复制一份设备快照进行模拟分配。true 表示不直接修改原始状态。
// - schedulePolicy: 设备级调度策略，例如 binpack 或 spread
//
// 返回：
// - bool: 是否可以调度到该节点
// - []ContainerDevices: 每个 container 分配到的设备列表
// - float64: 该次分配得到的调度分数
// - error: 不可调度原因
func checkNodeGPUSharingPredicateAndScore(pod *v1.Pod, gssnap *GPUDevices, replicate bool, schedulePolicy string) (bool, []ContainerDevices, float64, error) {
	score := float64(0)

	if !checkVGPUResourcesInPod(pod) {
		return true, []ContainerDevices{}, 0, nil
	}

	podSharingMode, ok := pod.Annotations[GPUModeAnnotation]
	if ok && podSharingMode != gssnap.Mode {
		return false, []ContainerDevices{}, 0, fmt.Errorf("pod required sharing mode %s is not the same as the node mode %s", podSharingMode, gssnap.Mode)
	}

	ctrReq := resourcereqs(pod)
	if len(ctrReq) == 0 {
		return true, []ContainerDevices{}, 0, nil
	}

	// 关键修改：
	// 无论 Filter 阶段还是 Allocate 阶段，都只在快照上 TryAddPod。
	// HAMi-core TryAddPod 会修改 UsedNum/UsedMem/UsedCore；
	// 如果 replicate=false 时直接改真实 gssnap，后面 Allocate() 里的 gs.addResource()
	// 还会调用 AddPod 再加一次，导致重复记账。
	//
	// HAMi-core 的 TryAddPod 和 AddPod 都会累加 UsedNum/UsedMem/UsedCore，
	// 所以真实账本只能让 Allocate() 后面的 gs.addResource() 更新一次。:contentReference[oaicite:0]{index=0}
	gs := getGPUDeviceSnapShot(gssnap)

	ctrdevs := []ContainerDevices{}

	for _, val := range ctrReq {
		devs := []ContainerDevice{}

		if int(val.Nums) > len(gs.Device) {
			return false, []ContainerDevices{}, 0, fmt.Errorf("no enough gpu cards on node %s", gs.Name)
		}

		klog.V(3).InfoS("Allocating device for container", "request", val)

		deviceIndexes := orderedGPUIndexes(gs, pod, val)

		for _, i := range deviceIndexes {
			klog.V(3).InfoS(
				"Scoring pod request",
				"memReq", val.Memreq,
				"memPercentageReq", val.MemPercentagereq,
				"coresReq", val.Coresreq,
				"Nums", val.Nums,
				"Index", i,
				"ID", gs.Device[i].ID,
			)
			klog.V(3).InfoS(
				"Current Device",
				"Index", i,
				"TotalMemory", gs.Device[i].Memory,
				"UsedMemory", gs.Device[i].UsedMem,
				"UsedCores", gs.Device[i].UsedCore,
				"replicate", replicate,
			)

			if gs.Device[i].Number <= uint(gs.Device[i].UsedNum) {
				continue
			}

			if val.MemPercentagereq != 101 && val.Memreq == 0 {
				val.Memreq = gs.Device[i].Memory * uint(val.MemPercentagereq) / 100
			}

			if int(gs.Device[i].Memory)-int(gs.Device[i].UsedMem) < int(val.Memreq) {
				continue
			}

			if gs.Device[i].UsedCore+val.Coresreq > 100 {
				continue
			}

			if val.Coresreq == 100 && gs.Device[i].UsedNum > 0 {
				continue
			}

			if gs.Device[i].UsedCore == 100 && val.Coresreq == 0 {
				continue
			}

			if !checkType(pod.Annotations, *gs.Device[i], val) {
				klog.Errorln("failed checktype", gs.Device[i].Type, val.Type)
				continue
			}

			fit, uuid := gs.Sharing.TryAddPod(gs.Device[i], uint(val.Memreq), uint(val.Coresreq))
			if !fit {
				klog.V(3).Info(gs.Device[i].ID, "not fit")
				continue
			}

			if val.Nums > 0 {
				val.Nums--
				klog.V(3).Info("fitted uuid: ", uuid)

				devs = append(devs, ContainerDevice{
					UUID:      uuid,
					Type:      val.Type,
					Usedmem:   val.Memreq,
					Usedcores: val.Coresreq,
				})

				score += GPUScore(schedulePolicy, gs.Device[i])
			}

			if val.Nums == 0 {
				break
			}
		}

		if val.Nums > 0 {
			return false, []ContainerDevices{}, 0, fmt.Errorf("not enough gpu fitted on this node")
		}

		ctrdevs = append(ctrdevs, devs)
	}

	return true, ctrdevs, score, nil
}

// GPUScore 根据设备级调度策略，对单张 GPU 打分。
//
// score 越高，表示在当前策略下越倾向选择该设备。
func GPUScore(schedulePolicy string, device *GPUDevice) float64 {
	// float64 默认零值是 0.0。
	var score float64

	switch schedulePolicy {
	case binpackPolicy:
		// binpack 策略：显存使用率越高，分数越高。
		// 目的：优先把新任务继续塞到已经使用较多的卡上，尽量保留完整空闲卡。
		score = binpackMultiplier * (float64(device.UsedMem) / float64(device.Memory))

	case spreadPolicy:
		// spread 策略：鼓励选择原本空闲的卡。
		// 注意：该函数通常在 TryAddPod 之后调用，此时 UsedNum 已经自增。
		// 因此 UsedNum == 1 往往意味着：这张卡在本次分配前是空的。
		if device.UsedNum == 1 {
			score = spreadMultiplier
		}

	default:
		// 未识别策略，不加分。
		score = float64(0)
	}
	return score
}

// patchPodAnnotations 给 Pod 打 patch，更新 annotations。
//
// kubeClient: Kubernetes client
// pod:        要 patch 的 Pod
// annotations: 要写入/更新的 annotations map
func patchPodAnnotations(kubeClient kubernetes.Interface, pod *v1.Pod, annotations map[string]string) error {
	// patch 只需要 metadata.annotations 字段。
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`
		// Spec patch 当前没有用到。
		// Spec     patchSpec     `json:"spec,omitempty"`
	}

	// 构造 patch 对象。
	p := patchPod{}
	p.Metadata.Annotations = annotations

	// 序列化成 JSON。
	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}

	// 对指定 namespace/name 的 Pod 发起 StrategicMergePatch。
	_, err = kubeClient.CoreV1().Pods(pod.Namespace).
		Patch(context.Background(), pod.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("patch pod %v failed, %v", pod.Name, err)
	}

	return err
}

// patchNodeAnnotations 给 Node 打 patch，更新 annotations。
//
// node:        要 patch 的 Node
// annotations: 要写入/更新的 annotations map
func patchNodeAnnotations(node *v1.Node, annotations map[string]string) error {
	// patch 只需要 metadata.annotations 字段。
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`
	}
	type patchNode struct {
		Metadata patchMetadata `json:"metadata"`
		// Spec patch 当前没有用到。
		// Spec     patchSpec     `json:"spec,omitempty"`
	}

	// 构造 patch 对象。
	p := patchNode{}
	p.Metadata.Annotations = annotations

	// 序列化成 JSON。
	bytes, err := json.Marshal(p)
	if err != nil {
		return err
	}

	// 使用 devices.GetClient() 获取 Kubernetes client，对 Node 发起 StrategicMergePatch。
	_, err = devices.GetClient().CoreV1().Nodes().
		Patch(context.Background(), node.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("patch node %v failed, %v", node.Name, err)
	}
	return err
}

// getConfig 获取 NVIDIA 设备配置。
//
// 如果全局配置存在，则返回全局配置中的 NvidiaConfig；
// 否则返回默认设备配置中的 NvidiaConfig。
func getConfig() config.NvidiaConfig {
	if config.GetConfig() != nil {
		return config.GetConfig().NvidiaConfig
	}
	return config.GetDefaultDevicesConfig().NvidiaConfig
}
