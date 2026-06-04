# DQN Success Case Search

This folder contains an attempted real-cluster preload case designed from a direct `ScheduleJob` search. The direct gRPC search found cases where DQN's final balance score is lower than spread/binpack under preloaded GPU states.

The current real-cluster translation is not yet a clean paper result because manually preloaded Pods interact with the device plugin state and not every target Pod reaches Running consistently. Treat this as a candidate workload generator, not final evidence.

For paper-quality real-cluster superiority, prefer implementing a controlled preload mechanism in the scheduler experiment harness or use real existing workloads accumulated by normal Volcano scheduling.
