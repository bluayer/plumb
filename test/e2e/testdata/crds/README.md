CRDs copied unchanged from upstream (Apache-2.0). Only the CRDs are installed; their
controllers are not run.

- `karpenter.sh_*`: sigs.k8s.io/karpenter v1.14.1 `pkg/apis/crds/`. Tests create NodeClaims
  and InsufficientCapacityError events in the shapes Karpenter v1.14.1 produces (see
  internal/adapters/aws/karpenter.go).
- `multicluster.x-k8s.io_clusterprofiles.yaml`: sigs.k8s.io/cluster-inventory-api v0.1.3
  `config/crd/bases/`. Tests play the cluster manager and write ClusterProfiles.
- `gateway.networking.k8s.io_httproutes.yaml`: sigs.k8s.io/gateway-api v1.6.2
  `config/crd/standard/`. No gateway runs; tests check the weights the hub writes.
