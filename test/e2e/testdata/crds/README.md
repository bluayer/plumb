Karpenter CRDs copied unchanged from sigs.k8s.io/karpenter v1.14.1 `pkg/apis/crds/`
(Apache-2.0). Only the CRDs are installed; the Karpenter controller is not run. Tests
create NodeClaims and InsufficientCapacityError events in the shapes Karpenter v1.14.1
produces (see internal/adapters/aws/karpenter/constants.go).
