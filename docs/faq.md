# FAQ

## What does Plumb change in my clusters?

Two things: a KEDA replica floor (through its external scaler) and cluster-level HTTPRoute backend weights. It never creates or deletes nodes or pods, never edits NodePools, and never routes requests itself. Set a policy to `shadow` and it changes nothing at all.

## Does Plumb deploy my workload to other clusters?

No. The same Deployment and `AdaptivePolicy` are expected in every member, usually from GitOps. Plumb decides how many replicas each cluster should hold at least, and how traffic is split; KEDA, the scheduler and Karpenter do the rest.

## How is this different from the Gateway API Inference Extension?

They work at different scales and fit together. The Inference Extension picks a pod for every request inside a cluster (e.g. by KV-cache state). Plumb moves a cluster's share of traffic every tens of seconds, and capacity with it. See [scope](architecture.md#scope-knobs-not-mechanisms).

## Do I need Karpenter?

No. A cluster without Karpenter takes part with static capacity only: the replicas that fit on its existing nodes.

## Can I start with one cluster?

Yes. Without other members it is a fleet of one: you get reports, metrics and the decision log, and can add members later with ClusterProfiles.

## Do I need a model?

No. Deterministic rules decide by default, and nothing leaves the cluster. The models are experimental and opt-in:

- **Ranking model (Jev):** reorders candidate clusters within a placement tier on the rules path.
- **Adaptive path (planner + Jev):** for workloads whose priorities are easier to state in words than as a scoring rule. A planner proposes plans, Plumb validates them, Jev picks one; or, with `chooser: planner`, the planner's one plan runs once validated. Each workload chooses: some can stay on the rules while others use the planner.

Both start in `shadow`: their picks are logged next to the rules' until you choose `model.mode=apply` (and, on the adaptive path, `mode: apply` on the policy).

## What happens when the hub goes away?

Another member takes over within about 15 seconds. With no hub at all, floors expire after 5 minutes and every cluster runs on its own KEDA and Karpenter, as if Plumb were not installed. A deposed hub's writes are fenced. See [failure modes](architecture.md#failure-modes).

## Can two workloads share the same idle GPUs?

Yes, without double-booking: each member takes other policies' promised but unplaced replicas out of its room before reporting it. See [shared capacity](architecture.md#shared-capacity).

## How do I stop Plumb immediately?

Set `spec.mode: shadow` in every copy of the policy. Scalers stop serving floors at once, and the hub stops writing weights. Route weights stay where they are.

## Which clouds are supported?

Any cluster running Karpenter v1: NodePools, NodeClaims and their events are the same on every cloud. What a cloud adds is its launch error codes and the node labels it sets, which tell a capacity shortage from a misconfiguration; AWS's are known. On another cloud Plumb still works, counting every launch failure alike, until its table is added ([how](../CONTRIBUTING.md#karpenter-cloud-providers)). Clusters without Karpenter, e.g. on-premises, lend their existing nodes.

## What do the models cost?

Nothing unless configured. When configured, the ranking model and Jev are called only while a member is short or the fleet is not Steady, and the planner at most once per `planner.interval` per policy under the same condition.
