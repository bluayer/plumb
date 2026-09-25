# Architecture

Plumb adds a slow, deliberate fleet loop on top of the fast per-cluster loops you already run. It never replaces them.

| Loop | Time scale | Who | What |
|---|---|---|---|
| Scheduling and scaling | ms – s | kube-scheduler, KEDA/HPA, Karpenter | Place pods, scale replicas, launch nodes. Plumb does not touch these. |
| Member | seconds | `plumb agent`, in every cluster | Observe the cluster, report it, apply local fixes |
| Fleet | tens of seconds – minutes | the elected hub (one `plumb agent`) | Move capacity and traffic between clusters |

## Components

Each member cluster runs the same two Deployments from the Helm chart:

- **`plumb agent`** (2 replicas; one is elected per cluster through a local Lease). It has three jobs:
  - **Member reconciler.** For every `AdaptivePolicy` that lists this cluster, writes `status.report`. The report holds desired, ready and unschedulable replicas, and static and dynamic room. It also counts recent launch failures (from Karpenter informers) and records demand and saturation (from Prometheus). It computes how many replicas are missing and since when. In `auto` mode it widens a NodePool from spot to on-demand when spot capacity errors recur.
  - **Fleet.** Connects to every other member listed as a ClusterProfile, and keeps an informer cache of their `AdaptivePolicy` objects.
  - **Hub candidate.** Stands in the fleet-wide election and, while elected, runs the planner.
- **`plumb scaler`** (2 replicas). A KEDA external scaler that serves the replica floor the hub asks of this cluster. It reads only informer caches, so it is never slower than KEDA's own loop.

There is no database and no central service. State lives in the `AdaptivePolicy` status of each member, with one writer per field:

| Field | Written by | Meaning |
|---|---|---|
| `status.report` | the member itself | this cluster's observation, refreshed every `--interval` |
| `status.intent` | the hub | replica floor for this cluster, its expiry, and which hub wrote it |
| `status.fleet` | the hub, on its own copy | phase, per-cluster plan, last decision |
| `status.conditions` | the member itself | `Ready` |

The same `AdaptivePolicy` spec is applied to every member (GitOps is the natural fit). Each report carries a hash of the spec it was computed from, and the hub ignores reports from a member whose copy differs from its own.

## Membership

Members are listed with SIG-Multicluster [ClusterProfile](https://github.com/kubernetes-sigs/cluster-inventory-api) objects in Plumb's namespace, one for each other member. Credentials come from the profile's `status.accessProviders`, resolved through the KEP-5339 exec-plugin mechanism (`--clusterprofile-provider-file`).

Adding a ClusterProfile adds a member to a running fleet, and deleting one removes it.

## Hub election

Every member keeps a `coordination.k8s.io/v1` Lease named `plumb-hub` in Plumb's namespace. The hub is elected with client-go's standard `leaderelection.LeaderElector`. Only the lock is Plumb's: a quorum lock over the members' Leases.

- A **read** returns the holder that most members name. On a tie, the latest renewal wins.
- A **write** is a compare-and-swap against each Lease's `resourceVersion`, and succeeds only on a **majority** of members. A Lease that changed since it was read is never overwritten.
- **Every listed member votes**, connected or not. A member that cannot reach its peers can therefore never elect itself.
- Timing is the Kubernetes default: 15s lease, 10s renew deadline, 2s retry.

So a partition's minority side cannot elect a hub. A two-member fleet needs both members to elect one, and three or more tolerate losing a minority. With no hub, nothing breaks: see [failure modes](#failure-modes).

### Fencing

A hub that has lost its lease may keep acting until its renew deadline passes. Plumb makes such writes harmless:

- The **scaler** serves an intent only if the intent's `hub` is the holder named by this member's own copy of the hub Lease, the Lease has not expired, and the intent itself has not expired.
- The **hub** writes HTTPRoute weights only after checking that the route cluster's copy of the Lease names it.

## Planning

At least every `--hub-interval` (10s), and sooner when another member's report changes, the hub runs `core.Plan` for each policy. The planner is a pure function of the members' reports, the current floors and weights, and the previous phase:

```text
Steady ──(a member short for `after`, or `staticAfter` if another has idle static room)──▶ Escalated
Escalated ──(no member short)──▶ Recovering ──(calm for `calmFor`, floors and weights back)──▶ Steady
Recovering ──(a member short again)──▶ Escalated
```

At most one step per `cooldown`. Each step does the following:

1. **Missing replicas.**
   - Each member reports what it needs: unschedulable replicas, demand beyond its desired replicas (`ceil(demand / replicaCapacity)`), and at least `step` while saturated.
   - The hub subtracts replicas it already requested that are not ready yet, so waiting for nodes never causes over-provisioning.
2. **Candidates.** Fresh reports from members that are not short themselves, with headroom under `maxReplicas`, and room left.
   - Static room: replicas that fit on existing nodes.
   - Dynamic room: NodePool limits minus usage. It counts as zero after recurring launch failures.
3. **Ranking.**
   - If a model is configured and there are at least two candidates, it gets a compact per-cluster summary and one typed question: which cluster should add replicas first.
   - Its probabilities order the candidates when the top one reaches `confidenceThresholdPercent`.
   - Otherwise the rules rank: static room, fewer launch failures, cost rank, more room, name.
4. **Allocation.**
   - Two passes over the ranking: first every candidate's **static room**, then **dynamic room**. The model can reorder clusters within a pass, but never put dynamic capacity ahead of idle static capacity.
   - Each cluster gains at most `step` replicas per step. Floors are absolute: `max(current floor, desired) + added`.
5. **Traffic.**
   - While any floor is held or any member is short, each cluster's target share is proportional to `readyReplicas × replicaCapacity`, clamped to `[minWeight, maxWeight]`.
   - Otherwise the target is the cluster's Steady `weight`.
   - Each share moves at most `stepPercent` points per step, and only as replicas become ready.
6. **Release.** After `calmFor` in Recovering:
   - Floors drop by `step`, dynamic ones before static ones, so Karpenter consolidates what it added while reserved and existing nodes stay in use.
   - Then weights return to Steady.

The model decides *where*; the policy and arithmetic decide *how much*. A model failure, timeout, open circuit breaker or low confidence falls back to the rules within the same step.

## Apply

In `auto` mode the hub:

- writes each changed floor to the member's `status.intent`, with a 5-minute expiry it renews while the floor is held;
- writes each changed weight to every configured HTTPRoute.

In `shadow` mode it writes neither, and simulates floors and weights forward in `status.fleet` so the plan can be reviewed as it would have unfolded.

Every step that changes something is recorded in three places:

- `status.fleet.lastDecision`
- an Event on the hub's copy of the policy
- a JSONL decision-log line: reports, model answer, plan before and after, applied or not, errors

## Failure modes

| Failure | What happens |
|---|---|
| Hub process dies | Another member takes over within about one lease duration (15s) and adopts current floors from the members' intents and weights from the routes |
| Member unreachable | Its report goes stale after 2 minutes and it stops being a candidate. Its route share is held as is. Floors and weights elsewhere keep working |
| Minority of members partitioned | The majority side keeps (or elects) the hub. The minority side has none |
| No hub at all (e.g. 2-member fleet split) | Intents expire after 5 minutes and every cluster runs on its own KEDA/Karpenter behaviour. Route weights stay where they were |
| Model down, slow or unsure | Rules rank the clusters in the same step. After 3 consecutive failures the model is skipped for a minute |
| Prometheus down | The report omits demand and saturation, and the policy's `Ready` condition turns false with the error. Unschedulable replicas still count |
| Policy copies differ | The hub ignores reports whose spec hash differs from its own copy |
| Two running fleets merged | Both hubs may act until one fails to renew (≤ 10s). Fencing voids the deposed hub's floors and route writes |

## Code layout

```
api/v1alpha1/          AdaptivePolicy types
cmd/plumb/             `plumb agent`, `plumb scaler`, `plumb version`
internal/core/         plan.go (the planner), model.go (/v1/systemone client), log.go (decision log)
internal/controller/   member.go (reports, local fixes, wiring), fleet.go (ClusterProfiles, quorum lock, election), hub.go, metrics.go
internal/adapters/     provider-neutral interfaces, placement simulation, Prometheus, Gateway API
internal/adapters/aws/ Karpenter v1: constants verified against source, launch-failure signals, NodePools
internal/scaler/       KEDA external scaler (externalscaler/ holds KEDA's .proto)
charts/plumb/          Helm chart; CRD and ClusterRole are generated into it
test/e2e/              two-cluster e2e on KWOK fake GPU nodes
```

Cloud-specific code stays under `internal/adapters/<provider>/`; `internal/adapters/boundary_test.go` enforces it.
