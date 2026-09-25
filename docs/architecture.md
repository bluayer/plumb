# Architecture

Plumb adds a slow, deliberate fleet loop on top of the fast per-cluster loops you already run. It never replaces them.

| Loop | Time scale | Who | What |
|---|---|---|---|
| Scheduling and scaling | ms – s | kube-scheduler, KEDA/HPA, Karpenter | Place pods, scale replicas, launch nodes. Plumb does not touch these. |
| Member | seconds | `plumb agent`, in every cluster | Observe the cluster and report it |
| Fleet | tens of seconds – minutes | the elected hub (one `plumb agent`) | Move capacity and traffic between clusters |

## Scope: knobs, not mechanisms

Plumb is a control plane. It decides from metrics and SLOs, and turns the knobs other components expose. It never takes over what those components do.

| Plumb sets | The mechanism, which stays where it is |
|---|---|
| A replica floor per cluster (KEDA external scaler) | KEDA/HPA scale the Deployment; kube-scheduler places pods |
| Cluster-level HTTPRoute backend weights | Your gateway routes each request. Inside a cluster, per-request and per-pod choices (e.g. KV-cache-aware routing with the Gateway API Inference Extension's InferencePool and endpoint picker) stay with that layer |

The two routing layers work at different scales. Plumb moves a cluster's share of traffic every tens of seconds; the in-cluster layer picks a pod for every request. If Plumb tried to steer requests, the two would fight.

## Components

Each member cluster runs the same two Deployments from the Helm chart:

- **`plumb agent`** (2 replicas; one is elected per cluster through a local Lease). It has three jobs:
  - **Member reconciler.** For every `AdaptivePolicy` that lists this cluster, writes `status.report`. The report holds desired, ready and unschedulable replicas, and static and dynamic room. It also counts recent launch failures (from Karpenter informers) and records demand and saturation (from Prometheus). It computes how many replicas are missing and since when. It changes nothing in its cluster: KEDA and Karpenter already handle what a cluster can handle alone, including Karpenter trying another capacity type after a launch failure when the NodePool allows it.
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
Steady ──(a member short for `after`, or `staticAfter` if static room it would use first is idle)──▶ Escalated
Escalated ──(no member short)──▶ Recovering ──(calm for `calmFor`, floors and weights back)──▶ Steady
Recovering ──(a member short again)──▶ Escalated
```

At most one step per `cooldown`. Each step does the following:

1. **Missing replicas.**
   - Each member reports what it needs: unschedulable replicas, demand beyond its desired replicas (`ceil(demand / replicaCapacity)`), and at least `step` while saturated or slower than `latencySLO`.
   - The hub subtracts every replica it has already added for the shortage, ready or not. A short member's own need doesn't shrink when capacity appears elsewhere: its autoscaler still wants the replicas until traffic moves. So added replicas are what offset it. The count travels with the floor in the member's intent, which keeps it across failed writes and failovers.
2. **Candidates.** Fresh reports from members that are not short themselves, with headroom under `maxReplicas`, and room left.
   - Static room: replicas that fit on existing nodes.
   - Dynamic room: NodePool limits minus usage. It counts as zero after recurring launch failures.
3. **Ranking.**
   - If a model is configured and there are at least two candidates, it gets a compact per-cluster summary and one typed question: which cluster should add replicas first.
   - Its probabilities order the candidates when the top one reaches `confidenceThresholdPercent`.
   - Otherwise the rules rank: static room, fewer launch failures, cost rank, more room, name.
4. **Allocation.** Four passes over the ranking, one per **placement tier**. A candidate is *near* when its `locality` matches a short member's; clusters without a locality are all near each other.

   | Tier | `LocalityFirst` (default) | `StaticFirst` |
   |---|---|---|
   | 0 | near, static room | near, static room |
   | 1 | near, dynamic room | far, static room |
   | 2 | far, static room | near, dynamic room |
   | 3 | far, dynamic room | far, dynamic room |

   - The model can reorder clusters within a tier, never across tiers.
   - Each cluster gains at most `step` replicas per step. Floors are absolute: `max(current floor, desired) + added`.
   - Under `LocalityFirst`, only near static room shortens the wait to `staticAfter`.
   - A cluster whose report predates the last floor the hub wrote there, for any policy, takes nothing in that step. Its room does not reflect that promise yet.
   - **Shared capacity.** Every member counts other policies' floors before reporting room. Replicas promised in its cluster but not on a node yet (hub floors KEDA has not realized, pods waiting to be scheduled) are placed first in its simulation, on the nodes they would take. What doesn't fit is charged to NodePool headroom. So two policies are never offered the same GPUs.
5. **Traffic.** While any floor is held or any member is short:
   - **With `signals.pressure`** (e.g. waiting requests per replica, KV-cache usage), the hub balances pressure. Each step it moves up to `stepPercent` from the busiest cluster to the least busy one.
     - Nothing moves while the two are within 20% of each other.
     - Nothing moves until both clusters have reported *after* the previous step, so a cluster that already feels the last shift is never compared with one that doesn't yet.
     - Ready replicas are only a prior; the measured pressure corrects it, whatever the GPU type, request lengths, concurrency or KV-cache state.
   - **Without pressure**, each cluster's target share is proportional to `readyReplicas × replicaCapacity`.
   - **In both modes:**
     - A cluster over its `latencySLO` or `errorRateSLO` never gains traffic.
     - In pressure mode, such a cluster is drained first.
     - Shares stay within `[minWeight, maxWeight]`.

   Otherwise the target is the cluster's Steady `weight`.
6. **Release.** After `calmFor` in Recovering:
   - Floors drop by `step`, highest tier first: the reverse of the order they were taken. Dynamic floors go before static ones, so Karpenter consolidates what it added while reserved and existing nodes stay in use. Under `LocalityFirst`, far floors go before near ones.
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

## Outcomes

A decision record says what the hub saw and did. **Outcome records** say what happened next, joined by the same `decisionId`. After each decision the hub follows it, persisted in `status.fleet.tracking` so a new hub carries on after a failover:

- **Readiness.** For each floor it raised: when that cluster first reported that many ready replicas. This is also exported as `plumb_decision_time_to_ready_seconds`.
- **Checkpoints.** At 1, 5 and 15 minutes: every cluster's ready, desired and needed replicas, floor, weight, demand, pressure, latency and error rate.
- **Follow-ups.** Later decisions for the same policy: more capacity, another shift, a release.

Outcomes record observations, not verdicts. Demand moves on its own, so judging a decision is left to whoever reads the log, with the decision record's reports as the baseline. The same log is a dataset for evaluating a ranking model, or the policy itself, on your own traffic.

## Failure modes

| Failure | What happens |
|---|---|
| Hub process dies | Another member takes over within about one lease duration (15s) and adopts current floors from the members' intents and weights from the routes |
| Member unreachable | Its report goes stale after 2 minutes and it stops being a candidate. Its route share is held as is. Floors and weights elsewhere keep working |
| Minority of members partitioned | The majority side keeps (or elects) the hub. The minority side has none |
| No hub at all (e.g. 2-member fleet split) | Intents expire after 5 minutes and every cluster runs on its own KEDA/Karpenter behaviour. Route weights stay where they were |
| Model down, slow or unsure | Rules rank the clusters in the same step. After 3 consecutive failures the model is skipped for a minute |
| Prometheus down | The report omits the metric signals, and the policy's `Ready` condition turns false with the error. Unschedulable replicas still count. Without pressure, traffic falls back to following ready capacity |
| Metrics lag | Pressure balancing waits for reports taken after the last step. Set `cooldown` longer than the signal's lag (scrape interval plus query window), or reports will not reflect the previous shift yet |
| Policy copies differ | The hub ignores reports whose spec hash differs from its own copy |
| Two running fleets merged | Both hubs may act until one fails to renew (≤ 10s). Fencing voids the deposed hub's floors and route writes |

## Code layout

```
api/v1alpha1/          AdaptivePolicy types
cmd/plumb/             `plumb agent`, `plumb scaler`, `plumb version`
internal/core/         plan.go (the planner), model.go (/v1/systemone client), log.go (decision log)
internal/controller/   member.go (reports, reservations, wiring), fleet.go (ClusterProfiles, quorum lock, election), hub.go, metrics.go
internal/adapters/     provider-neutral interfaces, placement simulation, Prometheus, Gateway API
internal/adapters/aws/ Karpenter v1: constants verified against source, launch-failure signals, NodePools
internal/scaler/       KEDA external scaler (externalscaler/ holds KEDA's .proto)
charts/plumb/          Helm chart; CRD and ClusterRole are generated into it
test/e2e/              two-cluster e2e on KWOK fake GPU nodes
```

Cloud-specific code stays under `internal/adapters/<provider>/`; `internal/adapters/boundary_test.go` enforces it.
