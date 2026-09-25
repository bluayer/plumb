# Operations

## What to watch

```sh
kubectl get adaptivepolicies -A                          # MODE, NEEDED, FLOOR, PHASE per member
kubectl get lease plumb-hub -n plumb -o jsonpath='{.spec.holderIdentity}'   # current hub: <cluster>/<pod>
kubectl get events -n <ns> --field-selector involvedObject.kind=AdaptivePolicy
```

### Metrics

Both components serve Prometheus metrics on `:8080/metrics`: controller-runtime's standard metrics plus Plumb's own. Set `serviceMonitor.enabled=true` to scrape them with the Prometheus Operator.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `plumb_hub_leading` | gauge | — | 1 while this agent is the fleet hub |
| `plumb_fleet_members` | gauge | — | Members this agent counts in the election, itself included |
| `plumb_fleet_escalated` | gauge | `policy` | 1 while the policy's fleet is escalated (hub only) |
| `plumb_hub_decisions_total` | counter | `policy`, `action`, `source`, `applied` | Hub decisions that changed something |
| `plumb_hub_step_errors_total` | counter | `policy` | Hub steps that failed to read or write a member or route |
| `plumb_model_request_duration_seconds` | histogram | `result` | Model ranking calls (`ok`, `error`) |
| `plumb_member_needed_replicas` | gauge | `policy` | Replicas this cluster needs beyond what it runs |
| `plumb_member_static_room_replicas` | gauge | `policy` | Replicas that still fit on this cluster's existing nodes |

Suggested alerts:

```yaml
- alert: PlumbNoHub                # no member leads the fleet
  expr: max(plumb_hub_leading) == 0     # needs every member's agents in one Prometheus (or federation)
  for: 5m
- alert: PlumbHubStepErrors
  expr: increase(plumb_hub_step_errors_total[10m]) > 5
- alert: PlumbEscalatedTooLong     # the fleet could not absorb the shortage
  expr: min_over_time(plumb_fleet_escalated[30m]) == 1
- alert: PlumbModelErrors
  expr: sum(rate(plumb_model_request_duration_seconds_count{result="error"}[10m])) > 0
  for: 15m
```

### Conditions and Events

- **`Ready`** on each member's copy: `True`/`Observed` when the last report succeeded, `False`/`ObservationFailed` with the error otherwise (e.g. Prometheus unreachable), `False`/`NotAMember` when the cluster is not in `spec.clusters`.
- **Hub decisions** are Events on the hub's copy of the policy, with reason `AddCapacity`, `ShiftTraffic`, `ReleaseCapacity` or a combination. They are `Warning` when applying failed.
- **Local fixes** are Events on the member's copy with reason `FallbackInCluster`.

### Decision log

Each hub decision that changes something is one JSON line (`--decision-log`, stdout by default):

| Field | Description |
|---|---|
| `decisionId`, `time`, `policy`, `hub`, `mode` | Identity |
| `reports` | Every member's report the hub used (`null` for missing, stale or divergent members) |
| `before`, `after` | Per-cluster floor, static flag and weight, before and after the step |
| `phase`, `action`, `source`, `message` | The decision (`source`: `model` or `rule`) |
| `model` | The model's probabilities, whether they were accepted, and any error |
| `applied`, `error` | Whether it was written (auto mode) and why not |

Ship it with your log pipeline. It is also the dataset for evaluating or fine-tuning a ranking model on your own traffic.

## Rolling out auto mode

1. Run `shadow` for at least a few real peaks. In shadow mode `status.fleet` simulates floors and weights forward, so the plan unfolds as it would have.
2. For each escalation in the decision log, check the following:
   - Did it start when you would have acted (`after`, `staticAfter`)?
   - Did the capacity go where you expect: static room first, then the right NodePools?
   - Would the weights have moved at a pace your gateway and model servers tolerate (`stepPercent`, `cooldown`)?
   - Did recovery start late enough (`calmFor`)?
3. Tune the policy and repeat. Then set `mode: auto` in every copy.
4. Watch `plumb_hub_decisions_total{applied="true"}` and the Events during the first peaks.

To stop acting immediately, set `mode: shadow`. Scalers stop serving floors at once, and the hub stops writing weights. Current route weights stay; set them back by hand if needed.

## Runbook

**No hub** (`plumb_hub_leading` is 0 everywhere; the agents log `members reachable, need N`)

A majority of members must be reachable from the candidate.

1. Check `plumb_fleet_members` against the ClusterProfiles.
2. Check the access providers' credentials, and network access to the peers' API servers.
3. Clusters keep scaling on their own meanwhile. Floors lapse after 5 minutes.

**A member is ignored by the hub** (its `reports` entry is `null` in the decision log)

Its report is missing, older than 2 minutes, or computed from a different spec:

1. Compare `status.report.specHash` across members and re-sync the policy.
2. Check that member's agent and its `Ready` condition.

**Floor set but replicas not rising**

1. The scaler serves the floor only in `auto` mode, before `status.intent.expires`, and while `status.intent.hub` matches the member's `plumb-hub` Lease holder.
2. Check that the ScaledObject has the `external-push` trigger and KEDA can reach `plumb-scaler`.
3. Check that `maxReplicaCount` is not below the floor.

**Escalated but nothing added** (`message: N replicas short and no other cluster has room`)

1. Every other member is at `maxReplicas`, has no static room, and has no NodePool headroom (or recurring launch failures void it).
2. Raise `maxReplicas` or NodePool limits, or add a member.

**Weights not moving**

1. Weights follow *ready* replicas: check the helper clusters' readiness.
2. Look for `does not hold the hub lease` errors (fencing during a hub change) and for route update errors in `plumb_hub_step_errors_total` and the Events.

**Model errors or timeouts**

Nothing breaks: rules rank. After 3 consecutive failures the model is skipped for a minute. Check the API key, `--model-url` reachability and `--model-timeout`.

## Upgrades

- Upgrade one member at a time. Mixed versions within a minor release are supported.
- Apply the release's CRD before upgrading the chart; Helm does not upgrade CRDs.
- An agent restart hands the per-cluster Lease over immediately (released on shutdown). A hub restart hands the fleet over within one lease duration.
