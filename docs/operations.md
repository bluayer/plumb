# Operations

## Checklist: know these before you run it

Each item says what goes wrong when it is off. Check them for every workload (policy) and every member.

**Scope: what Plumb counts**

- [ ] **`clusters[].nodePools` and `nodeSelector` list exactly where the pods can run.** Only registered NodePools and nodes count as room. Missing one: Plumb under-counts the cluster's room, so its shortages go to other clusters too early and it is offered less as a helper. Pods that can also land outside the registered ones make the counts wrong the other way.
- [ ] **`clusters[].maxReplicas` is what you are willing to pay for there.** It is the only ceiling on the floor the hub raises when another cluster is short.
- [ ] **Each member has a region**: nodes labelled `topology.kubernetes.io/region` (EKS does this), or `clusters[].region` set. Without it, clusters sharing cloud capacity are not recognized as such.

**Timing: set per model, not left at the defaults**

- [ ] **`escalation.after` ≥ the time from pending pod to ready in the cluster itself** (node launch, image pull, model load). Too short: shortages the cluster would have solved alone move to other clusters.
- [ ] **`escalation.readyTimeout` > the model's worst normal load time.** Too short: replicas still loading raise `ReplicasNotReady`, and replicas waiting for a node are taken back before they arrive.
- [ ] **`escalation.cooldown` > your signals' lag** (scrape interval + query window). Too short: each step reacts to a state that does not show the last one yet, and floors and weights overshoot.
- [ ] **`escalation.calmFor` longer than the gaps between your traffic bursts.** Too short: capacity is given back just before the next burst.

**Signals**

- [ ] **Every query is scoped to the one model** (e.g. vLLM's `model_name` label). A query over all models makes one model's load look like another's.
- [ ] **Queries return exactly one sample**, from the member's own Prometheus (`--prometheus-url`). A failing query shows in the `Ready` condition; that signal is then ignored.
- [ ] **`latencySLO` / `errorRateSLO` are set** if traffic is managed: a cluster over them never gains traffic.

**Outside Plumb**

- [ ] **The ScaledObject has the Plumb `external-push` trigger, and `maxReplicaCount` ≥ any floor you allow** (`maxReplicas`). Otherwise floors are written and nothing scales.
- [ ] **Every member reaches a majority of members** (ClusterProfiles, access providers, network). Without a majority there is no hub: clusters keep scaling on their own and floors lapse after 5 minutes.
- [ ] **The same policy is applied, unchanged, in every member.** A member whose copy differs reports a different `specHash` and is ignored by the hub.

**Going live**

- [ ] **Stay in `mode: shadow` through a few real peaks** and read the decision log before `auto` ([rolling out auto mode](#rolling-out-auto-mode)).
- [ ] **Alert on** `plumb_hub_leading` being 0 everywhere, `plumb_hub_step_errors_total` rising, and `ReplicasNotReady` events.

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
| `plumb_planner_request_duration_seconds` | histogram | `result` | Experimental planner calls (`ok`, `error`) |
| `plumb_decision_time_to_ready_seconds` | histogram | `policy` | From a decision raising a floor to the cluster having that many ready replicas |
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
- alert: PlumbPlannerErrors        # adaptive path only
  expr: sum(increase(plumb_planner_request_duration_seconds_count{result="error"}[30m])) > 3
```

### Conditions and Events

- **`Ready`** on each member's copy: `True`/`Observed` when the last report succeeded, `False`/`ObservationFailed` with the error otherwise (e.g. Prometheus unreachable), `False`/`NotAMember` when the cluster is not in `spec.clusters`.
- **Hub decisions** are Events on the hub's copy of the policy, with reason `AddCapacity`, `ShiftTraffic`, `ReleaseCapacity` or a combination. They are `Warning` when applying failed.
- **`ReplicasNotReady`** (`Warning`, hub's copy): replicas the hub added are on nodes but still not ready after `readyTimeout`. The floor stays; see the runbook.

### Decision log

Each hub decision that changes something is one JSON line (`--decision-log`, stdout by default), `kind: decision`:

| Field | Description |
|---|---|
| `kind`, `decisionId`, `time`, `policy`, `hub`, `mode` | Identity |
| `reports` | Every member's report the hub used (`null` for missing, stale or divergent members) |
| `before`, `after` | Per-cluster floor, static flag and weight, before and after the step |
| `phase`, `action`, `source`, `message` | The decision (`source`: `model` or `rule`; on the adaptive path the candidate that ran: `planner`, `rules`, `hold` or `enumerated`) |
| `model` | Experimental model only: its probabilities, whether they were confident enough to be used (`accepted`), `shadow` when only recorded, and any error |
| `adaptive` | Adaptive path only: candidates, rejections, Jev's probabilities, `chosen` and `executed` ([below](#evaluating-the-adaptive-path)) |
| `applied`, `error` | Whether it was written (auto mode) and why not |

Each decision is followed by `kind: outcome` lines with the same `decisionId`, at 1, 5 and 15 minutes ([why](architecture.md#outcomes)):

| Field | Description |
|---|---|
| `afterSeconds`, `final` | Which checkpoint |
| `clusters.<name>` | `ready`, `desired`, `needed`, `floor`, `weight`, and `demand`, `pressure`, `latency`, `errorRate` when configured; `stale` when the member did not report |
| `readyAfterSeconds.<name>` | For each floor the decision raised: how long until that many replicas were ready (one-second resolution); absent while they are not |
| `followedBy` | Later decisions for the same policy, `"<decisionId> <action>"` |

For example, to see how long capacity took and what the queues did after a shift:

```sh
jq -c 'select(.kind=="outcome" and .final) | {decisionId, readyAfterSeconds, pressure: (.clusters | map_values(.pressure))}' decisions.jsonl
```

Ship the log with your log pipeline. It is the dataset for evaluating the policy, or a ranking model, on your own traffic.

### Evaluating the model

With `model.mode=shadow`, every capacity decision taken while two or more clusters were candidates carries the model's ranking next to what the rules did. Where the model would have chosen differently:

```sh
jq -c 'select(.kind=="decision" and .model.shadow and .model.accepted)
  | {decisionId, rules: .message, model: (.model.probabilities | to_entries | max_by(.value) | .key)}' decisions.jsonl
```

Then compare the outcome lines of those decisions (`readyAfterSeconds`, pressure, follow-up decisions). Consider `model.mode=apply` only once the model's choices would have done at least as well as the rules' over several peaks.

### Evaluating the adaptive path

For policies with `experimental.adaptive`, decision lines carry `adaptive`: every candidate (`source`: `planner`, `rules`, `hold`, `enumerated`), why others were rejected, Jev's probabilities, who picked (`chooser`: `jev`, or `planner` with `planner.only`), the pick (`chosen`) and what ran (`executed`). Planner answers are `proposal` lines. Where Jev's pick differed from the rules':

```sh
jq -c 'select(.kind=="decision" and .adaptive.chosen and .adaptive.chosen != "rules") | .adaptive as $a
  | {decisionId, chosen: $a.chosen, executed: $a.executed, plan: ($a.candidates[] | select(.id == $a.chosen))}' decisions.jsonl
jq -c 'select(.kind=="proposal") | {time, policy, seconds, error, plans: [.candidates[]? | {id, actions, hypothesis}]}' decisions.jsonl
```

Shadow logs show how the models decide: whether a different intent or a different state changes the pick, whether a slow or failed earlier action is taken into account, and that nothing outside the limits is ever offered. They cannot show that a pick would have done better, since what was not carried out is never observed. For that, run the same load pattern in alternating time blocks with the rules and with `model.mode=apply`, and compare time over the target metric, time to recover, and cost.

## Rolling out auto mode

1. Run `shadow` for at least a few real peaks. In shadow mode `status.fleet` simulates floors and weights forward, so the plan unfolds as it would have.
2. For each escalation in the decision log, check the following:
   - Did it start when you would have acted (`earlyAfter` once the member's NodePools could not help, `after` otherwise)?
   - Did the short member get to use its own NodePools first (under `LocalFirst`), and did the capacity elsewhere go where you expect: static room first, then the right NodePools?
   - Would the weights have moved at a pace your gateway and model servers tolerate (`stepPercent`, `cooldown`)?
   - In the outcome lines that follow each decision: how long did capacity take (`readyAfterSeconds`), and did pressure, latency and errors settle, or did more decisions follow?
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

**Replicas added elsewhere never became ready**

- *Decision message `-N on <cluster>: not on a node after <readyTimeout>`*: the pods were not created or stayed unschedulable. The hub took the floor back, placed the shortage elsewhere and skips that member for another `readyTimeout` (`status.fleet.clusters[].skippedUntil`). Check its registered NodePools (limits, launch failures) and that the pod template fits its registered nodes.
- *`ReplicasNotReady` warning*: the pods are on nodes but not ready. Usually a model still loading: if this is normal for the model, raise `readyTimeout` (and `after`). Otherwise check the pods' readiness probes and logs.

**Static room is 0 although the cluster has idle nodes**

Only registered nodes count: the listed `nodePools`' nodes, and nodes no NodePool manages that match `nodeSelector`. The `filtered` count under the static diagnostics in the agent's logs shows how many nodes were left out.

**Weights not moving**

1. Weights follow *ready* replicas: check the helper clusters' readiness.
2. Look for `does not hold the hub lease` errors (fencing during a hub change) and for route update errors in `plumb_hub_step_errors_total` and the Events.

**Model errors or timeouts**

Nothing breaks: rules rank. After 3 consecutive failures the model is skipped for a minute. Check the API key, `--model-url` reachability and `--model-timeout`.

**Planner errors** (adaptive path; `proposal` lines with `error`)

Steps go on without the planner's plans. Check the role (`bedrock:InvokeModel` on the model, IRSA or Pod Identity), `planner.model` and `planner.region`, and `planner.timeout`. A planner answer that does not follow the schema is an error too.

## Upgrades

- Upgrade one member at a time. Mixed versions within a minor release are supported.
- Start using new policy fields only once every member runs a version that knows them: the hub ignores members whose copy of the spec hashes differently.
- Apply the release's CRD before upgrading the chart; Helm does not upgrade CRDs.
- An agent restart hands the per-cluster Lease over immediately (released on shutdown). A hub restart hands the fleet over within one lease duration.
