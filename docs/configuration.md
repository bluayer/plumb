# Configuration

## AdaptivePolicy

`plumb-k8s.github.io/v1alpha1`, namespaced. Apply the same spec in every member. The full schema is in the [CRD](../charts/plumb/crds/plumb-k8s.github.io_adaptivepolicies.yaml), and a complete example is in [the sample](../config/samples/plumb_v1alpha1_adaptivepolicy.yaml).

### `spec`

| Field | Default | Description |
|---|---|---|
| `mode` | `shadow` | `shadow` plans and records without changing anything. `auto` applies floors and route weights |
| `workload.name` | required | Deployment name, the same in every member |
| `workload.namespace` | policy namespace | Deployment namespace |
| `clusters[]` | required, 1–255 | Member clusters that run the workload. See below |
| `signals` | — | Prometheus queries each member evaluates locally. See below |
| `capacity` | — | How much may change per step. See below |
| `escalation` | — | When the fleet steps in and gives back. See below |
| `placement` | `LocalFirst` | Order of the capacity used for a shortage, after the short member's own existing nodes: `LocalFirst` (its own NodePools, then other members' existing nodes, then their NodePools) or `StaticFirst` (other members' existing nodes, then its own NodePools, then other members' NodePools). Released in reverse. See [placement](architecture.md#placement) |
| `traffic` | — | Gateway API routes to steer. Without it, Plumb only moves capacity |
| `experimental.adaptive` | — | **Experimental.** Hands this policy's decisions to a planner model and Jev. See below |
| `confidenceThresholdPercent` | `90` | Experimental models only: minimum probability Jev must give its pick (a ranking, or one of the adaptive path's plans) for it to be used, or in shadow recorded as accepted. The default follows [this study](https://arxiv.org/html/2609.26550v1) |

Validation rules:

- With `traffic` set, every cluster needs a `backend`.
- Every route must live in a listed cluster.

### `spec.clusters[]`

| Field | Default | Description |
|---|---|---|
| `name` | required | The member's ClusterProfile name and its agent's `--cluster-name` |
| `nodePools` | `[]` | The node pools of the cluster's node autoscaler that Plumb may use here (today Karpenter NodePools): their existing nodes are static capacity, their limits dynamic capacity. Other pools in the cluster are ignored. Without any, the cluster cannot add nodes as far as Plumb is concerned |
| `nodeSelector` | — | Registers nodes no NodePool manages (e.g. a reserved node group) as static capacity, and narrows the NodePools' nodes to those that match. Without it, only the listed NodePools' nodes count |
| `region` | the nodes' `topology.kubernetes.io/region` | Where the cluster gets new nodes from. Clusters in one region compete for the same cloud capacity: while a member keeps failing to launch nodes, or once a short member's launch failed, the others in its region go last for new nodes (see [placement](architecture.md#placement)). Set it where nodes lack the label, e.g. to a datacenter name |
| `maxReplicas` | required | Ceiling for the floor the hub may set here |
| `costRank` | `0` | Tie-breaker; lower is preferred |
| `replicaCapacity` | `spec.capacity.replicaCapacity` | Demand one replica serves here, for clusters with faster or slower GPUs |
| `backend` | — | The backendRef (`name`, optional `namespace`, `kind`) in the routes that reaches this cluster |
| `weight` | `0` | Traffic share in percent in the Steady phase |
| `minWeight` / `maxWeight` | `0` / `100` | Bounds on the share the hub may set |

### `spec.signals`

Each member evaluates these against its own Prometheus (`--prometheus-url`). Each query must return exactly one sample.

| Field | Description |
|---|---|
| `demand` | Current demand, in the unit of `replicaCapacity` (e.g. requests/s). The member needs `ceil(demand / replicaCapacity) − desiredReplicas` more replicas |
| `saturation` | A value compared with `saturationThreshold` (e.g. queued requests). While above it, the member needs at least `capacity.step` more replicas |
| `saturationThreshold` | Quantity, e.g. `"50"` |
| `pressure` | How loaded the cluster's replicas are; higher is busier. E.g. `avg(vllm:num_requests_waiting{model_name="llama"})` or `avg(vllm:kv_cache_usage_perc{...})`. With it, relief goes to the least busy member that carries its own share, no further than where both are equally busy, instead of following ready replicas alone |
| `latency` | The latency the service is held to, e.g. TTFT p95 in seconds: `histogram_quantile(0.95, sum by (le) (rate(vllm:time_to_first_token_seconds_bucket[2m])))` |
| `latencySLO` | Above it, the member needs at least `capacity.step` more replicas and receives no more traffic |
| `errorRate` | Fraction of failed requests, usually from your gateway's metrics |
| `errorRateSLO` | Above it, the member receives no more traffic, and gives traffic away |
| `metrics[]` | **Experimental**, for `experimental.adaptive`: `{name, query, unit, meaning, window, maxAge}`, up to 16. Each member reports the values with the time Prometheus sampled them; the models are told the unit and meaning, and a value older than `maxAge` (default `2m`) is marked stale. The rules do not read them |

Unschedulable replicas always count, with or without signals. Signals are read, never acted on directly: Plumb turns them into floors and cluster weights only. A query returning NaN or ±Inf is an error, not a value. vLLM metric names above are from vLLM's `vllm/v1/metrics/loggers.py`.

**Set `demand` with `capacity.replicaCapacity`.** Without them a member knows how many replicas it is short only from unschedulable replicas, plus one `capacity.step` at a time while saturated or over its latency SLO; how many replicas the load needs is never computed. Traffic weights then follow ready replicas alone (and pressure for relief). For vLLM, count the requests in flight and give the number one replica serves within the SLO:

```yaml
signals:
  demand: sum(vllm:num_requests_running{model_name="llama"}) + sum(vllm:num_requests_waiting{model_name="llama"})
  saturation: avg(vllm:num_requests_waiting{model_name="llama"})   # queued per replica
  saturationThreshold: "0.5"
  pressure: avg(vllm:num_requests_waiting{model_name="llama"})
capacity:
  replicaCapacity: "10"   # requests one replica serves at once within latencySLO; measure it per model and GPU
```

With these, a member running 1 replica with 35 requests in flight reports that it needs `ceil(35 / 10) − 1 = 3` more, rather than one `step` at a time; the hub still adds at most `capacity.step` per step.

### `spec.experimental.adaptive`

Experimental; its fields may change between releases. Each policy chooses on its own: policies without this section follow the rules, and each one with it sets its own chooser and mode, so one fleet can run some workloads on the rules and others on the adaptive path. It takes effect only on agents run with `--planner-provider`, plus `--model-provider` for `chooser: jev`; otherwise the policy follows the rules.

| Field | Default | Description |
|---|---|---|
| `intent` | required | What matters for this workload and which trade-offs are acceptable, in plain words (up to 1,000 characters). Sent to both models |
| `mode` | `shadow` | `shadow` records the adaptive pick in the decision log while the rules decide; `apply` carries it out. Agents run with `--model-mode=shadow` (Helm `model.mode`, the default) keep every policy in `shadow`, whatever it sets |
| `burst.step`, `burst.stepPercent` | the rules' | Raised limits for growing, never for giving back: a plan may add up to `burst.step` replicas to a cluster while a member is short, over its SLO or the fleet's load is climbing, and move up to `burst.stepPercent` of traffic per step away from a member that is short or over its SLO (never past where both are equally busy). Releasing replicas and bringing traffic back keep `capacity.step`, `traffic.stepPercent` and `calmFor`. Below the rules' values, the rules' apply |
| `chooser` | `jev` | `jev` picks among the planner's plans, the rules' plan and one-step changes; `planner` has the planner propose one plan, carried out once validated, without Jev ([planner only](architecture.md#the-adaptive-path)) |

`maxReplicas`, `capacity.step` and `traffic.stepPercent` (or `burst` while growing), `minWeight`/`maxWeight` and `escalation.cooldown` are enforced on every plan, whoever proposed it. While the load is climbing, a plan may add capacity before any member is short (not move traffic); what it added goes back after `calmFor`, like any floor, and capacity a plan adds restarts that calm. To keep capacity or traffic out of a cluster, bound it there (`maxReplicas`, `maxWeight`). `placement` becomes a preference the models are told, not a fixed order.

### `spec.capacity`

| Field | Default | Description |
|---|---|---|
| `replicaCapacity` | — | Demand one replica serves. Used for the demand signal and for traffic weights (share ∝ ready replicas × replica capacity). Without it, weights follow ready replicas |
| `step` | `2` | Most replicas the hub adds to or releases from one cluster per step |

### `spec.escalation`

| Field | Default | Description |
|---|---|---|
| `after` | `2m` | The most a short member tries on its own (its existing nodes and NodePools) before the fleet steps in |
| `earlyAfter` | `30s` | Used instead of `after` when the short member's NodePools cannot add nodes (none listed, at their limits, launches keep failing, or one failed and nothing is launching for its pending replicas) and it has no replicas on its nodes still becoming ready, nor pending ones that cover what it needs and will get a node of its own (`arrivingReplicas`). With `StaticFirst`, also while another member has idle static capacity; then only that capacity is used before `after` |
| `readyTimeout` | `10m` | How long replicas the hub adds may take to become ready (node launch, image pull, model load). Past it, replicas not on a node are taken back and placed elsewhere, and that member is skipped for another `readyTimeout`; replicas on nodes but not ready only raise a `ReplicasNotReady` warning. Outcomes are followed at least this long |
| `calmFor` | `10m` | How long no member may be short before the hub starts giving back, and the pace of the return: one `stepPercent` of traffic per `calmFor`. Also how long a member keeps traffic it gained before giving any away for its own shortage |
| `cooldown` | `1m` | Minimum time between two hub steps. Keep it longer than your signals' lag (scrape interval plus query window) |

Durations use Go's format (`0s`, `30s`, `2m`, `1h30m`; also `metrics[].window` and `maxAge`); anything else, or a negative one, is refused when the policy is written.

Every policy is one workload, usually one model, so each gets its own timing. Set them from what that model takes, not from the defaults:

| Model trait | Fields |
|---|---|
| Time from pending pod to ready (node launch, image pull, weights load) | `readyTimeout` above it; `after` about as long, so a member's own NodePools get the chance to finish first |
| How fast its signals react (scrape interval plus query window) | `cooldown` above it |
| How spiky its traffic is | `calmFor`, `capacity.step`, `traffic.stepPercent` |

### `spec.traffic`

| Field | Default | Description |
|---|---|---|
| `routes[]` | required, 1–16 | `{cluster, namespace, name}` of HTTPRoutes. The first route is read for the current weights, and all routes are written |
| `stepPercent` | `10` | Most percentage points a cluster's share moves per step |

### `status`

| Field | Writer | Description |
|---|---|---|
| `report` | member | Desired, ready and unschedulable replicas; `scaleDownHeld` (the workload's HPA holds replicas for its scale-down window, so as many of its unschedulable ones as it holds beyond what its metrics ask for are not counted as needed); `nominatedReplicas` (pending replicas the scheduler has placed on an existing node by preempting lower-priority pods, waiting for them to exit; not counted as needed); static and dynamic room; `recentLaunchFailures` (node launches in its pools that failed in the last 10 minutes for want of capacity or quota); `region` (most common `topology.kubernetes.io/region` on its nodes); demand, saturation, pressure, latency, error rate; `metrics`; `neededReplicas`; `shortSince`; `safePressure` (the highest pressure it served without being short, with every replica it wanted ready and every signal read, since `safeSince`; tracked for a day, then started over; the limit for returning traffic to it); `specHash` (of the spec fields the report is computed from: workload, signals, capacity, this member's `clusters[]` entry); `error` |
| `intent` | hub | `replicas` floor, how many of them were `added` for a shortage elsewhere (or so this cluster can take traffic back), the placement `tier` it was taken in (`0` existing nodes, `1` new nodes, `-1` raised for the return and released last), `hub` identity, `expires`, `decisionId` |
| `fleet` | hub (own copy) | `phase` (`Steady`, `Escalated`, `Recovering`), `phaseSince`, `lastStep`, per-cluster `clusters[]` plan (`floor`, `static`, `added`, `tier`, `weight`, `waitingSince`: since when the floor is above ready replicas, `skippedUntil`: no floor is added there before then, `gainedAt`: when it last gained traffic, which it keeps for `calmFor` unless it is over its SLO), `lastDecision`, `tracking`: decisions whose outcome is still being recorded, `recent`: the last 5 complete ones (action, time to ready per cluster, how many decisions followed), and `outOfSync`: members whose report is ignored because their copy differs |
| `conditions` | member | `Ready`: the member observed the cluster without errors |

`kubectl get adaptivepolicies` shows MODE, NEEDED, FLOOR and PHASE.

## Helm values

`charts/plumb/values.schema.json` checks values on `helm install`, `upgrade` and `template`: an unknown key (a typo), a wrong type, a duration that is not a Go duration (`30s`, `2m`), `model.mode` other than `shadow`/`apply`, or `planner.provider: openai` without `planner.endpoint` is rejected before anything is installed.

| Value | Default | Description |
|---|---|---|
| `clusterName` | required | This member's name |
| `prometheusURL` | `""` | Prometheus for `spec.signals`; empty disables metric signals |
| `model.provider` | `""` | **Experimental.** `""` is rules only. Otherwise who serves the ranking model: `typesafe` (TypeSafe's API, or any `/v1/systemone` server), `cloudflare` (Workers AI), `vercel` (AI Gateway) |
| `model.mode` | `shadow` | `shadow` records the model's pick in the decision log while the rules decide; `apply` carries it out: the ranking model's cluster order on the rules' path, and on the adaptive path the pick of each policy with `experimental.adaptive.mode: apply`. `shadow` keeps every policy recorded only |
| `model.url` | `""` | Endpoint; `""` is the provider's default (`https://api.typesafe.ai`, `https://ai-gateway.vercel.sh/typesafe`; `cloudflare` has none: `https://api.cloudflare.com/client/v4/accounts/<account-id>/ai/run`). Plain `http` only for in-cluster or local hosts |
| `model.name` | `""` | Model name; `""` is the provider's default (`jev-latest`, `typesafe/jev`, `typesafe-ai/jev`) |
| `model.timeout` | `1s` | Per-call timeout; rules decide on expiry |
| `model.apiKeySecret.{name,key}` | `""`, `api-key` | Secret exposed as `PLUMB_MODEL_API_KEY`: the TypeSafe or AI Gateway API key, or the Cloudflare API token. A provider's default endpoint is only called when set |
| `planner.provider` | `""` | **Experimental.** The planner for `experimental.adaptive`: `""` (none), `bedrock` (Amazon Bedrock Converse API), or `openai` (OpenAI-compatible Chat Completions) |
| `planner.model` | `""` | Bedrock model or inference profile id, or model id served by the OpenAI-compatible endpoint |
| `planner.region`, `planner.endpoint` | `""` | Region and endpoint override for Bedrock. `openai` requires an explicit `/v1` base URL; plain HTTP is allowed only for local or in-cluster hosts |
| `planner.responseFormat` | `""` | For `openai`: `""`/`text` uses the common Chat Completions fields; `json_schema` requests schema-constrained output if the server supports it |
| `planner.apiKeySecret.{name,key}` | `""`, `api-key` | Optional Bearer token for `openai`, exposed as `PLUMB_PLANNER_API_KEY`; in-cluster endpoints may omit it |
| `planner.interval` | `2m` | At most one planner call per policy per interval, only while a member is short, the fleet is not Steady, or its load is climbing. Calls run in the background |
| `planner.timeout` | `1m` | Per-call timeout |
| `accessProviders` | `[]` | KEP-5339 providers used to reach other members |
| `agent.replicas` | `2` | One is elected per cluster |
| `agent.interval` | `10s` | Member report interval. The hub moves traffic a step only after both members have reported since the last one, so this also paces traffic steps. Each report runs the policy's Prometheus queries and writes the policy's status once |
| `agent.hubInterval` | `10s` | Hub planning interval |
| `agent.decisionLog` | `-` | JSONL decision log path; `-` is stdout |
| `agent.extraVolumes` / `agent.extraVolumeMounts` | `[]` | E.g. for access-provider plugin binaries |
| `agent.serviceAccountAnnotations` | `{}` | E.g. `eks.amazonaws.com/role-arn` for IRSA, so the agent can call Bedrock |
| `scaler.replicas` | `2` | Scaler replicas |
| `scaler.tlsSecretName` | `""` | `kubernetes.io/tls` Secret to serve gRPC over TLS |
| `serviceMonitor.enabled` | `false` | Create Prometheus Operator ServiceMonitors for both components |
| `image.repository` / `image.tag` | `ghcr.io/bluayer/plumb` / appVersion | Image |
| `*.resources`, `*.nodeSelector`, `*.tolerations`, `*.priorityClassName`, `imagePullSecrets` | — | Standard pod settings for `agent` and `scaler` |
| `*.affinity` | `{}` | Empty: each component's replicas prefer different nodes, so one node failing does not take both down; on a single node they still schedule. Set it to replace that |

## Command-line flags

`plumb agent`

| Flag | Default | Description |
|---|---|---|
| `--cluster-name` | required | This member's name |
| `--namespace` | `$POD_NAMESPACE`, else `plumb-system` | Plumb's namespace: ClusterProfiles and the hub Lease |
| `--clusterprofile-provider-file` | `""` | Access providers JSON (`{"providers": [...]}`); empty is a fleet of one |
| `--prometheus-url` | `""` | Prometheus for `spec.signals` |
| `--model-provider`, `--model-mode`, `--model-url`, `--model`, `--model-timeout` | see Helm values | The experimental ranking model |
| `--planner-provider`, `--planner-model`, `--planner-region`, `--planner-endpoint`, `--planner-response-format`, `--planner-interval`, `--planner-timeout` | see Helm values | The experimental planner |
| `--interval` / `--hub-interval` | `10s` / `10s` | Report and planning intervals |
| `--decision-log` | `-` | Decision log path |
| `--leader-elect` | `true` | Elect one agent per cluster |
| `--metrics-bind-address` / `--health-probe-bind-address` | `:8080` / `:8081` | Endpoints |
| `--zap-log-level`, `--zap-encoder`, … | JSON, info | Logging (controller-runtime zap flags) |

Environment: `PLUMB_MODEL_API_KEY` (model API key; else the provider's own `TYPESAFE_API_KEY`, `CLOUDFLARE_API_TOKEN` or `AI_GATEWAY_API_KEY`), `PLUMB_PLANNER_API_KEY` (optional Bearer token for an OpenAI-compatible planner), `POD_NAMESPACE`, and for Bedrock the AWS SDK's (`AWS_REGION`, IRSA or Pod Identity).

`plumb suggest` (writes a first policy; see [installation](installation.md#3-apply-the-policy-in-every-member))

| Flag | Default | Description |
|---|---|---|
| `--workload` | required | The Deployment, the same in every member |
| `--namespace` | `default` | Its namespace |
| `--cluster` | required, repeated | A member: its name in the fleet, or `name=kubeconfig-context` |
| `--kubeconfig` | usual loading rules | Kubeconfig file |
| `--yes` | `false` | Take every suggested answer without asking |
| `--out` | stdout | Where to write the policy |

`plumb scaler`

| Flag | Default | Description |
|---|---|---|
| `--grpc-bind-address` | `:6000` | External scaler endpoint |
| `--tls-cert-file` / `--tls-key-file` | `""` | Serve gRPC over TLS |
| `--namespace` | as above | Where the hub Lease lives |
| `--cluster-name` | Helm `clusterName` | This member's name: a floor is served only up to its `maxReplicas` in this cluster's copy of the policy as it is now. Empty: not capped |
| `--metrics-bind-address` | `:8080` | Metrics |

`plumb version` prints the build version.
