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
| `nodePools` | `[]` | The Karpenter NodePools Plumb may use here: their existing nodes are static capacity, their limits dynamic capacity. Other NodePools in the cluster are ignored. Without any, the cluster cannot add nodes as far as Plumb is concerned |
| `nodeSelector` | — | Registers nodes no NodePool manages (e.g. a reserved node group) as static capacity, and narrows the NodePools' nodes to those that match. Without it, only the listed NodePools' nodes count |
| `region` | the nodes' `topology.kubernetes.io/region` | Where the cluster gets new nodes from. Clusters in one region compete for the same cloud capacity: while a member keeps failing to launch nodes, the others in its region go last for new nodes (see [placement](architecture.md#placement)). Set it where nodes lack the label, e.g. to a datacenter name |
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
| `pressure` | How loaded the cluster's replicas are; higher is busier. E.g. `avg(vllm:num_requests_waiting{model_name="llama"})` or `avg(vllm:kv_cache_usage_perc{...})`. With it, traffic balances pressure across clusters instead of following ready replicas alone |
| `latency` | The latency the service is held to, e.g. TTFT p95 in seconds: `histogram_quantile(0.95, sum by (le) (rate(vllm:time_to_first_token_seconds_bucket[2m])))` |
| `latencySLO` | Above it, the member needs at least `capacity.step` more replicas and receives no more traffic |
| `errorRate` | Fraction of failed requests, usually from your gateway's metrics |
| `errorRateSLO` | Above it, the member receives no more traffic, and gives some away when balancing pressure |
| `metrics[]` | **Experimental**, for `experimental.adaptive`: `{name, query, unit, meaning, window, maxAge}`, up to 16. Each member reports the values with the time Prometheus sampled them; the models are told the unit and meaning, and a value older than `maxAge` (default `2m`) is marked stale. The rules do not read them |

Unschedulable replicas always count, with or without signals. Signals are read, never acted on directly: Plumb turns them into floors and cluster weights only. A query returning NaN or ±Inf is an error, not a value. vLLM metric names above are from vLLM's `vllm/v1/metrics/loggers.py`.

### `spec.experimental.adaptive`

Experimental; its fields may change between releases. Takes effect only on agents run with both `--planner-provider` and `--model-provider`, or with `--planner-provider` and `--planner-only`; otherwise the policy follows the rules.

| Field | Default | Description |
|---|---|---|
| `intent` | required | What matters for this workload and which trade-offs are acceptable, in plain words (up to 1,000 characters). Sent to both models |

`maxReplicas`, `capacity.step`, `traffic.stepPercent`, `minWeight`/`maxWeight` and `escalation.cooldown` are enforced on every plan, whoever proposed it. To keep capacity or traffic out of a cluster, bound it there (`maxReplicas`, `maxWeight`). `placement` becomes a preference the models are told, not a fixed order.

### `spec.capacity`

| Field | Default | Description |
|---|---|---|
| `replicaCapacity` | — | Demand one replica serves. Used for the demand signal and for traffic weights (share ∝ ready replicas × replica capacity). Without it, weights follow ready replicas |
| `step` | `2` | Most replicas the hub adds to or releases from one cluster per step |

### `spec.escalation`

| Field | Default | Description |
|---|---|---|
| `after` | `2m` | The most a short member tries on its own (its existing nodes and NodePools) before the fleet steps in |
| `earlyAfter` | `30s` | Used instead of `after` when the short member's NodePools cannot add nodes (none listed, at their limits, or launches keep failing). With `StaticFirst`, also while another member has idle static capacity; then only that capacity is used before `after` |
| `readyTimeout` | `10m` | How long replicas the hub adds may take to become ready (node launch, image pull, model load). Past it, replicas not on a node are taken back and placed elsewhere, and that member is skipped for another `readyTimeout`; replicas on nodes but not ready only raise a `ReplicasNotReady` warning. Outcomes are followed at least this long |
| `calmFor` | `10m` | How long no member may be short before the hub starts giving back |
| `cooldown` | `1m` | Minimum time between two hub steps. Keep it longer than your signals' lag (scrape interval plus query window) |

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
| `report` | member | Desired, ready and unschedulable replicas; static and dynamic room; recent launch failures; `region` (most common `topology.kubernetes.io/region` on its nodes); demand, saturation, pressure, latency, error rate; `metrics`; `neededReplicas`; `shortSince`; `specHash`; `error` |
| `intent` | hub | `replicas` floor, how many of them were `added` for a shortage elsewhere, the placement `tier` it was taken in, `hub` identity, `expires`, `decisionId` |
| `fleet` | hub (own copy) | `phase` (`Steady`, `Escalated`, `Recovering`), `phaseSince`, `lastStep`, per-cluster `clusters[]` plan (`floor`, `static`, `added`, `tier`, `weight`, `waitingSince`: since when the floor is above ready replicas, `skippedUntil`: no floor is added there before then), `lastDecision`, `tracking`: decisions whose outcome is still being recorded, and `recent`: the last 5 complete ones (action, time to ready per cluster, how many decisions followed) |
| `conditions` | member | `Ready`: the member observed the cluster without errors |

`kubectl get adaptivepolicies` shows MODE, NEEDED, FLOOR and PHASE.

## Helm values

| Value | Default | Description |
|---|---|---|
| `clusterName` | required | This member's name |
| `prometheusURL` | `""` | Prometheus for `spec.signals`; empty disables metric signals |
| `model.provider` | `""` | **Experimental.** `""` is rules only. Otherwise who serves the ranking model: `typesafe` (TypeSafe's API, or any `/v1/systemone` server), `cloudflare` (Workers AI), `vercel` (AI Gateway) |
| `model.mode` | `shadow` | `shadow` records the model's pick in the decision log while the rules decide; `apply` carries it out (a cluster order on the rules' path, a whole plan on the adaptive path) |
| `model.url` | `""` | Endpoint; `""` is the provider's default (`https://api.typesafe.ai`, `https://ai-gateway.vercel.sh/typesafe`; `cloudflare` has none: `https://api.cloudflare.com/client/v4/accounts/<account-id>/ai/run`). Plain `http` only for in-cluster or local hosts |
| `model.name` | `""` | Model name; `""` is the provider's default (`jev-latest`, `typesafe/jev`, `typesafe-ai/jev`) |
| `model.timeout` | `1s` | Per-call timeout; rules decide on expiry |
| `model.apiKeySecret.{name,key}` | `""`, `api-key` | Secret exposed as `PLUMB_MODEL_API_KEY`: the TypeSafe or AI Gateway API key, or the Cloudflare API token. A provider's default endpoint is only called when set |
| `planner.provider` | `""` | **Experimental.** The planner for `experimental.adaptive`: `""` (none) or `bedrock` (Amazon Bedrock Converse API) |
| `planner.model` | `""` | Model id, e.g. a Bedrock model or inference profile id |
| `planner.region`, `planner.endpoint` | `""` | Overrides; for Bedrock, the AWS SDK's default chain otherwise |
| `planner.interval` | `2m` | At most one planner call per policy per interval, only while a member is short or the fleet is not Steady. Calls run in the background |
| `planner.timeout` | `1m` | Per-call timeout |
| `planner.only` | `false` | Planner only, without Jev: the planner proposes one plan, carried out once validated (recorded only while `model.mode=shadow`) |
| `accessProviders` | `[]` | KEP-5339 providers used to reach other members |
| `agent.replicas` | `2` | One is elected per cluster |
| `agent.interval` | `30s` | Member report interval |
| `agent.hubInterval` | `10s` | Hub planning interval |
| `agent.decisionLog` | `-` | JSONL decision log path; `-` is stdout |
| `agent.extraVolumes` / `agent.extraVolumeMounts` | `[]` | E.g. for access-provider plugin binaries |
| `agent.serviceAccountAnnotations` | `{}` | E.g. `eks.amazonaws.com/role-arn` for IRSA, so the agent can call Bedrock |
| `scaler.replicas` | `2` | Scaler replicas |
| `scaler.tlsSecretName` | `""` | `kubernetes.io/tls` Secret to serve gRPC over TLS |
| `serviceMonitor.enabled` | `false` | Create Prometheus Operator ServiceMonitors for both components |
| `image.repository` / `image.tag` | `ghcr.io/bluayer/plumb` / appVersion | Image |
| `*.resources`, `*.nodeSelector`, `*.tolerations`, `*.affinity`, `*.priorityClassName`, `imagePullSecrets` | — | Standard pod settings for `agent` and `scaler` |

## Command-line flags

`plumb agent`

| Flag | Default | Description |
|---|---|---|
| `--cluster-name` | required | This member's name |
| `--namespace` | `$POD_NAMESPACE`, else `plumb-system` | Plumb's namespace: ClusterProfiles and the hub Lease |
| `--clusterprofile-provider-file` | `""` | Access providers JSON (`{"providers": [...]}`); empty is a fleet of one |
| `--prometheus-url` | `""` | Prometheus for `spec.signals` |
| `--model-provider`, `--model-mode`, `--model-url`, `--model`, `--model-timeout` | see Helm values | The experimental ranking model |
| `--planner-provider`, `--planner-model`, `--planner-region`, `--planner-endpoint`, `--planner-interval`, `--planner-timeout`, `--planner-only` | see Helm values | The experimental planner |
| `--interval` / `--hub-interval` | `30s` / `10s` | Report and planning intervals |
| `--decision-log` | `-` | Decision log path |
| `--leader-elect` | `true` | Elect one agent per cluster |
| `--metrics-bind-address` / `--health-probe-bind-address` | `:8080` / `:8081` | Endpoints |
| `--zap-log-level`, `--zap-encoder`, … | JSON, info | Logging (controller-runtime zap flags) |

Environment: `PLUMB_MODEL_API_KEY` (model API key; else the provider's own `TYPESAFE_API_KEY`, `CLOUDFLARE_API_TOKEN` or `AI_GATEWAY_API_KEY`), `POD_NAMESPACE`, and for Bedrock the AWS SDK's (`AWS_REGION`, IRSA or Pod Identity).

`plumb scaler`

| Flag | Default | Description |
|---|---|---|
| `--grpc-bind-address` | `:6000` | External scaler endpoint |
| `--tls-cert-file` / `--tls-key-file` | `""` | Serve gRPC over TLS |
| `--namespace` | as above | Where the hub Lease lives |
| `--metrics-bind-address` | `:8080` | Metrics |

`plumb version` prints the build version.
