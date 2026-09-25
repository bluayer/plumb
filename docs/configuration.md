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
| `placement` | `LocalityFirst` | Order of the capacity taken for a shortage: `LocalityFirst` (near static, near dynamic, far static, far dynamic) or `StaticFirst` (near static, far static, near dynamic, far dynamic). Released in reverse |
| `traffic` | — | Gateway API routes to steer. Without it, Plumb only moves capacity |
| `confidenceThresholdPercent` | `70` | Minimum model probability for its ranking to be used |

Validation rules:

- With `traffic` set, every cluster needs a `backend`.
- Every route must live in a listed cluster.

### `spec.clusters[]`

| Field | Default | Description |
|---|---|---|
| `name` | required | The member's ClusterProfile name and its agent's `--cluster-name` |
| `nodePools` | `[]` | Karpenter NodePools that may add nodes for the workload. Without them the cluster offers static capacity only |
| `nodeSelector` | — | Narrows which existing nodes count as static capacity (e.g. only reserved nodes). The pod template's own constraints always apply |
| `maxReplicas` | required | Ceiling for the floor the hub may set here |
| `costRank` | `0` | Tie-breaker; lower is preferred |
| `locality` | `""` | Clusters with the same value are near each other (e.g. a region or metro). Clusters without one are all near each other |
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

Unschedulable replicas always count, with or without signals. Signals are read, never acted on directly: Plumb turns them into floors and cluster weights only. vLLM metric names above are from vLLM's `vllm/v1/metrics/loggers.py`.

### `spec.capacity`

| Field | Default | Description |
|---|---|---|
| `replicaCapacity` | — | Demand one replica serves. Used for the demand signal and for traffic weights (share ∝ ready replicas × replica capacity). Without it, weights follow ready replicas |
| `step` | `2` | Most replicas the hub adds to or releases from one cluster per step |

### `spec.escalation`

| Field | Default | Description |
|---|---|---|
| `after` | `2m` | How long a member must be short before the fleet steps in |
| `staticAfter` | `30s` | Used instead of `after` while another member has idle static capacity |
| `calmFor` | `10m` | How long no member may be short before the hub starts giving back |
| `cooldown` | `1m` | Minimum time between two hub steps. Keep it longer than your signals' lag (scrape interval plus query window) |

### `spec.traffic`

| Field | Default | Description |
|---|---|---|
| `routes[]` | required, 1–16 | `{cluster, namespace, name}` of HTTPRoutes. The first route is read for the current weights, and all routes are written |
| `stepPercent` | `10` | Most percentage points a cluster's share moves per step |

### `status`

| Field | Writer | Description |
|---|---|---|
| `report` | member | Desired, ready and unschedulable replicas; static and dynamic room; recent launch failures; demand, saturation, pressure, latency, error rate; `neededReplicas`; `shortSince`; `specHash`; `error` |
| `intent` | hub | `replicas` floor, how many of them were `added` for a shortage elsewhere, the placement `tier` it was taken in, `hub` identity, `expires`, `decisionId` |
| `fleet` | hub (own copy) | `phase` (`Steady`, `Escalated`, `Recovering`), `phaseSince`, `lastStep`, per-cluster `clusters[]` plan (`floor`, `static`, `added`, `tier`, `weight`), `lastDecision`, and `tracking`: decisions whose outcome is still being recorded |
| `conditions` | member | `Ready`: the member observed the cluster without errors |

`kubectl get adaptivepolicies` shows MODE, NEEDED, FLOOR and PHASE.

## Helm values

| Value | Default | Description |
|---|---|---|
| `clusterName` | required | This member's name |
| `prometheusURL` | `""` | Prometheus for `spec.signals`; empty disables metric signals |
| `model.url` | `https://api.typesafe.ai` | `/v1/systemone` endpoint; `""` for rules only. Plain `http` only for in-cluster or local hosts |
| `model.name` | `jev-latest` | Model name sent with each request |
| `model.timeout` | `1s` | Per-call timeout; rules decide on expiry |
| `model.apiKeySecret.{name,key}` | `""`, `api-key` | Secret exposed as `TYPESAFE_API_KEY`. The hosted API is only called when set |
| `accessProviders` | `[]` | KEP-5339 providers used to reach other members |
| `agent.replicas` | `2` | One is elected per cluster |
| `agent.interval` | `30s` | Member report interval |
| `agent.hubInterval` | `10s` | Hub planning interval |
| `agent.decisionLog` | `-` | JSONL decision log path; `-` is stdout |
| `agent.extraVolumes` / `agent.extraVolumeMounts` | `[]` | E.g. for access-provider plugin binaries |
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
| `--model-url`, `--model`, `--model-timeout` | see Helm values | The ranking model |
| `--interval` / `--hub-interval` | `30s` / `10s` | Report and planning intervals |
| `--decision-log` | `-` | Decision log path |
| `--leader-elect` | `true` | Elect one agent per cluster |
| `--metrics-bind-address` / `--health-probe-bind-address` | `:8080` / `:8081` | Endpoints |
| `--zap-log-level`, `--zap-encoder`, … | JSON, info | Logging (controller-runtime zap flags) |

Environment: `TYPESAFE_API_KEY` (model API key), `POD_NAMESPACE`.

`plumb scaler`

| Flag | Default | Description |
|---|---|---|
| `--grpc-bind-address` | `:6000` | External scaler endpoint |
| `--tls-cert-file` / `--tls-key-file` | `""` | Serve gRPC over TLS |
| `--namespace` | as above | Where the hub Lease lives |
| `--metrics-bind-address` | `:8080` | Metrics |

`plumb version` prints the build version.
