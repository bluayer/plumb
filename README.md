<p align="center">
  <img src="assets/plumb-mascot.png" alt="Plumb mascot holding a plumb line" width="240">
</p>

<h1 align="center">Plumb</h1>

<p align="center">
  <a href="https://github.com/bluayer/plumb/actions/workflows/ci.yaml"><img src="https://github.com/bluayer/plumb/actions/workflows/ci.yaml/badge.svg" alt="CI"></a>
  <a href="https://github.com/bluayer/plumb/releases"><img src="https://img.shields.io/github/v/release/bluayer/plumb?include_prereleases&sort=semver" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License"></a>
</p>

<p align="center">
  <strong>Multi-cluster adaptive capacity for Kubernetes inference workloads.</strong><br>
  Use the capacity you already have. Adapt across clusters when one falls short.
</p>

<p align="center">
  <a href="docs/architecture.md">Architecture</a> ·
  <a href="docs/installation.md">Installation</a> ·
  <a href="docs/configuration.md">Configuration</a> ·
  <a href="docs/operations.md">Operations</a> ·
  <a href="docs/security.md">Security</a>
</p>

---

Plumb is a control plane for GPU inference fleets that span several Kubernetes clusters. Each cluster keeps scaling itself with **KEDA** and **Karpenter**. When one cluster can't keep up on its own, the fleet steps in:

1. It notices, from unschedulable replicas or **Prometheus** demand and saturation, and from the cluster's NodePools running out of room or failing to launch nodes.
2. It places the missing replicas in other clusters, **existing nodes before new ones**.
3. It shifts **Gateway API** traffic as that capacity becomes ready.
4. It gives everything back once the fleet is calm.

Plumb never creates or deletes nodes or pods, and never edits NodePools. It adjusts only two things your controllers already read:

- a KEDA replica floor
- HTTPRoute backend weights

## Features

- **Own cluster first, then the fleet, in the order you choose.**
  - `LocalFirst` (the default): a short cluster uses its own existing nodes, then its own NodePools; only when those cannot add nodes (limits reached, launches failing) or after `after` does it borrow other clusters' idle nodes, then their NodePools.
  - `StaticFirst`: idle existing nodes anywhere in the fleet before any cluster launches new ones.
  - When a cluster keeps failing to launch nodes, the other clusters in its region (they compete for the same cloud capacity) go last for new nodes.
  - Only the NodePools and nodes you register per cluster are Plumb's; the rest of the cluster is left alone.
  - Each workload (model) has its own timing: how long to try locally, cooldowns, and how long new replicas may take to become ready. Past that, replicas that never reached a node move elsewhere; ones still loading only raise a warning.
  - Existing nodes, reserved ones included, are found by a placement simulation that applies the scheduler's hard constraints: affinity, taints, topology spread, pod (anti-)affinity and resources. The e2e suite checks it against the real kube-scheduler.
- **Several workloads, one pool of capacity.** Floors promised to one policy but not running yet are subtracted from every other policy's room, so two workloads are never offered the same GPUs.
- **Local first, fleet second.** Each cluster keeps scaling with its own KEDA and Karpenter, including Karpenter's own fallback between capacity types. A shortage becomes a fleet decision only when the cluster's own NodePools can't fix it, or it lasts beyond a threshold you set.
- **Kubernetes-native building blocks.**
  - SIG-Multicluster **ClusterProfile** inventory, with KEP-5339 access providers
  - client-go leader election over **`coordination.k8s.io` Leases**
  - **Gateway API** weights
  - a **KEDA** external scaler
  - standard Conditions, Events and Prometheus metrics
- **Metrics and SLOs in, knobs out.** Pressure (e.g. queued requests or KV-cache usage per replica), latency and error-rate SLOs from Prometheus decide where traffic goes: shares move from busier clusters to less busy ones, and never toward a cluster over its SLO. Plumb only turns knobs (replica floors, cluster weights); per-request routing stays with your gateway and the Gateway API Inference Extension.
- **You set the limits, Plumb computes within them.** Step sizes, weight bounds, replica capacity and timing come from the policy, and deterministic rules decide where capacity goes. *Experimental:* a fast typed-decision model (TypeSafe **Jev**, served by TypeSafe, Cloudflare Workers AI, Vercel AI Gateway or any `/v1/systemone` server) can be asked to rank candidate clusters within a placement tier. It is off by default, and when enabled it starts in shadow: its pick is logged next to the rules' decision until you choose to apply it. A further experimental path lets a workload state its operating intent and metrics in plain words: a planner model (Amazon Bedrock first, other hosts as plugins) proposes plans, Plumb validates them against the limits, and Jev picks one.
- **Safe by construction.**
  - Shadow mode is the default.
  - Floors expire and are fenced to the current hub.
  - A minority partition cannot elect a hub.
  - KEDA takes the maximum across triggers.
  - Without Plumb, clusters behave as if it were not installed.
- **Auditable, with outcomes.** Every hub decision is recorded with the reports it saw, the model's answer and the plan before and after, in status, an Event and a JSONL decision log. Outcome records join it by `decisionId`: when the capacity became ready, what queues, latency and errors did at 1, 5 and 15 minutes, and what was decided next.

## How it works

```text
 every member cluster                                   the hub (one member, elected)
 ────────────────────                                   ─────────────────────────────
 Deployment · nodes · NodePools ─┐
 Karpenter launch failures ──────┼─▶ status.report ────▶ core.Plan
 Prometheus signals and SLOs ────┘   (room net of         ├─ escalate when the member's own NodePools can't help, or after `after`
                                      other policies'     ├─ rank clusters: rules (model: experimental)
                                      floors)             ├─ floors elsewhere: static room, then dynamic
                                                          ├─ weights balance pressure; never toward a cluster over SLO
 KEDA ◀── plumb scaler ◀── status.intent ◀────────────────┤
                                                          └─ after `calmFor`: release in reverse order
                                      HTTPRoute weights ◀──┘
```

Read the [architecture](docs/architecture.md) for the election, fencing and planning details.

## Quick start

Install into every member cluster, with that cluster's name:

```sh
helm install plumb oci://ghcr.io/bluayer/charts/plumb -n plumb --create-namespace \
  --set clusterName=use1 \
  --set prometheusURL=http://prometheus.monitoring:9090
```

Then:

1. Connect the members with ClusterProfiles.
2. Apply the same `AdaptivePolicy` in every member.
3. Add the Plumb trigger to each ScaledObject.

The [installation guide](docs/installation.md) walks through these steps. Policies start in `shadow` mode: watch `kubectl get adaptivepolicies -A` and the decision log, then opt in with `mode: auto`.

## Compatibility

| Component | Requirement | Verified against |
|---|---|---|
| Kubernetes | 1.29+ (CRD validation uses CEL) | e2e suite on 1.36 |
| Karpenter | v1 API (`karpenter.sh/v1`) | v1.14.1 source: CRDs, event reasons, labels |
| KEDA | 2.x, `external-push` trigger | v2.21.0 external scaler protocol |
| Gateway API | `gateway.networking.k8s.io/v1` HTTPRoute | v1.6.2 CRDs |
| ClusterProfile | `multicluster.x-k8s.io/v1alpha1` | cluster-inventory-api v0.1.3 |
| Prometheus | HTTP API `/api/v1/query` | — |
| Cloud | Any cluster with Karpenter v1; AWS's launch errors and node labels are known, other clouds' can be added ([how](CONTRIBUTING.md#karpenter-cloud-providers)). Clusters without Karpenter lend static capacity | — |

The e2e suite runs against real API servers and the real kube-scheduler, with Karpenter, Gateway API and ClusterProfile CRDs installed but not their controllers.

## Project status

Plumb's API is `v1alpha1`: fields may still change between minor releases, with notes in each release. The models are experimental: off unless configured, and free to change in any release. Run `shadow` mode first and review the decision log before enabling `auto` in production.

## Documentation

| Guide | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Components, election and fencing, planning algorithm, failure modes |
| [Installation](docs/installation.md) | Prerequisites, installing members, connecting a fleet, KEDA, Gateway API, the experimental models |
| [Configuration](docs/configuration.md) | `AdaptivePolicy` reference, chart values, command-line flags |
| [Operations](docs/operations.md) | **Checklist before you run it**, metrics, conditions, events, decision log, runbook, shadow → auto rollout, upgrades |
| [Security](docs/security.md) | RBAC, credentials, what leaves the cluster, TLS |
| [FAQ](docs/faq.md) | Common questions: what Plumb does and does not do, when to use the models |
| [Contributing](CONTRIBUTING.md) | Development, tests, e2e on KWOK, release process |

## License

[Apache License 2.0](LICENSE).
