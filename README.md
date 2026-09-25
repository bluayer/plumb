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

Plumb is a control plane for GPU inference fleets that span several Kubernetes clusters. Each cluster keeps scaling itself with **KEDA** and **Karpenter**. When one cluster can't keep up, the fleet steps in:

1. It notices, from unschedulable replicas or **Prometheus** demand and saturation.
2. It places the missing replicas where capacity exists: **idle static capacity first**, then clusters whose NodePools can grow.
3. It shifts **Gateway API** traffic as that capacity becomes ready.
4. It gives everything back once the fleet is calm.

Plumb never creates or deletes nodes or pods. It adjusts only what your controllers already read:

- a KEDA replica floor
- HTTPRoute backend weights
- a NodePool's capacity types

## Features

- **Static capacity first.** Nodes you already pay for, reserved ones included, are filled fleet-wide before any cluster provisions new nodes. A placement simulation applies the scheduler's hard constraints: affinity, taints, topology spread, pod (anti-)affinity and resources. The simulation is checked against the real kube-scheduler in the e2e suite.
- **Local first, fleet second.** Each cluster handles what it can alone, e.g. falling back from spot to on-demand. Only a shortage that lasts beyond a threshold you set becomes a fleet decision.
- **Kubernetes-native building blocks.**
  - SIG-Multicluster **ClusterProfile** inventory, with KEP-5339 access providers
  - client-go leader election over **`coordination.k8s.io` Leases**
  - **Gateway API** weights
  - a **KEDA** external scaler
  - standard Conditions, Events and Prometheus metrics
- **You set the limits, Plumb computes within them.** Step sizes, weight bounds, replica capacity and timing come from the policy. A fast typed-decision model (TypeSafe **Jev** by default, or any `/v1/systemone` server) only ranks the candidate clusters. Rules take over whenever it is absent, slow or unsure.
- **Safe by construction.**
  - Shadow mode is the default.
  - Floors expire and are fenced to the current hub.
  - A minority partition cannot elect a hub.
  - KEDA takes the maximum across triggers.
  - Without Plumb, clusters behave as if it were not installed.
- **Auditable.** Every hub decision is recorded with the reports it saw, the model's answer and the plan before and after. It lands in status, an Event and a JSONL decision log.

## How it works

```text
 every member cluster                                   the hub (one member, elected)
 ────────────────────                                   ─────────────────────────────
 Deployment · nodes · NodePools ─┐
 Karpenter launch failures ──────┼─▶ status.report ────▶ core.Plan
 Prometheus demand/saturation ───┘                        ├─ escalate after `after` (`staticAfter` if static room exists)
                                                          ├─ rank clusters: model, or rules
 local fix: spot → on-demand                              ├─ floors: static room fleet-wide, then NodePools
                                                          ├─ weights ∝ ready replicas × replica capacity
 KEDA ◀── plumb scaler ◀── status.intent ◀────────────────┤
                                                          └─ after `calmFor`: release dynamic, then static
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
| Cloud | AWS (EKS + Karpenter). The core is provider-neutral; other providers need an adapter | — |

The e2e suite runs against real API servers and the real kube-scheduler, with Karpenter, Gateway API and ClusterProfile CRDs installed but not their controllers.

## Project status

Plumb's API is `v1alpha1`: fields may still change between minor releases, with notes in each release. Run `shadow` mode first and review the decision log before enabling `auto` in production.

## Documentation

| Guide | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Components, election and fencing, planning algorithm, failure modes |
| [Installation](docs/installation.md) | Prerequisites, installing members, connecting a fleet, KEDA, Gateway API, the model |
| [Configuration](docs/configuration.md) | `AdaptivePolicy` reference, chart values, command-line flags |
| [Operations](docs/operations.md) | Metrics, conditions, events, decision log, runbook, shadow → auto rollout, upgrades |
| [Security](docs/security.md) | RBAC, credentials, what leaves the cluster, TLS |
| [Contributing](CONTRIBUTING.md) | Development, tests, e2e on KWOK, release process |

## License

[Apache License 2.0](LICENSE).
