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

## Why Plumb

Scaling inference is not scaling a web application.

- **Every replica is expensive.** One GPU node can cost more than a whole web tier, so idle or duplicated capacity shows up on the bill at once. The reserved and committed GPUs you already pay for should be used before anything new is bought.
- **GPU capacity is hard to get.** Asking for a node does not mean getting one: launches fail for lack of capacity, quotas run out, and a short region is short for everyone in it.
- **Scaling up is slow and depends on many things.** A new replica needs a node with the right GPU, an image pull and a model load that can take minutes. Scale on the same signals as a web app and capacity arrives too late, or too much of it arrives.
- **Multi-cluster is the starting point.** GPU supply, reservations and demand are spread across regions and clusters. When one cluster runs out, the answer is often another cluster, and today that call is made by a person.

Plumb makes that call. It respects the capacity each cluster already has and the autoscaling it already runs, and adapts across clusters as inference load changes. Each cluster keeps scaling itself with its own replica autoscaler and node provisioner, today **KEDA** and **Karpenter**. Plumb only needs two narrow things from them, a replica floor to set and a view of node capacity, so others can take their place. When one cluster cannot keep up on its own, the fleet steps in:

1. It notices, from unschedulable replicas or **Prometheus** demand and saturation, and from the cluster's node provisioning running out of room or failing to launch nodes.
2. It places the missing replicas in other clusters, **existing nodes before new ones**.
3. It shifts **Gateway API** traffic as that capacity becomes ready.
4. It brings traffic back slowly, only as far as the home cluster has served safely, and gives capacity back once no traffic depends on it.

Plumb never creates or deletes nodes or pods, and never edits the provisioner's configuration (NodePools). It adjusts only two things your controllers already read:

- a replica floor for the workload's autoscaler (a KEDA external scaler today)
- HTTPRoute backend weights

## How it works

```mermaid
flowchart LR
  subgraph IN["In every member cluster"]
    direction TB
    W["Deployment · nodes<br/>NodePools · Prometheus"] --> A["plumb agent"]
  end
  A -->|status.report| H{{"Hub<br/>one agent, elected<br/>by a majority"}}
  H -->|status.intent| S
  H -->|weights| R["Gateway API<br/>HTTPRoute"]
  H -.->|status.fleet · Events<br/>decision log| O(["You"])
  subgraph OUT["In every member cluster"]
    direction TB
    S["plumb scaler"] -->|replica floor| K["KEDA → Deployment"]
  end
```

- **Every cluster reports.** Each member runs `plumb agent`, which writes what its cluster has and needs (replicas, room on existing nodes and in NodePools, launch failures, Prometheus signals) to its own copy of the `AdaptivePolicy`.
- **One cluster decides.** A majority of members elect one agent as hub. It reads every report and, only when a cluster stays short, raises replica floors elsewhere and shifts route weights, within the limits the policy sets. Later it returns traffic a step at a time and gives capacity back in reverse order.
- **Nothing is hidden.** Every decision is in the hub's `status.fleet`, an Event and a JSONL decision log, with what followed at 1, 5 and 15 minutes.
- **Losing it is harmless.** Floors expire after 5 minutes and are fenced to the current hub, so without a hub every cluster runs as if Plumb were not installed.

Read the [architecture](docs/architecture.md), starting with [a shortage, start to finish](docs/architecture.md#a-shortage-start-to-finish).

## Features

- **Own cluster first, then the fleet, in the order you choose.**
  - `LocalFirst` (the default): a short cluster uses its own existing nodes, then its own NodePools; only when those cannot add nodes (none registered, limits reached, launches failing; replicas already loading on its own nodes, or getting nodes it has or is launching, get `after`) or after `after` does it borrow other clusters' idle nodes, then their NodePools.
  - `StaticFirst`: idle existing nodes anywhere in the fleet before any cluster launches new ones.
  - When a cluster keeps failing to launch nodes, the other clusters in its region (they compete for the same cloud capacity) go last for new nodes.
  - Only the NodePools and nodes you register per cluster are Plumb's; the rest of the cluster is left alone.
  - Each workload (model) has its own timing: how long to try locally, cooldowns, and how long new replicas may take to become ready. Past that, replicas that never reached a node move elsewhere; ones still loading only raise a warning.
  - Existing nodes, reserved ones included, are found by a placement simulation that applies the scheduler's hard constraints: affinity, taints, topology spread, pod (anti-)affinity and resources. The e2e suite checks it against the real kube-scheduler.
- **Several workloads, one pool of capacity.** Floors promised to one policy but not running yet are subtracted from every other policy's room, so two workloads are never offered the same GPUs.
- **Local first, fleet second.** Each cluster keeps scaling with its own autoscaler and provisioner (KEDA and Karpenter today), including the provisioner's own fallback between capacity types. A shortage becomes a fleet decision only when the cluster's own NodePools can't fix it, or it lasts beyond a threshold you set.
- **Kubernetes-native building blocks.**
  - SIG-Multicluster **ClusterProfile** inventory, with KEP-5339 access providers
  - client-go leader election over **`coordination.k8s.io` Leases**
  - **Gateway API** weights
  - a **KEDA** external scaler
  - standard Conditions, Events and Prometheus metrics
- **Metrics and SLOs in, knobs out.** Pressure (e.g. queued requests or KV-cache usage per replica), latency and error-rate SLOs from Prometheus decide where traffic goes: it moves only away from a cluster that cannot carry it (short, over its SLO), to one that carries its own share, and never toward a cluster over its SLO. Traffic that is served well stays where it is; moving it back is slow and bounded by what the home cluster has served safely, and it never bounces between clusters that are both out of room. Plumb only turns knobs (replica floors, cluster weights); per-request routing stays with your gateway and the Gateway API Inference Extension.
- **You set the limits, Plumb computes within them.** Step sizes, weight bounds, replica capacity and timing come from the policy, and deterministic rules decide where capacity goes. *Experimental:* a fast typed-decision model (TypeSafe **Jev**, served by TypeSafe, Cloudflare Workers AI, Vercel AI Gateway or any `/v1/systemone` server) can be asked to rank candidate clusters within a placement tier. It is off by default, and when enabled it starts in shadow: its pick is logged next to the rules' decision until you choose to apply it. A further experimental path lets a workload state its operating intent and metrics in plain words: a planner model (Amazon Bedrock or any OpenAI-compatible Chat Completions endpoint; other hosts as plugins) proposes plans, Plumb validates them against the limits, and Jev picks one.
- **Safe by construction.**
  - Shadow mode is the default.
  - Floors expire and are fenced to the current hub.
  - A minority partition cannot elect a hub.
  - KEDA takes the maximum across triggers.
  - Without Plumb, clusters behave as if it were not installed.
- **Auditable, with outcomes.** Every hub decision is recorded with the reports it saw, the model's answer and the plan before and after, in status, an Event and a JSONL decision log. Outcome records join it by `decisionId`: when the capacity became ready, what queues, latency and errors did at 1, 5 and 15 minutes, and what was decided next.

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

The e2e suite runs against three real API servers and the real kube-scheduler; KEDA, Karpenter and traffic are played by the test harness. Fleet simulations check the planner against invariants over hours of simulated time.

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
| [Contributing](CONTRIBUTING.md) | Issues, pull requests, DCO, AI-assisted contributions, development, tests, releases. AI assistants: [AGENTS.md](AGENTS.md) |

## License

[Apache License 2.0](LICENSE).
