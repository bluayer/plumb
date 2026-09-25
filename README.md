<h1 align="center">Plumb</h1>

<p align="center">
  <strong>Adaptive scheduling for Kubernetes inference workloads.</strong><br>
  Use the capacity you already have. Adapt when provisioning falls short.
</p>

<p align="center">
  <a href="#how-it-works">How it works</a> ·
  <a href="#getting-started">Getting started</a> ·
  <a href="#development">Development</a> ·
  <a href="#current-scope">Current scope</a>
</p>

---

Plumb watches scheduling demand and capacity signals, then adjusts **Karpenter NodePools** and **KEDA replica floors**. It works alongside Kubernetes scheduling, Karpenter, and KEDA: Plumb does not create or delete nodes or pods.

> **MVP:** Shadow mode on AWS (EKS + Karpenter v1.14.1). Rules make the live decision. Optional model plugins run asynchronously in shadow and log their answers for comparison. `auto` requires an explicit opt-in.

## Why Plumb?

When a GPU workload cannot be scheduled, adding capacity is not always the right first move. Existing nodes may have room; a Spot capacity error may call for another capacity type or another region. Plumb checks static capacity first, applies guarded rule decisions, and records what happened afterward.

- **Static first.** Placement simulation checks the Deployment's hard scheduling constraints before requesting a move.
- **Work with existing controllers.** Plumb can adjust NodePool configuration and expose a replica floor to KEDA; Kubernetes and Karpenter still make the final scheduling and provisioning decisions.
- **Keep models off the critical path.** Optional local or hosted models run independently in shadow, with timeouts and circuit breakers. The scaler reads cached state only.
- **Fail without influence.** In shadow mode, or with a stale heartbeat or mismatched generation, the scaler returns 0. Existing KEDA triggers remain in control.

## How it works

```text
Karpenter events + cluster state
              │
              ▼
    capacity and placement checks
              │
              ▼
      guarded rule decision ──────► async model comparisons (shadow)
              │
              ▼
   AdaptivePolicy status + decision log
              │
              ▼
     KEDA external scaler (cached state)
```

Plumb normalizes Karpenter insufficient-capacity signals, estimates static and dynamic capacity per region, and chooses a rule action. Validation, cooldowns, relative margins, and replica floors guard the result. Each decision, shadow model response, and later outcome can be joined by `decisionId` in the JSONL log.

The detailed decision path, design principles, constraints, and repository layout are below.

## Getting started

The current repository provides a Helm chart and sample configuration under [`charts/`](charts/) and [`config/`](config/). Start with `spec.mode: shadow` (the default) to observe decisions without applying them. The model sidecar is optional; rule decisions do not wait for it.

For a local scheduling run without GPUs:

```sh
make e2e-up       # PROVIDER=kind (default) | minikube | kwok
make e2e
make e2e-down
```

The E2E suite uses the real kube-scheduler with KWOK fake GPU nodes. It does not need Karpenter itself; it installs Karpenter CRDs and creates representative NodeClaims and events. See [E2E scheduling tests](#e2e-scheduling-tests-no-gpus-needed) for setup and coverage.

## Development

```sh
make test          # go vet + go test -race ./..., pytest
make generate     # DeepCopy, CRD and RBAC, written into the chart
make proto         # regenerate Go/Python gRPC code
```

## Current scope

| Area | Current behavior |
|---|---|
| Provider | AWS adapter for EKS and Karpenter v1.14.1; adapter interfaces keep the core provider neutral |
| Decisions | Rules act; Laya, TypeSafe Jev, and custom plugins are shadow comparisons |
| Safety | Shadow is the default; auto mode is opt-in; cooldown and margin gates limit churn |
| Multi-region shifts | Raises the destination replica floor; traffic routing is handled separately |
| Capacity fit | Simulates hard pod constraints; volume/CSI topology, host ports, DRA, preemption, and soft preferences are not covered |

## Architecture and internals

## Flow

```
Karpenter NodeClaim/Event ─▶ adapters/aws (ICE normalization: capacity/quota/config/unknown)
                                     │
Deployment·Node·NodePool ─▶ adapters/aws + adapters.Fit (static/dynamic capacity, pending pods)
                                     ▼
                         controller (AdaptivePolicy reconcile)
                                     ▼
 core: Summarize (JSON ≤320 tokens) → StaticFirst → DecideICE (rules)
      → Validate → Gate (cooldown + margin) → region replica floors
      └▶ (OOD check passed) Shadow ×N → decision-service (model plugins: laya, jev, ...), shadow only, never waited on
                                     ▼
       AdaptivePolicy.status (floors, heartbeat)      decision log JSONL (decision / model / outcome)
                                     ▼
       plumb scaler (KEDA external scaler, reads the cache only)
```

| Principle | Implementation |
|---|---|
| Static capacity first | `core.StaticFirst`: if existing nodes (including reserved and non-Karpenter node groups) can take the pending demand, the result is `use_static`, with no model call and no move. `RegionScore` also ranks any region with static room above every region that would need provisioning. Static room comes from `adapters.Fit`, a placement simulation that applies the scheduler's hard constraints to the Deployment's pod template: node selector and required affinity, taints and tolerations, resources and pod count, required pod (anti-)affinity in both directions, and `DoNotSchedule` topology spread |
| Don't fight the controllers | Nodes and pods are never created or deleted. Only the NodePool capacity-type requirement (auto mode) and the status the KEDA trigger reads are changed |
| No model call on the hot path | The scaler reads only the informer cache. Model calls are async with a timeout, an in-flight limit and a circuit breaker |
| Safe without the agent | The scaler returns 0 (no influence) in shadow mode, on a stale heartbeat, or on a generation mismatch. KEDA takes the max across triggers, so the existing triggers keep working unchanged |
| Oscillation prevention | `core.Gate` (cooldown + relative margin), per-action cooldown for fallback hints, event bursts coalesced into one decision per region per reconcile |
| Shadow first | `spec.mode` defaults to `shadow`; `auto` is an explicit opt-in |

## Layout

```
api/v1alpha1/          AdaptivePolicy CRD types
cmd/plumb/             one binary: `plumb agent` (operator), `plumb scaler` (KEDA external scaler)
internal/adapters/     adapters.go (CSP-neutral interfaces), kube.go (Deployment observer, placement simulation)
internal/adapters/aws/ karpenter.go (constants verified against source, ICE normalization), capacity.go, provisioner.go
internal/core/         state.go, rules.go (rules, guardrails, gate), model.go (shadow models), log.go, engine.go
internal/controller/   reconciler + per-region adapters, event hub
internal/scaler/       external scaler; externalscaler/ holds KEDA v2.21.0's .proto and its generated code
decision-service/      Python model sidecar: plumb_decision/{server,plugins,answers}.py, decision.proto (agent ↔ sidecar API), questions/*.json
charts/plumb/          Helm chart; CRD and agent ClusterRole are generated into it
config/samples/        AdaptivePolicy and ScaledObject examples
```

The CSP import boundary is enforced by `internal/adapters/boundary_test.go`, and Go/Python question-set consistency by `TestQuestionSetMatchesPython`.

## E2E scheduling tests (no GPUs needed)

`test/e2e` runs Plumb against real API servers and the **real kube-scheduler**. GPU nodes are [KWOK](https://kwok.sigs.k8s.io) fake nodes: plain Node objects that advertise `nvidia.com/gpu` with no kubelet behind them. Everything runs on a laptop, and Karpenter is not needed (only its v1.14.1 CRDs are installed; tests create NodeClaims and `InsufficientCapacityError` events in the shapes Karpenter produces).

```sh
make e2e-up                     # PROVIDER=kind (default) | minikube | kwok
make e2e                        # ~1 min
make e2e-down
```

`hack/e2e.sh up` creates two small clusters (`home`, `remote`), installs the KWOK controller in them (rendered from the pinned Go module, no GitHub download), and writes kubeconfigs to `.e2e/`. With `PROVIDER=kwok` it uses `kwokctl` and needs no containers at all. `REMOTE=0` creates only one cluster; the multi-region test then skips. The suite refuses to run against a kubeconfig whose context is not `kind-*`, `kwok-*`, `minikube` or `plumb-e2e*` unless `PLUMB_E2E_ALLOW_ANY_CLUSTER=1`.

| Test | What it checks |
|---|---|
| `TestStaticCapacityMatchesScheduler` | For 10 scenarios (resources, multi-GPU replicas, taints and tolerations, node affinity, one-per-host anti-affinity, zone spread, existing pods' anti-affinity, pod affinity, max pods), Plumb's static capacity N equals a hand-computed expectation, **and** scaling to N+2 makes the real scheduler bind exactly N and mark 2 Unschedulable |
| `TestAgentStaticFirst` | An ICE while existing nodes have room gives `use_static`, no floors, and the capacity breakdown in the decision log |
| `TestAgentSpotFallback` | Recurring spot ICE with nowhere to shift gives `fallback_in_region`: shadow leaves the NodePool untouched, auto widens its capacity types |
| `TestAgentShiftToOtherRegion` | Two clusters: recurring ICE at home triggers a shift to the remote region with floor = replicas + pending. The scaler reports 0 in shadow and the floor in auto, and scaling the remote Deployment to that floor (as KEDA would) gets every replica scheduled |

The agent runs in the test process with the real reconciler, Hub and AWS adapters (short poll intervals). Each test uses its own namespace and node label and cleans up after itself.

## Model plugins

Every decision model sits behind a plugin in `decision-service/plumb_decision/plugins`:

| Plugin | What | Where the state goes |
|---|---|---|
| `laya` | Local Laya checkpoint (`options.model` = HF id or path) | stays in the pod |
| `jev` | TypeSafe Jev (`POST /v1/systemone`, bearer `TYPESAFE_API_KEY`, default `jev-latest`), or any server speaking that protocol, such as a self-hosted `laya-serve` (`options.base_url`) | `api.typesafe.ai` by default: **opt-in** |
| `uniform` | No model; uniform answers for wiring tests | stays in the pod |

The Jev wire format was taken from `typesafe-sdk` 0.7.1 source. The plugin uses only the standard library and refuses plain HTTP to non-local hosts unless `allow_http` is set.

Instances are configured in a JSON file (`--models-config`, format in `plumb_decision/server.py`, or `decisionService.models` in the chart). Each instance has its own plugin options and temperatures. The agent shadows every enabled instance independently (`--decision-models=laya=500ms,jev=3s`): separate timeout, in-flight limit and circuit breaker, and a separate `model` record in the decision log. A slow or failing hosted model never affects a local one or the rule decision.

To add a model, implement `ModelPlugin.predict(state, questions)` and return raw probabilities (see `plumb_decision/plugins.py`). The service validates the output against the question (labels, levels, finite values), renormalizes it and applies the instance's calibration, so a plugin cannot hand malformed answers to the agent. Reference the plugin as `"plugin": "pkg.module:Class"` or publish it under the `plumb.decision.plugins` entry point group.

## Decision log (fine-tuning data)

JSONL, joined on `decisionId`:

- `decision`: compressed input state, OOD reasons, rule proposal, guardrail results, final action (mode, applied, suppressed reason, floors)
- `model`: one per shadow instance: instance/plugin/model, answers, calibrated confidence, accepted or not, agreement with the rule
- `outcome`: pending pods, ICE count and per-region replicas after `outcomeHorizon`

## Known limits (MVP)

- The static capacity simulation (`adapters.Fit`) leaves out soft constraints (preferred affinity, `ScheduleAnyway` spread), volume/CSI topology, host ports, DRA and preemption. It evaluates a pod (anti-)affinity `namespaceSelector` as all namespaces, which can only err toward blocking more placements.
- The dynamic NodePool compatibility check covers taints and node selector/affinity only; Karpenter's own scheduler has the final say.
- A shift only raises the destination region's replica floor. Traffic weights are out of scope, so moving the actual traffic is a separate job.
- In-flight decisions awaiting an outcome record are kept in memory and are lost on restart.
- Rotating a kubeconfig secret rebuilds the region client, but the existing signal watcher keeps running until the agent restarts.
- Laya's base checkpoint is close to random zero-shot on this domain. Temperatures (per instance and question type or id, `plumb_decision/answers.py`) should be fitted after fine-tuning.
- The agent keeps the region choice at 20 options (Laya's limit) for every model, even though Jev accepts more. Plugins report their own `max_choices`, and the service enforces it.
