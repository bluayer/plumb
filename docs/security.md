# Security

## Permissions

**In its own cluster**, the agent (`plumb-agent` ClusterRole, generated from the code) has these permissions:

| Resource | Verbs | Why |
|---|---|---|
| `adaptivepolicies` | get, list, watch | Read policies |
| `adaptivepolicies/status` | get, update, patch | Reports, intents, fleet status |
| `deployments`, `pods`, `nodes` | get, list, watch | Replicas and the placement simulation |
| `events` (core) | get, list, watch | Karpenter launch-failure events |
| `events` (`events.k8s.io`) | create, patch | Decision Events |
| `nodepools` (`karpenter.sh`) | get, list, watch | Dynamic room (limits minus usage) |
| `nodeclaims` (`karpenter.sh`) | get, list, watch | Launch failures |
| `leases` | get, list, watch, create, update, patch, delete | Per-cluster and fleet leader election |
| `clusterprofiles` | get, list, watch | Fleet membership |
| `httproutes` | get, update, patch | Traffic weights |
| `secrets` (Plumb's namespace only, Role) | get | For `kubeconfig-secretreader`-style access providers |

The **scaler** reads `adaptivepolicies` and the `plumb-hub` Lease, and nothing else.

**In other members**, the identity each access provider yields needs only the subset the hub uses: policies (read, status patch), leases in Plumb's namespace, and the HTTPRoutes it steers. Binding it to `plumb-agent` is simplest. A narrower ClusterRole with only those rules is tighter.

Plumb never creates or deletes nodes or pods, and never edits NodePools.

## Credentials

- **Member credentials** come from ClusterProfile access providers (KEP-5339 exec plugins), resolved per request. Prefer short-lived tokens from your cloud's identity (e.g. workload identity) over long-lived kubeconfigs in Secrets.
- **The model API key** is read from `PLUMB_MODEL_API_KEY` (else the provider's own variable). Set it from a Secret with `model.apiKeySecret`; it is never logged.
- **The planner** (experimental) uses its host's own credentials: for Bedrock, the AWS SDK's default chain, i.e. IRSA (`agent.serviceAccountAnnotations`) or EKS Pod Identity. Grant the role `bedrock:InvokeModel` on the one model it uses. For an OpenAI-compatible host, Plumb requires an explicit endpoint and optionally sends `PLUMB_PLANNER_API_KEY` as a Bearer token from `planner.apiKeySecret`; it never logs the token.

## What leaves the cluster

Nothing, by default. Data leaves only through the experimental models, and only when you configure them. Both are called only while there is something to decide (a member short, or the fleet not Steady), in `shadow` mode too.

| Call | When | What it carries |
|---|---|---|
| Ranking model (rules path) | `model.provider` and a key are set, and two or more clusters are candidates | Per candidate: cluster name, ready replicas, static and dynamic room, recent launch-failure count, cost rank, max replicas |
| Planner and Jev (adaptive path) | A policy has `experimental.adaptive`, and `planner.provider` is set, with `model.provider` for `chooser: jev` (with `chooser: planner` only the planner is called) | The policy's `intent` text and placement preference; per cluster: name, replicas, room, launch failures, floor, traffic share, and the `signals.metrics` values with their names, units and meanings; recent decisions and their outcomes; the candidate plans |

Workload names, namespaces, pod specs and node names are not sent. The `intent` and metric meanings are sent as written: keep secrets out of them.

- **Hosted Jev** (`https://api.typesafe.ai`) is called only when an API key is set. With `model.provider=cloudflare` or `vercel` the summary goes to Cloudflare Workers AI (listed as zero data retention for this model in Cloudflare's model catalog) or Vercel AI Gateway instead, again only with a key.
- **Plain `http` model URLs** are refused unless the host is `localhost`, a loopback address, or an in-cluster `*.svc` name.
- **Keeping it in-house.** Rules only is the default (`model.provider=""`); a compatible model can also be served in-cluster.

## Transport

- Kubernetes API calls use each cluster's TLS and credentials.
- **The scaler's gRPC endpoint** is plaintext by default and reachable in-cluster only (ClusterIP). Set `scaler.tlsSecretName` to serve TLS, and restrict access with a NetworkPolicy that admits only KEDA's operator.
- **Metrics** (`:8080`) are plain HTTP. Scrape them in-cluster, or front them with your usual metrics authentication.

## Integrity of decisions

- **Hub election** needs a majority of members, with compare-and-swap writes on each Lease. See [architecture](architecture.md#hub-election).
- **Fencing.** Floors are served only while their writer holds the member's copy of the hub Lease, and only until they expire (5 minutes). Route weights are written only after the same check.
- **Model output is untrusted input.** Probabilities for unknown clusters or outside [0, 1] are rejected. On the rules path the model can reorder candidates, but never change how much moves or put dynamic capacity ahead of idle static capacity. On the adaptive path every plan, whoever proposed it, is validated against `maxReplicas`, `step`, `stepPercent`, weight bounds, the cooldown and other policies' reservations before Jev may pick it, and Jev can only pick a plan it was offered.

## Supply chain

- Release images are multi-arch, distroless (`nonroot`), and published with SBOM and provenance attestations.
- Pods run non-root with a read-only root filesystem, no privilege escalation, all capabilities dropped, and the `RuntimeDefault` seccomp profile.

## Reporting a vulnerability

See [SECURITY.md](../SECURITY.md).
