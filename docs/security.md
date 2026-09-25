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
| `nodepools` (`karpenter.sh`) | get, list, watch, update, patch | Headroom; spot → on-demand fallback in `auto` mode |
| `nodeclaims` (`karpenter.sh`) | get, list, watch | Launch failures |
| `leases` | get, list, watch, create, update, patch, delete | Per-cluster and fleet leader election |
| `clusterprofiles` | get, list, watch | Fleet membership |
| `httproutes` | get, update, patch | Traffic weights |
| `secrets` (Plumb's namespace only, Role) | get | For `kubeconfig-secretreader`-style access providers |

The **scaler** reads `adaptivepolicies` and the `plumb-hub` Lease, and nothing else.

**In other members**, the identity each access provider yields needs only the subset the hub uses: policies (read, status patch), leases in Plumb's namespace, and the HTTPRoutes it steers. Binding it to `plumb-agent` is simplest. A narrower ClusterRole with only those rules is tighter.

Plumb never creates or deletes nodes or pods, and changes NodePools only in `auto` mode, only their capacity-type requirement.

## Credentials

- **Member credentials** come from ClusterProfile access providers (KEP-5339 exec plugins), resolved per request. Prefer short-lived tokens from your cloud's identity (e.g. workload identity) over long-lived kubeconfigs in Secrets.
- **The model API key** is read from `TYPESAFE_API_KEY`. Set it from a Secret with `model.apiKeySecret`; it is never logged.

## What leaves the cluster

Only the model call, and only when a model is configured and the fleet is escalated with two or more candidates. The request carries a per-candidate summary:

- cluster name
- ready replicas
- static and dynamic room
- recent launch-failure count
- cost rank
- max replicas

No workload names, namespaces, pod specs, metrics or node names are sent.

- **Hosted Jev** (`https://api.typesafe.ai`) is called only when an API key is set.
- **Plain `http` model URLs** are refused unless the host is `localhost`, a loopback address, or an in-cluster `*.svc` name.
- **Keeping it in-house.** Serve a compatible model in-cluster, or run rules only with `model.url=""`.

## Transport

- Kubernetes API calls use each cluster's TLS and credentials.
- **The scaler's gRPC endpoint** is plaintext by default and reachable in-cluster only (ClusterIP). Set `scaler.tlsSecretName` to serve TLS, and restrict access with a NetworkPolicy that admits only KEDA's operator.
- **Metrics** (`:8080`) are plain HTTP. Scrape them in-cluster, or front them with your usual metrics authentication.

## Integrity of decisions

- **Hub election** needs a majority of members, with compare-and-swap writes on each Lease. See [architecture](architecture.md#hub-election).
- **Fencing.** Floors are served only while their writer holds the member's copy of the hub Lease, and only until they expire (5 minutes). Route weights are written only after the same check.
- **Model output is untrusted input.** Probabilities for unknown clusters or outside [0, 1] are rejected. The model can reorder candidates, but it can never change how much moves or put dynamic capacity ahead of idle static capacity.

## Supply chain

- Release images are multi-arch, distroless (`nonroot`), and published with SBOM and provenance attestations.
- Pods run non-root with a read-only root filesystem, no privilege escalation, all capabilities dropped, and the `RuntimeDefault` seccomp profile.

## Reporting a vulnerability

See [SECURITY.md](../SECURITY.md).
