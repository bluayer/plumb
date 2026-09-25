# Installation

This guide sets up a fleet of member clusters. Every step is repeated in each member unless it says otherwise.

## Prerequisites

In every member cluster:

- **Kubernetes 1.29+.**
- **KEDA 2.x**, with a `ScaledObject` for the inference Deployment.
- **Karpenter v1** (AWS) for dynamic capacity. Clusters without Karpenter still take part with static capacity only.
- **The `ClusterProfile` CRD** (`multicluster.x-k8s.io`). Your cluster manager may already install it; otherwise apply it from [cluster-inventory-api](https://github.com/kubernetes-sigs/cluster-inventory-api/tree/main/config/crd/bases).
- **Prometheus** reachable from the agent, if you use demand or saturation signals.

Fleet-wide:

- **Gateway API HTTPRoute(s)** that split traffic between the clusters, if Plumb should move traffic. They are served by a gateway implementation that can route across clusters.
- **Network access** from every member's agent to every other member's API server.

## 1. Install the chart in every member

```sh
helm install plumb oci://ghcr.io/bluayer/charts/plumb \
  --namespace plumb --create-namespace \
  --set clusterName=use1 \
  --set prometheusURL=http://prometheus.monitoring:9090
```

- **Cluster name.** `clusterName` must be unique in the fleet, and it is the name the other members use for this cluster's ClusterProfile.
- **What the chart installs:**
  - the `AdaptivePolicy` CRD
  - the agent and scaler Deployments, with PodDisruptionBudgets
  - RBAC
  - metrics Services, and optionally ServiceMonitors
- **Pinning.** Pin a chart version with `--version`. The image defaults to the chart's `appVersion`.

A single cluster works on its own: without other members it is a fleet of one, and still gets reports, metrics and decision logs.

## 2. Connect the members

Each member needs a ClusterProfile for every other member, in Plumb's namespace (`plumb` above). It also needs an access provider that turns the profile into credentials.

**Access providers** are exec plugins (KEP-5339) listed in the chart's `accessProviders`. The agent picks the first provider whose name appears in a profile's `status.accessProviders`. The plugin binary must exist in the agent container: mount it with `agent.extraVolumes` / `agent.extraVolumeMounts`, or build an image `FROM ghcr.io/bluayer/plumb` that adds it.

Example with the cluster-inventory-api [`kubeconfig-secretreader`](https://github.com/kubernetes-sigs/cluster-inventory-api/tree/main/plugins/kubeconfig-secretreader/cmd/plugin) plugin, which the e2e suite uses:

```yaml
# values.yaml
accessProviders:
  - name: kubeconfig-secretreader
    execConfig:
      apiVersion: client.authentication.k8s.io/v1
      command: /plugins/kubeconfig-secretreader
      provideClusterInfo: true
agent:
  extraVolumes: [{name: plugins, emptyDir: {}}]   # filled by an init container or a custom image
  extraVolumeMounts: [{name: plugins, mountPath: /plugins}]
```

```yaml
# In cluster use1: how to reach usw2.
apiVersion: multicluster.x-k8s.io/v1alpha1
kind: ClusterProfile
metadata:
  name: usw2
  namespace: plumb
spec:
  clusterManager: {name: my-fleet}
status:   # written by your cluster manager (status subresource)
  accessProviders:
    - name: kubeconfig-secretreader
      cluster:
        server: https://usw2.example.com
        certificate-authority-data: <base64 CA>
        extensions:
          - name: client.authentication.k8s.io/exec
            extension: {name: usw2-kubeconfig, key: kubeconfig, namespace: plumb}
```

**Remote permissions.** The identity a member uses in another member needs these permissions there:

- `get/list/watch` on `adaptivepolicies`
- `patch` on `adaptivepolicies/status`
- `get/list/watch/create/update` on `leases` in Plumb's namespace
- `get/update` on the `httproutes` it steers

The chart's `plumb-agent` ClusterRole covers all of this, so binding the remote identity to it is enough.

**Changing membership.** Add a ClusterProfile at any time and the member joins the running fleet. Add members one at a time; see [architecture](architecture.md#hub-election).

## 3. Apply the policy in every member

Apply the same `AdaptivePolicy` everywhere (GitOps is the natural fit). Start with the [sample](../config/samples/plumb_v1alpha1_adaptivepolicy.yaml), and see the [configuration reference](configuration.md) for every field.

```sh
kubectl apply -f adaptivepolicy.yaml          # in every member
kubectl get adaptivepolicies -A               # MODE, NEEDED, FLOOR, PHASE
```

## 4. Add the Plumb trigger to KEDA

In every member, add an `external-push` trigger next to your existing ones ([sample](../config/samples/keda_v1alpha1_scaledobject.yaml)):

```yaml
triggers:
  - type: prometheus      # your load-based trigger stays in charge
    metadata: {...}
  - type: external-push
    metadata:
      scalerAddress: plumb-scaler.plumb:6000
      policy: llm-inference
      policyNamespace: inference
```

KEDA takes the maximum across triggers, so Plumb can only raise replicas. The trigger reports 0 in shadow mode or when no valid floor exists. For TLS, set `scaler.tlsSecretName` and give KEDA the CA as `caCert` in a `TriggerAuthentication` referenced by the trigger (KEDA reads it from auth parameters).

## 5. Traffic (optional)

Point `spec.traffic.routes` at the HTTPRoute(s) that split traffic between clusters, and give each cluster the backendRef that reaches it (`spec.clusters[].backend`). The hub sets the weight of every matching backendRef in every rule of those routes. Other fields are left untouched.

## 6. The model (optional)

By default the agent calls TypeSafe Jev at `https://api.typesafe.ai`, but only when an API key is present:

```sh
kubectl -n plumb create secret generic typesafe --from-literal=api-key=...
helm upgrade plumb ... --set model.apiKeySecret.name=typesafe
```

- **Your own model.** Any server speaking the `/v1/systemone` protocol works: set `model.url` (in-cluster `http://….svc` is allowed) and `model.name`.
- **Rules only.** Set `model.url=""`.
- **What gets sent.** See [security](security.md#what-leaves-the-cluster).

## 7. Go live

1. Run in `shadow` mode through a few real peaks. Review `status.fleet`, the Events and the decision log ([operations](operations.md#rolling-out-auto-mode)).
2. Set `spec.mode: auto` in every copy of the policy.

## Upgrade and uninstall

```sh
helm upgrade plumb oci://ghcr.io/bluayer/charts/plumb --version <new> -n plumb --reuse-values
```

Helm does not upgrade CRDs from a chart's `crds/` directory. Apply the new CRD first; it is attached to every [GitHub release](https://github.com/bluayer/plumb/releases).

```sh
kubectl apply -f https://github.com/bluayer/plumb/releases/download/<tag>/plumb-k8s.github.io_adaptivepolicies.yaml
```

To uninstall, set every policy to `shadow` (scalers stop serving floors at once), then run `helm uninstall plumb -n plumb`. Deleting the CRD deletes all policies.
