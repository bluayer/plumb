# Installation

This guide sets up a fleet of member clusters. Every step is repeated in each member unless it says otherwise.

## Prerequisites

In every member cluster:

- **Kubernetes 1.29+.**
- **KEDA 2.x**, with a `ScaledObject` for the inference Deployment.
- **Karpenter v1** for dynamic capacity, on any cloud (AWS's launch errors are classified; see the [FAQ](faq.md#which-clouds-are-supported)). Clusters without Karpenter still take part with static capacity only.
- **The `ClusterProfile` CRD** (`multicluster.x-k8s.io`). Your cluster manager may already install it; otherwise apply it from [cluster-inventory-api](https://github.com/kubernetes-sigs/cluster-inventory-api/tree/main/config/crd/bases).
- **Prometheus** reachable from the agent, if the policy uses `spec.signals`.

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
- **Before the first release** the chart is not on ghcr.io yet: install it from a checkout, as for `main` below.
- **Trying `main`.** Every commit on `main` is published as `ghcr.io/bluayer/plumb:main` and `:sha-<commit>`. Install the chart from a checkout with `helm install plumb ./charts/plumb --set image.tag=sha-<commit> ...`. Apply `charts/plumb/crds/` first when the CRD changed.
- **Private registry access.** If the packages are private, create a `docker-registry` Secret for `ghcr.io` and set `imagePullSecrets`.

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

Or let `plumb suggest` write a first one from what your clusters show. It only reads: the Deployment and its pods (how long they took to become ready, and how much of that was loading the model), the nodes and NodePools running them (how long new nodes took), the NodePools the pod also fits, idle room on existing nodes, the region, and the ScaledObject (its `maxReplicaCount`, and whether Plumb's trigger is there). It then asks a few questions, each with a suggested answer: interactive or batch, the TTFT objective, the vLLM model name for the signals, the placement, and each cluster's `maxReplicas`.

```sh
plumb suggest --workload llm --namespace inference \
  --cluster use1=ctx-use1 --cluster usw2=ctx-usw2 --out adaptivepolicy.yaml   # --yes takes every suggestion
```

The policy starts in `shadow`. Every value carries the reason it was chosen, what needs checking is listed on top, and what could not be seen (traffic routes, saturation and demand signals) is left as a TODO. Its timing comes from the pods and nodes running now; values from history (burst gaps for `calmFor`, capacity per replica) are defaults to tune.

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

## 6. The model (experimental, optional)

Plumb ranks clusters with rules. A model (TypeSafe Jev) can be asked as well. It is experimental, and its values and flags may change between releases. It starts in `shadow` mode: the rules decide, and the model's ranking is only recorded in the decision log, so you can compare the two on your own traffic ([how](operations.md#evaluating-the-model)) before setting `model.mode=apply`.

- **TypeSafe's API.**

  ```sh
  kubectl -n plumb create secret generic typesafe --from-literal=api-key=...
  helm upgrade plumb ... --set model.provider=typesafe --set model.apiKeySecret.name=typesafe
  ```

- **Jev on Cloudflare Workers AI.** Use an API token with Workers AI permission:

  ```sh
  kubectl -n plumb create secret generic cloudflare --from-literal=api-key=<api-token>
  helm upgrade plumb ... --set model.provider=cloudflare \
    --set model.url=https://api.cloudflare.com/client/v4/accounts/<account-id>/ai/run \
    --set model.apiKeySecret.name=cloudflare
  ```
- **Jev through Vercel AI Gateway.** With an AI Gateway API key:

  ```sh
  kubectl -n plumb create secret generic ai-gateway --from-literal=api-key=<api-key>
  helm upgrade plumb ... --set model.provider=vercel --set model.apiKeySecret.name=ai-gateway
  ```
- **Your own model.** Any server speaking the `/v1/systemone` protocol works with the `typesafe` provider: set `model.url` (in-cluster `http://….svc` is allowed) and `model.name`.
- **Another host.** Add a provider: see [CONTRIBUTING](../CONTRIBUTING.md#model-providers).
- **Rules only** is the default: `model.provider=""`.
- **What gets sent.** See [security](security.md#what-leaves-the-cluster).

### The adaptive path (experimental)

Policies with `spec.experimental.adaptive` are planned by a planner model and Jev together ([how](architecture.md#the-adaptive-path)). Set up Jev as above, then the planner. With Amazon Bedrock, give the agent's ServiceAccount a role allowed `bedrock:InvokeModel` on the model (IRSA shown; EKS Pod Identity needs no annotation):

```sh
helm upgrade plumb ... --set planner.provider=bedrock --set planner.model=<model or inference profile id> \
  --set planner.region=us-east-1 \
  --set agent.serviceAccountAnnotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::<account>:role/<role>
```

An in-cluster model serving OpenAI-compatible Chat Completions can be the planner instead. Give `planner.endpoint` its explicit `/v1` base URL; Plumb calls `/v1/chat/completions`. The model must return a JSON object with `plans` in the assistant message. Plumb includes the plan schema in the system message and validates every proposed action before it can run. An invalid or incomplete response is logged and the rules decide that step.

```sh
helm upgrade plumb ... --set planner.provider=openai --set planner.model=<served-model-id> \
  --set planner.endpoint=http://model.models.svc:8000/v1 \
  --set planner.responseFormat=json_schema
```

`planner.responseFormat=json_schema` asks a server that supports structured output to enforce the plan schema. Omit it for compatible servers that only accept basic Chat Completions fields; Plumb still checks the answer before use. Without Jev (no `model.provider`), give the policy `chooser: planner`: the planner proposes one plan, carried out once validated. To let Jev choose among several planner proposals instead, configure `model.provider` as above and keep the default `chooser: jev`. For an authenticated endpoint, create a Secret and set `planner.apiKeySecret.name` (and optionally `.key`); its value is sent as a Bearer token. An endpoint outside the cluster must use HTTPS.

Then add to the policy, in every member:

```yaml
spec:
  experimental:
    adaptive:
      intent: Interactive service. Protect TTFT first; reduce cost when there is slack.
      chooser: jev   # or planner: the planner's one plan, without Jev
      mode: shadow   # the default; apply carries out the pick (needs model.mode=apply on the agents)
  signals:
    metrics:
      - {name: ttft_p95, unit: seconds, meaning: time to first token p95, window: 2m,
         query: 'histogram_quantile(0.95, sum by (le) (rate(vllm:time_to_first_token_seconds_bucket[2m])))'}
```

Each policy starts in `mode: shadow`; see [evaluating the adaptive path](operations.md#evaluating-the-adaptive-path). To carry out its picks, set `mode: apply` on that policy and `model.mode=apply` on the agents; other policies stay as they are, on the rules or in shadow ([planner only](architecture.md#the-adaptive-path)). Another planner host: see [CONTRIBUTING](../CONTRIBUTING.md#planner-providers).

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
