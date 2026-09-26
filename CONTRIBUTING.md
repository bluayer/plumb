# Contributing

Thanks for helping. Taking part means following the [code of conduct](CODE_OF_CONDUCT.md). Working with an AI coding assistant? Point it at [AGENTS.md](AGENTS.md) too.

## Issues

[Search existing issues](https://github.com/bluayer/plumb/issues?q=is%3Aissue) first, then [file a new one](https://github.com/bluayer/plumb/issues/new/choose). Open an issue before any change beyond a small fix. Security vulnerabilities go through [SECURITY.md](SECURITY.md), never a public issue.

## Development

Requirements: Go (see `go.mod`), make, and for e2e one of kind, minikube or kwokctl. Every other tool (controller-gen, golangci-lint, actionlint, helm, kwokctl, protoc plugins) runs through `go tool` at the version pinned in `hack/tools/`, so there is nothing else to install.

```sh
pip install pre-commit && pre-commit install   # gofmt/goimports, golangci-lint, shellcheck, actionlint, codegen, license headers
make test          # go vet + go test -race ./...
make lint          # golangci-lint (.golangci.yaml)
make generate      # DeepCopy, CRD and ClusterRole (written into charts/plumb)
make verify-generate   # fail if DeepCopy, CRD or ClusterRole are stale
make verify-mod    # fail if go.mod or go.sum is not tidy (all three modules)
make proto         # regenerate KEDA's external scaler gRPC code (needs protoc)
make build         # bin/plumb
```

## End-to-end tests

The e2e suite runs Plumb in-process against two real, disposable clusters with the real kube-scheduler. GPU nodes are [KWOK](https://kwok.sigs.k8s.io) fake nodes, so no GPUs or cloud accounts are needed.

```sh
make e2e-up       # PROVIDER=kind (default) | minikube | kwok; creates clusters "home" and "remote"
make e2e          # about 3 minutes
make e2e-down
```

- **Setup.** `hack/e2e.sh up` installs the KWOK controller (the version in `hack/tools/go.mod`) and writes kubeconfigs to `.e2e/`. Karpenter, Gateway API and ClusterProfile CRDs are installed; their controllers are not run.
- **Permissions.** Members run as a ServiceAccount bound to the chart's generated `plumb-agent` ClusterRole, not as admin, so a permission Plumb needs but the chart lacks fails the suite. Leftovers of an earlier run (`e2e-*` namespaces, scenario nodes) are removed first.
- **Safety check.** The suite refuses to run against a context that is not `kind-*`, `kwok-*`, `minikube` or `plumb-e2e*`, unless `PLUMB_E2E_ALLOW_ANY_CLUSTER=1`.
- **Logs.** `PLUMB_E2E_LOG=1` prints the agents' logs.

| Test | What it checks |
|---|---|
| `TestStaticCapacityMatchesScheduler` | The placement simulation against the real scheduler in 10 scenarios: predicts N, then scaling to N+2 binds exactly N |
| `TestFleetEscalationToStaticCapacity` | A cluster joins a running fleet; a short member's replicas land on the other member's idle static capacity, once; the scaler serves the floor; traffic balances pressure from a fake Prometheus that answers with each cluster's share per ready replica, and holds when even; outcome records are joined to the decision; everything returns to Steady; the hub fails over |
| `TestMemberReportsLaunchFailures` | Karpenter launch failures appear in the member's report and the NodePool is never edited, even in `auto` mode; status writes stay at one per report interval |
| `TestFleetPoliciesShareCapacity` | Two policies short at once compete for 4 idle GPUs on the other member while KEDA reacts slowly: together they are promised exactly 4, and following the floors leaves nothing unschedulable |
| `TestAdaptivePlannerAndJev` | The experimental adaptive path with a scripted planner and Jev: the planner answers in the background, the plan beyond the limits is rejected, Jev's pick of the valid one becomes a floor on the other member, and no floor ever exceeds the room it reported |

## Conventions

- **Code layout.** Cloud-specific imports stay under `internal/adapters/<provider>/` (enforced by `internal/adapters/boundary_test.go`).

  ```
  api/v1alpha1/          AdaptivePolicy types
  cmd/plumb/             `plumb agent`, `plumb scaler`, `plumb suggest`, `plumb version`
  internal/core/         plan.go (the rules), adaptive.go and planner.go (the experimental adaptive path and planner registry), model.go (System One client and provider registry), provider_*.go (one file per model host), log.go (decision log)
  internal/controller/   member.go (reports, reservations, wiring), fleet.go (ClusterProfiles, quorum lock, election), hub.go, metrics.go
  internal/adapters/     provider-neutral interfaces, placement simulation, Prometheus, Gateway API
  internal/adapters/karpenter/  Karpenter v1 on any cloud; cloud providers register what their cloud adds
  internal/adapters/aws/ karpenter.go (EC2 error codes, launch reasons, node labels), bedrock.go (planner on Bedrock)
  internal/scaler/       KEDA external scaler (externalscaler/ holds KEDA's .proto)
  charts/plumb/          Helm chart; CRD and ClusterRole are generated into it
  test/e2e/              two-cluster e2e on KWOK fake GPU nodes
  ```
- **Strings from other projects.** Event reasons, labels, field paths and protocol details from Karpenter, Gateway API, cluster-inventory-api, KEDA, typesafe-sdk, the model hosts (Cloudflare, Vercel) or the AWS SDK are copied from the pinned version's source or the host's documentation, never guessed. A comment names the file or page.
- **Behaviour changes.** New behaviour works in `shadow` mode first. Every hub decision is recorded in the decision log.
- **Keep it small.** No interfaces with a single implementation, no config nobody sets, standard library first. Safety checks, validation and fencing are never simplified away.
- **License header.** Every source file carries the Apache-2.0 header (`hack/boilerplate.go.txt`).

## Model providers

The ranking model (TypeSafe Jev) is experimental: off by default, and in `shadow` mode (recorded, not used) until `--model-mode=apply`. It can be served from different hosts. Each host is a provider: one file, `internal/core/provider_<name>.go`, that registers itself:

```go
func init() {
	RegisterProvider("myhost", ProviderSpec{URL: "https://…", Model: "…", KeyEnv: "MYHOST_API_KEY",
		New: func(o ProviderOptions) (Provider, error) { return myHost(o), nil }})
}

type myHost ProviderOptions

// NewRequest builds the HTTP request for {"state", "questions"}; postJSON covers most hosts.
func (p myHost) NewRequest(ctx context.Context, e Evaluation) (*http.Request, error)
// Output returns {"model", "answers", "usage"} from a 200 response, unwrapping any envelope.
func (myHost) Output(body []byte) ([]byte, error)
```

It is then selectable with `--model-provider=myhost` (Helm: `model.provider`). A provider only handles the wire format. Timeouts, the circuit breaker, validation of the answers, the https rule and "a hosted default is only called with a key" apply to every provider unchanged. Copy the request and response shapes from the host's documentation or source, cite it in a comment, and test them against an `httptest` server as `model_test.go` does. A host that already speaks `/v1/systemone` needs no new code: use `typesafe` with `--model-url`, or give it a name and defaults by registering a spec that reuses the TypeSafe wire format, as `provider_vercel.go` does in a few lines.

### Planner providers

The experimental planner (the model proposing plans for `spec.experimental.adaptive`) plugs in the same way: one file whose `init` calls `core.RegisterPlanner`, selectable with `--planner-provider`. A planner host only carries a request: it gets a system prompt, the request (JSON) and the answer's JSON schema, and returns the model's answer as JSON. Prompt, schema, parsing, validation and the background scheduling are shared. A host that needs a cloud SDK lives under `internal/adapters/<cloud>/` (Bedrock: `internal/adapters/aws/bedrock.go`); test it against an `httptest` server as `bedrock_test.go` does.

```go
func init() { core.RegisterPlanner("myhost", core.PlannerSpec{New: newMyHost}) }

func (p *myHost) Propose(ctx context.Context, system, request string, schema map[string]any) (json.RawMessage, error)
```

### Karpenter cloud providers

NodePools, NodeClaims and their events are Karpenter core and work on every cloud (`internal/adapters/karpenter/`). A cloud provider adds two things Plumb wants to know: the error codes and launch reasons that tell a capacity shortage or a quota from a misconfiguration, and the labels it sets on its nodes on its own. Without them its launch failures all count, and pod requirements on its labels are assumed unmet. To add a cloud, write `internal/adapters/<cloud>/karpenter.go` registering them for its NodeClass group (taken from the provider's `pkg/apis`), as `internal/adapters/aws/karpenter.go` does, and import the package in `cmd/plumb/main.go`:

```go
func init() {
	karpenter.RegisterProvider(karpenter.Provider{
		Group:         "karpenter.example.com",          // nodeClassRef.group
		ICECodes:      map[string]adapters.ErrorKind{...}, // "<Code>: " in InsufficientCapacityError messages
		LaunchReasons: map[string]adapters.ErrorKind{...}, // Launched condition reasons of its CreateErrors
		WellKnownLabelPrefixes: []string{"karpenter.example.com/"},
	})
}
```

Copy every string from the provider's source at the version you support, cite the file in a comment, and test the classification as `internal/adapters/aws/karpenter_test.go` does.

## Versions

Each version lives in one place, and Dependabot proposes updates weekly ([`.github/dependabot.yml`](.github/dependabot.yml)):

| What | Where | Updated by |
|---|---|---|
| Go libraries | `go.mod` (`k8s.io/*` and `sigs.k8s.io/*` grouped: they must move together) | Dependabot |
| Dev tools | `hack/tools/go.mod` (`tool` directives); actionlint in `hack/tools/actionlint/go.mod`, since its YAML library conflicts with the others | Dependabot |
| GitHub Actions | workflows, pinned by commit SHA with the tag in a comment | Dependabot |
| Base images | `Dockerfile`, pinned by digest | Dependabot |
| Go toolchain | `go` (minimum, set by the libraries) and `toolchain` lines in the three `go.mod` files; keep the `golang` image in the `Dockerfile` on the same release. CI reads `go.mod` | By hand |
| Third-party CRDs and protocols | `test/e2e/testdata/crds/` (see its README), `internal/scaler/externalscaler/externalscaler.proto` (KEDA). Copied from a named release; re-check the strings Plumb relies on when bumping | By hand |
| pre-commit hygiene hooks | `.pre-commit-config.yaml` | `pre-commit autoupdate` |

A Kubernetes library bump may also bump controller-gen's output: run `make generate` and commit the result.

## Pull requests

`Test and lint` runs on every PR commit: unit tests, lint, generated-code checks and module checks. E2E starts after a human with write access approves the current head. Until then, the required `E2E passed` status stays pending; waiting for review is not a test failure. A new commit or revoked approval requires review again. Only the repository owner merges after both required checks pass. Helm checks and the edge image build run after merging to `main`.

For their own PR, the owner can submit a review with `Comment` and body `/approve`; GitHub records the reviewed commit. `/unapprove` revokes it. This is a review comment, not a conversation comment. The owner must still pass both required checks.

Branch and tag protection must also be configured in GitHub; `.github/rulesets/` contains the settings to apply.

- `make test lint verify-generate verify-mod` passes. PR CI runs these without waiting for approval. Approved PRs also run `make e2e-up`, `make e2e` and cleanup with `PROVIDER=kwok` and `REMOTE=1`.
- If changing the review/status scripts, run `node --test .github/scripts/pr-checks.test.cjs` (also run by CI). Node is only needed for these GitHub automation tests.
- Every commit is signed off (`git commit -s`), certifying the [DCO](DCO).
- The title is a type tag and an imperative summary, e.g. `[Bugfix] Keep reports from members out of sync`. Types: `Bugfix`, `Feature`, `Perf`, `Refactor`, `Doc`, `Test`, `CI`, `Deps`, `Misc`. Release notes come from titles.

### AI-assisted contributions

- **No pure-agent PRs.** You review every changed line, run the tests, and can defend the change.
- **No one-off busywork** (a single typo, an isolated style fix).
- **Disclose it:** say so in the PR description, and add a trailer such as `Co-authored-by: Claude` to the commits.

AI assistants read these rules, and the ones Plumb never relaxes, in [AGENTS.md](AGENTS.md).

## Releases

Maintainers push a semver tag on `main`:

```sh
git tag v0.2.0 && git push origin v0.2.0      # v0.2.0-rc.1 for a pre-release
```

Fast CI runs on every pull request commit and every push to `main`; PR E2E waits for approval. The release workflow checks that the tag is on `main`, reruns fast CI and validates the chart, then publishes:

- the multi-arch image `ghcr.io/bluayer/plumb:<version>`, with SBOM and provenance
- the Helm chart `oci://ghcr.io/bluayer/charts/plumb`, with the chart version and appVersion set to the tag
- a GitHub release with generated notes, the chart package and the CRD

Every push to `main` validates the Helm chart and publishes the edge image `ghcr.io/bluayer/plumb:main` (and `:sha-<commit>`), from the main build workflow (`edge.yaml`), the only one besides the release that may write packages. Versioned image and Helm chart publishing remains tied to release tags.

**First publish.** GHCR links both packages to this repository through `org.opencontainers.image.source` (the Dockerfile label, and `sources` in `Chart.yaml` for the chart), so the workflows' `GITHUB_TOKEN` can keep writing them. A new package may start private: after the first edge image and the first release, check each package's visibility under the owner's Packages (`plumb`, `charts/plumb`) and make it public, or have clusters pull with `imagePullSecrets`.
