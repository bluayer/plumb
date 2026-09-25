# Contributing

## Development

Requirements: Go (see `go.mod`), make, and for e2e one of kind, minikube or kwokctl. Every other tool (controller-gen, golangci-lint, actionlint, helm, kwokctl, protoc plugins) runs through `go tool` at the version pinned in `hack/tools/`, so there is nothing else to install.

```sh
pip install pre-commit && pre-commit install   # gofmt/goimports, golangci-lint, shellcheck, actionlint, codegen, license headers
make test          # go vet + go test -race ./...
make lint          # golangci-lint (.golangci.yaml)
make generate      # DeepCopy, CRD and ClusterRole (written into charts/plumb)
make verify-generate   # fail if DeepCopy, CRD or ClusterRole are stale
make proto         # regenerate KEDA's external scaler gRPC code (needs protoc)
make build         # bin/plumb
```

## End-to-end tests

The e2e suite runs Plumb in-process against two real, disposable clusters with the real kube-scheduler. GPU nodes are [KWOK](https://kwok.sigs.k8s.io) fake nodes, so no GPUs or cloud accounts are needed.

```sh
make e2e-up       # PROVIDER=kind (default) | minikube | kwok; creates clusters "home" and "remote"
make e2e          # about a minute
make e2e-down
```

- **Setup.** `hack/e2e.sh up` installs the KWOK controller (the version in `hack/tools/go.mod`) and writes kubeconfigs to `.e2e/`. Karpenter, Gateway API and ClusterProfile CRDs are installed; their controllers are not run.
- **Safety check.** The suite refuses to run against a context that is not `kind-*`, `kwok-*`, `minikube` or `plumb-e2e*`, unless `PLUMB_E2E_ALLOW_ANY_CLUSTER=1`.
- **Logs.** `PLUMB_E2E_LOG=1` prints the agents' logs.

| Test | What it checks |
|---|---|
| `TestStaticCapacityMatchesScheduler` | The placement simulation against the real scheduler in 10 scenarios: predicts N, then scaling to N+2 binds exactly N |
| `TestFleetEscalationToStaticCapacity` | A cluster joins a running fleet; a short member's replicas land on the other member's idle static capacity, once; the scaler serves the floor; traffic balances pressure from a fake Prometheus that answers with each cluster's share per ready replica, and holds when even; outcome records are joined to the decision; everything returns to Steady; the hub fails over |
| `TestMemberReportsLaunchFailures` | Karpenter launch failures appear in the member's report and the NodePool is never edited, even in `auto` mode; status writes stay at one per report interval |
| `TestFleetPoliciesShareCapacity` | Two policies short at once compete for 4 idle GPUs on the other member while KEDA reacts slowly: together they are promised exactly 4, and following the floors leaves nothing unschedulable |

## Conventions

- **Code layout.** See [docs/architecture.md](docs/architecture.md#code-layout). Cloud-specific imports stay under `internal/adapters/<provider>/` (enforced by `boundary_test.go`).
- **Strings from other projects.** Event reasons, labels, field paths and protocol details from Karpenter, Gateway API, cluster-inventory-api, KEDA or typesafe-sdk are copied from the pinned version's source, never guessed. A comment names the file.
- **Behaviour changes.** New behaviour works in `shadow` mode first. Every hub decision is recorded in the decision log.
- **Keep it small.** No interfaces with a single implementation, no config nobody sets, standard library first. Safety checks, validation and fencing are never simplified away.
- **License header.** Every source file carries the Apache-2.0 header (`hack/boilerplate.go.txt`).

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

Keep changes focused. `make test lint verify-generate` must pass, and CI runs the same checks plus a chart lint and an image build. Describe user-visible changes in the PR; release notes are generated from PR titles.

## Releases

Maintainers push a semver tag on `main`:

```sh
git tag v0.2.0 && git push origin v0.2.0      # v0.2.0-rc.1 for a pre-release
```

The release workflow runs CI, then publishes:

- the multi-arch image `ghcr.io/bluayer/plumb:<version>`, with SBOM and provenance
- the Helm chart `oci://ghcr.io/bluayer/charts/plumb`, with the chart version and appVersion set to the tag
- a GitHub release with generated notes, the chart package and the CRD
