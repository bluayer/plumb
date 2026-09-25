# Contributing

## Development

Requirements: Go (see `go.mod`), make, and for e2e one of kind, minikube or kwokctl.

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

- **Setup.** `hack/e2e.sh up` installs the KWOK controller and writes kubeconfigs to `.e2e/`. Karpenter, Gateway API and ClusterProfile CRDs are installed; their controllers are not run.
- **Safety check.** The suite refuses to run against a context that is not `kind-*`, `kwok-*`, `minikube` or `plumb-e2e*`, unless `PLUMB_E2E_ALLOW_ANY_CLUSTER=1`.
- **Logs.** `PLUMB_E2E_LOG=1` prints the agents' logs.

| Test | What it checks |
|---|---|
| `TestStaticCapacityMatchesScheduler` | The placement simulation against the real scheduler in 10 scenarios: predicts N, then scaling to N+2 binds exactly N |
| `TestFleetEscalationToStaticCapacity` | A cluster joins a running fleet; a short member's replicas land on the other member's idle static capacity; the scaler serves the floor; traffic follows ready replicas; everything returns to Steady; the hub fails over |
| `TestMemberSpotFallback` | Recurring spot capacity errors widen the NodePool in `auto` mode only |

## Conventions

- **Code layout.** See [docs/architecture.md](docs/architecture.md#code-layout). Cloud-specific imports stay under `internal/adapters/<provider>/` (enforced by `boundary_test.go`).
- **Strings from other projects.** Event reasons, labels, field paths and protocol details from Karpenter, Gateway API, cluster-inventory-api, KEDA or typesafe-sdk are copied from the pinned version's source, never guessed. A comment names the file.
- **Behaviour changes.** New behaviour works in `shadow` mode first. Every hub decision is recorded in the decision log.
- **Keep it small.** No interfaces with a single implementation, no config nobody sets, standard library first. Safety checks, validation and fencing are never simplified away.
- **License header.** Every source file carries the Apache-2.0 header (`hack/boilerplate.go.txt`).

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
