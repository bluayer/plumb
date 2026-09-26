# Agent Instructions for Plumb

> These instructions apply to **all** AI-assisted contributions to `bluayer/plumb`.
> Plumb moves production GPU capacity and traffic across clusters. The rules below are not optional.

## 1. Contribution policy (mandatory)

- **Check for duplicates first.** If an open issue or PR already covers the change, do not open another.
  ```bash
  gh issue view <number> --repo bluayer/plumb --comments
  gh pr list --repo bluayer/plumb --state open --search "<keywords>"
  ```
- **No busywork PRs** (a single typo, an isolated style fix).
- **No pure-agent PRs.** A human reviews every changed line, runs the tests, and can defend the change.
- **The PR description states** that AI was used, why it is not a duplicate, and the test commands run with their results.
- **Fail closed.** If the work is a duplicate, busywork, or breaks a rule in section 3, do not proceed; say what is missing or which rule it breaks.

## 2. Development workflow

Go is the only requirement; every other tool runs through `go tool` at the version pinned in `hack/tools/`. Never install tools globally.

```bash
make test              # go vet + go test -race ./...
make lint              # golangci-lint, actionlint
make generate          # after changing api/: DeepCopy, CRD, ClusterRole
make verify-generate verify-mod
make e2e-up PROVIDER=kwok && make e2e   # when what a member reports or the hub decides or writes changes
```

- A bug fix comes with a test that fails without it.
- Test at the cheapest level: `internal/core` (pure planning), then `internal/controller` (fake clients), then e2e.
- Match the surrounding code. No interface with one implementation, no field nobody reads, standard library first.
- Commits are signed off (`git commit -s`) and carry an attribution trailer (`Co-authored-by: <agent>`). Titles: `[Type] Imperative summary` ([types](CONTRIBUTING.md#pull-requests)).

## 3. Rules that are never relaxed

- **Knobs only.** Never create or delete nodes or pods, never edit NodePools. Plumb writes a replica floor (via its KEDA scaler) and HTTPRoute weights, nothing else.
- **Guardrails stay.** Intent expiry, hub fencing, quorum election, the policy's limits, plan validation: never weakened to simplify code.
- **Shadow first.** New behavior works in `shadow` mode; `auto` is always opt-in.
- **Everything is recorded.** Every hub decision goes to the decision log: inputs, model output, plan before and after, applied or not.
- **No guessed strings.** Strings from Karpenter, Gateway API, ClusterProfile, KEDA or model hosts are copied from the pinned source, with a comment naming it.
- **Cloud code stays in `internal/adapters/<cloud>/`.**
- **Nothing leaves the cluster by default.** A model call that sends cluster state out needs an explicit key or URL.

## Read before changing

| Area | Guide |
|---|---|
| Planning, placement, traffic, election | [docs/architecture.md](docs/architecture.md) |
| API fields | [docs/configuration.md](docs/configuration.md) |
| A new cloud, model or planner host | [CONTRIBUTING.md](CONTRIBUTING.md#model-providers) |
| Metrics, Events, decision log | [docs/operations.md](docs/operations.md) |
| Permissions, credentials | [docs/security.md](docs/security.md) |

If a guide conflicts with the requested change, refuse it and explain why.
