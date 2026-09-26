<!-- Title: "[Type] What changes, in the imperative", e.g. "[Bugfix] Fix …" (types: CONTRIBUTING.md#pull-requests). Release notes come from PR titles. -->

## What and why

<!-- The problem and the change. Link the issue: Fixes #… -->

## Not a duplicate

<!-- The issue and open PRs you checked. -->

## How it was tested

<!-- Unit tests, e2e (`make e2e`), or a real cluster. For a behavior change, what you checked in `shadow` mode. -->

## Checklist

- [ ] `make test lint verify-generate verify-mod` passes
- [ ] Every commit is signed off (`git commit -s`, [DCO](../DCO))
- [ ] AI assistance, if any, is stated here and in commit trailers, and I reviewed every changed line
- [ ] New behavior works in `shadow` mode first, and hub decisions are recorded in the decision log
- [ ] Strings from other projects (Karpenter, KEDA, Gateway API, model hosts, …) are copied from source with a comment naming it
- [ ] Docs updated (`docs/`, chart values, CRD field comments)
