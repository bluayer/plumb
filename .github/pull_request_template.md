<!-- Title: what changes, in the imperative ("Add …", "Fix …"). Release notes are generated from PR titles. -->

## What and why

<!-- The problem and the change. Link the issue: Fixes #… -->

## How it was tested

<!-- Unit tests, e2e (`make e2e`), or a real cluster. For a behavior change, what you checked in `shadow` mode. -->

## Checklist

- [ ] `make test lint verify-generate` passes
- [ ] New behavior works in `shadow` mode first, and hub decisions are recorded in the decision log
- [ ] Strings from other projects (Karpenter, KEDA, Gateway API, model hosts, …) are copied from source with a comment naming it
- [ ] Docs updated (`docs/`, chart values, CRD field comments)
