# Repository governance

## Pull request flow

1. Open a PR targeting `main`. Only the small approval check runs before review; the Go, chart and image jobs wait. Until approval, `Review approved` and `CI passed` intentionally fail.
2. A human reviewer with write access approves the current head. For an owner-authored PR, @bluayer can instead use **Files changed → Review changes → Comment**, with the body `/approve`. A conversation comment does not count. GitHub records the review's commit ID, so this grants approval only to that head. CI tests GitHub's PR merge commit, including the base branch.
3. All three test/build jobs must succeed. The required `CI passed` check rejects failed, cancelled, skipped or missing prerequisites.
4. Only @bluayer merges. Approval is not permission to merge.

A new head needs a new approval. Review dismissal and change requests rerun the gate; older in-progress PR runs are cancelled. Ordinary review comments do not erase an existing approval. The required CI gate enforces current-head approval for every PR, including the owner command. Native review rules additionally enforce review for non-bypassing actors. Conversation resolution remains mandatory in the non-bypassable PR/CI ruleset.

GitHub does not offer native self-approval. The explicit `/approve` review comment is accepted only when the reviewer and PR author are both the repository owner. A new commit needs a new approval; `/unapprove` revokes owner approval. Another user cannot self-approve with this command. `CODEOWNERS` routes reviews to the owner. CI is mandatory for everyone, including the owner.

The approval job calls GitHub with read permissions and never checks out PR code. Test jobs have no publishing permissions. Main pushes and the release workflow still run all CI jobs without a PR gate, because approval is enforced before merging to main.

## Activate the server-side rules

**Committing these files does not enable protection.** The repository became public during setup on 2026-09-26. Ruleset reads now succeed and return an empty list; the earlier private-repository plan restriction is resolved. Server-side activation still requires authenticated access to repository settings.

Import the five JSON files from `.github/rulesets/` in **Settings → Rules → Rulesets → New ruleset → Import a ruleset**, or use the REST API with the owner's repository administration permission. Inspect existing rules first; update a matching rule instead of creating duplicates.

| File | Enforcement |
|---|---|
| `main-ci.json` | PR required, resolved conversations, current-base `CI passed`, no deletion or force push. No bypass actors. |
| `main-review.json` | One native approval, stale approval dismissal and latest-push approval; owner exception through PRs only. |
| `main-owner-only.json` | Only user ID `37579681` (@bluayer) may update main, and only through a PR. |
| `release-owner-only.json` | Only @bluayer may create `v*` tags. |
| `release-immutable.json` | Nobody may replace or delete `v*` tags through normal pushes. No bypass actors. |

Keep the three main rulesets separate. The owner can bypass the native review requirement and update restriction only through PRs, but cannot bypass `main-ci.json`. GitHub's owner review exception is actor-based, not PR-author-based: the mandatory CI gate therefore still requires a normal eligible approval for other authors' PRs. Only owner-authored PRs accept the explicit owner command. GitHub administrators can still edit the rules themselves.

The required check is bound to GitHub Actions app ID `15368`, verified from this repository's check runs. The owner ID is from the repository metadata. Review these IDs before copying the configuration to another repository.

Merge the workflow through a human-reviewed bootstrap PR before requiring the new check. Then activate all three main rulesets and both tag rulesets. Keep auto-merge and merge queues disabled so the final merge remains the owner's explicit action.

Verify in GitHub with a disposable PR: no approval blocks expensive CI and merge; a current approval starts CI; an owner-authored PR accepts `/approve` but another author's self-command does not; `/unapprove` blocks it again; a failing test blocks merge; a passing run permits only the owner to merge; another push or dismissed approval blocks merge again. Confirm there is no direct-push bypass for main. Do not test tag deletion on an existing release.

GitHub write access also permits editing GitHub Releases. Tag rules alone do not remove that permission: grant write only to trusted maintainers. Protecting package publishing from untrusted writers requires additional repository/package permission design.

## References

- [Ruleset REST schema](https://docs.github.com/en/rest/repos/rules#create-a-repository-ruleset)
- [Creating and importing rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/creating-rulesets-for-a-repository)
- [PR review events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request_review)
- [Required status checks and skipped jobs](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks)
