/*
Copyright The Plumb Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

const fs = require('node:fs');

// GitHub REST: pull request reviews, collaborator permissions, workflow runs/jobs,
// and commit statuses. https://docs.github.com/en/rest
function githubAPI() {
  return async (path, body) => {
    const response = await fetch(`${process.env.GITHUB_API_URL}/repos/${process.env.GITHUB_REPOSITORY}${path}`, {
      method: body ? 'POST' : 'GET',
      headers: {
        Authorization: `Bearer ${process.env.GH_TOKEN}`,
        Accept: 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2026-03-10',
        ...(body && {'Content-Type': 'application/json'}),
      },
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(30000),
    });
    if (!response.ok) throw new Error(`GitHub API returned ${response.status} for ${path}`);
    return response.json();
  };
}

async function list(api, path, field) {
  const result = [];
  for (let page = 1; ; page++) {
    const response = await api(`${path}${path.includes('?') ? '&' : '?'}per_page=100&page=${page}`);
    const items = field ? response[field] : response;
    result.push(...items);
    if (items.length < 100) return result;
  }
}

async function approval(api, pr, ownerID, expectedSHA) {
  if (pr.state !== 'open' || pr.draft || pr.base.ref !== 'main' || pr.head.sha !== expectedSHA) {
    return {approved: false, reason: 'Waiting for a ready PR targeting main at this commit'};
  }
  const latest = new Map();
  for (let review of await list(api, `/pulls/${pr.number}/reviews`)) {
    // Only the owner reviewing their own PR can use the explicit self-approval.
    // A review's commit_id binds the command to the head it actually reviewed.
    const ownerSelf = review.user.id === ownerID && review.user.id === pr.user.id;
    const command = (review.body || '').trim();
    if (ownerSelf && review.state === 'COMMENTED' && ['/approve', '/unapprove'].includes(command)) {
      review = {...review, state: command === '/approve' ? 'APPROVED' : 'DISMISSED', ownerApproval: true};
    }
    if (!['APPROVED', 'CHANGES_REQUESTED', 'DISMISSED'].includes(review.state)) continue;
    const previous = latest.get(review.user.id);
    if (!previous || review.id > previous.id) latest.set(review.user.id, review);
  }
  let approved = false;
  for (const review of latest.values()) {
    if (review.user.type !== 'User' || (review.user.id === pr.user.id && !review.ownerApproval)) continue;
    const access = await api(`/collaborators/${encodeURIComponent(review.user.login)}/permission`);
    // The permissions API maps maintain to write and triage to read.
    if (!['write', 'admin'].includes(access.permission)) continue;
    if (review.state === 'CHANGES_REQUESTED') {
      return {approved: false, reason: 'Waiting for requested changes to be resolved'};
    }
    if (review.state === 'APPROVED' && review.commit_id === pr.head.sha) approved = true;
  }
  return {
    approved,
    reason: approved ? 'Current commit is approved' : 'Waiting for approval of the current commit',
  };
}

function e2eResult(review, run, jobs) {
  if (!review.approved) return {state: 'pending', description: review.reason};
  if (run.status !== 'completed') return {state: 'pending', description: 'Approved; E2E is queued or running'};
  const gate = jobs.find(job => job.name === 'Review readiness');
  const test = jobs.find(job => job.name === 'E2E tests');
  if (run.conclusion === 'success' && gate?.conclusion === 'success' && test?.conclusion === 'success') {
    return {state: 'success', description: 'Current commit approved and E2E passed'};
  }
  // Approval may arrive just after the readiness job checked. Its skipped test
  // is never success; the new review event will start the approved run.
  if (run.conclusion === 'success' && gate?.conclusion === 'success' && test?.conclusion === 'skipped') {
    return {state: 'pending', description: 'Approved; waiting for an E2E test run'};
  }
  return {state: 'failure', description: 'E2E did not pass; inspect the run and rerun after fixing it'};
}

async function report(api, event) {
  const trigger = event.workflow_run;
  if (!['pull_request', 'pull_request_review'].includes(trigger.event)) return;
  // The workflow name alone is not an identity check. Verify its canonical path.
  const workflow = await api(`/actions/workflows/${trigger.workflow_id}`);
  if (workflow.path !== '.github/workflows/e2e.yaml') return;
  const sha = trigger.head_sha;
  if (!/^[0-9a-f]{40}$/.test(sha)) throw new Error('Invalid workflow head SHA');
  const publish = result => api(`/statuses/${sha}`, {
    context: 'E2E passed', target_url: trigger.html_url, ...result,
  });
  // Clear an earlier success before any review/API operation that could fail.
  await publish({state: 'pending', description: 'Checking current approval and E2E result'});
  try {
    const runs = await list(api, `/actions/workflows/${trigger.workflow_id}/runs?head_sha=${sha}`, 'workflow_runs');
    // Delayed completion events must never overwrite a newer run's result.
    // A rerun has the same id; use fresh API data, not the webhook's conclusion.
    const run = runs.filter(candidate =>
      ['pull_request', 'pull_request_review'].includes(candidate.event) &&
      candidate.head_sha === sha && candidate.head_branch === trigger.head_branch &&
      candidate.head_repository?.id === trigger.head_repository?.id
    ).sort((a, b) => b.id - a.id)[0];
    if (!run) throw new Error('No matching E2E workflow run');
    // Fork workflow_run payloads can omit pull_requests. Resolve through the
    // commit API, then re-read the PR rather than trusting artifacts from a PR.
    const candidates = run.pull_requests?.length ? run.pull_requests : await list(api, `/commits/${sha}/pulls`);
    const prs = [];
    for (const candidate of candidates) {
      const pr = await api(`/pulls/${candidate.number}`);
      if (pr.state === 'open' && pr.base.ref === 'main' && pr.head.sha === sha &&
          pr.head.ref === run.head_branch && pr.head.repo?.id === run.head_repository?.id) prs.push(pr);
    }
    if (prs.length !== 1) {
      await publish({state: 'pending', description: 'Waiting for one current, open PR targeting main'});
      return;
    }
    const review = await approval(api, prs[0], event.repository.owner.id, sha);
    const jobs = review.approved && run.status === 'completed'
      ? await list(api, `/actions/runs/${run.id}/jobs?filter=latest`, 'jobs') : [];
    await publish({...e2eResult(review, run, jobs), target_url: run.html_url});
  } catch (error) {
    await publish({state: 'error', description: 'Unable to verify E2E; inspect the E2E status workflow'});
    throw error;
  }
}

async function main() {
  const event = JSON.parse(fs.readFileSync(process.env.GITHUB_EVENT_PATH, 'utf8'));
  const api = githubAPI();
  if (process.argv[2] === 'report') return report(api, event);
  if (process.argv[2] !== 'approval') throw new Error('Expected approval or report');
  const pr = await api(`/pulls/${event.pull_request.number}`);
  const result = await approval(api, pr, event.repository.owner.id, event.pull_request.head.sha);
  fs.appendFileSync(process.env.GITHUB_OUTPUT, `approved=${result.approved}\n`);
  fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${result.reason}.\n`);
  console.log(result.reason);
}

module.exports = {approval, e2eResult, report};
if (require.main === module) main().catch(error => {
  console.error(error.message);
  process.exitCode = 1;
});
