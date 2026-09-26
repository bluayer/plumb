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

const assert = require('node:assert/strict');
const {test} = require('node:test');
const {approval, e2eResult, report} = require('./pr-checks.cjs');

const sha = 'a'.repeat(40);
const owner = {id: 1, login: 'owner', type: 'User'};
const author = {id: 2, login: 'author', type: 'User'};
const reviewer = {id: 3, login: 'reviewer', type: 'User'};
const pr = {number: 8, state: 'open', draft: false, base: {ref: 'main'}, user: author,
  head: {sha, ref: 'ci/example', repo: {id: 10}}};
const review = (id, state, user = reviewer, commit_id = sha, body = '') => ({id, state, user, commit_id, body});
const approved = review(1, 'APPROVED');
const command = (id, body) => review(id, 'COMMENTED', owner, sha, body);

function reviewAPI(reviews, permission = 'write') {
  return async path => {
    if (path.includes('/reviews?')) {
      const page = Number(new URLSearchParams(path.split('?')[1]).get('page'));
      return reviews.slice((page - 1) * 100, page * 100);
    }
    if (path.endsWith('/permission')) return {permission};
    throw new Error(`Unexpected API path: ${path}`);
  };
}

const cases = [
  ['no approval waits without throwing', [], false],
  ['current human approval', [approved], true],
  ['old-head approval', [review(1, 'APPROVED', reviewer, 'b'.repeat(40))], false],
  ['ordinary review comment preserves approval', [approved, review(2, 'COMMENTED')], true],
  ['dismissed approval', [approved, review(2, 'DISMISSED')], false],
  ['change request overrides approval', [approved, review(2, 'CHANGES_REQUESTED')], false],
  ['another maintainer requesting changes blocks approval', [approved, review(2, 'CHANGES_REQUESTED', owner)], false],
  ['later approval resolves change request', [review(1, 'CHANGES_REQUESTED'), review(2, 'APPROVED')], true],
  ['review IDs determine latest, not response order', [review(2, 'DISMISSED'), approved], false],
  ['bot approval cannot count', [review(1, 'APPROVED', {...reviewer, type: 'Bot'})], false],
  ['non-owner cannot approve their own PR', [review(1, 'APPROVED', author)], false],
  ['non-owner cannot use the self command', [review(1, 'COMMENTED', author, sha, '/approve')], false],
  ['owner command cannot approve another author', [command(1, '/approve')], false],
  ['read-only reviewer cannot approve', [approved], false, {permission: 'read'}],
  ['admin reviewer can approve', [approved], true, {permission: 'admin'}],
  ['owner self approval', [command(1, '/approve')], true, {ownerPR: true}],
  ['owner revokes approval', [command(1, '/approve'), command(2, '/unapprove')], false, {ownerPR: true}],
  ['owner command must be exact', [command(1, 'please /approve')], false, {ownerPR: true}],
  ['owner cannot natively self approve', [review(1, 'APPROVED', owner)], false, {ownerPR: true}],
  ['owner approval is also head-bound', [{...command(1, '/approve'), commit_id: 'b'.repeat(40)}], false, {ownerPR: true}],
  ['draft waits', [approved], false, {draft: true}],
  ['closed PR waits', [approved], false, {state: 'closed'}],
  ['different base waits', [approved], false, {base: {ref: 'release'}}],
  ['head changed while queued', [approved], false, {expectedSHA: 'b'.repeat(40)}],
];
for (const [name, reviews, expected, options = {}] of cases) {
  test(name, async () => {
    const current = {...pr, ...options, user: options.ownerPR ? owner : author};
    const result = await approval(reviewAPI(reviews, options.permission), current, owner.id, options.expectedSHA || sha);
    assert.equal(result.approved, expected);
  });
}

test('approval after the first page is considered', async () => {
  const reviews = Array.from({length: 100}, (_, i) => review(i + 1, 'COMMENTED'));
  reviews.push(review(101, 'APPROVED'));
  assert.equal((await approval(reviewAPI(reviews), pr, owner.id, sha)).approved, true);
});

test('API failure is not an approval', async () => {
  await assert.rejects(approval(async () => {throw new Error('API unavailable');}, pr, owner.id, sha), /API unavailable/);
});

const run = {id: 100, workflow_id: 12, event: 'pull_request_review', head_sha: sha,
  head_branch: pr.head.ref, head_repository: {id: 10}, pull_requests: [{number: 8}],
  status: 'completed', conclusion: 'success', html_url: 'https://github.com/example/repo/actions/runs/100'};
const passedJobs = [{name: 'Review readiness', conclusion: 'success'}, {name: 'E2E tests', conclusion: 'success'}];
for (const conclusion of ['failure', 'cancelled', 'timed_out', 'neutral', null]) {
  test(`E2E ${conclusion} cannot pass`, () => {
    assert.equal(e2eResult({approved: true}, {...run, conclusion}, passedJobs).state, 'failure');
  });
}
test('skipped E2E remains pending, even if the workflow is green', () => {
  assert.equal(e2eResult({approved: true}, run, [passedJobs[0], {...passedJobs[1], conclusion: 'skipped'}]).state, 'pending');
});
test('missing E2E job cannot pass', () => {
  assert.equal(e2eResult({approved: true}, run, [passedJobs[0]]).state, 'failure');
});
test('passing E2E requires approval', () => {
  assert.equal(e2eResult({approved: false, reason: 'Waiting for approval'}, run, passedJobs).state, 'pending');
});

async function reportFixture(options = {}) {
  const statuses = [];
  const paths = [];
  const latest = options.run || run;
  const trigger = options.trigger || run;
  const currentPR = options.pr || pr;
  const reviews = options.reviews || [approved];
  const api = async (path, body) => {
    paths.push(path);
    if (path === `/statuses/${sha}`) {
      statuses.push(body);
      return {};
    }
    if (options.failAt && path.includes(options.failAt)) throw new Error('API unavailable');
    if (path === '/actions/workflows/12') return {path: options.workflowPath || '.github/workflows/e2e.yaml'};
    if (path.startsWith('/actions/workflows/12/runs?')) return {workflow_runs: options.runs || [latest]};
    if (path.startsWith(`/commits/${sha}/pulls?`)) return [currentPR];
    if (path === '/pulls/8') return currentPR;
    if (path.startsWith(`/actions/runs/${latest.id}/jobs?filter=latest&`)) return {jobs: options.jobs || passedJobs};
    return reviewAPI(reviews)(path);
  };
  let error;
  try {await report(api, {repository: {owner}, workflow_run: trigger});} catch (caught) {error = caught;}
  return {statuses, paths, error, final: statuses.at(-1)};
}

test('approved successful run publishes success on the exact head', async () => {
  const result = await reportFixture();
  assert.ifError(result.error);
  assert.deepEqual(result.statuses.map(status => status.state), ['pending', 'success']);
  assert.equal(result.final.context, 'E2E passed');
});
test('unapproved PR is pending without listing skipped test jobs', async () => {
  const result = await reportFixture({reviews: []});
  assert.ifError(result.error);
  assert.equal(result.final.state, 'pending');
  assert.equal(result.paths.some(path => path.includes('/jobs?')), false);
});
test('revocation after passing tests returns to pending', async () => {
  const result = await reportFixture({reviews: [approved, review(2, 'DISMISSED')]});
  assert.equal(result.final.state, 'pending');
});
test('fork run without PR metadata resolves and verifies the PR', async () => {
  const forkRun = {...run, pull_requests: [], head_repository: {id: 99}};
  const result = await reportFixture({trigger: forkRun, run: forkRun, pr: {...pr, head: {...pr.head, repo: {id: 99}}}});
  assert.ifError(result.error);
  assert.equal(result.final.state, 'success');
  assert.ok(result.paths.some(path => path.startsWith(`/commits/${sha}/pulls?`)));
});
test('PR metadata for a different head repository cannot pass', async () => {
  const result = await reportFixture({pr: {...pr, head: {...pr.head, repo: {id: 99}}}});
  assert.equal(result.final.state, 'pending');
});
test('changed head cannot reuse the old result', async () => {
  const result = await reportFixture({pr: {...pr, head: {...pr.head, sha: 'b'.repeat(40)}}});
  assert.equal(result.final.state, 'pending');
});
test('delayed successful event uses the newer failed run', async () => {
  const newer = {...run, id: 101, conclusion: 'failure', html_url: run.html_url + '1'};
  const result = await reportFixture({run: newer, runs: [run, newer]});
  assert.ifError(result.error);
  assert.equal(result.final.state, 'failure');
  assert.equal(result.final.target_url, newer.html_url);
});
test('rerunning the same run ID clears its old success', async () => {
  const result = await reportFixture({run: {...run, status: 'in_progress', conclusion: null}});
  assert.equal(result.final.state, 'pending');
});
test('delayed requested event uses the fresh completed result', async () => {
  const result = await reportFixture({trigger: {...run, status: 'queued', conclusion: null}});
  assert.equal(result.final.state, 'success');
});
test('review API failure publishes error after clearing old success', async () => {
  const result = await reportFixture({failAt: '/reviews?'});
  assert.match(result.error.message, /API unavailable/);
  assert.deepEqual(result.statuses.map(status => status.state), ['pending', 'error']);
});
test('an unrelated workflow named E2E cannot publish a status', async () => {
  const result = await reportFixture({workflowPath: '.github/workflows/unrelated.yaml'});
  assert.deepEqual(result.statuses, []);
});
test('non-PR runs cannot publish an approval result', async () => {
  const result = await reportFixture({trigger: {...run, event: 'push'}});
  assert.deepEqual(result.statuses, []);
});
