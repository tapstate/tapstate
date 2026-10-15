import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { setImmediate } from 'node:timers/promises';
import { notifyCloudRelease } from './notify-cloud-release.mjs';

const digest = `sha256:${'a'.repeat(64)}`;
const config = {
  RELEASE_VERSION: '0.7.0', RELEASE_TAG: 'v0.7.0', RELEASE_SHA: 'b'.repeat(40), RELEASE_BRANCH: 'main',
  CLOUD_IMAGE: `ghcr.io/tapstate/tapstate-cloud@${digest}`,
  CLOUD_IMAGE_TAG: 'ghcr.io/tapstate/tapstate-cloud:0.7.0', CLOUD_IMAGE_DIGEST: digest,
  CLOUD_RELEASE_API_URL: 'https://cloud.example.test/v1/api/cluster-versions',
  CLOUD_CLUSTER_VERSION_RELEASE_TOKEN: 'test-release-secret', CLOUD_RELEASE_CODE: 'BETA',
  RELEASE_RUN_URL: 'https://github.com/tapstate/tapstate/actions/runs/123',
};

test('the workflow group serializes the latest check and insert across versions', async () => {
  const workflow = await readFile(new URL('../.github/workflows/release.yml', import.meta.url), 'utf8');
  const job = workflow.match(/^  cloud-ghcr:\n([\s\S]*?)(?=^  [a-z][a-z0-9_-]*:)/m)?.[1];
  const group = job?.match(/^      group: (.+)$/m)?.[1];
  assert.ok(group, 'the Cloud publication job must declare a concurrency group');
  const tails = new Map();
  const withinWorkflowGroup = (env, operation) => {
    const key = group.replaceAll('${{ needs.version.outputs.version }}', env.RELEASE_VERSION);
    const running = (tails.get(key) ?? Promise.resolve()).then(operation);
    tails.set(key, running);
    return running;
  };
  let latestTag = 'v0.7.0';
  let releaseOld;
  let reachedOld;
  const oldCanCommit = new Promise(resolve => { releaseOld = resolve; });
  const oldAtInsert = new Promise(resolve => { reachedOld = resolve; });
  const catalog = [];
  const fetchImpl = async (_url, options) => {
    if (options.method !== 'POST') return new Response(JSON.stringify({ tag_name: latestTag }));
    const body = JSON.parse(options.body);
    if (body.version === '0.7.0') { reachedOld(); await oldCanCommit; }
    catalog.push(body.version);
    return new Response(JSON.stringify({ code: 'ok', data: { ...body, state: 'ACTIVE' } }), { status: 201 });
  };
  const oldRun = withinWorkflowGroup(config, () => notifyCloudRelease(config, fetchImpl));
  await oldAtInsert;
  latestTag = 'v0.8.0';
  const newer = { ...config, RELEASE_VERSION: '0.8.0', RELEASE_TAG: 'v0.8.0',
    CLOUD_IMAGE_TAG: 'ghcr.io/tapstate/tapstate-cloud:0.8.0' };
  const newRun = withinWorkflowGroup(newer, () => notifyCloudRelease(newer, fetchImpl));
  // Drain ready callbacks while the old insert is deliberately blocked, without a timed sleep.
  await setImmediate();
  releaseOld();
  await Promise.all([oldRun, newRun]);
  assert.deepEqual(catalog, ['0.7.0', '0.8.0'], 'the newest Cloud sequence must belong to the newer release');
});

function transport({ latestTag = 'v0.7.0', latestStatus = 200, status = 201, transform = x => x } = {}) {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    if (calls.length === 1) return new Response(JSON.stringify({ tag_name: latestTag }), { status: latestStatus });
    const sent = JSON.parse(options.body);
    return new Response(JSON.stringify(transform({ code: 'ok', data: { ...sent, state: 'ACTIVE' } })), { status });
  };
  return { calls, fetchImpl };
}

test('registers the published source and immutable image through the existing API', async () => {
  const remote = transport();
  assert.match(await notifyCloudRelease(config, remote.fetchImpl), /Registered Cloud version 0.7.0/);
  assert.equal(remote.calls.length, 2);
  assert.equal(remote.calls[0].options.headers.Authorization, undefined);
  const { url, options } = remote.calls[1];
  assert.equal(url, config.CLOUD_RELEASE_API_URL);
  assert.equal(options.method, 'POST');
  assert.equal(options.headers.Authorization, 'Bearer test-release-secret');
  assert.equal(options.redirect, 'error');
  assert.ok(options.signal instanceof AbortSignal);
  const body = JSON.parse(options.body);
  assert.equal(body.version, '0.7.0');
  assert.equal(body.releaseCode, 'BETA');
  assert.equal(body.git.commitId, config.RELEASE_SHA);
  assert.equal(body.git.branch, 'main');
  assert.equal(body.git.extra.image, config.CLOUD_IMAGE);
  assert.equal(body.git.extra.imageTag, config.CLOUD_IMAGE_TAG);
  assert.equal(body.git.extra.imageDigest, digest);
  assert.equal(body.git.extra.tag, 'v0.7.0');
  assert.equal(body.git.extra.workflowUrl, config.RELEASE_RUN_URL);
  assert.equal(body.releaseNotes, body.git.extra.releaseUrl);
});

test('a release already overtaken at notification time is skipped', async () => {
  const remote = transport({ latestTag: 'v0.8.0' });
  assert.match(await notifyCloudRelease(config, remote.fetchImpl), /Skipped.*no longer the current release/);
  assert.equal(remote.calls.length, 1);
});

test('malformed JSON is not accepted as a registration receipt', async () => {
  const remote = transport();
  await assert.rejects(notifyCloudRelease(config, async (url, options) => {
    if (options.method === 'POST') return new Response('<html>gateway error</html>', { status: 201 });
    return remote.fetchImpl(url, options);
  }), /invalid JSON response/);
});

test('an invalid latest release response prevents the POST', async () => {
  let calls = 0;
  await assert.rejects(notifyCloudRelease(config, async () => {
    calls++;
    return new Response('null', { status: 200 });
  }), /did not return a published numeric release/);
  assert.equal(calls, 1);
});

for (const [name, value] of [
  ['CLOUD_RELEASE_API_URL', ''], ['CLOUD_RELEASE_API_URL', 'http://cloud.example.test/v1/api/cluster-versions'],
  ['CLOUD_RELEASE_API_URL', 'https://user:secret@cloud.example.test/v1/api/cluster-versions'],
  ['CLOUD_RELEASE_API_URL', 'https://cloud.example.test/v1/api/cluster-versions?token=secret'],
  ['CLOUD_RELEASE_API_URL', 'https://cloud.example.test/wrong-route'],
  ['CLOUD_CLUSTER_VERSION_RELEASE_TOKEN', ''], ['CLOUD_CLUSTER_VERSION_RELEASE_TOKEN', 'bad\ntoken'],
  ['CLOUD_RELEASE_CODE', ''], ['CLOUD_RELEASE_CODE', 'stable'], ['RELEASE_TAG', 'v0.6.0'],
  ['RELEASE_SHA', 'main'], ['CLOUD_IMAGE_TAG', 'ghcr.io/tapstate/tapstate-cloud:latest'],
  ['CLOUD_IMAGE_DIGEST', 'sha256:bad'], ['CLOUD_IMAGE', `ghcr.io/tapstate/tapstate@${digest}`],
]) {
  test(`invalid ${name} is rejected before network access: ${JSON.stringify(value)}`, async () => {
    let called = false;
    await assert.rejects(notifyCloudRelease({ ...config, [name]: value }, async () => { called = true; }));
    assert.equal(called, false);
  });
}

for (const status of [301, 401, 403, 409, 429, 500, 503]) {
  test(`HTTP ${status} fails without retrying or disclosing the credential`, async () => {
    const remote = transport({ status, transform: () => ({ message: config.CLOUD_CLUSTER_VERSION_RELEASE_TOKEN }) });
    await assert.rejects(notifyCloudRelease(config, remote.fetchImpl), error => {
      assert.match(error.message, new RegExp(`HTTP ${status}`));
      assert.ok(!error.message.includes(config.CLOUD_CLUSTER_VERSION_RELEASE_TOKEN));
      if (status === 409) assert.match(error.message, /reconciled before retrying/);
      return true;
    });
    assert.equal(remote.calls.length, 2);
  });
}

test('failure to read latest prevents catalog mutation', async () => {
  const remote = transport({ latestStatus: 503 });
  await assert.rejects(notifyCloudRelease(config, remote.fetchImpl), /Could not verify the current release/);
  assert.equal(remote.calls.length, 1);
});

test('transport failure after POST is uncertain delivery and is not retried', async () => {
  const remote = transport();
  let posts = 0;
  await assert.rejects(notifyCloudRelease(config, async (url, options) => {
    if (options.method === 'POST') { posts++; throw new Error(config.CLOUD_CLUSTER_VERSION_RELEASE_TOKEN); }
    return remote.fetchImpl(url, options);
  }), /failed or timed out; delivery may be uncertain/);
  assert.equal(posts, 1);
});

for (const transform of [
  r => ({ ...r, code: 'error' }),
  r => ({ ...r, data: { ...r.data, version: '0.6.0' } }),
  r => ({ ...r, data: { ...r.data, state: 'DELISTED' } }),
  r => ({ ...r, data: { ...r.data, git: { ...r.data.git, extra: {} } } }),
]) {
  test('a successful status with a mismatched receipt is rejected', async () => {
    const remote = transport({ transform });
    await assert.rejects(notifyCloudRelease(config, remote.fetchImpl), /receipt does not match/);
  });
}
