import assert from 'node:assert/strict';
import test from 'node:test';
import { notifyCloudDev, validateCloudDevConfiguration } from './notify-cloud-dev.mjs';

const digest = `sha256:${'a'.repeat(64)}`;
function environment(overrides = {}) {
  return {
    DEV_VERSION: '0.7.0', DEV_SHA: 'b'.repeat(40), DEV_WEB_SHA: 'c'.repeat(40),
    DEV_BRANCH: 'main', DEV_RUN_URL: 'https://github.com/tapstate/tapstate/actions/runs/123',
    CLOUD_IMAGE: `ghcr.io/tapstate/tapstate-cloud@${digest}`,
    CLOUD_IMAGE_TAG: 'ghcr.io/tapstate/tapstate-cloud:0.7.0-dev', CLOUD_IMAGE_DIGEST: digest,
    CLOUD_RELEASE_API_URL: 'https://dev.cloud.tapstate.com/v1/api/cluster-versions',
    CLOUD_RELEASE_CODE: 'BETA', CLOUD_CLUSTER_VERSION_RELEASE_TOKEN: 'private-dev-token',
    ...overrides,
  };
}
function receipt(options, overrides = {}) {
  return { code: 'ok', data: { ...JSON.parse(options.body), state: 'ACTIVE', ...overrides } };
}

test('registers only the checked dev image, with both pinned revisions and a verified receipt', async () => {
  const calls = [];
  const result = await notifyCloudDev(environment(), async (url, options) => {
    calls.push({ url, options });
    return { status: 201, json: async () => receipt(options) };
  });
  assert.equal(calls.length, 1);
  const [{ url, options }] = calls;
  assert.equal(url, 'https://dev.cloud.tapstate.com/v1/api/cluster-versions');
  assert.equal(options.method, 'POST');
  assert.equal(options.redirect, 'error');
  assert.ok(options.signal instanceof AbortSignal);
  assert.equal(options.headers.Authorization, 'Bearer private-dev-token');
  const body = JSON.parse(options.body);
  assert.equal(body.version, '0.7.0-dev');
  assert.equal(body.releaseCode, 'BETA');
  assert.equal(body.git.branch, 'main');
  assert.equal(body.git.commitId, 'b'.repeat(40));
  assert.equal(body.git.extra.webCommitId, 'c'.repeat(40));
  assert.equal(body.git.extra.imageDigest, digest);
  assert.equal(body.git.extra.imageTag, 'ghcr.io/tapstate/tapstate-cloud:0.7.0-dev');
  assert.equal(body.git.extra.workflowUrl, environment().DEV_RUN_URL);
  assert.equal(body.git.extra.tag, undefined);
  assert.equal(body.git.extra.releaseUrl, undefined);
  assert.match(result, /Registered Cloud dev version 0\.7\.0-dev/);
  assert.ok(!result.includes('private-dev-token'));
});

for (const [name, value] of [
  ['DEV_VERSION', '0.7.0-dev'], ['DEV_VERSION', 'v0.7.0'], ['DEV_SHA', 'bbbbbbb'],
  ['DEV_WEB_SHA', 'ccccccc'], ['DEV_BRANCH', 'release/0.7.0'],
  ['CLOUD_IMAGE_TAG', 'ghcr.io/tapstate/tapstate-cloud:0.7.0'],
  ['CLOUD_IMAGE_TAG', 'ghcr.io/tapstate/tapstate-cloud:0.8.0-dev'],
  ['CLOUD_IMAGE', `ghcr.io/other/cloud@${digest}`],
  ['CLOUD_IMAGE_DIGEST', `sha256:${'d'.repeat(64)}`],
  ['DEV_RUN_URL', 'https://github.com/other/repo/actions/runs/123'],
  ['CLOUD_RELEASE_API_URL', 'https://cloud.tapstate.com/v1/api/cluster-versions'],
  ['CLOUD_RELEASE_API_URL', 'http://dev.cloud.tapstate.com/v1/api/cluster-versions'],
  ['CLOUD_RELEASE_API_URL', 'https://dev.cloud.tapstate.com/v1/api/cluster-versions?token=secret'],
  ['CLOUD_RELEASE_CODE', 'STABLE'], ['CLOUD_CLUSTER_VERSION_RELEASE_TOKEN', 'bad\nvalue'],
]) {
  test(`rejects invalid ${name}: ${name.includes('TOKEN') ? 'line break' : value}`, async () => {
    let calls = 0;
    await assert.rejects(notifyCloudDev(environment({ [name]: value }), async () => { calls++; }));
    assert.equal(calls, 0);
  });
}

for (const name of Object.keys(environment())) {
  test(`requires ${name} before any request`, async () => {
    let calls = 0;
    await assert.rejects(notifyCloudDev(environment({ [name]: ' ' }), async () => { calls++; }), /required/);
    assert.equal(calls, 0);
  });
}

test('preflight checks only configuration, before expensive image construction', () => {
  assert.doesNotThrow(() => validateCloudDevConfiguration({
    CLOUD_RELEASE_API_URL: environment().CLOUD_RELEASE_API_URL,
    CLOUD_RELEASE_CODE: 'BETA', CLOUD_CLUSTER_VERSION_RELEASE_TOKEN: 'private-dev-token',
  }));
  assert.throws(() => validateCloudDevConfiguration(environment({ CLOUD_CLUSTER_VERSION_RELEASE_TOKEN: '' })), /required/);
});

for (const status of [200, 301, 400, 401, 403, 409, 500, 503]) {
  test(`does not retry or accept HTTP ${status}, or disclose its body`, async () => {
    let calls = 0;
    let reads = 0;
    await assert.rejects(notifyCloudDev(environment(), async () => {
      calls++;
      return { status, json: async () => { reads++; throw new Error('private-dev-token'); } };
    }), error => error.message.includes(`HTTP ${status}`) && !error.message.includes('private-dev-token'));
    assert.equal(calls, 1);
    assert.equal(reads, 0);
  });
}

test('transport errors do not disclose credentials or automatically retry an uncertain write', async () => {
  let calls = 0;
  await assert.rejects(notifyCloudDev(environment(), async () => {
    calls++;
    throw new Error('private-dev-token');
  }), error => /delivery may be uncertain/.test(error.message) && !error.message.includes('private-dev-token'));
  assert.equal(calls, 1);
});

test('rejects invalid JSON without echoing parser details', async () => {
  await assert.rejects(notifyCloudDev(environment(), async () => ({
    status: 201, json: async () => { throw new Error('private-dev-token'); },
  })), error => /invalid JSON/.test(error.message) && !error.message.includes('private-dev-token'));
});

for (const [name, mutate] of [
  ['envelope', r => { r.code = 'error'; }],
  ['version', r => { r.data.version = '0.7.0'; }],
  ['channel', r => { r.data.releaseCode = 'STABLE'; }],
  ['state', r => { r.data.state = 'OFFLINE'; }],
  ['source', r => { r.data.git.commitId = 'd'.repeat(40); }],
  ['branch', r => { r.data.git.branch = 'old-main'; }],
  ['digest', r => { r.data.git.extra.imageDigest = `sha256:${'d'.repeat(64)}`; }],
  ['web source', r => { r.data.git.extra.webCommitId = 'd'.repeat(40); }],
  ['missing provenance', r => { delete r.data.git.extra; }],
]) {
  test(`rejects a receipt with mismatched ${name}`, async () => {
    await assert.rejects(notifyCloudDev(environment(), async (_url, options) => ({
      status: 201, json: async () => { const r = receipt(options); mutate(r); return r; },
    })), /receipt does not match/);
  });
}
