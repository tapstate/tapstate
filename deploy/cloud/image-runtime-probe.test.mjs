import assert from 'node:assert/strict';
import { createPublicKey, verify } from 'node:crypto';
import { createServer } from 'node:http';
import test from 'node:test';
import { CLUSTER, CODE, TOKEN, createWireFixture } from './image-runtime-probe.mjs';

async function fixture(t) {
  const peer = createWireFixture({ issuer: 'http://gateway:3000', version: '0.6.0' });
  const server = createServer(peer.handler);
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const post = (path, body, headers) => fetch(base + path, { method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body),
    signal: AbortSignal.timeout(5000) });
  return { ...peer, base, post };
}

test('the controlled peer signs real RS256 claims and consumes a code once', async (t) => {
  const peer = await fixture(t);
  const exchange = () => peer.post('/v1/api/auth/exchange', { exchangeCode: CODE, clusterId: CLUSTER },
    { 'X-Cluster-Identity-Secret': TOKEN });
  const response = await exchange();
  assert.equal(response.status, 200);
  const { jwt, jti } = (await response.json()).data;
  const [head, payload, signature] = jwt.split('.');
  const claims = JSON.parse(Buffer.from(payload, 'base64url'));
  assert.equal(claims.iss, 'http://gateway:3000');
  assert.equal(claims.aud, '127.0.0.1');
  assert.equal(claims.cluster_id, CLUSTER);
  assert.equal(claims.user_id, 'image-runtime-user');
  assert.equal(claims.jti, jti);
  assert.equal(claims.exp - claims.iat, 900);
  const keys = await (await fetch(peer.base + '/v1/api/jwks.json')).json();
  assert(verify('RSA-SHA256', Buffer.from(`${head}.${payload}`),
    createPublicKey({ format: 'jwk', key: keys.keys[0] }), Buffer.from(signature, 'base64url')));
  assert.equal((await exchange()).status, 401);
  assert.equal(peer.state.exchanges, 2);
  assert.equal(peer.state.failures, 0);
});

for (const [name, body, secret] of [
  ['wrong service identity', { exchangeCode: CODE, clusterId: CLUSTER }, 'wrong-sentinel'],
  ['extra credential field', { exchangeCode: CODE, clusterId: CLUSTER, token: TOKEN }, TOKEN],
  ['wrong field naming', { exchange_code: CODE, cluster_id: CLUSTER }, TOKEN],
]) {
  test(`the peer detects ${name} instead of silently making a session`, async (t) => {
    const peer = await fixture(t);
    const response = await peer.post('/v1/api/auth/exchange', body, { 'X-Cluster-Identity-Secret': secret });
    assert.equal(response.status, 400);
    assert.deepEqual(await response.json(), { code: 'fixture.contract-rejected' });
    assert.equal(peer.state.failures, 1);
  });
}

test('C2 proof requires exact non-secret fields, counts, version and a fresh nonce', async (t) => {
  const peer = await fixture(t);
  const body = { nonce: 'nonce-one', runtimeVersion: '0.6.0', activePipelines: 0, uptimeMs: 123 };
  const post = (value) => peer.post(`/v1/api/clusters/${CLUSTER}/status-report`, value,
    { Authorization: `Bearer ${TOKEN}` });
  assert.equal((await post(body)).status, 200);
  assert.equal((await post(body)).status, 400);
  assert.equal((await post({ ...body, nonce: 'nonce-two', config: 'must-not-be-in-c2' })).status, 400);
  const { activePipelines, ...missing } = body;
  assert.equal(activePipelines, 0);
  assert.equal((await post({ ...missing, nonce: 'nonce-three' })).status, 400);
  assert.equal(peer.state.reports.length, 1);
  assert.equal(peer.state.failures, 3);
  const proof = await (await fetch(peer.base + '/proof')).text();
  assert(!proof.includes(TOKEN) && !proof.includes(CODE) && !proof.includes('must-not-be-in-c2'));
});

test('malformed and unexpected requests cannot become successful fixture evidence', async (t) => {
  const peer = await fixture(t);
  assert.equal((await peer.post('/unknown', {}, {})).status, 404);
  assert.equal((await peer.post(`/v1/api/clusters/${CLUSTER}/status-report`, {
    nonce: 'nonce', runtimeVersion: '0.6.0', activePipelines: 0, uptimeMs: 1 }, {})).status, 400);
  assert.equal(peer.state.failures, 2);
});
