import assert from 'node:assert/strict';
import { createHash, createPublicKey, verify } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import test from 'node:test';
import { CLUSTER, CODE, FORMAL_CODE, TOKEN, createWireFixture, verify as verifyPhase, web } from './image-runtime-probe.mjs';

test('the Web probe requests HTML documents while preserving asset and API dispatch', async (t) => {
  const html = '<html><script src="/assets/runtime.js"></script></html>';
  const asset = 'export const runtime = true;';
  const documents = ['/', '/login', '/pipelines/runtime-image-smoke/edit'];
  const observed = [];
  const server = createServer((req, res) => {
    observed.push([req.url, req.headers.accept]);
    if (documents.includes(req.url) && req.headers.accept === 'text/html') {
      res.writeHead(200, { 'Content-Type': 'text/html' });
      return res.end(html);
    }
    if (req.url === '/assets/runtime.js') {
      res.writeHead(200, { 'Content-Type': 'application/javascript' });
      return res.end(asset);
    }
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end('{"code":"not-found"}');
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const sha = (value) => createHash('sha256').update(value).digest('hex');
  await web(`http://127.0.0.1:${server.address().port}`, {
    webFiles: { 'index.html': sha(html), 'assets/runtime.js': sha(asset) },
  });
  assert.deepEqual(observed, [
    ...documents.map((path) => [path, 'text/html']),
    ['/assets/runtime.js', '*/*'], ['/api/runtime-image-smoke-missing', '*/*'],
  ]);
});

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

test('the real-image peer grants a fresh CONFIG initialization claim', async (t) => {
  const peer = await fixture(t);
  const contract = JSON.parse(readFileSync(new URL(
    '../../app/src/test/resources/cloud-configuration-initialization-v1.json', import.meta.url), 'utf8'));
  const path = contract.claimPath.replace(contract.clusterId, CLUSTER);
  const response = await peer.post(path, contract.claimRequest, { Authorization: `Bearer ${TOKEN}` });
  const body = await response.json();
  assert.equal(response.status, 200,
    `fresh CONFIG initialization claim must be supported by the smoke peer: ${JSON.stringify(body)}`);
  assert.equal(body.code, contract.claimResponse.code);
  const { operationId } = body.data;
  assert.match(operationId, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  assert.deepEqual(body.data, { ...contract.claimResponse.data, clusterId: CLUSTER, operationId });
  assert.equal(peer.state.failures, 0);
});

test('the controlled peer signs real RS256 claims and consumes a code once', async (t) => {
  const peer = await fixture(t);
  const exchange = () => peer.post('/v1/api/auth/exchange', { exchangeCode: CODE, clusterId: CLUSTER },
    { 'X-Cluster-Identity-Secret': TOKEN });
  const response = await exchange();
  assert.equal(response.status, 200);
  const data = (await response.json()).data;
  const { jwt, jti, orgId, clusterId, organizationName, clusterName, region } = data;
  assert.deepEqual({ orgId, clusterId, organizationName, clusterName, region }, {
    orgId: 'image-runtime-org', clusterId: CLUSTER,
    organizationName: 'Image Smoke Organization', clusterName: 'Image Smoke Cluster', region: 'us-east-1',
  });
  const [head, payload, signature] = jwt.split('.');
  const claims = JSON.parse(Buffer.from(payload, 'base64url'));
  assert.equal(claims.iss, 'http://gateway:3000');
  assert.equal(claims.aud, '127.0.0.1');
  assert.equal(claims.org_id, orgId);
  assert.equal(claims.cluster_id, clusterId);
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

test('configuration claim, bodyless egress and completion retain the original scope', async (t) => {
  const peer = await fixture(t);
  const prefix = `/v1/api/clusters/${CLUSTER}/configuration-initialization/`;
  const headers = { Authorization: `Bearer ${TOKEN}` };
  const claim = { bootId: '550e8400-e29b-41d4-a716-446655440001', configurationHash: 'a'.repeat(64) };
  const granted = await peer.post(prefix + 'claim', claim, headers);
  assert.equal(granted.status, 200);
  const { data } = await granted.json();
  assert.equal(data.creationGranted, true);
  const egress = await fetch(peer.base + `/v1/api/clusters/${CLUSTER}/egress-address`, { headers });
  assert.equal(egress.status, 200);
  assert.deepEqual((await egress.json()).data,
    { organizationId: 'image-runtime-org', clusterId: CLUSTER, egressAddressStatus: 'DYNAMIC' });
  const completion = { operationId: data.operationId, configurationHash: claim.configurationHash };
  assert.equal((await peer.post(prefix + 'complete', { ...completion, configurationHash: 'b'.repeat(64) }, headers)).status, 400);
  assert.equal((await peer.post(prefix + 'complete', completion, headers)).status, 200);
  const repeated = await peer.post(prefix + 'claim', claim, headers);
  assert.deepEqual((await repeated.json()).data, { ...data, creationGranted: false, phase: 'COMPLETE' });
  assert.equal((await peer.post(prefix + 'claim', { ...claim, bootId: '550e8400-e29b-41d4-a716-446655440002' }, headers)).status, 400);
  assert.equal((await peer.post(prefix + 'claim', claim, { Authorization: 'Bearer wrong' })).status, 400);
  assert.equal(peer.state.claims.length, 2);
  assert.equal(peer.state.completions.length, 1);
  assert.equal(peer.state.failures, 3);
});

test('setup and formal exchange codes are independently consumed and never leak in proof', async (t) => {
  const peer = await fixture(t);
  const exchange = (exchangeCode) => peer.post('/v1/api/auth/exchange', { exchangeCode, clusterId: CLUSTER },
    { 'X-Cluster-Identity-Secret': TOKEN });
  const jtis = [];
  for (const code of [CODE, FORMAL_CODE]) {
    const response = await exchange(code);
    assert.equal(response.status, 200);
    jtis.push((await response.json()).data.jti);
    assert.equal((await exchange(code)).status, 401);
  }
  assert.notEqual(jtis[0], jtis[1]);
  const proof = await (await fetch(peer.base + '/proof')).text();
  for (const secret of [TOKEN, CODE, FORMAL_CODE]) assert(!proof.includes(secret));
  assert.equal(peer.state.exchanges, 4);
  assert.equal(peer.state.failures, 0);
});

test('a seed refusal requires the unavailable business code while retaining the login root', async (t) => {
  t.mock.method(console, 'log', () => {});
  let status = 503;
  let code = 'control.storage-unavailable';
  let entry = 'LOGIN';
  const server = createServer((req, res) => {
    const body = req.url === '/auth/storage-entry' ? { mode: 'CLOUD', entry }
      : { code };
    res.writeHead(req.url === '/auth/storage-entry' ? 200 : req.url === '/auth/bootstrap' ? 403 : status,
      { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(body));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const probe = () => verifyPhase('seed-refused', base, undefined,
    new URL('./connectors.lock.json', import.meta.url), undefined);
  await probe();
  code = 'control.cloud-auth-unavailable';
  await assert.rejects(probe);
  code = 'control.storage-unavailable'; status = 200;
  await assert.rejects(probe);
  status = 503; entry = 'UNAVAILABLE';
  await assert.rejects(probe);
});
