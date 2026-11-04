#!/usr/bin/env node
// Controlled wire peers and HTTP assertions for real, already-built runtime images.
// This is not a browser, an Atlas service, or a replacement Cloud implementation.
import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { pathToFileURL } from 'node:url';

export const TOKEN = 'image-runtime-static-token-sentinel';
export const CODE = 'image-runtime-one-time-code-sentinel';
export const FORMAL_CODE = 'image-runtime-formal-code-sentinel';
export const CLUSTER = 'image-runtime-cluster';
export const COOKIE = '__Host-tapstate-cloud-session';
const PASSWORD = 'image-runtime-onprem-password-sentinel';
const USER = 'image-runtime-admin';
const sha = (bytes) => createHash('sha256').update(bytes).digest('hex');
const encode = (value) => Buffer.from(JSON.stringify(value)).toString('base64url');
let stage = 'inputs';

export function createWireFixture({ issuer, version, audience = '127.0.0.1' }) {
  const { privateKey, publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const jwk = { ...publicKey.export({ format: 'jwk' }), kid: 'image-runtime-key', use: 'sig', alg: 'RS256' };
  const state = { exchanges: 0, jwks: 0, reports: [], failures: 0, claims: [], completions: [], egress: 0 };
  const consumed = new Set();
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  const operationId = '550e8400-e29b-41d4-a716-446655440000';
  let initialization;
  const reply = (res, status, value) => {
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(value));
  };
  const envelope = (data) => ({ opId: 'image-runtime-operation', code: 'ok', msg: 'ok', data });
  const handler = async (req, res) => {
    try {
      if (req.url === '/healthz' && req.method === 'GET') return reply(res, 200, { ready: true });
      if (req.url === '/proof' && req.method === 'GET') return reply(res, 200, state);
      if (req.url === '/v1/api/jwks.json' && req.method === 'GET') {
        state.jwks++;
        return reply(res, 200, { keys: [jwk] });
      }
      let bytes = '';
      for await (const chunk of req) {
        bytes += chunk;
        if (bytes.length > 16384) throw new Error('oversized fixture request');
      }
      const body = bytes ? JSON.parse(bytes) : undefined;
      if (req.url === `/v1/api/clusters/${CLUSTER}/egress-address` && req.method === 'GET') {
        assert.equal(req.headers.authorization, `Bearer ${TOKEN}`);
        assert.equal(body, undefined);
        state.egress++;
        return reply(res, 200, envelope({ organizationId: 'image-runtime-org', clusterId: CLUSTER,
          egressAddressStatus: 'DYNAMIC' }));
      }
      const prefix = `/v1/api/clusters/${CLUSTER}/configuration-initialization/`;
      if ([`${prefix}claim`, `${prefix}complete`].includes(req.url) && req.method === 'POST') {
        assert.equal(req.headers.authorization, `Bearer ${TOKEN}`);
        assert.match(body.configurationHash, /^[0-9a-f]{64}$/);
        if (req.url === `${prefix}claim`) {
          assert.deepEqual(Object.keys(body).sort(), ['bootId', 'configurationHash']);
          assert.match(body.bootId, uuid);
          if (initialization) assert.deepEqual(body, initialization);
          else initialization = body;
          state.claims.push(body);
          return reply(res, 200, envelope({ clusterId: CLUSTER, operationId,
            configurationHash: body.configurationHash, phase: state.completions.length ? 'COMPLETE' : 'CLAIMED',
            creationGranted: state.completions.length === 0 }));
        }
        assert.deepEqual(Object.keys(body).sort(), ['configurationHash', 'operationId']);
        assert(initialization && state.egress > 0);
        assert.equal(body.operationId, operationId);
        assert.equal(body.configurationHash, initialization.configurationHash);
        state.completions.push(body);
        return reply(res, 200, envelope({ clusterId: CLUSTER, operationId,
          configurationHash: body.configurationHash, phase: 'COMPLETE', creationGranted: false }));
      }
      if (req.url === '/v1/api/auth/exchange' && req.method === 'POST') {
        state.exchanges++;
        assert.equal(req.headers['x-cluster-identity-secret'], TOKEN);
        assert.deepEqual(Object.keys(body).sort(), ['clusterId', 'exchangeCode']);
        assert.equal(body.clusterId, CLUSTER);
        assert([CODE, FORMAL_CODE].includes(body.exchangeCode));
        if (consumed.has(body.exchangeCode)) return reply(res, 401, { code: 'exchange.code-consumed', msg: 'consumed' });
        consumed.add(body.exchangeCode);
        const jti = body.exchangeCode === CODE ? 'image-runtime-jti' : 'image-runtime-formal-jti';
        const now = Math.floor(Date.now() / 1000);
        const head = encode({ alg: 'RS256', typ: 'JWT', kid: jwk.kid });
        const payload = encode({ sub: 'image-user@example.test', iss: issuer, aud: audience,
          iat: now, exp: now + 900, jti, user_id: 'image-runtime-user',
          org_id: 'image-runtime-org', cluster_id: CLUSTER, scope: ['workload:read', 'workload:write'] });
        const signed = `${head}.${payload}`;
        const jwt = `${signed}.${sign('RSA-SHA256', Buffer.from(signed), privateKey).toString('base64url')}`;
        return reply(res, 200, envelope({ jwt, expiresAt: new Date((now + 900) * 1000).toISOString(),
          jti, userEmail: 'image-user@example.test',
          orgId: 'image-runtime-org', clusterId: CLUSTER,
          organizationName: 'Image Smoke Organization', clusterName: 'Image Smoke Cluster', region: 'us-east-1' }));
      }
      if (req.url === `/v1/api/clusters/${CLUSTER}/status-report` && req.method === 'POST') {
        assert.equal(req.headers.authorization, `Bearer ${TOKEN}`);
        assert.deepEqual(Object.keys(body).sort(), ['activePipelines', 'nonce', 'runtimeVersion', 'uptimeMs']);
        assert.equal(body.runtimeVersion, version);
        assert.equal(body.activePipelines, 0);
        assert(Number.isSafeInteger(body.uptimeMs) && body.uptimeMs >= 0);
        assert.equal(typeof body.nonce, 'string');
        assert(body.nonce.length > 0 && !state.reports.some((row) => row.nonce === body.nonce));
        state.reports.push(body);
        return reply(res, 200, envelope({ status: 'accepted' }));
      }
      state.failures++;
      return reply(res, 404, { code: 'fixture.unexpected-request' });
    } catch {
      // Never print a raw SDK request, JWT, authorization header, or assertion values.
      state.failures++;
      return reply(res, 400, { code: 'fixture.contract-rejected' });
    }
  };
  return { handler, state };
}

async function request(base, path, options = {}) {
  return fetch(new URL(path, base), { redirect: 'manual', signal: AbortSignal.timeout(10000), ...options });
}

async function proof(base, minimumReports = 1) {
  const until = Date.now() + 20000;
  do {
    const response = await request(base, '/proof');
    assert.equal(response.status, 200);
    const value = await response.json();
    assert.equal(value.failures, 0, 'controlled SDK peer rejected a wire contract');
    if (value.reports.length >= minimumReports) return value;
    await new Promise((resolve) => setTimeout(resolve, 200));
  } while (Date.now() < until);
  throw new Error('no fresh status report');
}

export async function web(base, metadata) {
  stage = 'Web entry bytes';
  const navigation = { headers: { Accept: 'text/html' } };
  const root = await request(base, '/', navigation);
  assert.equal(root.status, 200);
  const html = await root.text();
  assert.equal(sha(html), metadata.webFiles['index.html']);
  for (const path of ['/login', '/pipelines/runtime-image-smoke/edit']) {
    const page = await request(base, path, navigation);
    assert.equal(page.status, 200);
    assert.equal(sha(await page.text()), metadata.webFiles['index.html']);
  }
  const assets = [...html.matchAll(/(?:src|href)="(\/assets\/[^"?#]+)"/g)].map((match) => match[1]);
  assert(assets.length > 0, 'real Web entry has no referenced production assets');
  for (const path of new Set(assets)) {
    assert(metadata.webFiles[path.slice(1)], 'Web entry references an unmanifested asset');
    const response = await request(base, path);
    assert.equal(response.status, 200);
    assert.equal(sha(Buffer.from(await response.arrayBuffer())), metadata.webFiles[path.slice(1)]);
  }
  stage = 'unmapped API rejection';
  const missingApi = await request(base, '/api/runtime-image-smoke-missing');
  assert.equal(missingApi.status, 404);
  assert((missingApi.headers.get('content-type') ?? '').includes('application/json'));
}

function save(path, value) {
  writeFileSync(path, JSON.stringify(value), { mode: 0o600 });
}

function cookieHeader(value) {
  const [cookie, ...attributes] = value.split(';').map((part) => part.trim());
  assert(cookie.startsWith(`${COOKIE}=`) && cookie.length > COOKIE.length + 10);
  assert.deepEqual(attributes.sort(), ['Path=/', 'Secure', 'HttpOnly', 'SameSite=Lax'].sort());
  return cookie;
}

async function connectors(base, cookie, expected) {
  stage = 'registered connector list and details';
  const response = await request(base, '/api/connectors', { headers: { Cookie: cookie } });
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.deepEqual(body.connectors.map((row) => row.id).sort(), expected.sort());
  for (const id of expected) {
    const detail = await request(base, `/api/connectors/${id}`, { headers: { Cookie: cookie } });
    assert.equal(detail.status, 200);
    const described = await detail.json();
    assert.equal(described.id, id);
    assert.equal(described.runtimeAvailable, true);
  }
}

async function jsonRequest(base, path, body, cookie) {
  const response = await request(base, path, { method: 'POST',
    headers: { 'Content-Type': 'application/json', Cookie: cookie }, body: JSON.stringify(body) });
  const value = await response.json();
  if (response.status !== 200) {
    const code = /^[a-z][a-z0-9.-]{0,100}$/.test(value.code) ? value.code : 'unavailable';
    stage += ` (HTTP ${response.status}, code ${code})`;
  }
  assert.equal(response.status, 200);
  return value;
}

async function exchange(base, code) {
  const response = await request(base, `/auth/exchange?code=${code}`);
  assert.equal(response.status, 302);
  assert.equal(response.headers.get('location'), '/');
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.equal(await response.text(), '');
  return cookieHeader(response.headers.get('set-cookie') ?? '');
}

async function awaitState(read, accept) {
  const until = Date.now() + 120000;
  do {
    const value = await read();
    if (accept(value)) return value;
    await new Promise((resolve) => setTimeout(resolve, 200));
  } while (Date.now() < until);
  throw new Error('runtime state did not reach its required phase');
}

export async function verify(phase, base, fixture, metadataPath, statePath) {
  const metadata = JSON.parse(readFileSync(metadataPath, 'utf8'));
  if (phase === 'cloud') {
    await web(base, metadata);
    stage = 'fresh Cloud storage setup entry';
    await awaitState(async () => (await request(base, '/auth/storage-entry')).json(),
      (value) => value.mode === 'CLOUD' && value.entry === 'SETUP_REQUIRED');
    stage = 'temporary SDK exchange and storage setup';
    const temporary = await exchange(base, CODE);
    const snapshot = await jsonRequest(base, '/api/storage/status', {}, temporary);
    assert.equal(snapshot.setupPhase, 'CONFIG_PREPARED');
    const candidate = JSON.parse(readFileSync(process.env.TAPSTATE_RUNTIME_SMOKE_CANDIDATE, 'utf8'));
    const original = { operationId: snapshot.operationId, generation: snapshot.setupGeneration, candidate };
    const tested = await jsonRequest(base, '/api/storage/test', { ...original, layer: 'CACHE' }, temporary);
    const committed = await jsonRequest(base, '/api/storage/setup/commit',
      { ...original, testStamps: { CACHE: tested.testStamp } }, temporary);
    assert(['STORAGE_BOUND', 'COMPLETE'].includes(committed.setupPhase));
    stage = 'same-process setup completes and retires temporary authentication';
    await awaitState(async () => (await request(base, '/auth/storage-entry')).json(),
      (value) => value.entry === 'LOGIN');
    assert.equal((await request(base, '/api/storage/status', { method: 'POST',
      headers: { Cookie: temporary, 'Content-Type': 'application/json' }, body: '{}' })).status, 401);
    stage = 'formal SDK reauthentication and business readiness';
    const cookie = await exchange(base, FORMAL_CODE);
    const ready = await awaitState(() => jsonRequest(base, '/api/storage/status', {}, cookie),
      (value) => value.businessPhase === 'READY' && value.applicationDataVerification.status === 'PASSED');
    assert.equal(ready.setupPhase, 'COMPLETE');
    console.log('PASS: real image Cloud setup, temporary session retirement and formal reauthentication');
    stage = 'anonymous API rejection';
    assert.equal((await request(base, '/api/connectors')).status, 401);
    stage = 'SDK exchange and local Cookie creation';
    await connectors(base, cookie, metadata.connectorIds);
    stage = 'mixed Bearer credential rejection';
    assert.equal((await request(base, '/api/connectors', {
      headers: { Authorization: `Bearer ${TOKEN}` } })).status, 401);
    stage = 'initial SDK exchange, JWKS and status proof';
    const before = await proof(fixture);
    assert.equal(before.exchanges, 2);
    assert.equal(before.jwks, 1);
    assert.equal(before.claims.length, 1);
    assert.equal(before.completions.length, 1);
    save(statePath, { cookie, exchanges: before.exchanges, jwks: before.jwks,
      reports: before.reports.length, nonces: before.reports.map((row) => row.nonce) });
  } else if (phase === 'cloud-stopped') {
    stage = 'C2 baseline after the first JVM has stopped';
    const saved = JSON.parse(readFileSync(statePath, 'utf8'));
    const stopped = await proof(fixture);
    assert.equal(stopped.exchanges, saved.exchanges);
    assert.equal(stopped.jwks, saved.jwks);
    save(statePath, { ...saved, reports: stopped.reports.length, nonces: stopped.reports.map((row) => row.nonce) });
  } else if (phase === 'restart') {
    const saved = JSON.parse(readFileSync(statePath, 'utf8'));
    await web(base, metadata);
    await connectors(base, saved.cookie, metadata.connectorIds);
    stage = 'fresh restart status without exchange or JWKS';
    const after = await proof(fixture, saved.reports + 1);
    assert.equal(after.exchanges, saved.exchanges, 'restarting a Cluster must not redeem another code');
    assert.equal(after.jwks, saved.jwks, 'stored sessions must not revalidate a JWT');
    assert(after.reports.some((row) => !saved.nonces.includes(row.nonce)));
  } else if (phase === 'logout') {
    stage = 'local session logout and revocation';
    const saved = JSON.parse(readFileSync(statePath, 'utf8'));
    const response = await request(base, '/auth/logout', { method: 'POST',
      headers: { Cookie: saved.cookie, Origin: new URL(base).origin } });
    assert.equal(response.status, 204);
    assert((response.headers.get('set-cookie') ?? '').includes('Max-Age=0'));
    assert.equal((await request(base, '/api/connectors', { headers: { Cookie: saved.cookie } })).status, 401);
  } else if (phase === 'seed-refused') {
    stage = 'invalid seed keeps the diagnostic root but cannot publish business';
    const entry = await request(base, '/auth/storage-entry');
    assert.equal(entry.status, 200);
    const status = await entry.json();
    assert.equal(status.mode, 'CLOUD');
    assert.equal(status.entry, 'LOGIN');
    const response = await request(base, '/api/connectors');
    assert.equal(response.status, 503);
    assert((response.headers.get('content-type') ?? '').includes('application/json'));
    assert.equal((await response.json()).code, 'control.storage-unavailable');
    assert.equal((await request(base, '/auth/bootstrap', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: '{}' })).status, 403);
  } else if (phase === 'onprem') {
    await web(base, metadata);
    stage = 'onprem login and Bearer API';
    const response = await request(base, '/auth/login', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: USER, password: PASSWORD }) });
    assert.equal(response.status, 200);
    const login = await response.json();
    assert.equal(typeof login.token, 'string');
    const rows = await request(base, '/api/connectors', { headers: { Authorization: `Bearer ${login.token}` } });
    assert.equal(rows.status, 200);
    assert.deepEqual((await rows.json()).connectors.map((row) => row.id), ['mongodb']);
    assert.equal((await request(base, '/auth/exchange?code=onprem-refused')).status, 403);
    save(statePath, { token: login.token, issuer: login.issuer });
  } else if (phase === 'onprem-restart') {
    stage = 'onprem persisted user and issuer with default ephemeral signing key';
    const saved = JSON.parse(readFileSync(statePath, 'utf8'));
    // The unchanged default intentionally invalidates local JWTs across a restart when no signing
    // secret is configured. Persistence here means the same local user and Cluster issuer, not a new
    // promise that an ephemeral signing key survives or becomes a fifth Cloud setting.
    assert.equal((await request(base, '/api/connectors', {
      headers: { Authorization: `Bearer ${saved.token}` } })).status, 401);
    const response = await request(base, '/auth/login', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: USER, password: PASSWORD }) });
    assert.equal(response.status, 200);
    const login = await response.json();
    assert.equal(login.issuer, saved.issuer);
    const rows = await request(base, '/api/connectors', { headers: { Authorization: `Bearer ${login.token}` } });
    assert.equal(rows.status, 200);
    assert.deepEqual((await rows.json()).connectors.map((row) => row.id), ['mongodb']);
  } else {
    throw new Error('unknown runtime probe phase');
  }
  console.log(`PASS: real image HTTP phase ${phase}`);
}

async function main(args) {
  if (args[0] === 'serve') {
    const peer = createWireFixture({ issuer: 'http://gateway:3000', version: args[1] });
    const server = createServer(peer.handler);
    server.requestTimeout = 15000;
    server.headersTimeout = 10000;
    server.listen(3000, '0.0.0.0');
    for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => server.close(() => process.exit(0)));
  } else if (args[0] === 'verify' && args.length === 6) {
    await verify(...args.slice(1));
  } else {
    throw new Error('invalid runtime probe arguments');
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch(() => {
    console.error(`FAIL: runtime HTTP probe at ${stage}; no credentials are printed`);
    process.exitCode = 1;
  });
}
