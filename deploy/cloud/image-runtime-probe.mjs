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
  const state = { exchanges: 0, jwks: 0, reports: [], failures: 0 };
  let consumed = false;
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
      const body = JSON.parse(bytes);
      if (req.url === '/v1/api/auth/exchange' && req.method === 'POST') {
        state.exchanges++;
        assert.equal(req.headers['x-cluster-identity-secret'], TOKEN);
        assert.deepEqual(body, { exchangeCode: CODE, clusterId: CLUSTER });
        if (consumed) return reply(res, 401, { code: 'exchange.code-consumed', msg: 'consumed' });
        consumed = true;
        const now = Math.floor(Date.now() / 1000);
        const head = encode({ alg: 'RS256', typ: 'JWT', kid: jwk.kid });
        const payload = encode({ sub: 'image-user@example.test', iss: issuer, aud: audience,
          iat: now, exp: now + 900, jti: 'image-runtime-jti', user_id: 'image-runtime-user',
          org_id: 'image-runtime-org', cluster_id: CLUSTER, scope: ['workload:read', 'workload:write'] });
        const signed = `${head}.${payload}`;
        const jwt = `${signed}.${sign('RSA-SHA256', Buffer.from(signed), privateKey).toString('base64url')}`;
        return reply(res, 200, envelope({ jwt, expiresAt: new Date((now + 900) * 1000).toISOString(),
          jti: 'image-runtime-jti', userEmail: 'image-user@example.test',
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

async function web(base, metadata) {
  stage = 'Web entry bytes';
  const root = await request(base, '/');
  assert.equal(root.status, 200);
  const html = await root.text();
  assert.equal(sha(html), metadata.webFiles['index.html']);
  for (const path of ['/login', '/pipelines/runtime-image-smoke/edit']) {
    const page = await request(base, path);
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

async function verify(phase, base, fixture, metadataPath, statePath) {
  const metadata = JSON.parse(readFileSync(metadataPath, 'utf8'));
  if (phase === 'cloud') {
    await web(base, metadata);
    stage = 'anonymous API rejection';
    assert.equal((await request(base, '/api/connectors')).status, 401);
    stage = 'SDK exchange and local Cookie creation';
    const exchanged = await request(base, `/auth/exchange?code=${CODE}`);
    assert.equal(exchanged.status, 302);
    assert.equal(exchanged.headers.get('location'), '/');
    assert.equal(exchanged.headers.get('cache-control'), 'no-store');
    assert.equal(await exchanged.text(), '');
    const cookie = cookieHeader(exchanged.headers.get('set-cookie') ?? '');
    await connectors(base, cookie, metadata.connectorIds);
    stage = 'mixed Bearer credential rejection';
    assert.equal((await request(base, '/api/connectors', {
      headers: { Authorization: `Bearer ${TOKEN}` } })).status, 401);
    stage = 'initial SDK exchange, JWKS and status proof';
    const before = await proof(fixture);
    assert.equal(before.exchanges, 1);
    assert.equal(before.jwks, 1);
    save(statePath, { cookie, reports: before.reports.length, nonces: before.reports.map((row) => row.nonce) });
  } else if (phase === 'cloud-stopped') {
    stage = 'C2 baseline after the first JVM has stopped';
    const saved = JSON.parse(readFileSync(statePath, 'utf8'));
    const stopped = await proof(fixture);
    assert.equal(stopped.exchanges, 1);
    assert.equal(stopped.jwks, 1);
    save(statePath, { ...saved, reports: stopped.reports.length, nonces: stopped.reports.map((row) => row.nonce) });
  } else if (phase === 'restart') {
    const saved = JSON.parse(readFileSync(statePath, 'utf8'));
    await web(base, metadata);
    await connectors(base, saved.cookie, metadata.connectorIds);
    stage = 'fresh restart status without exchange or JWKS';
    const after = await proof(fixture, saved.reports + 1);
    assert.equal(after.exchanges, 1, 'restarting a Cluster must not redeem another code');
    assert.equal(after.jwks, 1, 'stored sessions must not revalidate a JWT');
    assert(after.reports.some((row) => !saved.nonces.includes(row.nonce)));
  } else if (phase === 'logout') {
    stage = 'local session logout and revocation';
    const saved = JSON.parse(readFileSync(statePath, 'utf8'));
    const response = await request(base, '/auth/logout', { method: 'POST',
      headers: { Cookie: saved.cookie, Origin: new URL(base).origin } });
    assert.equal(response.status, 204);
    assert((response.headers.get('set-cookie') ?? '').includes('Max-Age=0'));
    assert.equal((await request(base, '/api/connectors', { headers: { Cookie: saved.cookie } })).status, 401);
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
    assert.deepEqual((await rows.json()).connectors, []);
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
    assert.deepEqual((await rows.json()).connectors, []);
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
