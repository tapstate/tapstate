// Tests for the install-event receiver's decision logic, run with `node --test` -- no dependencies
// and no package.json, so this adds a test surface without adding a toolchain.
//
// Everything here exercises buildEvent(), which is pure on purpose: what the endpoint stores has to be
// assertable without a network, a Vercel runtime, or a GitHub token.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildEvent, STORED_FIELDS } from './event.js';

const good = {
  installation_id: '7f3a1b2c-0000-4444-8888-abcdefabcdef',
  version: '0.3.0',
  os: 'darwin',
  arch: 'arm64',
  entrypoint: 'cli',
  timestamp: '2026-09-02T01:02:03Z',
};
const headers = { 'x-vercel-ip-country': 'SG' };

test('a valid event stores exactly the agreed fields and nothing else', () => {
  const { event, error } = buildEvent(good, headers);
  assert.equal(error, undefined);
  assert.deepEqual(Object.keys(event).sort(), [...STORED_FIELDS].sort());
  assert.equal(event.country, 'SG');
  assert.equal(event.version, '0.3.0');
});

// The defect this guards is the one-line implementation: {...body, country}. It passes every test
// that only checks the fields we do want, and quietly stores whatever a client chose to send.
test('fields the client invented are dropped, not stored', () => {
  const { event } = buildEvent(
    { ...good, hostname: 'laptop.local', ip: '203.0.113.9', db_url: 'mysql://u:p@h/db' },
    headers,
  );
  assert.deepEqual(Object.keys(event).sort(), [...STORED_FIELDS].sort());
  assert.equal(JSON.stringify(event).includes('203.0.113'), false);
  assert.equal(JSON.stringify(event).includes('laptop'), false);
  assert.equal(JSON.stringify(event).includes('mysql'), false);
});

// The address must never reach the stored record, and the strongest way to guarantee that is for the
// code never to read it. Vercel resolves the country upstream, so the address is not ours to handle.
test('an address present on the request never reaches the record', () => {
  const { event } = buildEvent(good, {
    ...headers,
    'x-forwarded-for': '203.0.113.9, 198.51.100.4',
    'x-real-ip': '203.0.113.9',
    'user-agent': 'curl/8.4.0 (some-very-identifying-build)',
  });
  const asText = JSON.stringify(event);
  assert.equal(asText.includes('203.0.113'), false);
  assert.equal(asText.includes('198.51.100'), false);
  assert.equal(asText.includes('curl/'), false);
});

test('a missing country becomes ZZ rather than being invented or omitted', () => {
  const { event } = buildEvent(good, {});
  assert.equal(event.country, 'ZZ');
});

test('a malformed installation id is refused', () => {
  for (const bad of ['', 'x', 'no spaces allowed here', 'a'.repeat(200), '../../etc/passwd']) {
    const { error } = buildEvent({ ...good, installation_id: bad }, headers);
    assert.ok(error, `expected refusal for installation_id=${JSON.stringify(bad)}`);
  }
});

test('an unknown entry point is refused rather than recorded as-is', () => {
  const { error } = buildEvent({ ...good, entrypoint: 'somewhere-else' }, headers);
  assert.ok(error);
});

test('an unknown platform is refused', () => {
  assert.ok(buildEvent({ ...good, os: 'plan9' }, headers).error);
  assert.ok(buildEvent({ ...good, arch: 'sparc' }, headers).error);
});

test('an oversized version string is refused', () => {
  assert.ok(buildEvent({ ...good, version: 'v'.repeat(100) }, headers).error);
});

test('the timestamp is taken from the request only when it is well formed', () => {
  assert.ok(buildEvent({ ...good, timestamp: 'yesterday' }, headers).error);
});
