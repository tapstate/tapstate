// Tests for the install-event receiver's decision logic, run with `node --test` -- no dependencies
// and no package.json, so this adds a test surface without adding a toolchain.
//
// Everything here exercises buildEvent(), which is pure on purpose: what the endpoint stores has to be
// assertable without a network, a Vercel runtime, or a GitHub token.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import handler, { buildEvent, store, STORED_FIELDS, STORE_ATTEMPTS, MAX_BODY_BYTES } from './event.js';

const good = {
  installation_id: '7f3a1b2c-0000-4444-8888-abcdefabcdef',
  version: '0.3.0',
  os: 'darwin',
  arch: 'arm64',
  entrypoint: 'cli',
  timestamp: '2026-09-02T01:02:03Z',
};
const headers = { 'x-vercel-ip-country': 'SG' };

// The clock is injected, never read. A fixture with a hard-coded timestamp and a real clock is a test
// that passes today and fails on a date nobody chose -- and the skew rule below would be what breaks
// it, long after whoever wrote it stopped watching.
const NOW = Date.parse(good.timestamp);

test('a valid event stores exactly the agreed fields and nothing else', () => {
  const { event, error } = buildEvent(good, headers, NOW);
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
    NOW,
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
  }, NOW);
  const asText = JSON.stringify(event);
  assert.equal(asText.includes('203.0.113'), false);
  assert.equal(asText.includes('198.51.100'), false);
  assert.equal(asText.includes('curl/'), false);
});

test('a missing country becomes ZZ rather than being invented or omitted', () => {
  const { event } = buildEvent(good, {}, NOW);
  assert.equal(event.country, 'ZZ');
});

test('a malformed installation id is refused', () => {
  for (const bad of ['', 'x', 'no spaces allowed here', 'a'.repeat(200), '../../etc/passwd']) {
    const { error } = buildEvent({ ...good, installation_id: bad }, headers, NOW);
    assert.ok(error, `expected refusal for installation_id=${JSON.stringify(bad)}`);
  }
});

test('an unknown entry point is refused rather than recorded as-is', () => {
  const { error } = buildEvent({ ...good, entrypoint: 'somewhere-else' }, headers, NOW);
  assert.ok(error);
});

test('an unknown platform is refused', () => {
  assert.ok(buildEvent({ ...good, os: 'plan9' }, headers, NOW).error);
  assert.ok(buildEvent({ ...good, arch: 'sparc' }, headers, NOW).error);
});

test('an oversized version string is refused', () => {
  assert.ok(buildEvent({ ...good, version: 'v'.repeat(100) }, headers, NOW).error);
});

test('the timestamp is taken from the request only when it is well formed', () => {
  assert.ok(buildEvent({ ...good, timestamp: 'yesterday' }, headers, NOW).error);
});

// --- storing ---------------------------------------------------------------------------------------
// buildEvent decides what is stored; these decide whether it is stored at all. A concurrent install
// and a dead credential are the two ways an event disappears, and both used to answer 204.

const NO_SLEEP = () => Promise.resolve();

// AWAITS fn. A non-async version restores the variable the moment fn returns its promise -- which is
// before anything inside it has run past its first await. That held together only while every caller
// read the token synchronously; the first one that did not saw an unset token and a 500, and the
// helper looked innocent because the tests it broke were not the ones it was written for.
async function withToken(fn) {
  const before = process.env.FUNNEL_STORE_TOKEN;
  process.env.FUNNEL_STORE_TOKEN = 'test-token';
  try { return await fn(); } finally {
    if (before === undefined) delete process.env.FUNNEL_STORE_TOKEN;
    else process.env.FUNNEL_STORE_TOKEN = before;
  }
}

// handler() reads the real clock, so its fixture carries a real timestamp. Hard-coding one here is
// the same time bomb as above, one layer further in.
function nowEvent() {
  return { ...good, timestamp: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z') };
}

function fakeRes() {
  return { code: null, status(c) { this.code = c; return this; }, end() { return this; }, json() { return this; } };
}

// The Contents API writes a commit on a branch, so two installs racing get 409 for the loser however
// distinct their paths are. Without a retry that event is gone -- silently, from the one figure this
// whole line exists to produce.
test('a 409 from a concurrent write is retried until it lands', async () => {
  await withToken(async () => {
    const seen = [];
    const fetchImpl = async () => {
      seen.push(1);
      return seen.length < 3 ? { ok: false, status: 409 } : { ok: true, status: 201 };
    };
    const { event } = buildEvent(good, headers, NOW);
    await store(event, 'abcd1234', fetchImpl, NO_SLEEP);
    assert.equal(seen.length, 3, 'expected the 409s to be retried, not accepted as failure');
  });
});

test('a retry that never wins gives up rather than looping forever', async () => {
  await withToken(async () => {
    let calls = 0;
    const fetchImpl = async () => { calls += 1; return { ok: false, status: 409 }; };
    const { event } = buildEvent(good, headers, NOW);
    await assert.rejects(() => store(event, 'abcd1234', fetchImpl, NO_SLEEP), /store failed: 409/);
    assert.equal(calls, STORE_ATTEMPTS);
  });
});

// A rejected credential answers the same way forever. Retrying it spends the function's budget to
// arrive at the identical failure, which on a serverless platform is a bill rather than a fix.
test('a rejected credential is not retried', async () => {
  await withToken(async () => {
    let calls = 0;
    const fetchImpl = async () => { calls += 1; return { ok: false, status: 401 }; };
    const { event } = buildEvent(good, headers, NOW);
    await assert.rejects(() => store(event, 'abcd1234', fetchImpl, NO_SLEEP), /store failed: 401/);
    assert.equal(calls, 1, 'a 401 will be a 401 on every attempt');
  });
});

// The installer discards this status entirely, so it cannot reach a user either way -- which is the
// reason it must not be 204. A 204 for a dropped event makes the platform's own success rate read
// 100% while every event is lost, and nothing else watches this endpoint.
test('an event that could not be stored answers non-2xx, not 204', async () => {
  const before = process.env.FUNNEL_STORE_TOKEN;
  delete process.env.FUNNEL_STORE_TOKEN;               // store() throws: nowhere to put it
  const errors = [];
  const realError = console.error;
  console.error = (m) => errors.push(m);
  try {
    const res = fakeRes();
    await handler({ method: 'POST', body: nowEvent(), headers }, res);
    assert.equal(res.code, 500);
    assert.ok(errors.some((m) => String(m).includes('store failed')), 'the reason belongs in the log');
  } finally {
    console.error = realError;
    if (before !== undefined) process.env.FUNNEL_STORE_TOKEN = before;
  }
});

test('a stored event answers 204 and says nothing else', async () => {
  await withToken(async () => {
    const res = fakeRes();
    globalThis.fetch = async () => ({ ok: true, status: 201 });
    await handler({ method: 'POST', body: nowEvent(), headers }, res);
    assert.equal(res.code, 204);
  });
});

// --- bounds ----------------------------------------------------------------------------------------
// The timestamp decides which week an install lands in, and it comes from the client. Unbounded, a
// machine with a wrong clock -- or anyone at all, the endpoint being public -- writes into a week
// already reported, and nothing later would disagree with it.
test('a timestamp far from now is refused rather than filed in the wrong week', () => {
  const lastYear = { ...good, timestamp: '2025-09-02T01:02:03Z' };
  assert.ok(buildEvent(lastYear, headers, NOW).error, 'a year-old event was accepted');
  const nextYear = { ...good, timestamp: '2027-09-02T01:02:03Z' };
  assert.ok(buildEvent(nextYear, headers, NOW).error, 'an event from the future was accepted');
});

// Generous on purpose: the rule exists to keep an event in roughly its own week, not to police drift.
test('ordinary clock drift is still accepted', () => {
  const drifted = { ...good, timestamp: '2026-09-01T13:02:03Z' };  // ~12h earlier
  assert.equal(buildEvent(drifted, headers, NOW).error, undefined);
});

test('a body far larger than any valid event is refused before it is parsed field by field', () => {
  const huge = { ...good, padding: 'x'.repeat(MAX_BODY_BYTES * 2) };
  assert.ok(buildEvent(huge, headers, NOW).error);
});

// --- reading the body ------------------------------------------------------------------------------
// The shape the platform hands over is not ours to assume. Measured in production on 0.4.4: req.body
// was not a parsed object, so a perfectly valid event answered 400 -- every event rejected, the
// installer swallowing the status by design, and the deployment probe green throughout because
// "rejected by the validator" and "never read at all" were the same 400.

function streamReq(text, extra = {}) {
  // an IncomingMessage-shaped request whose body is only available by reading it
  return {
    method: 'POST',
    headers,
    ...extra,
    async *[Symbol.asyncIterator]() { yield text; },
  };
}

test('a body the platform did not parse is read from the request', async () => {
  await withToken(async () => {
    let stored = null;
    globalThis.fetch = async (url, init) => { stored = init; return { ok: true, status: 201 }; };
    const res = fakeRes();
    await handler(streamReq(JSON.stringify(nowEvent())), res);
    assert.equal(res.code, 204, 'a valid event arriving as an unparsed stream must be stored');
    assert.ok(stored, 'nothing was written');
  });
});

test('a body handed over as a string is parsed rather than refused', async () => {
  await withToken(async () => {
    globalThis.fetch = async () => ({ ok: true, status: 201 });
    const res = fakeRes();
    await handler({ method: 'POST', headers, body: JSON.stringify(nowEvent()) }, res);
    assert.equal(res.code, 204);
  });
});

// The distinction the deployment probe depends on: 400 means the JSON was read and the event was
// wrong; 415 means nothing readable arrived. Collapsing them is what hid the outage.
test('a request with no readable JSON body answers 415, not 400', async () => {
  const res = fakeRes();
  await handler(streamReq(''), res);
  assert.equal(res.code, 415);
});

test('a body that is not JSON at all answers 415, not 400', async () => {
  const res = fakeRes();
  await handler(streamReq('not json at all'), res);
  assert.equal(res.code, 415);
});

test('JSON that reads fine but is not a valid event still answers 400', async () => {
  const res = fakeRes();
  await handler(streamReq(JSON.stringify({ installation_id: 'no' })), res);
  assert.equal(res.code, 400, 'this is what proves the parsing ran at all');
});

test('a streamed body past the bound is refused while streaming, not after', async () => {
  const res = fakeRes();
  const huge = 'x'.repeat(MAX_BODY_BYTES * 3);
  let yielded = 0;
  const req = {
    method: 'POST', headers,
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < 3; i += 1) { yielded += 1; yield huge; }
    },
  };
  await handler(req, res);
  assert.equal(res.code, 415);
  assert.equal(yielded, 1, 'it kept reading after the bound was already exceeded');
});

// A platform that fails to parse and leaves an empty object behind is indistinguishable from one
// handed an empty body -- so an empty object is not taken as the body. Without this, a valid event
// would be refused without the request ever being read, which is the shape that shipped.
test('an empty parsed body is not taken as the event; the request is read instead', async () => {
  await withToken(async () => {
    globalThis.fetch = async () => ({ ok: true, status: 201 });
    const res = fakeRes();
    await handler(streamReq(JSON.stringify(nowEvent()), { body: {} }), res);
    assert.equal(res.code, 204, 'req.body was {} and the real event was in the stream');
  });
});
