// Receives one install event and stores it.
//
// What is stored is a fixed, closed set of fields. It is built by naming each one, never by spreading
// the request body: {...body, country} passes every test that checks the fields we do want, while
// quietly storing whatever a client chose to send. The client is an installer we ship, but the
// endpoint is public and anyone can post to it.
//
// The address is never handled here at all. Vercel resolves the country upstream and hands it over as
// a header, so there is no step in this code that reads an address and discards it -- the strongest
// form of "the address is never stored" is code that never touches one. The same goes for the
// User-Agent: the platform is reported explicitly by the installer, which is both more accurate than
// parsing a UA and far less identifying.
//
// Each event is written to its own path. Appending to a shared file would need read-modify-write with
// a SHA and a retry loop for concurrent installs; a unique path per event cannot conflict at all. The
// weekly capture job compacts them.

export const STORED_FIELDS = [
  'installation_id', 'version', 'os', 'arch', 'entrypoint', 'country', 'timestamp',
];

const OS = new Set(['darwin', 'linux']);
const ARCH = new Set(['arm64', 'x64']);
const ENTRYPOINT = new Set(['cli', 'quickstart']);

const ID_RE = /^[A-Za-z0-9][A-Za-z0-9-]{7,63}$/;
const VERSION_RE = /^[A-Za-z0-9][A-Za-z0-9.+-]{0,31}$/;
const TS_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;
const COUNTRY_RE = /^[A-Z]{2}$/;

// Returns { event } or { error }. Pure: no clock, no network, no environment -- which is what makes
// the stored shape assertable without a runtime.
export function buildEvent(body, headers) {
  if (!body || typeof body !== 'object') return { error: 'body must be an object' };

  const id = body.installation_id;
  if (typeof id !== 'string' || !ID_RE.test(id)) return { error: 'bad installation_id' };

  const version = body.version;
  if (typeof version !== 'string' || !VERSION_RE.test(version)) return { error: 'bad version' };

  if (!OS.has(body.os)) return { error: 'bad os' };
  if (!ARCH.has(body.arch)) return { error: 'bad arch' };
  if (!ENTRYPOINT.has(body.entrypoint)) return { error: 'bad entrypoint' };

  const timestamp = body.timestamp;
  if (typeof timestamp !== 'string' || !TS_RE.test(timestamp)) return { error: 'bad timestamp' };

  // Unknown to us is "ZZ", never guessed and never left out: a missing key and an unknown origin are
  // different facts, and a reader cannot tell them apart once the key is absent.
  const raw = headers?.['x-vercel-ip-country'];
  const country = typeof raw === 'string' && COUNTRY_RE.test(raw) ? raw : 'ZZ';

  return {
    event: {
      installation_id: id,
      version,
      os: body.os,
      arch: body.arch,
      entrypoint: body.entrypoint,
      country,
      timestamp,
    },
  };
}

// A unique path per event, so concurrent installs never contend.
export function eventPath(event, nonce) {
  const stamp = event.timestamp.replace(/[:]/g, '-');
  return `funnel/events/${stamp}-${nonce}.json`;
}

async function store(event, nonce, fetchImpl = fetch) {
  const token = process.env.FUNNEL_STORE_TOKEN;
  const repo = process.env.FUNNEL_STORE_REPO || 'tapstate/gtm';
  if (!token) throw new Error('FUNNEL_STORE_TOKEN is not configured');

  const path = eventPath(event, nonce);
  const res = await fetchImpl(`https://api.github.com/repos/${repo}/contents/${path}`, {
    method: 'PUT',
    headers: {
      authorization: `Bearer ${token}`,
      accept: 'application/vnd.github+json',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      message: `funnel: install event ${event.timestamp}`,
      content: Buffer.from(JSON.stringify(event) + '\n', 'utf8').toString('base64'),
    }),
  });
  if (!res.ok) throw new Error(`store failed: ${res.status}`);
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ error: 'method not allowed' });
    return;
  }
  const { event, error } = buildEvent(req.body, req.headers);
  if (error) {
    // The reason is not echoed to the caller: this endpoint is public, and a validator that explains
    // itself is a validator that teaches whoever is probing it what shape to send next.
    res.status(400).end();
    return;
  }
  try {
    await store(event, crypto.randomUUID().slice(0, 8));
  } catch {
    // The installer already succeeded on the user's machine; nothing about that outcome depends on
    // this. Failing loudly here would only turn our outage into their error message.
  }
  res.status(204).end();
}
