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
// Each event is written to its own path, so two installs never try to write the same file. That is a
// path collision avoided, NOT a conflict avoided: the Contents API does not write a path, it creates a
// commit on a branch, and two writes advancing the same branch from the same parent race -- the loser
// gets 409 regardless of how distinct the paths were. So the retry below is not optional bookkeeping;
// without it a concurrent install is dropped from the denominator with no signal anywhere.
//
// The weekly capture job folds these files into one JSONL, which is the shape the report counts from.

export const STORED_FIELDS = [
  'installation_id', 'version', 'os', 'arch', 'entrypoint', 'country', 'timestamp',
];

const OS = new Set(['darwin', 'linux']);
const ARCH = new Set(['arm64', 'x64']);
const ENTRYPOINT = new Set(['cli', 'quickstart']);

// The client's clock is the client's, and the timestamp decides which week an install is counted in.
// Unbounded, a wrong clock -- or anyone at all, the endpoint being public -- writes into a week that
// has already been reported, and no later run would disagree. Bounded generously: NTP-synced machines
// are within seconds, and the point is to keep an event in roughly its own week rather than to police
// drift. An event outside the window is refused rather than re-stamped: re-stamping would file a
// measurement under a time it did not happen.
const MAX_SKEW_MS = 48 * 60 * 60 * 1000;

// A request body larger than this cannot be a valid event -- every field is bounded above -- so
// reading further is work done on behalf of whoever sent it.
export const MAX_BODY_BYTES = 2048;

const ID_RE = /^[A-Za-z0-9][A-Za-z0-9-]{7,63}$/;
const VERSION_RE = /^[A-Za-z0-9][A-Za-z0-9.+-]{0,31}$/;
const TS_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;
const COUNTRY_RE = /^[A-Z]{2}$/;

// Returns { event } or { error }. Pure: no network, no environment, and the clock is an argument
// rather than a read -- which is what keeps the stored shape assertable without a runtime.
export function buildEvent(body, headers, now = Date.now()) {
  if (!body || typeof body !== 'object') return { error: 'body must be an object' };

  // Cheap first, and before anything is parsed field by field.
  const size = typeof body === 'string' ? body.length : JSON.stringify(body).length;
  if (size > MAX_BODY_BYTES) return { error: 'body too large' };

  const id = body.installation_id;
  if (typeof id !== 'string' || !ID_RE.test(id)) return { error: 'bad installation_id' };

  const version = body.version;
  if (typeof version !== 'string' || !VERSION_RE.test(version)) return { error: 'bad version' };

  if (!OS.has(body.os)) return { error: 'bad os' };
  if (!ARCH.has(body.arch)) return { error: 'bad arch' };
  if (!ENTRYPOINT.has(body.entrypoint)) return { error: 'bad entrypoint' };

  const timestamp = body.timestamp;
  if (typeof timestamp !== 'string' || !TS_RE.test(timestamp)) return { error: 'bad timestamp' };
  const at = Date.parse(timestamp);
  if (!Number.isFinite(at) || Math.abs(at - now) > MAX_SKEW_MS) return { error: 'timestamp out of range' };

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

// A unique path per event, so no two installs write the same file. Ref contention is handled in store().
export function eventPath(event, nonce) {
  const stamp = event.timestamp.replace(/[:]/g, '-');
  return `funnel/events/${stamp}-${nonce}.json`;
}

// 409 is the expected answer to a concurrent write, not an error to give up on: the paths differ, only
// the branch tip is contended, so the same request succeeds once the other commit lands. Retried a
// bounded number of times -- a serverless function that retries forever is a bill, not a fix.
export const STORE_ATTEMPTS = 4;

export async function store(event, nonce, fetchImpl = fetch, sleep = defaultSleep) {
  const token = process.env.FUNNEL_STORE_TOKEN;
  const repo = process.env.FUNNEL_STORE_REPO || 'tapstate/gtm';
  if (!token) throw new Error('FUNNEL_STORE_TOKEN is not configured');

  const path = eventPath(event, nonce);
  const body = JSON.stringify({
    message: `funnel: install event ${event.timestamp}`,
    content: Buffer.from(JSON.stringify(event) + '\n', 'utf8').toString('base64'),
  });

  let last = 0;
  for (let attempt = 0; attempt < STORE_ATTEMPTS; attempt += 1) {
    const res = await fetchImpl(`https://api.github.com/repos/${repo}/contents/${path}`, {
      method: 'PUT',
      headers: {
        authorization: `Bearer ${token}`,
        accept: 'application/vnd.github+json',
        'content-type': 'application/json',
      },
      body,
    });
    if (res.ok) return;
    last = res.status;
    // Only ref contention and a server-side fault can succeed on a second try. A rejected credential
    // is rejected identically forever, and 422 means this exact path already exists -- which the same
    // path will keep meaning. Retrying either burns the function's budget and delays only the failure.
    if (res.status !== 409 && res.status < 500) break;
    if (attempt + 1 < STORE_ATTEMPTS) await sleep(50 * (attempt + 1));
  }
  throw new Error(`store failed: ${last}`);
}

function defaultSleep(ms) {
  return new Promise((resolve) => { setTimeout(resolve, ms); });
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
  } catch (err) {
    // The installer already succeeded on the user's machine and discards this status entirely
    // (`curl -fsS ... || :`), so the code we answer with cannot reach the user either way. That is
    // exactly why it must not be 204: a 204 for a dropped event makes the platform's own success rate
    // read 100% while every event is lost, and nothing else is watching this endpoint. The reason
    // goes to the log, never to the caller -- the endpoint is public.
    console.error(`event: store failed: ${err?.message ?? err}`);
    res.status(500).end();
    return;
  }
  res.status(204).end();
}
