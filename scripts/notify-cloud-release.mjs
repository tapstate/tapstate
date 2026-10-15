#!/usr/bin/env node
import { pathToFileURL } from 'node:url';

function required(env, name) {
  const value = env[name]?.trim();
  if (!value) throw new Error(`${name} is required for Cloud version registration`);
  return value;
}

export async function notifyCloudRelease(env = process.env, fetchImpl = fetch) {
  const version = required(env, 'RELEASE_VERSION');
  const tag = required(env, 'RELEASE_TAG');
  const commitId = required(env, 'RELEASE_SHA');
  const branch = required(env, 'RELEASE_BRANCH');
  const image = required(env, 'CLOUD_IMAGE');
  const imageTag = required(env, 'CLOUD_IMAGE_TAG');
  const imageDigest = required(env, 'CLOUD_IMAGE_DIGEST');
  const releaseCode = required(env, 'CLOUD_RELEASE_CODE');
  if (!/^\d+\.\d+\.\d+$/.test(version) || tag !== `v${version}`) {
    throw new Error('The release version and tag must identify the same numeric release');
  }
  if (!/^[0-9a-f]{40}$/.test(commitId)) throw new Error('RELEASE_SHA must be a full commit SHA');
  if (!['ALPHA', 'BETA', 'RC', 'STABLE'].includes(releaseCode)) {
    throw new Error('CLOUD_RELEASE_CODE must be ALPHA, BETA, RC or STABLE');
  }
  if (!/^sha256:[0-9a-f]{64}$/.test(imageDigest)
      || image !== `ghcr.io/tapstate/tapstate-cloud@${imageDigest}`
      || imageTag !== `ghcr.io/tapstate/tapstate-cloud:${version}`) {
    throw new Error('The Cloud image must match the published release version and verified digest');
  }
  let endpoint;
  try {
    endpoint = new URL(required(env, 'CLOUD_RELEASE_API_URL'));
  } catch {
    throw new Error('CLOUD_RELEASE_API_URL must be an HTTPS cluster-versions endpoint');
  }
  if (endpoint.protocol !== 'https:' || endpoint.username || endpoint.password
      || endpoint.search || endpoint.hash
      || !endpoint.pathname.endsWith('/v1/api/cluster-versions')) {
    throw new Error('CLOUD_RELEASE_API_URL must be an HTTPS cluster-versions endpoint');
  }
  const token = required(env, 'CLOUD_CLUSTER_VERSION_RELEASE_TOKEN');
  if (/[\r\n]/.test(token)) throw new Error('The Cloud release token must not contain line breaks');
  const extra = {
    tag, image, imageTag, imageDigest,
    releaseUrl: `https://github.com/tapstate/tapstate/releases/tag/${tag}`,
    workflowUrl: required(env, 'RELEASE_RUN_URL'),
  };

  async function request(url, options) {
    try {
      return await fetchImpl(url, { redirect: 'error', signal: AbortSignal.timeout(30_000), ...options });
    } catch {
      // Neither transport errors nor response bodies are safe to echo with a release credential.
      throw new Error('Cloud version registration request failed or timed out; delivery may be uncertain');
    }
  }
  async function json(response) {
    try { return await response.json(); }
    catch { throw new Error('Cloud version registration received an invalid JSON response'); }
  }

  // A newer release can overtake this run while its checked Cloud archive is being published.
  const latest = await request('https://api.github.com/repos/tapstate/tapstate/releases/latest', {
    headers: { Accept: 'application/vnd.github+json' },
  });
  if (latest.status !== 200) throw new Error(`Could not verify the current release (HTTP ${latest.status})`);
  const published = await json(latest);
  if (!/^v\d+\.\d+\.\d+$/.test(published?.tag_name) || published.draft || published.prerelease) {
    throw new Error('GitHub did not return a published numeric release');
  }
  if (published.tag_name !== tag) {
    return `Skipped Cloud version registration: ${tag} is no longer the current release`;
  }

  // The API allocates a new sequence on insert. Never retry a POST or accept an unverified 409.
  const response = await request(endpoint.href, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ version, releaseCode, releaseNotes: extra.releaseUrl,
      git: { branch, commitId, extra } }),
  });
  if (response.status !== 201) {
    const detail = response.status === 409 ? '; version already exists and must be reconciled before retrying' : '';
    throw new Error(`Cloud version registration failed (HTTP ${response.status})${detail}`);
  }
  const receipt = await json(response);
  const saved = receipt?.data;
  if (receipt?.code !== 'ok' || saved?.version !== version || saved?.releaseCode !== releaseCode
      || saved?.state !== 'ACTIVE' || saved?.git?.commitId !== commitId || saved?.git?.branch !== branch
      || Object.entries(extra).some(([key, value]) => saved?.git?.extra?.[key] !== value)) {
    throw new Error('Cloud version registration receipt does not match the published release');
  }
  return `Registered Cloud version ${version}: ${image}`;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { console.log(await notifyCloudRelease()); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
