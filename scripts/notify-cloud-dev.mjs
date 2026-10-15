#!/usr/bin/env node
import { pathToFileURL } from 'node:url';

function required(env, name) {
  const value = env[name]?.trim();
  if (!value) throw new Error(`${name} is required for Cloud dev version registration`);
  return value;
}

export function validateCloudDevConfiguration(env = process.env) {
  const endpoint = required(env, 'CLOUD_RELEASE_API_URL');
  if (endpoint !== 'https://dev.cloud.tapstate.com/v1/api/cluster-versions') {
    throw new Error('Cloud dev registration requires the dev HTTPS cluster-versions endpoint');
  }
  const releaseCode = required(env, 'CLOUD_RELEASE_CODE');
  if (!['ALPHA', 'BETA', 'RC'].includes(releaseCode)) {
    throw new Error('Cloud dev registration requires an ALPHA, BETA or RC channel');
  }
  const token = required(env, 'CLOUD_CLUSTER_VERSION_RELEASE_TOKEN');
  if (/[\r\n]/.test(token)) throw new Error('The Cloud release token must not contain line breaks');
  return { endpoint, releaseCode, token };
}

export async function notifyCloudDev(env = process.env, fetchImpl = fetch) {
  const { endpoint, releaseCode, token } = validateCloudDevConfiguration(env);
  const buildVersion = required(env, 'DEV_VERSION');
  const commitId = required(env, 'DEV_SHA');
  const webCommitId = required(env, 'DEV_WEB_SHA');
  if (!/^\d+\.\d+\.\d+$/.test(buildVersion)) {
    throw new Error('DEV_VERSION must be a numeric release version');
  }
  if (!/^[0-9a-f]{40}$/.test(commitId) || !/^[0-9a-f]{40}$/.test(webCommitId)) {
    throw new Error('Dev source revisions must be full commit SHAs');
  }
  if (required(env, 'DEV_BRANCH') !== 'main') {
    throw new Error('Only main builds can register Cloud dev versions');
  }
  const version = `${buildVersion}-dev`;
  const image = required(env, 'CLOUD_IMAGE');
  const imageTag = required(env, 'CLOUD_IMAGE_TAG');
  const imageDigest = required(env, 'CLOUD_IMAGE_DIGEST');
  if (!/^sha256:[0-9a-f]{64}$/.test(imageDigest)
      || image !== `ghcr.io/tapstate/tapstate-cloud@${imageDigest}`
      || imageTag !== `ghcr.io/tapstate/tapstate-cloud:${version}`) {
    throw new Error('The Cloud dev image must match its dev tag and verified digest');
  }
  const workflowUrl = required(env, 'DEV_RUN_URL');
  if (!/^https:\/\/github\.com\/tapstate\/tapstate\/actions\/runs\/\d+$/.test(workflowUrl)) {
    throw new Error('DEV_RUN_URL must identify the Tapstate workflow run');
  }
  const extra = { buildVersion, image, imageTag, imageDigest, webCommitId, workflowUrl };
  const body = { version, releaseCode, releaseNotes: workflowUrl,
    git: { branch: 'main', commitId, extra } };
  let response;
  try {
    response = await fetchImpl(endpoint, {
      method: 'POST', redirect: 'error', signal: AbortSignal.timeout(30_000),
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch {
    // A transport failure can occur after the write; do not retry or expose credential-bearing errors.
    throw new Error('Cloud dev version registration failed or timed out; delivery may be uncertain');
  }
  if (response.status === 409) {
    let conflict;
    try { conflict = await response.json(); }
    catch { throw new Error('Cloud dev version registration received an invalid conflict response'); }
    if (conflict?.code === 'cluster.version-duplicate') {
      // Rebuilds intentionally retain the first catalog record, including its Git and image metadata.
      return `Cloud dev version ${version} already exists; kept the original catalog record. Rebuilt image: ${image}`;
    }
    throw new Error('Cloud dev version registration failed (HTTP 409); unrecognized conflict');
  }
  if (response.status !== 201) {
    throw new Error(`Cloud dev version registration failed (HTTP ${response.status}); reconcile before retrying`);
  }
  let receipt;
  try { receipt = await response.json(); }
  catch { throw new Error('Cloud dev version registration received an invalid JSON response'); }
  const saved = receipt?.data;
  if (receipt?.code !== 'ok' || saved?.version !== version || saved?.releaseCode !== releaseCode
      || saved?.state !== 'ACTIVE' || saved?.git?.commitId !== commitId || saved?.git?.branch !== 'main'
      || Object.entries(extra).some(([key, value]) => saved?.git?.extra?.[key] !== value)) {
    throw new Error('Cloud dev version registration receipt does not match the published image');
  }
  return `Registered Cloud dev version ${version}: ${image}`;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    if (process.argv[2] === '--validate') {
      validateCloudDevConfiguration();
      console.log('Cloud dev registration configuration is present');
    } else {
      console.log(await notifyCloudDev());
    }
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
