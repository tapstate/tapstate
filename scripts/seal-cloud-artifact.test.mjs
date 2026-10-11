import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import { mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { transformArtifact } from './seal-cloud-artifact.mjs'

const key = 'ab'.repeat(32)
const context = 'tapstate/tapstate:42:1:cloud-image'
async function fixture(t, plaintext = Buffer.from('private-cloud-archive-sentinel')) {
  const root = await mkdtemp(join(tmpdir(), 'cloud-artifact-test-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  const input = join(root, 'input'), sealed = join(root, 'sealed'), output = join(root, 'output')
  await writeFile(input, plaintext, { mode: 0o600 })
  await transformArtifact('seal', input, sealed, context, key)
  return { root, input, sealed, output, plaintext }
}

for (const length of [0, 31, 4 * 1024 * 1024]) {
  test(`round trips ${length} bytes without exposing plaintext in the artifact`, async t => {
    const f = await fixture(t, randomBytes(length))
    await transformArtifact('open', f.sealed, f.output, context, key)
    assert.deepEqual(await readFile(f.output), f.plaintext)
    assert.equal((await stat(f.output)).mode & 0o777, 0o600)
    assert.equal((await stat(f.sealed)).size, length + 68)
    if (length > 0) assert.equal((await readFile(f.sealed)).includes(f.plaintext), false)
  })
}

test('a new salt and nonce give distinct ciphertext for the same input', async t => {
  const f = await fixture(t)
  const again = join(f.root, 'again')
  await transformArtifact('seal', f.input, again, context, key)
  assert.notDeepEqual(await readFile(f.sealed), await readFile(again))
})

for (const offset of [0, 8, 40, 52, -1]) {
  test(`a changed format, salt, nonce, ciphertext or tag (${offset}) leaves no plaintext`, async t => {
    const f = await fixture(t)
    const bytes = await readFile(f.sealed)
    bytes[offset < 0 ? bytes.length + offset : offset] ^= 1
    await writeFile(f.sealed, bytes)
    await assert.rejects(transformArtifact('open', f.sealed, f.output, context, key))
    await assert.rejects(stat(f.output), { code: 'ENOENT' })
    assert.equal((await readdir(f.root)).some(name => name.startsWith('.cloud-artifact-')), false)
  })
}

for (const other of ['tapstate/tapstate:43:1:cloud-image', 'tapstate/tapstate:42:2:cloud-image',
  'tapstate/tapstate:42:1:boot-jar', 'other/repo:42:1:cloud-image']) {
  test(`artifacts cannot cross context ${other}`, async t => {
    const f = await fixture(t)
    await assert.rejects(transformArtifact('open', f.sealed, f.output, other, key))
    await assert.rejects(stat(f.output), { code: 'ENOENT' })
  })
}

test('a wrong key is refused without leaving a partial output', async t => {
  const f = await fixture(t)
  await assert.rejects(transformArtifact('open', f.sealed, f.output, context, 'cd'.repeat(32)))
  await assert.rejects(stat(f.output), { code: 'ENOENT' })
})

for (const invalid of ['', 'not-a-secret-key', 'a'.repeat(63), 'g'.repeat(64)]) {
  test(`missing or invalid CI key is refused (${invalid.length})`, async t => {
    const f = await fixture(t)
    await assert.rejects(transformArtifact('open', f.sealed, f.output, context, invalid))
    await assert.rejects(stat(f.output), { code: 'ENOENT' })
  })
}

test('an existing destination or destination symlink is not overwritten', async t => {
  const f = await fixture(t)
  await writeFile(f.output, 'original')
  await assert.rejects(transformArtifact('open', f.sealed, f.output, context, key))
  assert.equal(await readFile(f.output, 'utf8'), 'original')
  const linked = join(f.root, 'linked')
  await symlink(f.output, linked)
  await assert.rejects(transformArtifact('open', f.sealed, linked, context, key))
  assert.equal(await readFile(f.output, 'utf8'), 'original')
})

test('plaintext and truncated archives are not treated as legacy ciphertext', async t => {
  const f = await fixture(t)
  await assert.rejects(transformArtifact('open', f.input, f.output, context, key))
  await writeFile(f.sealed, (await readFile(f.sealed)).subarray(0, 67))
  await assert.rejects(transformArtifact('open', f.sealed, f.output, context, key))
  await assert.rejects(stat(f.output), { code: 'ENOENT' })
})
