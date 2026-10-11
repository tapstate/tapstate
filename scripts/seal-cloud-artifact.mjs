#!/usr/bin/env node
import { createCipheriv, createDecipheriv, hkdfSync, randomBytes } from 'node:crypto'
import { createReadStream, createWriteStream } from 'node:fs'
import { access, link, mkdtemp, open, rm } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { pipeline } from 'node:stream/promises'
import { pathToFileURL } from 'node:url'

const MAGIC = Buffer.from('TSCLD001')
const HEADER_SIZE = MAGIC.length + 32 + 12
const TAG_SIZE = 16

// Public CI artifacts carry only authenticated ciphertext. The independent CI key never ships
// in a server, image, artifact, output, or diagnostic. Each file gets a fresh salt and nonce.
export async function transformArtifact(mode, input, output, context, keyHex) {
  if (!['seal', 'open'].includes(mode) || !input || !output
      || !/^[A-Za-z0-9:/._-]{1,256}$/.test(context ?? '')
      || !/^[0-9a-f]{64}$/.test(keyHex ?? '')) {
    throw new Error('invalid artifact operation, context, or CI key')
  }
  try {
    await access(output)
    throw new Error('artifact output already exists')
  } catch (error) {
    if (error.code !== 'ENOENT') throw error
  }
  const staging = await mkdtemp(join(dirname(output), '.cloud-artifact-'))
  const temporary = join(staging, 'artifact')
  let handle
  try {
    let header, tag, size
    if (mode === 'seal') {
      header = Buffer.concat([MAGIC, randomBytes(32), randomBytes(12)])
    } else {
      handle = await open(input, 'r')
      size = (await handle.stat()).size
      if (size < HEADER_SIZE + TAG_SIZE) throw new Error('invalid sealed artifact')
      header = Buffer.alloc(HEADER_SIZE)
      tag = Buffer.alloc(TAG_SIZE)
      const head = await handle.read(header, 0, HEADER_SIZE, 0)
      const tail = await handle.read(tag, 0, TAG_SIZE, size - TAG_SIZE)
      if (head.bytesRead !== HEADER_SIZE || tail.bytesRead !== TAG_SIZE
          || !header.subarray(0, MAGIC.length).equals(MAGIC)) {
        throw new Error('invalid sealed artifact')
      }
      await handle.close()
      handle = undefined
    }
    const salt = header.subarray(MAGIC.length, MAGIC.length + 32)
    const nonce = header.subarray(MAGIC.length + 32)
    const key = hkdfSync('sha256', Buffer.from(keyHex, 'hex'), salt,
      Buffer.from('tapstate-cloud-artifact-v1:' + context), 32)
    const crypto = mode === 'seal'
      ? createCipheriv('aes-256-gcm', key, nonce)
      : createDecipheriv('aes-256-gcm', key, nonce)
    crypto.setAAD(Buffer.concat([header, Buffer.from(context)]))
    if (mode === 'open') crypto.setAuthTag(tag)
    if (mode === 'seal') {
      const file = await open(temporary, 'wx', 0o600)
      await file.write(header)
      await file.close()
      await pipeline(createReadStream(input), crypto,
        createWriteStream(temporary, { flags: 'r+', start: HEADER_SIZE, mode: 0o600 }))
      const fileEnd = await open(temporary, 'a')
      await fileEnd.write(crypto.getAuthTag())
      await fileEnd.close()
    } else if (size === HEADER_SIZE + TAG_SIZE) {
      // A zero-byte input still has an authentication tag to check.
      const plaintext = crypto.final()
      const file = await open(temporary, 'wx', 0o600)
      await file.write(plaintext)
      await file.close()
    } else {
      await pipeline(createReadStream(input, { start: HEADER_SIZE, end: size - TAG_SIZE - 1 }),
        crypto, createWriteStream(temporary, { flags: 'wx', mode: 0o600 }))
    }
    // Authentication must finish before the destination becomes consumable. link refuses an
    // existing destination atomically; failures leave neither partial plaintext nor a replaced file.
    await link(temporary, output)
  } finally {
    await handle?.close()
    await rm(staging, { recursive: true, force: true })
  }
}

async function main() {
  const [mode, ...args] = process.argv.slice(2)
  const options = {}
  for (let i = 0; i < args.length; i += 2) {
    if (!['--input', '--output', '--context'].includes(args[i]) || !args[i + 1]
        || options[args[i]]) throw new Error('invalid arguments')
    options[args[i]] = args[i + 1]
  }
  await transformArtifact(mode, options['--input'], options['--output'],
    options['--context'], process.env.CLOUD_ARTIFACT_KEY)
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(() => {
    // Do not print arguments, keys, data, paths, or underlying crypto/I/O exceptions.
    process.stderr.write('Cloud artifact operation refused.\n')
    process.exitCode = 1
  })
}
