import { test } from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import loader from '../.vitepress/theme/content.data.js'
import { fileFor } from './helpers.js'

// Builds a throwaway site folder and runs the build-time loader on it.
function load(items, order = ['payjoin', 'ldk']) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'site-'))
  const write = (rel, text) => {
    fs.mkdirSync(path.dirname(path.join(root, rel)), { recursive: true })
    fs.writeFileSync(path.join(root, rel), text)
    return path.join(root, rel)
  }
  try {
    const files = [
      write('data/order.json', JSON.stringify(order)),
      write('data/projects/ldk.json', '{ "name": "LDK" }'),
      write('data/projects/payjoin.json', '{ "name": "payjoin" }'),
      ...Object.entries(items).map(([rel, text]) => write(rel, text))
    ]
    return loader.load(files)
  } finally {
    fs.rmSync(root, { recursive: true })
  }
}

test('projects follow order.json, unlisted ones go last', () => {
  assert.deepEqual(load({}).projects.map((p) => p.slug), ['payjoin', 'ldk'])
  assert.deepEqual(load({}, ['ldk']).projects.map((p) => p.slug), ['ldk', 'payjoin'])
})

test('items get ids, urls and archive flags; nextId covers the archive', () => {
  const { items, nextId } = load({
    'items/2.md': fileFor({}),
    'items/archive/5.md': fileFor({ status: 'done', completed: '2026-11-04' })
  })
  const byId = Object.fromEntries(items.map((i) => [i.id, i]))
  assert.equal(byId[2].url, '/items/2')
  assert.equal(byId[2].archived, false)
  assert.equal(byId[5].url, '/items/archive/5')
  assert.equal(byId[5].file, 'items/archive/5.md')
  assert.equal(byId[5].archived, true)
  assert.equal(nextId, 6)
})

test('nextId is 1 with no items', () => {
  assert.equal(load({}).nextId, 1)
})

test('the build fails on invalid items, naming the file', () => {
  assert.throws(() => load({ 'items/3.md': fileFor({ project: 'nope' }) }), /items\/3\.md: unknown project "nope"/)
  assert.throws(() => load({ 'items/foo.md': fileFor({}) }), /items\/foo\.md: filename must be a number/)
  assert.throws(() => load({ 'items/3.md': 'no frontmatter' }), /items\/3\.md: missing frontmatter/)
  assert.throws(
    () => load({ 'items/2.md': fileFor({}), 'items/archive/2.md': fileFor({}) }),
    /Item id 2 is used by both/
  )
})
