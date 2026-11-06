import { test } from 'node:test'
import assert from 'node:assert/strict'
import { advanceItem, advanceLabel, canCancel, cancelItem } from '../.vitepress/theme/actions.js'
import { parseItemFile, renderItemFile, today } from '../.vitepress/theme/itemFile.js'
import { baseFields, decodePut, fakeRepo, fileFor, gh, itemFor } from './helpers.js'

test('advance button labels', () => {
  const label = (status, archived = false) => advanceLabel({ ...baseFields, status, archived })
  assert.equal(label('todo'), 'Queued')
  assert.equal(label('queued'), 'Active')
  assert.equal(label('active'), 'Done')
  assert.equal(label('done'), 'Archive')
  assert.equal(label('cancelled'), 'Archive')
  assert.equal(label('done', true), null)
})

test('cancel is offered only for open items', () => {
  const can = (status, archived = false) => canCancel({ ...baseFields, status, archived })
  assert.ok(can('todo') && can('queued') && can('active'))
  assert.ok(!can('done') && !can('cancelled') && !can('todo', true))
})

for (const [from, to, dateField] of [
  ['todo', 'queued', null],
  ['queued', 'active', 'activated'],
  ['active', 'done', 'completed']
]) {
  test(`advance ${from} to ${to}`, async () => {
    const fields = { ...baseFields, status: from, activated: from === 'active' ? '2026-11-02' : null }
    const calls = fakeRepo(renderItemFile(fields, 'keep me'))
    const result = await advanceItem(gh, itemFor(fields))

    const put = decodePut(calls)
    const written = parseItemFile(put.text)
    assert.equal(put.sha, 'S1')
    assert.match(put.message, new RegExp(`^Move item 7 to ${to}: `))
    assert.equal(written.fields.status, to)
    if (dateField) assert.equal(written.fields[dateField], today())
    assert.equal(written.fields.title, baseFields.title)
    assert.equal(written.description, 'keep me')
    assert.equal(result.item.status, to)
    assert.equal(result.commitUrl, 'C')
  })
}

test('advance keeps an existing status date', async () => {
  const fields = { ...baseFields, status: 'queued', activated: '2026-01-01' }
  const calls = fakeRepo(renderItemFile(fields, ''))
  await advanceItem(gh, itemFor(fields))
  assert.equal(parseItemFile(decodePut(calls).text).fields.activated, '2026-01-01')
})

for (const status of ['done', 'cancelled']) {
  test(`archiving a ${status} item moves the file in one commit`, async () => {
    const fields = {
      ...baseFields,
      status,
      completed: status === 'done' ? '2026-11-04' : null,
      cancelled: status === 'cancelled' ? '2026-11-05' : null
    }
    const text = renderItemFile(fields, 'body')
    const calls = fakeRepo(text)
    const result = await advanceItem(gh, itemFor(fields))

    const tree = calls.find((c) => c.path === '/repos/o/r/git/trees').body
    assert.equal(tree.base_tree, 'TREE')
    assert.deepEqual(tree.tree, [
      { path: 'items/archive/7.md', mode: '100644', type: 'blob', content: text },
      { path: 'items/7.md', mode: '100644', type: 'blob', sha: null }
    ])
    assert.deepEqual(calls.find((c) => c.path === '/repos/o/r/git/commits').body.parents, ['HEAD'])
    assert.equal(calls.find((c) => c.method === 'PATCH').body.sha, 'NEWC')
    assert.equal(calls.filter((c) => c.method === 'PUT').length, 0)
    assert.equal(result.item.archived, true)
    assert.equal(result.item.file, 'items/archive/7.md')
    assert.equal(result.item.url, '/items/archive/7')
    assert.equal(result.commitUrl, 'C2')
  })
}

test('cancel sets status and cancelled date', async () => {
  const calls = fakeRepo(renderItemFile(baseFields, ''))
  const result = await cancelItem(gh, itemFor(baseFields))
  const written = parseItemFile(decodePut(calls).text)
  assert.equal(written.fields.status, 'cancelled')
  assert.equal(written.fields.cancelled, today())
  assert.match(decodePut(calls).message, /^Cancel item 7: /)
  assert.equal(result.item.status, 'cancelled')
})

test('actions stop without committing when GitHub has a different status', async () => {
  const calls = fakeRepo(fileFor({ status: 'queued' }))
  await assert.rejects(advanceItem(gh, itemFor(baseFields)), /is queued on GitHub but todo here/)
  await assert.rejects(cancelItem(gh, itemFor(baseFields)), /is queued on GitHub but todo here/)
  assert.equal(calls.filter((c) => c.method !== 'GET').length, 0)
})

test('actions stop when the file is gone', async () => {
  fakeRepo(null)
  await assert.rejects(advanceItem(gh, itemFor(baseFields)), /no longer on GitHub/)
})

test('archiving reports a repo that moved on mid-commit', async () => {
  const fields = { ...baseFields, status: 'done', completed: '2026-11-04' }
  const text = renderItemFile(fields, '')
  const calls = fakeRepo(text)
  const route = globalThis.fetch
  globalThis.fetch = async (url, opts) =>
    opts.method === 'PATCH' ? { status: 422, json: async () => ({}) } : route(url, opts)
  await assert.rejects(advanceItem(gh, itemFor(fields)), /repo changed while archiving/)
  assert.ok(calls.length > 0)
})
