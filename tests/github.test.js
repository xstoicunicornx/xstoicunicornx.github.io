import { test } from 'node:test'
import assert from 'node:assert/strict'
import { GitHubError, createItem, getFile, getIssue, updateFile } from '../.vitepress/theme/github.js'
import { fakeGitHub, fakeRepo } from './helpers.js'

const render = (id) => ({ content: 'x', message: `Add item ${id}` })
const listing = (...ids) =>
  ids.map((id) => ({ type: 'file', name: `${id}.md` })).concat([{ type: 'dir', name: 'archive' }])

test('createItem takes the next number across items and archive', async () => {
  fakeGitHub(({ method, path }) => {
    if (path.startsWith('/repos/r/contents/items/archive?')) return [200, listing(9)]
    if (method === 'GET') return [200, listing(1, 3)]
    return [201, { commit: { html_url: 'C' } }]
  })
  assert.deepEqual(await createItem('t', 'r', 'main', render), { id: 10, commitUrl: 'C' })
})

test('createItem moves to the next number when one is taken', async () => {
  const calls = fakeGitHub(({ method, path }) => {
    if (path.startsWith('/repos/r/contents/items/archive?')) return [404, {}]
    if (path === '/repos/r') return [200, {}]
    if (method === 'GET') return [200, listing(1, 3)]
    return path.endsWith('/4.md') ? [422, {}] : [201, { commit: { html_url: 'C' } }]
  })
  assert.deepEqual(await createItem('t', 'r', 'main', render), { id: 5, commitUrl: 'C' })
  assert.deepEqual(
    calls.filter((c) => c.method === 'PUT').map((c) => c.path),
    ['/repos/r/contents/items/4.md', '/repos/r/contents/items/5.md']
  )
})

test('createItem works when the items folder does not exist yet', async () => {
  fakeGitHub(({ method, path }) => {
    if (path === '/repos/r') return [200, {}]
    if (method === 'GET') return [404, {}]
    return [201, { commit: {} }]
  })
  assert.equal((await createItem('t', 'r', 'main', render)).id, 1)
})

test('createItem gives up after 5 taken numbers', async () => {
  fakeGitHub(({ method }) => (method === 'GET' ? [200, []] : [422, {}]))
  await assert.rejects(createItem('t', 'r', 'main', render), /free item number after 5 tries/)
})

test('GitHub errors are explained', async () => {
  const cases = [
    [401, /rejected the token/],
    [403, /does not have permission/],
    [404, /could not find the repo/],
    [500, /GitHub returned 500: boom/]
  ]
  for (const [status, message] of cases) {
    fakeGitHub(() => [status, { message: 'boom' }])
    await assert.rejects(createItem('t', 'r', 'main', render), (err) => err instanceof GitHubError && message.test(err.message))
  }
})

test('getFile decodes UTF-8 and returns null when missing', async () => {
  fakeRepo('héllo\n')
  assert.deepEqual(await getFile('t', 'o/r', 'main', 'items/1.md'), { text: 'héllo\n', sha: 'S1' })
  fakeRepo(null)
  assert.equal(await getFile('t', 'o/r', 'main', 'items/1.md'), null)
})

test('updateFile refuses to overwrite a changed file', async () => {
  fakeRepo('x')
  await assert.rejects(updateFile('t', 'o/r', 'main', 'items/7.md', 'x', 'OLD', 'm'), /changed on GitHub/)
  assert.deepEqual(await updateFile('t', 'o/r', 'main', 'items/7.md', 'x', 'S1', 'm'), { commitUrl: 'C' })
})

test('requests send the token only when there is one', async () => {
  const headers = []
  globalThis.fetch = async (url, opts) => {
    headers.push(opts.headers)
    return { status: 200, json: async () => ({ title: 't', state: 'open' }) }
  }
  await getIssue('tok', 'o/r', 1)
  await getIssue('', 'o/r', 1)
  assert.equal(headers[0].Authorization, 'Bearer tok')
  assert.equal('Authorization' in headers[1], false)
})
