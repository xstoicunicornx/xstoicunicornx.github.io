import { test } from 'node:test'
import assert from 'node:assert/strict'
import { loadRef, parseRef, refState } from '../.vitepress/theme/githubRefs.js'
import { fakeGitHub } from './helpers.js'

test('parseRef reads GitHub issue and PR URLs only', () => {
  assert.deepEqual(parseRef('https://github.com/payjoin/rust-payjoin/pull/12'), { repo: 'payjoin/rust-payjoin', number: 12 })
  assert.deepEqual(parseRef('https://github.com/a/b/issues/3#issuecomment-9'), { repo: 'a/b', number: 3 })
  assert.deepEqual(parseRef('https://github.com/a/b/pull/4/files'), { repo: 'a/b', number: 4 })
  assert.equal(parseRef('https://github.com/a/b/pull/4x'), null)
  assert.equal(parseRef('https://github.com/a/b'), null)
  assert.equal(parseRef('https://example.com/a/b/issues/1'), null)
  assert.equal(parseRef(null), null)
})

test('refState maps issues and PRs to GitHub states', () => {
  const s = (issue) => refState({ title: 't', ...issue }).state
  assert.equal(s({ state: 'open' }), 'issue-open')
  assert.equal(s({ state: 'closed', state_reason: 'completed' }), 'issue-completed')
  assert.equal(s({ state: 'closed', state_reason: null }), 'issue-completed')
  assert.equal(s({ state: 'closed', state_reason: 'not_planned' }), 'issue-not-planned')
  assert.equal(s({ state: 'closed', state_reason: 'duplicate' }), 'issue-not-planned')
  const pr = (extra, merged_at = null) => s({ pull_request: { merged_at }, ...extra })
  assert.equal(pr({ state: 'open' }), 'pr-open')
  assert.equal(pr({ state: 'open', draft: true }), 'pr-draft')
  assert.equal(pr({ state: 'closed' }, '2026-01-01T00:00:00Z'), 'pr-merged')
  assert.equal(pr({ state: 'closed' }), 'pr-closed')
  assert.deepEqual(refState({ title: 'Fix it', state: 'open' }), { state: 'issue-open', title: 'Fix it' })
})

test('loadRef shares one request per ref and returns null on errors', async () => {
  const calls = fakeGitHub(({ path }) =>
    path === '/repos/o/r/issues/1' ? [200, { title: 'One', state: 'open' }] : path === '/repos/o/r/issues/2' ? [404, {}] : [500, {}]
  )
  const [a, b] = await Promise.all([loadRef('t', { repo: 'o/r', number: 1 }), loadRef('t', { repo: 'o/r', number: 1 })])
  assert.deepEqual(a, { state: 'issue-open', title: 'One' })
  assert.equal(b, a)
  assert.equal(calls.length, 1)
  assert.equal(await loadRef('t', { repo: 'o/r', number: 2 }), null)
  assert.equal(await loadRef('t', { repo: 'o/r', number: 3 }), null)
})
