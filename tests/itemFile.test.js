import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  fieldsToForm, formToFields, parseItemFile, renderItemFile, repoFromLinks, repoFromUrl, withStatus
} from '../.vitepress/theme/itemFile.js'
import { validateItem } from '../.vitepress/theme/validate.js'
import { baseFields } from './helpers.js'

test('render and parse round trip, including quotes, colons and unicode', () => {
  const fields = {
    ...baseFields,
    links: ['https://a.example/x?y=1#z'],
    pr: 'https://github.com/p/r/pull/2',
    repo: 'p/r',
    status: 'active',
    activated: '2026-11-02'
  }
  const text = renderItemFile(fields, '  Some **markdown**.\n\nSecond para.  ')
  const parsed = parseItemFile(text)
  assert.deepEqual(parsed.fields, fields)
  assert.equal(parsed.description, 'Some **markdown**.\n\nSecond para.')
  assert.doesNotMatch(text, / +$/m)
})

test('parse rejects missing frontmatter and bad dates', () => {
  assert.throws(() => parseItemFile('no frontmatter'), /missing frontmatter/)
  assert.throws(() => parseItemFile('---\ncreated: tomorrow\n---\n'), /created must be a date/)
})

test('withStatus fills the date for the new status but keeps an existing one', () => {
  assert.equal(withStatus(baseFields, 'active', '2026-12-01').activated, '2026-12-01')
  assert.equal(withStatus(baseFields, 'cancelled', '2026-12-01').cancelled, '2026-12-01')
  assert.equal(withStatus({ ...baseFields, activated: '2026-01-01' }, 'active', '2026-12-01').activated, '2026-01-01')
  assert.deepEqual(withStatus(baseFields, 'queued', '2026-12-01'), { ...baseFields, status: 'queued' })
})

test('form round trip leaves fields unchanged', () => {
  const fields = { ...baseFields, links: ['https://a', 'https://b'], issue: 'https://i' }
  assert.deepEqual(formToFields(fieldsToForm(fields, 'd'), fields, '2026-12-01'), fields)
})

test('changing status in the form fills only that status date', () => {
  const form = fieldsToForm(baseFields, '')
  const done = formToFields({ ...form, status: 'done' }, baseFields, '2026-12-01')
  assert.equal(done.completed, '2026-12-01')
  assert.equal(done.activated, null)
  assert.equal(done.cancelled, null)
})

test('new item form trims input and sets created and status dates', () => {
  const base = { status: null, repo: null, created: '2026-12-01', activated: null, completed: null, cancelled: null }
  const form = {
    title: ' t ', project: 'ldk', type: 'review', status: 'active',
    issue: ' ', pr: '', links: ' https://x \n\n', description: ''
  }
  assert.deepEqual(formToFields(form, base, '2026-12-01'), {
    status: 'active', created: '2026-12-01', activated: '2026-12-01', completed: null, cancelled: null,
    title: 't', project: 'ldk', type: 'review', links: ['https://x'], issue: null, pr: null, repo: null
  })
})

test('repo comes from GitHub issue and PR URLs only', () => {
  assert.equal(repoFromUrl('https://github.com/payjoin/rust-payjoin/pull/12'), 'payjoin/rust-payjoin')
  assert.equal(repoFromUrl('https://github.com/lightningdevkit/rust-lightning/issues/3#issuecomment-1'), 'lightningdevkit/rust-lightning')
  assert.equal(repoFromUrl('https://github.com/a/b'), null)
  assert.equal(repoFromUrl('https://gitlab.com/a/b/issues/1'), null)
  assert.equal(repoFromUrl(null), null)
})

test('repo prefers the PR link over the issue link', () => {
  assert.equal(repoFromLinks('https://github.com/p/r/pull/1', 'https://github.com/i/r/issues/2'), 'p/r')
  assert.equal(repoFromLinks(null, 'https://github.com/i/r/issues/2'), 'i/r')
  assert.equal(repoFromLinks('https://example.com/x', 'https://github.com/i/r/issues/2'), 'i/r')
})

test('saving the form recomputes repo', () => {
  const fields = { ...baseFields, pr: 'https://github.com/p/r/pull/1', issue: 'https://github.com/i/r/issues/2', repo: 'p/r' }
  const form = fieldsToForm(fields, '')
  assert.equal(formToFields({ ...form, pr: '' }, fields, '2026-12-01').repo, 'i/r')
  assert.equal(formToFields({ ...form, pr: '', issue: '' }, fields, '2026-12-01').repo, null)
})

test('files without a repo line derive it from the links', () => {
  const text = renderItemFile({ ...baseFields, pr: 'https://github.com/p/r/pull/1', repo: 'p/r' }, '')
  assert.match(text, /^repo: "p\/r"$/m)
  assert.equal(parseItemFile(text.replace(/^repo:.*\n/m, '')).fields.repo, 'p/r')
})

test('validateItem', () => {
  const projects = ['payjoin', 'ldk']
  assert.deepEqual(validateItem(baseFields, projects), [])
  const errors = (fields) => validateItem({ ...baseFields, ...fields }, projects)
  assert.match(errors({ title: ' ' })[0], /title is required/)
  assert.match(errors({ project: 'nope' })[0], /unknown project/)
  assert.match(errors({ type: 'bug' })[0], /type must be one of/)
  assert.match(errors({ status: 'blocked' })[0], /status must be one of/)
  assert.match(errors({ links: ['ftp://x'] })[0], /links must be a http/)
  assert.match(errors({ issue: 'nope' })[0], /issue must be a http/)
  assert.match(errors({ repo: 'nope' })[0], /owner\/name/)
  assert.match(errors({ status: 'active' })[0], /activated is required/)
  assert.match(errors({ status: 'done' })[0], /completed is required/)
  assert.match(errors({ status: 'cancelled' })[0], /cancelled is required/)
})
