import { test } from 'node:test'
import assert from 'node:assert/strict'
import { validateProjectPage } from '../.vitepress/theme/validate.js'

test('org projects and personal repos are valid', () => {
  assert.deepEqual(validateProjectPage({ org: 'payjoin', repo: 'payjoin/rust-payjoin', repos: ['payjoin/cli', 'other/x'] }), [])
  assert.deepEqual(validateProjectPage({ repo: 'romanz/electrs' }), [])
  assert.deepEqual(validateProjectPage({ org: 'xstoicunicornx', repos: ['xstoicunicornx/a'] }), [])
})

test('project page frontmatter errors', () => {
  const errors = (fm) => validateProjectPage(fm).join('; ')
  assert.match(errors({}), /repo is required when there is no org/)
  assert.match(errors({ repo: 'electrs' }), /owner\/name/)
  assert.match(errors({ org: 'o', repo: 'electrs' }), /owner\/name/)
  assert.match(errors({ org: 'a/b', repo: 'a/b' }), /org must be/)
  assert.match(errors({ repo: 'a/b', repos: 'a/c' }), /repos must be a list/)
  assert.match(errors({ repo: 'a/b', repos: ['nope'] }), /repos entry/)
  assert.match(errors({ repo: 'a/b', repos: ['a/b'] }), /repeats the main repo/)
})
