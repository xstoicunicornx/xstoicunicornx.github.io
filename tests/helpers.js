import { renderItemFile } from '../.vitepress/theme/itemFile.js'

export const gh = { token: 't', repo: 'o/r', branch: 'main' }

export const baseFields = {
  title: 'Ünï "x": y',
  project: 'payjoin',
  type: 'pr',
  status: 'todo',
  links: [],
  issue: null,
  pr: null,
  repo: null,
  created: '2026-11-01',
  activated: null,
  completed: null,
  cancelled: null
}

export function itemFor(fields, file = 'items/7.md') {
  return { ...fields, id: 7, file, url: '/items/7', archived: false }
}

export const fileFor = (fields, description = '') => renderItemFile({ ...baseFields, ...fields }, description)

// Replaces global fetch with a fake GitHub API. `route` receives
// { method, path, body } and returns [status, json]. Returns the call log.
export function fakeGitHub(route) {
  const calls = []
  globalThis.fetch = async (url, opts) => {
    const call = {
      method: opts.method,
      path: url.replace('https://api.github.com', ''),
      body: opts.body ? JSON.parse(opts.body) : null
    }
    calls.push(call)
    const [status, data] = route(call)
    return { status, json: async () => data }
  }
  return calls
}

// A fake repo holding one item file at version S1, with working Git Data endpoints.
export function fakeRepo(text) {
  return fakeGitHub(({ method, path, body }) => {
    if (method === 'GET' && path.startsWith('/repos/o/r/contents/')) {
      return text === null ? [404, {}] : [200, { content: Buffer.from(text).toString('base64'), sha: 'S1' }]
    }
    if (method === 'PUT') return body.sha === 'S1' ? [200, { commit: { html_url: 'C' } }] : [409, {}]
    if (path === '/repos/o/r/git/ref/heads/main') return [200, { object: { sha: 'HEAD' } }]
    if (path === '/repos/o/r/git/commits/HEAD') return [200, { tree: { sha: 'TREE' } }]
    if (path === '/repos/o/r/git/trees') return [201, { sha: 'NEWTREE' }]
    if (path === '/repos/o/r/git/commits') return [201, { sha: 'NEWC', html_url: 'C2' }]
    if (path === '/repos/o/r/git/refs/heads/main') return [200, {}]
    return [500, { message: `unexpected ${method} ${path}` }]
  })
}

export function decodePut(calls) {
  const put = calls.find((c) => c.method === 'PUT')
  return { ...put.body, text: Buffer.from(put.body.content, 'base64').toString() }
}
