// Commits from the browser through the GitHub REST API, using a
// fine-grained personal access token kept in this browser's localStorage.

import { ref } from 'vue'

const TOKEN_KEY = 'github-token'

// The saved token, shared by every component. Empty until loadToken() runs
// in the browser, since localStorage does not exist during the build.
export const token = ref('')

export function loadToken() {
  try {
    token.value = localStorage.getItem(TOKEN_KEY) ?? ''
  } catch {
    token.value = ''
  }
}

// Returns false if the browser blocked storage (private window, blocked
// site data). The token still works until the page is reloaded.
export function saveToken(value) {
  token.value = value
  try {
    localStorage.setItem(TOKEN_KEY, value)
    return true
  } catch {
    return false
  }
}

export function clearToken() {
  token.value = ''
  try {
    localStorage.removeItem(TOKEN_KEY)
  } catch {}
}

export class GitHubError extends Error {}

async function request(token, method, path, body) {
  const res = await fetch(`https://api.github.com${path}`, {
    method,
    cache: 'no-store',
    headers: {
      Accept: 'application/vnd.github+json',
      // Public reads work without a token, at a lower rate limit.
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      'X-GitHub-Api-Version': '2022-11-28',
      ...(body ? { 'Content-Type': 'application/json' } : {})
    },
    body: body ? JSON.stringify(body) : undefined
  })
  const data = await res.json().catch(() => ({}))
  return { status: res.status, data }
}

function explain(status, data) {
  if (status === 401) return 'GitHub rejected the token. It may be mistyped or expired.'
  if (status === 403) return 'The token does not have permission to write to this repo.'
  if (status === 404) return 'GitHub could not find the repo. Check that the token has access to it.'
  return `GitHub returned ${status}: ${data.message ?? 'unknown error'}`
}

// Reads an issue or PR; GitHub serves both from the issues endpoint. An empty
// token reads without one. Returns null if it does not exist or cannot be seen.
export async function getIssue(token, repo, number) {
  const { status, data } = await request(token, 'GET', `/repos/${repo}/issues/${number}`)
  if (status === 404 || status === 410) return null
  if (status !== 200) throw new GitHubError(explain(status, data))
  return data
}

// Confirms the token can see the repo. Write access is only proven by a commit.
export async function checkRepoAccess(token, repo) {
  const { status, data } = await request(token, 'GET', `/repos/${repo}`)
  if (status !== 200) throw new GitHubError(explain(status, data))
}

// Item numbers already used in a folder. A missing folder has none.
async function idsIn(token, repo, branch, dir) {
  const { status, data } = await request(token, 'GET', `/repos/${repo}/contents/${dir}?ref=${branch}`)
  if (status === 404) {
    // 404 also means a bad token scope, so confirm the repo itself is visible.
    const check = await request(token, 'GET', `/repos/${repo}`)
    if (check.status !== 200) throw new GitHubError(explain(check.status, check.data))
    return []
  }
  if (status !== 200) throw new GitHubError(explain(status, data))
  return data
    .filter((entry) => entry.type === 'file' && /^[1-9]\d*\.md$/.test(entry.name))
    .map((entry) => Number(entry.name.slice(0, -3)))
}

// Commits a new item file under the next free number. `render` receives the
// number and returns { content, message }. Returns { id, commitUrl }.
export async function createItem(token, repo, branch, render) {
  const used = [
    ...(await idsIn(token, repo, branch, 'items')),
    ...(await idsIn(token, repo, branch, 'items/archive'))
  ]
  let id = Math.max(0, ...used) + 1

  // If another commit took the number in the meantime, try the next one.
  for (let attempt = 0; attempt < 5; attempt++, id++) {
    const { content, message } = render(id)
    const { status, data } = await request(token, 'PUT', `/repos/${repo}/contents/items/${id}.md`, {
      message,
      content: toBase64(content),
      branch
    })
    if (status === 201) return { id, commitUrl: data.commit?.html_url }
    if (status !== 422) throw new GitHubError(explain(status, data))
  }
  throw new GitHubError('Could not find a free item number after 5 tries.')
}

// Reads a file. Returns { text, sha }, or null if it does not exist.
export async function getFile(token, repo, branch, path) {
  const { status, data } = await request(token, 'GET', `/repos/${repo}/contents/${path}?ref=${branch}`)
  if (status === 404) return null
  if (status !== 200) throw new GitHubError(explain(status, data))
  return { text: fromBase64(data.content), sha: data.sha }
}

// Overwrites a file. `sha` must be the version that was read, so a file
// changed on GitHub in the meantime is rejected instead of overwritten.
export async function updateFile(token, repo, branch, path, text, sha, message) {
  const { status, data } = await request(token, 'PUT', `/repos/${repo}/contents/${path}`, {
    message,
    content: toBase64(text),
    sha,
    branch
  })
  if (status === 409) throw new GitHubError('The file changed on GitHub since it was read. Reload and try again.')
  if (status !== 200) throw new GitHubError(explain(status, data))
  return { commitUrl: data.commit?.html_url }
}

// Moves a file in a single commit using the Git Data API.
export async function moveFile(token, repo, branch, from, to, text, message) {
  const git = (method, path, body) => request(token, method, `/repos/${repo}/git${path}`, body)
  const check = ({ status, data }, ok) => {
    if (status !== ok) throw new GitHubError(explain(status, data))
    return data
  }

  const ref = check(await git('GET', `/ref/heads/${branch}`), 200)
  const parent = check(await git('GET', `/commits/${ref.object.sha}`), 200)
  const tree = check(
    await git('POST', '/trees', {
      base_tree: parent.tree.sha,
      tree: [
        { path: to, mode: '100644', type: 'blob', content: text },
        { path: from, mode: '100644', type: 'blob', sha: null }
      ]
    }),
    201
  )
  const commit = check(
    await git('POST', '/commits', { message, tree: tree.sha, parents: [ref.object.sha] }),
    201
  )
  const updated = await git('PATCH', `/refs/heads/${branch}`, { sha: commit.sha })
  if (updated.status === 422) {
    throw new GitHubError('The repo changed while archiving. Try again.')
  }
  check(updated, 200)
  return { commitUrl: commit.html_url }
}

function fromBase64(encoded) {
  const binary = atob(encoded.replace(/\n/g, ''))
  const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0))
  return new TextDecoder().decode(bytes)
}

function toBase64(text) {
  const bytes = new TextEncoder().encode(text)
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary)
}
