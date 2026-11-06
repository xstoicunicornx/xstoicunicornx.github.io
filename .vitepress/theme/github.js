// Commits from the browser through the GitHub REST API, using a
// fine-grained personal access token kept in this browser's localStorage.

const TOKEN_KEY = 'github-token'

export function getToken() {
  try {
    return localStorage.getItem(TOKEN_KEY) ?? ''
  } catch {
    return ''
  }
}

export function setToken(token) {
  try {
    localStorage.setItem(TOKEN_KEY, token)
  } catch {
    // Storage unavailable (private window, blocked site data). The token
    // still works for this page load because the form keeps it in memory.
  }
}

export function clearToken() {
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
      Authorization: `Bearer ${token}`,
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

function toBase64(text) {
  const bytes = new TextEncoder().encode(text)
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary)
}
