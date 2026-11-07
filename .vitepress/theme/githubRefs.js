import { getIssue } from './github.js'

// Status of the GitHub issues and PRs that items link to, shown the way
// GitHub shows references: a state icon, the number, and the title.

const URL_PATTERN = /^https?:\/\/github\.com\/([^/\s]+)\/([^/\s]+)\/(?:pull|issues)\/(\d+)(?:[/?#]|$)/

// { repo, number } for a GitHub issue or PR URL, or null for any other URL.
export function parseRef(url) {
  const match = url?.match(URL_PATTERN)
  return match ? { repo: `${match[1]}/${match[2]}`, number: Number(match[3]) } : null
}

// Maps an issues API response to what the link shows. The API decides
// whether it is a PR, so an issue URL that points at a PR still shows as one.
export function refState(issue) {
  let state
  if (issue.pull_request) {
    if (issue.pull_request.merged_at) state = 'pr-merged'
    else if (issue.state === 'closed') state = 'pr-closed'
    else if (issue.draft) state = 'pr-draft'
    else state = 'pr-open'
  } else if (issue.state === 'open') {
    state = 'issue-open'
  } else {
    state = issue.state_reason === 'not_planned' || issue.state_reason === 'duplicate'
      ? 'issue-not-planned'
      : 'issue-completed'
  }
  return { state, title: issue.title }
}

export const STATE_LABELS = {
  'issue-open': 'Open issue',
  'issue-completed': 'Closed issue',
  'issue-not-planned': 'Closed as not planned',
  'pr-open': 'Open pull request',
  'pr-draft': 'Draft pull request',
  'pr-merged': 'Merged pull request',
  'pr-closed': 'Closed pull request'
}

// Results are cached in localStorage for a few minutes so moving between
// pages does not refetch, and requests for the same ref are shared.
const CACHE_KEY = 'github-refs'
const TTL = 5 * 60 * 1000
const inflight = new Map()

function readCache() {
  try {
    return JSON.parse(localStorage.getItem(CACHE_KEY) ?? '{}')
  } catch {
    return {}
  }
}

function writeCache(key, value) {
  const cache = readCache()
  const now = Date.now()
  for (const [k, entry] of Object.entries(cache)) {
    if (now - entry.at > TTL) delete cache[k]
  }
  cache[key] = { at: now, value }
  try {
    localStorage.setItem(CACHE_KEY, JSON.stringify(cache))
  } catch {}
}

// Resolves to { state, title }, or null when the ref cannot be read
// (deleted, private, or any request error). Never rejects.
export function loadRef(token, ref) {
  const key = `${ref.repo}#${ref.number}`
  const cached = readCache()[key]
  if (cached && Date.now() - cached.at < TTL) return Promise.resolve(cached.value)
  if (inflight.has(key)) return inflight.get(key)

  const promise = getIssue(token, ref.repo, ref.number)
    .then((issue) => {
      const value = issue ? refState(issue) : null
      writeCache(key, value)
      return value
    })
    .catch(() => null)
    .finally(() => inflight.delete(key))
  inflight.set(key, promise)
  return promise
}
