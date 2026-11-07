import { STATUSES, TYPES } from './constants.js'

// Shared by the build (content.data.js) and the new item form.
// Returns a list of problems; empty means the item is valid.
export function validateItem(item, projectSlugs) {
  const errors = []

  if (typeof item.title !== 'string' || !item.title.trim()) errors.push('title is required')
  if (!projectSlugs.includes(item.project)) errors.push(`unknown project "${item.project}"`)
  if (!TYPES.includes(item.type)) errors.push(`type must be one of: ${TYPES.join(', ')}`)
  if (!STATUSES.includes(item.status)) errors.push(`status must be one of: ${STATUSES.join(', ')}`)
  if (!Array.isArray(item.links)) {
    errors.push('links must be a list')
  } else {
    for (const link of item.links) checkUrl(link, 'links', errors)
  }
  if (item.issue !== null) checkUrl(item.issue, 'issue', errors)
  if (item.pr !== null) checkUrl(item.pr, 'pr', errors)
  if (item.repo !== null && !/^[^/\s]+\/[^/\s]+$/.test(item.repo)) {
    errors.push(`repo must look like owner/name, got ${JSON.stringify(item.repo)}`)
  }
  if (!item.created) errors.push('created is required')
  if (item.status === 'active' && !item.activated) errors.push('activated is required when status is active')
  if (item.status === 'done' && !item.completed) errors.push('completed is required when status is done')
  if (item.status === 'cancelled' && !item.cancelled) errors.push('cancelled is required when status is cancelled')

  return errors
}

function checkUrl(value, field, errors) {
  if (typeof value !== 'string' || !/^https?:\/\/\S+$/.test(value)) {
    errors.push(`${field} must be a http(s) URL, got ${JSON.stringify(value)}`)
  }
}

const REPO = /^[^/\s]+\/[^/\s]+$/

// Checks the frontmatter of a projects/<name>.md page.
// `org` is a GitHub org or user; leave it out for a single personal repo.
// `repo` is the main repo, optional when `org` is set.
export function validateProjectPage(fm) {
  const errors = []
  if (fm.org !== undefined && (typeof fm.org !== 'string' || !/^[^/\s]+$/.test(fm.org))) {
    errors.push('org must be a GitHub org or user name, like payjoin')
  }
  if (fm.repo === undefined) {
    if (fm.org === undefined) errors.push('repo is required when there is no org')
  } else if (typeof fm.repo !== 'string' || !REPO.test(fm.repo)) {
    errors.push('repo must look like owner/name')
  }
  const repos = fm.repos ?? []
  if (!Array.isArray(repos)) {
    errors.push('repos must be a list')
  } else {
    for (const repo of repos) {
      if (typeof repo !== 'string' || !REPO.test(repo)) errors.push(`repos entry must look like owner/name, got ${JSON.stringify(repo)}`)
      else if (repo === fm.repo) errors.push(`repos repeats the main repo ${repo}`)
    }
  }
  return errors
}
