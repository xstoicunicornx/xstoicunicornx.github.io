import { load } from 'js-yaml'
import { STATUS_DATE_FIELDS } from './constants.js'

// Item files are Markdown with YAML frontmatter. The build and the
// browser both read and write them through these functions.

const FRONTMATTER = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/

// Returns { fields, description }. Throws on malformed frontmatter or dates.
export function parseItemFile(text) {
  const match = text.match(FRONTMATTER)
  if (!match) throw new Error('missing frontmatter')
  const fm = load(match[1]) ?? {}
  const fields = {
    title: fm.title,
    project: fm.project,
    type: fm.type,
    status: fm.status,
    links: fm.links ?? [],
    issue: fm.issue ?? null,
    pr: fm.pr ?? null,
    // Files written by hand may not have a repo yet, so derive it.
    repo: fm.repo ?? repoFromLinks(fm.pr ?? null, fm.issue ?? null),
    created: toDate(fm.created, 'created'),
    activated: toDate(fm.activated, 'activated'),
    completed: toDate(fm.completed, 'completed'),
    cancelled: toDate(fm.cancelled, 'cancelled')
  }
  return { fields, description: text.slice(match[0].length).trim() }
}

function toDate(value, field) {
  if (value === undefined || value === null) return null
  if (value instanceof Date) return value.toISOString().slice(0, 10)
  if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)) return value
  throw new Error(`${field} must be a date like 2026-11-06`)
}

// JSON strings and arrays are valid YAML, so JSON.stringify quotes values safely.
export function renderItemFile(fields, description) {
  const value = (v) => (v === null ? '' : JSON.stringify(v))
  const date = (v) => v ?? ''
  const lines = [
    '---',
    `title: ${value(fields.title)}`,
    `project: ${fields.project}`,
    `type: ${fields.type}`,
    `status: ${fields.status}`,
    `links: ${value(fields.links)}`,
    `issue: ${value(fields.issue)}`,
    `pr: ${value(fields.pr)}`,
    `repo: ${value(fields.repo)}`,
    `created: ${date(fields.created)}`,
    `activated: ${date(fields.activated)}`,
    `completed: ${date(fields.completed)}`,
    `cancelled: ${date(fields.cancelled)}`,
    '---',
    ''
  ]
  const body = description.trim()
  return lines.join('\n').replace(/ +$/gm, '') + (body ? `\n${body}\n` : '')
}

// "owner/name" from a GitHub issue or PR URL, or null for any other URL.
export function repoFromUrl(url) {
  const match = url?.match(/^https?:\/\/github\.com\/([^/\s]+)\/([^/\s]+)\/(?:pull|issues)\/\d+/)
  return match ? `${match[1]}/${match[2]}` : null
}

// The PR link takes precedence over the issue link.
export function repoFromLinks(pr, issue) {
  return repoFromUrl(pr) ?? repoFromUrl(issue)
}

// Returns a copy with the new status, filling in that status's date if unset.
export function withStatus(fields, status, date) {
  const next = { ...fields, status }
  const dateField = STATUS_DATE_FIELDS[status]
  if (dateField && !next[dateField]) next[dateField] = date
  return next
}

export function today() {
  const d = new Date()
  const pad = (n) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
}

// Form state uses strings for every input; links are one per line.
export function fieldsToForm(fields, description) {
  return {
    title: fields.title ?? '',
    project: fields.project,
    type: fields.type,
    status: fields.status,
    issue: fields.issue ?? '',
    pr: fields.pr ?? '',
    links: fields.links.join('\n'),
    description
  }
}

// Applies form input on top of `base`. Changing the status fills in the
// date for the new status if it is not already set.
export function formToFields(form, base, date) {
  const optional = (v) => v.trim() || null
  const issue = optional(form.issue)
  const pr = optional(form.pr)
  const fields = {
    ...base,
    title: form.title.trim(),
    project: form.project,
    type: form.type,
    links: form.links.split('\n').map((l) => l.trim()).filter(Boolean),
    issue,
    pr,
    repo: repoFromLinks(pr, issue)
  }
  return form.status === base.status ? fields : withStatus(fields, form.status, date)
}
