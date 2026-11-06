import fs from 'node:fs'
import path from 'node:path'
import matter from 'gray-matter'
import { validateItem } from './validate.js'

// Loads projects and items at build time and fails the build on invalid data.
export default {
  watch: [
    '../../data/order.json',
    '../../data/projects/*.json',
    '../../items/*.md',
    '../../items/archive/*.md'
  ],
  load(files) {
    const orderFile = files.find((file) => path.basename(file) === 'order.json')
    const projectFiles = files.filter((file) => file !== orderFile && file.endsWith('.json'))
    const itemFiles = files.filter((file) => file.endsWith('.md'))

    const projects = loadProjects(orderFile, projectFiles)
    const slugs = projects.map((project) => project.slug)
    const items = itemFiles.map((file) => loadItem(file, slugs))

    const seen = new Map()
    for (const item of items) {
      if (seen.has(item.id)) {
        throw new Error(`Item id ${item.id} is used by both ${seen.get(item.id)} and ${item.file}`)
      }
      seen.set(item.id, item.file)
    }

    const nextId = Math.max(0, ...items.map((item) => item.id)) + 1
    return { projects, items, nextId }
  }
}

// Projects appear in the order listed in data/order.json.
// Any project missing from that list goes at the end.
function loadProjects(orderFile, projectFiles) {
  const order = JSON.parse(fs.readFileSync(orderFile, 'utf-8'))
  const rank = (slug) => {
    const i = order.indexOf(slug)
    return i === -1 ? order.length : i
  }

  return projectFiles
    .map((file) => {
      const project = JSON.parse(fs.readFileSync(file, 'utf-8'))
      return { slug: path.basename(file, '.json'), ...project }
    })
    .sort((a, b) => rank(a.slug) - rank(b.slug))
}

function loadItem(file, slugs) {
  const match = file.match(/(?:^|\/)items\/(?:(archive)\/)?([^/]+)\.md$/)
  const name = match[1] ? `items/archive/${match[2]}.md` : `items/${match[2]}.md`
  const fail = (message) => {
    throw new Error(`${name}: ${message}`)
  }

  if (!/^[1-9]\d*$/.test(match[2])) fail('filename must be a number, like 12.md')

  const fm = matter(fs.readFileSync(file, 'utf-8')).data

  const item = {
    id: Number(match[2]),
    file: name,
    url: match[1] ? `/items/archive/${match[2]}` : `/items/${match[2]}`,
    archived: Boolean(match[1]),
    title: fm.title,
    project: fm.project,
    related: fm.related ?? [],
    type: fm.type,
    status: fm.status,
    links: fm.links ?? [],
    issue: fm.issue ?? null,
    pr: fm.pr ?? null,
    created: toDate(fm.created, 'created', fail),
    activated: toDate(fm.activated, 'activated', fail),
    completed: toDate(fm.completed, 'completed', fail)
  }

  const errors = validateItem(item, slugs)
  if (errors.length) fail(errors.join('; '))

  return item
}

// YAML parses an unquoted 2026-11-06 as a Date at UTC midnight.
function toDate(value, field, fail) {
  if (value === undefined || value === null) return null
  if (value instanceof Date) return value.toISOString().slice(0, 10)
  if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)) return value
  fail(`${field} must be a date like 2026-11-06`)
}
