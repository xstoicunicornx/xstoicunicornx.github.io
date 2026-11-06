import { NEXT_STATUS, STATUS_LABELS } from './constants.js'
import { getFile, moveFile, updateFile, GitHubError } from './github.js'
import { parseItemFile, renderItemFile, today, withStatus } from './itemFile.js'
import { recordChange } from './localChanges.js'

// Label for the advance button, or null when there is nothing to advance to.
export function advanceLabel(item) {
  if (item.archived) return null
  if (NEXT_STATUS[item.status]) return STATUS_LABELS[NEXT_STATUS[item.status]]
  return 'Archive'
}

export function canCancel(item) {
  return !item.archived && item.status !== 'done' && item.status !== 'cancelled'
}

// Reads the item from GitHub and stops if it no longer matches what the page shows.
async function readCurrent(gh, item) {
  const file = await getFile(gh.token, gh.repo, gh.branch, item.file)
  if (!file) {
    throw new GitHubError(`${item.file} is no longer on GitHub. Reload once the site rebuilds.`)
  }
  const parsed = parseItemFile(file.text)
  if (parsed.fields.status !== item.status) {
    throw new GitHubError(
      `Item ${item.id} is ${parsed.fields.status} on GitHub but ${item.status} here. Reload once the site rebuilds.`
    )
  }
  return { ...file, ...parsed }
}

export async function advanceItem(gh, item) {
  const current = await readCurrent(gh, item)
  const next = NEXT_STATUS[item.status]

  if (next) {
    const fields = withStatus(current.fields, next, today())
    const result = await updateFile(
      gh.token, gh.repo, gh.branch, item.file,
      renderItemFile(fields, current.description), current.sha,
      `Move item ${item.id} to ${next}: ${fields.title}`
    )
    recordChange({ ...item, ...fields })
    return { ...result, text: `Moved item ${item.id} to ${next}.` }
  }

  const to = `items/archive/${item.id}.md`
  const result = await moveFile(
    gh.token, gh.repo, gh.branch, item.file, to, current.text,
    `Archive item ${item.id}: ${current.fields.title}`
  )
  recordChange({
    ...item,
    ...current.fields,
    archived: true,
    file: to,
    url: `/items/archive/${item.id}`
  })
  return { ...result, text: `Archived item ${item.id}.` }
}

export async function cancelItem(gh, item) {
  const current = await readCurrent(gh, item)
  const fields = withStatus(current.fields, 'cancelled', today())
  const result = await updateFile(
    gh.token, gh.repo, gh.branch, item.file,
    renderItemFile(fields, current.description), current.sha,
    `Cancel item ${item.id}: ${fields.title}`
  )
  recordChange({ ...item, ...fields })
  return { ...result, text: `Cancelled item ${item.id}.` }
}
