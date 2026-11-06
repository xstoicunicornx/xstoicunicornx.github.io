// Writes an item as a Markdown file with YAML frontmatter.
// JSON strings and arrays are valid YAML, so JSON.stringify quotes values safely.
export function renderItemFile(item, description) {
  const value = (v) => (v === null ? '' : JSON.stringify(v))
  const date = (v) => v ?? ''
  const lines = [
    '---',
    `title: ${value(item.title)}`,
    `project: ${item.project}`,
    `related: ${value(item.related)}`,
    `type: ${item.type}`,
    `status: ${item.status}`,
    `links: ${value(item.links)}`,
    `issue: ${value(item.issue)}`,
    `pr: ${value(item.pr)}`,
    `created: ${date(item.created)}`,
    `activated: ${date(item.activated)}`,
    `completed: ${date(item.completed)}`,
    '---',
    ''
  ]
  const body = description.trim()
  return lines.join('\n').replace(/ +$/gm, '') + (body ? `\n${body}\n` : '')
}
