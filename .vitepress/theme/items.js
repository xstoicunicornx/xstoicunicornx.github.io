// The date that matters most for an item in its current status.
export function relevantDate(item) {
  if (item.status === 'active') return item.activated
  if (item.status === 'done') return item.completed
  return item.created
}

// Newest first by relevant date, then by id.
export function sortItems(items) {
  return [...items].sort(
    (a, b) => (relevantDate(b) ?? '').localeCompare(relevantDate(a) ?? '') || b.id - a.id
  )
}
