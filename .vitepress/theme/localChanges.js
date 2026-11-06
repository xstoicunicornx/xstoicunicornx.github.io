import { computed, onMounted, ref } from 'vue'
import { data } from './content.data.js'

// Changes committed from this browser show up right away, before the site
// rebuilds. Each one is dropped once the built data matches it, or after
// 30 minutes in case the build never catches up (for example a failed deploy).

const KEY = 'item-changes'
const MAX_AGE = 30 * 60 * 1000
const COMPARED = [
  'archived', 'title', 'project', 'type', 'status', 'links', 'issue', 'pr', 'repo',
  'created', 'activated', 'completed', 'cancelled'
]

const changes = ref({})
let loaded = false

function persist() {
  try {
    localStorage.setItem(KEY, JSON.stringify(changes.value))
  } catch {}
}

function load() {
  if (loaded) return
  loaded = true
  try {
    changes.value = JSON.parse(localStorage.getItem(KEY) ?? '{}')
  } catch {
    changes.value = {}
  }

  const built = new Map(data.items.map((item) => [item.id, item]))
  const same = (a, b) => COMPARED.every((k) => JSON.stringify(a[k]) === JSON.stringify(b[k]))
  const kept = {}
  for (const [id, change] of Object.entries(changes.value)) {
    if (Date.now() - change.savedAt > MAX_AGE) continue
    const item = built.get(Number(id))
    if (item && same(item, change.item)) continue
    kept[id] = change
  }
  changes.value = kept
  persist()
}

export function recordChange(item) {
  changes.value = { ...changes.value, [item.id]: { item, savedAt: Date.now() } }
  persist()
}

// Built items with local changes applied. Changes apply only after mount,
// so the server-rendered HTML and the first client render match.
export function useItems() {
  const mounted = ref(false)
  onMounted(() => {
    load()
    mounted.value = true
  })
  return computed(() =>
    mounted.value ? data.items.map((item) => changes.value[item.id]?.item ?? item) : data.items
  )
}
