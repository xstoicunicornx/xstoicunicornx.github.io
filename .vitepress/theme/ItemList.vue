<script setup>
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { useData, withBase } from 'vitepress'
import { data } from './content.data.js'
import { relevantDate, sortItems } from './items.js'
import { advanceItem, advanceLabel, canCancel, cancelItem } from './actions.js'
import { GitHubError, token } from './github.js'
import { openEdit, openToken } from './dialogs.js'
import { recordChange } from './localChanges.js'
import { showToast } from './toast.js'

const props = defineProps({
  items: { type: Array, required: true },
  showProject: { type: Boolean, default: false }
})

const { site } = useData()
const projectNames = Object.fromEntries(data.projects.map((p) => [p.slug, p.name]))
const busy = ref(new Set())
const list = ref(null)

async function run(item, action) {
  if (!token.value) {
    showToast('No GitHub token is set.', { error: true, link: { action: openToken, text: 'Set a token' } })
    return
  }
  busy.value = new Set(busy.value).add(item.id)
  try {
    const { text, commitUrl, item: changed } = await action({ token: token.value, ...site.value.themeConfig }, item)
    recordChange(changed)
    showToast(text, commitUrl ? { link: { href: commitUrl, text: 'View commit' } } : {})
  } catch (err) {
    showToast(err instanceof GitHubError ? err.message : `Request failed: ${err.message}`, { error: true })
  } finally {
    const next = new Set(busy.value)
    next.delete(item.id)
    busy.value = next
  }
}

function cancel(item, event) {
  event.target.closest('details').open = false
  if (confirm(`Cancel item ${item.id}, "${item.title}"?`)) run(item, cancelItem)
}

// Close an open quick actions menu when clicking anywhere else.
function closeMenus(event) {
  for (const menu of list.value?.querySelectorAll('details.menu[open]') ?? []) {
    if (!menu.contains(event.target)) menu.open = false
  }
}
onMounted(() => document.addEventListener('click', closeMenus))
onBeforeUnmount(() => document.removeEventListener('click', closeMenus))
</script>

<template>
  <ul ref="list" class="item-list">
    <li
      v-for="item in sortItems(props.items)"
      :key="item.id"
      class="item-row"
      :class="{ 'item-busy': busy.has(item.id) }"
    >
      <a class="item-main" :href="withBase(item.url)">
        <span class="item-title">{{ item.title }}</span>
        <span class="item-meta">
          <span v-if="showProject">{{ projectNames[item.project] }}</span>
          <span>{{ item.type }}</span>
          <span>{{ relevantDate(item) }}</span>
        </span>
      </a>

      <div class="item-actions">
        <button
          type="button"
          class="icon-button"
          title="Edit"
          aria-label="Edit"
          :disabled="busy.has(item.id)"
          @click="openEdit(item)"
        >
          <svg viewBox="0 0 16 16" width="15" height="15" aria-hidden="true">
            <path
              d="M11.5 1.5l3 3L5 14H2v-3z"
              fill="none"
              stroke="currentColor"
              stroke-width="1.4"
              stroke-linejoin="round"
            />
          </svg>
        </button>

        <button
          v-if="advanceLabel(item)"
          type="button"
          class="action-button"
          :disabled="busy.has(item.id)"
          @click="run(item, advanceItem)"
        >
          {{ advanceLabel(item) }}
        </button>

        <details v-if="canCancel(item)" class="menu">
          <summary class="icon-button" title="More actions" aria-label="More actions">
            <svg viewBox="0 0 16 16" width="15" height="15" aria-hidden="true">
              <circle cx="3" cy="8" r="1.4" fill="currentColor" />
              <circle cx="8" cy="8" r="1.4" fill="currentColor" />
              <circle cx="13" cy="8" r="1.4" fill="currentColor" />
            </svg>
          </summary>
          <div class="menu-panel">
            <button type="button" :disabled="busy.has(item.id)" @click="cancel(item, $event)">
              Cancel item
            </button>
          </div>
        </details>
      </div>
    </li>
  </ul>
</template>
