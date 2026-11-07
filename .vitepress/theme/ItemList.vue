<script setup>
import { ref } from 'vue'
import { useData, useRouter, withBase } from 'vitepress'
import { data } from './content.data.js'
import { relevantDate, sortItems } from './items.js'
import { advanceItem, advanceLabel } from './actions.js'
import { GitHubError, token } from './github.js'
import { openEdit, openToken } from './dialogs.js'
import { recordChange } from './localChanges.js'
import { showToast } from './toast.js'
import ItemFields from './ItemFields.vue'

const props = defineProps({
  items: { type: Array, required: true },
  showProject: { type: Boolean, default: false }
})

const { site } = useData()
const router = useRouter()
const projectNames = Object.fromEntries(data.projects.map((p) => [p.slug, p.name]))
const busy = ref(new Set())

// Clicking anywhere on a row opens the item page, except on its own links
// and buttons. The title link covers keyboard and middle-click.
function openItem(item, event) {
  if (event.target.closest('a, button')) return
  router.go(withBase(item.url))
}

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
</script>

<template>
  <ul class="item-list">
    <li
      v-for="item in sortItems(props.items)"
      :key="item.id"
      class="item-row"
      :class="{ 'item-busy': busy.has(item.id) }"
      @click="openItem(item, $event)"
    >
      <div class="item-head">
        <a class="item-title" :href="withBase(item.url)">{{ item.title }}</a>
        <span class="item-meta">
          <span v-if="showProject">{{ projectNames[item.project] }}</span>
          <span>{{ item.type }}</span>
          <span>{{ relevantDate(item) }}</span>
        </span>

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
        </div>
      </div>

      <ItemFields class="item-details" :item="item" brief />
    </li>
  </ul>
</template>
