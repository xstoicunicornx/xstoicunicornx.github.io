<script setup>
import { ref, watch } from 'vue'
import { token } from './github.js'
import { STATE_LABELS, loadRef, parseRef } from './githubRefs.js'
import { REF_ICONS } from './octicons.js'

// A link that shows a GitHub issue or PR like GitHub does: state icon,
// number and title. Needs a saved token; otherwise, and for any URL that is
// not an issue or PR, it is a plain link. Opens in a new tab.
const props = defineProps({ url: { type: String, required: true } })

const parsed = parseRef(props.url)
const info = ref(null)

watch(
  token,
  async (value) => {
    if (!value || !parsed) return
    info.value = await loadRef(value, parsed)
  },
  { immediate: true }
)
</script>

<template>
  <a class="gh-ref" :href="url" target="_blank" rel="noopener" :title="info ? STATE_LABELS[info.state] : undefined">
    <template v-if="info">
      <svg
        class="gh-ref-icon"
        :class="`gh-${info.state}`"
        viewBox="0 0 16 16"
        width="16"
        height="16"
        aria-hidden="true"
      >
        <path v-for="d in REF_ICONS[info.state]" :key="d" :d="d" fill="currentColor" />
      </svg>
      <span class="gh-ref-title">{{ info.title }}</span>
      <span class="gh-ref-number">{{ parsed.repo }}#{{ parsed.number }}</span>
    </template>
    <template v-else>{{ url }}</template>
  </a>
</template>
