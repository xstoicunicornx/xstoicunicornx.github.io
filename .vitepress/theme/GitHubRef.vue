<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { token } from './github.js'
import { STATE_LABELS, loadRef, parseRef } from './githubRefs.js'
import { REF_ICONS } from './octicons.js'

// A link that shows a GitHub issue or PR like GitHub does: state icon,
// title and number. Any other URL, or one GitHub can't return, stays a
// plain link. Opens in a new tab.
//
// It fetches only once the link is on screen (an item page, or a list row
// expanded on hover), using the saved token when there is one. Without a
// token GitHub allows 60 reads an hour, so fetching every hidden row up
// front would run out quickly.
const props = defineProps({ url: { type: String, required: true } })

const parsed = parseRef(props.url)
const info = ref(null)
const link = ref(null)
const visible = ref(false)
let observer

onMounted(() => {
  if (!parsed) return
  observer = new IntersectionObserver((entries) => {
    if (entries.some((entry) => entry.isIntersecting)) {
      visible.value = true
      observer.disconnect()
    }
  })
  observer.observe(link.value)
})

onBeforeUnmount(() => observer?.disconnect())

// Also retries when a token is saved after a failed tokenless read.
watch([visible, token], async ([isVisible, value]) => {
  if (!isVisible || info.value) return
  info.value = await loadRef(value, parsed)
})
</script>

<template>
  <a
    ref="link"
    class="gh-ref"
    :href="url"
    target="_blank"
    rel="noopener"
    :title="info ? STATE_LABELS[info.state] : undefined"
  >
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
