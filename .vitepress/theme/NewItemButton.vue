<script setup>
import { computed } from 'vue'
import { withBase } from 'vitepress'

const props = defineProps({ project: { type: String, default: '' } })

const href = computed(() =>
  withBase(props.project ? `/new?project=${encodeURIComponent(props.project)}` : '/new')
)
</script>

<template>
  <a class="new-item" :href="href">New item</a>
</template>
