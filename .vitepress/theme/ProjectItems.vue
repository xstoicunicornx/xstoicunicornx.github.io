<script setup>
import { computed } from 'vue'
import { SECTION_ORDER, STATUS_LABELS } from './constants.js'
import { useItems } from './localChanges.js'
import ItemList from './ItemList.vue'

const props = defineProps({ project: { type: String, required: true } })

const items = useItems()

const sections = computed(() => {
  const mine = items.value.filter((item) => !item.archived && item.project === props.project)
  return SECTION_ORDER.map((status) => ({
    status,
    items: mine.filter((item) => item.status === status)
  })).filter((section) => section.items.length)
})
</script>

<template>
  <section v-for="section in sections" :key="section.status" class="item-section">
    <h2 class="section-label">{{ STATUS_LABELS[section.status] }}</h2>
    <ItemList :items="section.items" />
  </section>
</template>
