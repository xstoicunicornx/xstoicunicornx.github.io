<script setup>
import { computed } from 'vue'
import { data } from './content.data.js'
import { STATUSES, STATUS_LABELS } from './constants.js'
import ItemList from './ItemList.vue'

const props = defineProps({ project: { type: String, required: true } })

const open = computed(() => data.items.filter((item) => !item.archived))

const sections = computed(() =>
  STATUSES.map((status) => ({
    status,
    items: open.value.filter((item) => item.project === props.project && item.status === status)
  })).filter((section) => section.items.length)
)

const related = computed(() =>
  open.value.filter((item) => item.related.includes(props.project))
)
</script>

<template>
  <section v-for="section in sections" :key="section.status" class="item-section">
    <h2 class="section-label">{{ STATUS_LABELS[section.status] }}</h2>
    <ItemList :items="section.items" />
  </section>
  <section v-if="related.length" class="item-section">
    <h2 class="section-label">Related</h2>
    <ItemList :items="related" show-project />
  </section>
</template>
