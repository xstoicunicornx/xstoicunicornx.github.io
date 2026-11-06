<script setup>
import { computed } from 'vue'
import { useItems } from './localChanges.js'
import ItemList from './ItemList.vue'

const items = useItems()
const archived = computed(() => items.value.filter((item) => item.archived))
</script>

<template>
  <section>
    <h1 class="page-title">Archive</h1>
    <ItemList v-if="archived.length" :items="archived" show-project />
    <p v-else class="empty">Nothing archived yet.</p>
  </section>
</template>
