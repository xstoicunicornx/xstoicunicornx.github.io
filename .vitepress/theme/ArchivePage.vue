<script setup>
import { data } from './content.data.js'
import ItemList from './ItemList.vue'

const archived = data.items.filter((item) => item.archived)
</script>

<template>
  <section>
    <h1 class="page-title">Archive</h1>
    <ItemList v-if="archived.length" :items="archived" show-project />
    <p v-else class="empty">Nothing archived yet.</p>
  </section>
</template>
