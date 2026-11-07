<script setup>
import ItemFields from './ItemFields.vue'

defineProps({ item: { type: Object, required: true } })
</script>

<template>
  <article class="item-page">
    <h1 class="page-title">{{ item.title }}</h1>
    <ItemFields :item="item" />
    <div class="prose">
      <Content />
    </div>
  </article>
</template>
