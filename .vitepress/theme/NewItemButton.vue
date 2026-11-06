<script setup>
import { openNew } from './dialogs.js'

defineProps({ project: { type: String, default: '' } })
</script>

<template>
  <button type="button" class="new-item" @click="openNew(project)">New item</button>
</template>
