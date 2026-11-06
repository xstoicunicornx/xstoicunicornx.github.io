<script setup>
import { openToken } from './dialogs.js'

defineProps({ action: { type: String, required: true } })
</script>

<template>
  <p class="notice">
    No GitHub token is set, so items can't be {{ action }} yet.
    <button type="button" class="link-button" @click="openToken">Set a token</button>
  </p>
</template>
