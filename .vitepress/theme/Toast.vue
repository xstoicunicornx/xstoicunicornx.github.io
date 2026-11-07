<script setup>
import { toast } from './toast.js'
</script>

<template>
  <div v-if="toast" class="toast" :class="{ 'toast-error': toast.error }" role="status">
    {{ toast.text }}
    <a v-if="toast.link?.href" :href="toast.link.href" target="_blank" rel="noopener">{{ toast.link.text }}</a>
    <button v-else-if="toast.link" type="button" class="link-button" @click="toast.link.action">
      {{ toast.link.text }}
    </button>
    <button type="button" class="toast-close" aria-label="Dismiss" @click="toast = null">×</button>
  </div>
</template>
