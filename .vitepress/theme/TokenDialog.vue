<script setup>
import { ref, watch } from 'vue'
import { useData } from 'vitepress'
import { GitHubError, checkRepoAccess, clearToken, saveToken, token } from './github.js'
import { tokenOpen } from './dialogs.js'
import BaseDialog from './BaseDialog.vue'

const { site } = useData()

const input = ref('')
const message = ref(null)
const saving = ref(false)

watch(tokenOpen, (open) => {
  if (open) {
    input.value = ''
    message.value = null
  }
})

async function save() {
  const value = input.value.trim()
  message.value = null
  if (!value) return

  saving.value = true
  try {
    await checkRepoAccess(value, site.value.themeConfig.repo)
    const stored = saveToken(value)
    input.value = ''
    message.value = stored
      ? { error: false, text: 'Token saved.' }
      : { error: true, text: 'Token works, but this browser blocked saving it. It will be forgotten on reload.' }
  } catch (err) {
    message.value = { error: true, text: err instanceof GitHubError ? err.message : `Request failed: ${err.message}` }
  } finally {
    saving.value = false
  }
}

function forget() {
  clearToken()
  message.value = { error: false, text: 'Token removed from this browser.' }
}
</script>

<template>
  <BaseDialog :open="tokenOpen" title="GitHub token" @close="tokenOpen = false">
    <p class="token-status">
      <template v-if="token">
        A token is saved in this browser.
        <button type="button" class="link-button" @click="forget">Forget token</button>
      </template>
      <template v-else>No token is saved in this browser.</template>
    </p>

    <p v-if="message" :class="message.error ? 'form-errors' : 'notice'">{{ message.text }}</p>

    <form class="item-form" @submit.prevent="save">
      <label>
        <span>{{ token ? 'Replace token' : 'Token' }}</span>
        <input v-model="input" type="password" autocomplete="off" />
        <small>
          A fine-grained token with Contents read and write access to
          {{ site.themeConfig.repo }} only. It is checked against GitHub before saving.
        </small>
      </label>
      <button type="submit" class="submit" :disabled="saving || !input.trim()">
        {{ saving ? 'Checking…' : 'Save token' }}
      </button>
    </form>
  </BaseDialog>
</template>
