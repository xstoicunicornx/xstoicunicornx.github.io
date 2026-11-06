<script setup>
import { onMounted, ref } from 'vue'
import { useData } from 'vitepress'
import { GitHubError, checkRepoAccess, clearToken, getToken, setToken } from './github.js'

const { site } = useData()

const saved = ref(false)
const input = ref('')
const message = ref(null)
const saving = ref(false)

onMounted(() => {
  saved.value = Boolean(getToken())
})

async function save() {
  const token = input.value.trim()
  message.value = null
  if (!token) return

  saving.value = true
  try {
    await checkRepoAccess(token, site.value.themeConfig.repo)
    setToken(token)
    if (getToken() !== token) {
      message.value = { error: true, text: "This browser blocked saving the token (private window or blocked site data)." }
      return
    }
    saved.value = true
    input.value = ''
    message.value = { error: false, text: 'Token saved.' }
  } catch (err) {
    const text = err instanceof GitHubError ? err.message : `Request failed: ${err.message}`
    message.value = { error: true, text }
  } finally {
    saving.value = false
  }
}

function forget() {
  clearToken()
  saved.value = false
  message.value = { error: false, text: 'Token removed from this browser.' }
}
</script>

<template>
  <section>
    <h1 class="page-title">GitHub token</h1>

    <p class="token-status">
      <template v-if="saved">
        A token is saved in this browser.
        <button type="button" class="link-button" @click="forget">Forget token</button>
      </template>
      <template v-else>No token is saved in this browser.</template>
    </p>

    <p v-if="message" :class="message.error ? 'form-errors' : 'notice'">{{ message.text }}</p>

    <form class="item-form" @submit.prevent="save">
      <label>
        <span>{{ saved ? 'Replace token' : 'Token' }}</span>
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
  </section>
</template>
