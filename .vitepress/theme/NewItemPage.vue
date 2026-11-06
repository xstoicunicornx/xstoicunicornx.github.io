<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import { useData, withBase } from 'vitepress'
import { data } from './content.data.js'
import { STATUSES, STATUS_LABELS, TYPES } from './constants.js'
import { validateItem } from './validate.js'
import { renderItemFile } from './itemFile.js'
import { GitHubError, createItem, getToken } from './github.js'

const { site } = useData()
const slugs = data.projects.map((p) => p.slug)

const form = reactive({
  title: '',
  project: slugs[0] ?? '',
  related: [],
  type: TYPES[0],
  status: 'todo',
  issue: '',
  pr: '',
  links: '',
  description: ''
})

const token = ref('')
// localStorage is only readable in the browser, so wait for mount before
// deciding the token is missing.
const ready = ref(false)
const errors = ref([])
const submitting = ref(false)
const result = ref(null)

onMounted(() => {
  const project = new URLSearchParams(location.search).get('project')
  if (slugs.includes(project)) form.project = project
  token.value = getToken()
  ready.value = true
})

const otherProjects = computed(() => data.projects.filter((p) => p.slug !== form.project))

function today() {
  const d = new Date()
  const pad = (n) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
}

function buildItem() {
  const date = today()
  const optional = (v) => v.trim() || null
  return {
    title: form.title.trim(),
    project: form.project,
    related: form.related.filter((slug) => slug !== form.project),
    type: form.type,
    status: form.status,
    links: form.links.split('\n').map((l) => l.trim()).filter(Boolean),
    issue: optional(form.issue),
    pr: optional(form.pr),
    created: date,
    activated: form.status === 'active' ? date : null,
    completed: form.status === 'done' ? date : null
  }
}

async function submit() {
  result.value = null
  const item = buildItem()
  errors.value = validateItem(item, slugs)
  if (!token.value) errors.value.push('no GitHub token is set')
  if (errors.value.length) return

  submitting.value = true
  try {
    const { repo, branch } = site.value.themeConfig
    const { id, commitUrl } = await createItem(token.value, repo, branch, (id) => ({
      content: renderItemFile(item, form.description),
      message: `Add item ${id}: ${item.title}`
    }))
    result.value = { id, commitUrl }
    Object.assign(form, { title: '', related: [], issue: '', pr: '', links: '', description: '' })
  } catch (err) {
    errors.value = [err instanceof GitHubError ? err.message : `Request failed: ${err.message}`]
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <section class="new-item-page">
    <h1 class="page-title">New item</h1>

    <div v-if="result" class="notice">
      Created item {{ result.id }}.
      <a v-if="result.commitUrl" :href="result.commitUrl">View commit</a>.
      It will be at <a :href="withBase(`/items/${result.id}`)">/items/{{ result.id }}</a>
      once the site rebuilds, usually within a couple of minutes.
    </div>

    <p v-if="ready && !token" class="notice">
      No GitHub token is set, so items can't be committed yet.
      <a :href="withBase('/token')">Set a token</a>.
    </p>

    <ul v-if="errors.length" class="form-errors">
      <li v-for="error in errors" :key="error">{{ error }}</li>
    </ul>

    <form class="item-form" @submit.prevent="submit">
      <label>
        <span>Title</span>
        <input v-model="form.title" type="text" required />
      </label>

      <div class="form-row">
        <label>
          <span>Project</span>
          <select v-model="form.project">
            <option v-for="p in data.projects" :key="p.slug" :value="p.slug">{{ p.name }}</option>
          </select>
        </label>
        <label>
          <span>Type</span>
          <select v-model="form.type">
            <option v-for="t in TYPES" :key="t" :value="t">{{ t }}</option>
          </select>
        </label>
        <label>
          <span>Status</span>
          <select v-model="form.status">
            <option v-for="s in STATUSES" :key="s" :value="s">{{ STATUS_LABELS[s] }}</option>
          </select>
        </label>
      </div>

      <fieldset>
        <legend>Related</legend>
        <label v-for="p in otherProjects" :key="p.slug" class="checkbox">
          <input v-model="form.related" type="checkbox" :value="p.slug" />
          {{ p.name }}
        </label>
      </fieldset>

      <label>
        <span>Issue</span>
        <input v-model="form.issue" type="url" placeholder="https://" />
      </label>

      <label>
        <span>PR</span>
        <input v-model="form.pr" type="url" placeholder="https://" />
      </label>

      <label>
        <span>Links, one per line</span>
        <textarea v-model="form.links" rows="3"></textarea>
      </label>

      <label>
        <span>Description</span>
        <textarea v-model="form.description" rows="6"></textarea>
      </label>

      <button type="submit" class="submit" :disabled="submitting">
        {{ submitting ? 'Committing…' : 'Create item' }}
      </button>
    </form>
  </section>
</template>
