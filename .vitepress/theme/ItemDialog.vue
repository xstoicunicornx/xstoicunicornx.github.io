<script setup>
import { computed, reactive, ref, watch } from 'vue'
import { useData } from 'vitepress'
import { data } from './content.data.js'
import { TYPES } from './constants.js'
import { validateItem } from './validate.js'
import { fieldsToForm, formToFields, parseItemFile, renderItemFile, today } from './itemFile.js'
import { GitHubError, createItem, getFile, token, updateFile } from './github.js'
import { recordChange } from './localChanges.js'
import { itemDialog } from './dialogs.js'
import { showToast } from './toast.js'
import BaseDialog from './BaseDialog.vue'
import ItemForm from './ItemForm.vue'
import TokenNotice from './TokenNotice.vue'

// Creates a new item or edits an existing one, depending on itemDialog.mode.

const { site } = useData()
const slugs = data.projects.map((p) => p.slug)

const form = reactive({})
const base = ref(null) // fields the form applies on top of; set once the form is ready
const sha = ref(null) // version read from GitHub, when editing
const loading = ref(false)
const saving = ref(false)
const errors = ref([])

const editItem = computed(() => (itemDialog.value?.mode === 'edit' ? itemDialog.value.item : null))
const title = computed(() => (editItem.value ? `Edit item ${editItem.value.id}` : 'New item'))

watch(itemDialog, (state) => {
  base.value = null
  sha.value = null
  errors.value = []
  if (state?.mode === 'new') {
    const date = today()
    base.value = { status: null, repo: null, created: date, activated: null, completed: null, cancelled: null }
    Object.assign(form, {
      title: '',
      project: slugs.includes(state.project) ? state.project : slugs[0] ?? '',
      type: TYPES[0],
      status: 'todo',
      issue: '',
      pr: '',
      links: '',
      description: ''
    })
  }
})

// Edit the version on GitHub, not the built one, which may be out of date.
// Also reruns when a token is set from inside this dialog.
watch([editItem, token], async ([item]) => {
  if (!item || !token.value || base.value || loading.value) return
  loading.value = true
  try {
    const { repo, branch } = site.value.themeConfig
    const file = await getFile(token.value, repo, branch, item.file)
    if (!file) throw new GitHubError(`${item.file} is no longer on GitHub. Reload once the site rebuilds.`)
    const { fields, description } = parseItemFile(file.text)
    Object.assign(form, fieldsToForm(fields, description))
    base.value = fields
    sha.value = file.sha
  } catch (err) {
    errors.value = [err instanceof GitHubError ? err.message : `Could not load item: ${err.message}`]
  } finally {
    loading.value = false
  }
})

async function submit() {
  const fields = formToFields(form, base.value, today())
  errors.value = validateItem(fields, slugs)
  if (!token.value) errors.value.push('no GitHub token is set')
  if (errors.value.length) return

  saving.value = true
  try {
    const { repo, branch } = site.value.themeConfig
    const content = renderItemFile(fields, form.description)
    const item = editItem.value
    let text, commitUrl
    if (item) {
      ;({ commitUrl } = await updateFile(
        token.value, repo, branch, item.file, content, sha.value,
        `Edit item ${item.id}: ${fields.title}`
      ))
      recordChange({ ...item, ...fields })
      text = `Saved item ${item.id}.`
    } else {
      let id
      ;({ id, commitUrl } = await createItem(token.value, repo, branch, (id) => ({
        content,
        message: `Add item ${id}: ${fields.title}`
      })))
      text = `Created item ${id}. It appears once the site rebuilds, usually within a couple of minutes.`
    }
    showToast(text, commitUrl ? { link: { href: commitUrl, text: 'View commit' } } : {})
    itemDialog.value = null
  } catch (err) {
    errors.value = [err instanceof GitHubError ? err.message : `Request failed: ${err.message}`]
  } finally {
    saving.value = false
  }
}
</script>

<template>
  <BaseDialog :open="Boolean(itemDialog)" :title="title" @close="itemDialog = null">
    <TokenNotice v-if="!token" :action="editItem ? 'edited' : 'committed'" />
    <p v-else-if="loading" class="empty">Loading from GitHub…</p>

    <ul v-if="errors.length" class="form-errors">
      <li v-for="error in errors" :key="error">{{ error }}</li>
    </ul>

    <ItemForm
      v-if="base"
      :form="form"
      :submit-label="editItem ? 'Save' : 'Create item'"
      :busy="saving"
      @submit="submit"
    />
  </BaseDialog>
</template>
