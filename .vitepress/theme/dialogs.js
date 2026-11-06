import { ref } from 'vue'

// The item pop-up: null, { mode: 'new', project }, or { mode: 'edit', item }.
export const itemDialog = ref(null)
export const tokenOpen = ref(false)

export function openNew(project = '') {
  itemDialog.value = { mode: 'new', project }
}

export function openEdit(item) {
  itemDialog.value = { mode: 'edit', item }
}

export function openToken() {
  tokenOpen.value = true
}
