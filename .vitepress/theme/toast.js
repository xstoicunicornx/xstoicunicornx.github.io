import { ref } from 'vue'

export const toast = ref(null)
let timer

// Shows a short message at the bottom of the screen. `link` is optional:
// { text, href } for a link or { text, action } for a button.
export function showToast(text, { error = false, link = null } = {}) {
  clearTimeout(timer)
  toast.value = { text, error, link }
  timer = setTimeout(() => (toast.value = null), error ? 10000 : 6000)
}
