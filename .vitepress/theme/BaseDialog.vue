<script setup>
import { ref, watch } from 'vue'

// A modal pop-up. Opens while `open` is true; emits close when dismissed
// by the × button, Escape, or a click on the backdrop.
const props = defineProps({
  open: { type: Boolean, required: true },
  title: { type: String, required: true }
})
const emit = defineEmits(['close'])

const dialog = ref(null)

watch(
  () => props.open,
  (open) => {
    if (open && !dialog.value.open) dialog.value.showModal()
    if (!open && dialog.value.open) dialog.value.close()
  },
  { flush: 'post' }
)

// Clicks on the backdrop target the dialog element itself, but so do clicks
// on its padding, so check whether the click landed outside the box.
function onBackdrop(event) {
  if (event.target !== dialog.value) return
  const box = dialog.value.getBoundingClientRect()
  const inside =
    event.clientX >= box.left && event.clientX <= box.right &&
    event.clientY >= box.top && event.clientY <= box.bottom
  if (!inside) dialog.value.close()
}
</script>

<template>
  <dialog ref="dialog" class="dialog" @close="emit('close')" @click="onBackdrop">
    <div class="dialog-head">
      <h2>{{ title }}</h2>
      <button type="button" class="icon-button" aria-label="Close" @click="dialog.close()">×</button>
    </div>
    <slot />
  </dialog>
</template>
