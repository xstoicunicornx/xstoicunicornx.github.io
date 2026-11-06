<script setup>
import { data } from './content.data.js'
import { STATUSES, STATUS_LABELS, TYPES } from './constants.js'

// The parent owns `form` (see fieldsToForm) and handles submit.
defineProps({
  form: { type: Object, required: true },
  submitLabel: { type: String, required: true },
  busy: { type: Boolean, default: false }
})
defineEmits(['submit'])
</script>

<template>
  <form class="item-form" @submit.prevent="$emit('submit')">
    <label>
      <span>Title</span>
      <input v-model="form.title" type="text" required />
    </label>

    <fieldset class="choice">
      <legend>Type</legend>
      <label v-for="t in TYPES" :key="t">
        <input v-model="form.type" type="radio" name="type" :value="t" />
        <span>{{ t }}</span>
      </label>
    </fieldset>

    <div class="form-row">
      <label>
        <span>Project</span>
        <select v-model="form.project">
          <option v-for="p in data.projects" :key="p.slug" :value="p.slug">{{ p.name }}</option>
        </select>
      </label>
      <label>
        <span>Status</span>
        <select v-model="form.status">
          <option v-for="s in STATUSES" :key="s" :value="s">{{ STATUS_LABELS[s] }}</option>
        </select>
      </label>
    </div>

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

    <button type="submit" class="submit" :disabled="busy">
      {{ busy ? 'Committing…' : submitLabel }}
    </button>
  </form>
</template>
