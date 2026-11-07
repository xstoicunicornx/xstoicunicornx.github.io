<script setup>
import { withBase } from 'vitepress'
import { data } from './content.data.js'
import { STATUS_LABELS } from './constants.js'
import GitHubRef from './GitHubRef.vue'

// An item's fields as a definition list. Used on the item page and in the
// hover expansion of item lists, which pass `brief` to skip the project and
// type already shown in the row. External links open in a new tab.
defineProps({
  item: { type: Object, required: true },
  brief: { type: Boolean, default: false }
})

const projectNames = Object.fromEntries(data.projects.map((p) => [p.slug, p.name]))
</script>

<template>
  <dl class="item-fields">
    <template v-if="!brief">
      <dt>Project</dt>
      <dd><a :href="withBase(`/projects/${item.project}`)">{{ projectNames[item.project] }}</a></dd>

      <dt>Type</dt>
      <dd>{{ item.type }}</dd>
    </template>

    <dt>Status</dt>
    <dd>{{ STATUS_LABELS[item.status] }}<span v-if="item.archived">, archived</span></dd>

    <template v-if="item.repo">
      <dt>Repo</dt>
      <dd><a :href="`https://github.com/${item.repo}`" target="_blank" rel="noopener">{{ item.repo }}</a></dd>
    </template>

    <template v-if="item.issue">
      <dt>Issue</dt>
      <dd><GitHubRef :url="item.issue" /></dd>
    </template>

    <template v-if="item.pr">
      <dt>PR</dt>
      <dd><GitHubRef :url="item.pr" /></dd>
    </template>

    <template v-if="item.links.length">
      <dt>Links</dt>
      <dd>
        <GitHubRef v-for="link in item.links" :key="link" class="item-link" :url="link" />
      </dd>
    </template>

    <dt>Created</dt>
    <dd>{{ item.created }}</dd>

    <template v-if="item.activated">
      <dt>Activated</dt>
      <dd>{{ item.activated }}</dd>
    </template>

    <template v-if="item.completed">
      <dt>Completed</dt>
      <dd>{{ item.completed }}</dd>
    </template>

    <template v-if="item.cancelled">
      <dt>Cancelled</dt>
      <dd>{{ item.cancelled }}</dd>
    </template>
  </dl>
</template>
