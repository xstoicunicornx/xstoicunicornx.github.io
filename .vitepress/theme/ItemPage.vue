<script setup>
import { withBase } from 'vitepress'
import { data } from './content.data.js'
import { STATUS_LABELS } from './constants.js'

defineProps({ item: { type: Object, required: true } })

const projectNames = Object.fromEntries(data.projects.map((p) => [p.slug, p.name]))
</script>

<template>
  <article class="item-page">
    <h1 class="page-title">{{ item.title }}</h1>

    <dl class="item-fields">
      <dt>Project</dt>
      <dd><a :href="withBase(`/projects/${item.project}`)">{{ projectNames[item.project] }}</a></dd>

      <dt>Type</dt>
      <dd>{{ item.type }}</dd>

      <dt>Status</dt>
      <dd>{{ STATUS_LABELS[item.status] }}<span v-if="item.archived">, archived</span></dd>

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

      <template v-if="item.issue">
        <dt>Issue</dt>
        <dd><a :href="item.issue">{{ item.issue }}</a></dd>
      </template>

      <template v-if="item.pr">
        <dt>PR</dt>
        <dd><a :href="item.pr">{{ item.pr }}</a></dd>
      </template>

      <template v-if="item.repo">
        <dt>Repo</dt>
        <dd><a :href="`https://github.com/${item.repo}`">{{ item.repo }}</a></dd>
      </template>

      <template v-if="item.links.length">
        <dt>Links</dt>
        <dd>
          <a v-for="link in item.links" :key="link" class="item-link" :href="link">{{ link }}</a>
        </dd>
      </template>
    </dl>

    <div class="prose">
      <Content />
    </div>
  </article>
</template>
