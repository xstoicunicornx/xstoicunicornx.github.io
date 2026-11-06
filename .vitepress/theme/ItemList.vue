<script setup>
import { withBase } from 'vitepress'
import { data } from './content.data.js'
import { relevantDate, sortItems } from './items.js'

const props = defineProps({
  items: { type: Array, required: true },
  showProject: { type: Boolean, default: false }
})

const projectNames = Object.fromEntries(data.projects.map((p) => [p.slug, p.name]))
</script>

<template>
  <ul class="item-list">
    <li v-for="item in sortItems(props.items)" :key="item.id">
      <a class="item-row" :href="withBase(item.url)">
        <span class="item-title">{{ item.title }}</span>
        <span class="item-meta">
          <span v-if="showProject">{{ projectNames[item.project] }}</span>
          <span>{{ item.type }}</span>
          <span>{{ relevantDate(item) }}</span>
        </span>
      </a>
    </li>
  </ul>
</template>
