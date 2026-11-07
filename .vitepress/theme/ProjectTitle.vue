<script setup>
import { computed } from 'vue'
import { useData } from 'vitepress'
import { data } from './content.data.js'

// Project name for the page header. Links to the project's GitHub org, or
// straight to the repo for a personal project (no `org` in the frontmatter).
const props = defineProps({ project: { type: String, required: true } })

const { frontmatter } = useData()

const name = computed(() => data.projects.find((p) => p.slug === props.project)?.name ?? props.project)
const href = computed(() => `https://github.com/${frontmatter.value.org ?? frontmatter.value.repo}`)
</script>

<template>
  <h1 class="page-title project-title">
    <a :href="href" target="_blank" rel="noopener">{{ name }}</a>
  </h1>
</template>
