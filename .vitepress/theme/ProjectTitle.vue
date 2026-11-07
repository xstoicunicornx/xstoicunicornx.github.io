<script setup>
import { computed } from 'vue'
import { useData } from 'vitepress'
import { data } from './content.data.js'

// Project name and avatar for the page header. Links to the project's GitHub
// org or user, or straight to the repo for a single-repo project (no `org`).
// The avatar is the org's, or the repo owner's, served live by GitHub.
const props = defineProps({ project: { type: String, required: true } })

const { frontmatter } = useData()

const name = computed(() => data.projects.find((p) => p.slug === props.project)?.name ?? props.project)
const href = computed(() => `https://github.com/${frontmatter.value.org ?? frontmatter.value.repo}`)
const owner = computed(() => frontmatter.value.org ?? frontmatter.value.repo.split('/')[0])
</script>

<template>
  <h1 class="page-title project-title">
    <a :href="href" target="_blank" rel="noopener">
      <img class="avatar" :src="`https://github.com/${owner}.png?size=96`" alt="" />
      {{ name }}
    </a>
  </h1>
</template>
