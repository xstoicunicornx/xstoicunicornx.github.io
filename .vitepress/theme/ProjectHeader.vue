<script setup>
import { computed } from 'vue'
import { useData } from 'vitepress'
import { data } from './content.data.js'

// Header for a project page, built from the page's org/repo/repos frontmatter.
// The title links to the org, or straight to the repo for a personal project.
const props = defineProps({ project: { type: String, required: true } })

const { frontmatter } = useData()

const name = computed(() => data.projects.find((p) => p.slug === props.project)?.name ?? props.project)
const titleHref = computed(() => `https://github.com/${frontmatter.value.org ?? frontmatter.value.repo}`)

// Repos inside the project's org show just their name; others show owner/name.
const label = (repo) => {
  const [owner, repoName] = repo.split('/')
  return owner === frontmatter.value.org ? repoName : repo
}

// For a personal project the title already links to the main repo.
const links = computed(() => {
  const others = frontmatter.value.repos ?? []
  return frontmatter.value.org ? [frontmatter.value.repo, ...others] : others
})
</script>

<template>
  <header class="project-header">
    <h1 class="page-title">
      <a :href="titleHref">{{ name }}</a>
    </h1>
    <ul v-if="links.length" class="repo-links">
      <li v-for="(repo, i) in links" :key="repo" :class="{ 'repo-main': frontmatter.org && i === 0 }">
        <a :href="`https://github.com/${repo}`">{{ label(repo) }}</a>
      </li>
    </ul>
  </header>
</template>
