<script setup>
import { computed } from 'vue'
import { useData } from 'vitepress'
import { REPO_ICON } from './octicons.js'

// Boxed repo links at the top of a project page, from its repo/repos
// frontmatter. The main repo comes first and is highlighted.
const { frontmatter } = useData()

// Repos inside the project's org show just their name; others show owner/name.
const label = (repo) => {
  const [owner, repoName] = repo.split('/')
  return owner === frontmatter.value.org ? repoName : repo
}

// For a personal project the title already links to the main repo.
// An org project may have no main repo, in which case none is highlighted.
const links = computed(() => {
  const { org, repo, repos = [] } = frontmatter.value
  return org && repo ? [repo, ...repos] : repos
})
</script>

<template>
  <nav v-if="links.length" class="repo-box" aria-label="Repositories">
    <ul class="repo-links">
      <li v-for="(repo, i) in links" :key="repo">
        <a
          class="repo-link"
          :class="{ 'repo-main': frontmatter.org && frontmatter.repo && i === 0 }"
          :href="`https://github.com/${repo}`"
          target="_blank"
          rel="noopener"
        >
          <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">
            <path v-for="d in REPO_ICON" :key="d" :d="d" fill="currentColor" />
          </svg>
          {{ label(repo) }}
        </a>
      </li>
    </ul>
  </nav>
</template>
