<script setup>
import { computed, onMounted } from 'vue'
import { useData, withBase } from 'vitepress'
import { data } from './content.data.js'
import { useItems } from './localChanges.js'
import { loadToken } from './github.js'
import { openToken } from './dialogs.js'
import Home from './Home.vue'
import ProjectTitle from './ProjectTitle.vue'
import ProjectRepos from './ProjectRepos.vue'
import ProjectItems from './ProjectItems.vue'
import ItemPage from './ItemPage.vue'
import ArchivePage from './ArchivePage.vue'
import NewItemButton from './NewItemButton.vue'
import ItemDialog from './ItemDialog.vue'
import TokenDialog from './TokenDialog.vue'
import Toast from './Toast.vue'

const { site, frontmatter, page } = useData()
const items = useItems()

onMounted(loadToken)

const projectSlug = computed(() => page.value.relativePath.match(/^projects\/([^/]+)\.md$/)?.[1])

// Match the page to the built item by path, then show its latest local version.
const item = computed(() => {
  const url = '/' + page.value.relativePath.replace(/\.md$/, '')
  const built = data.items.find((item) => item.url === url)
  return built && items.value.find((item) => item.id === built.id)
})

const isHome = computed(() => frontmatter.value.layout === 'home')

const currentProject = computed(() => projectSlug.value ?? item.value?.project ?? '')
</script>

<template>
  <div class="site">
    <header class="site-header">
      <a v-if="isHome" class="site-title" :href="withBase('/')">
        <img class="avatar" :src="withBase('/avatar.jpg')" alt="" />
        {{ site.title }}
      </a>
      <template v-else-if="projectSlug">
        <a class="back-link" :href="withBase('/')" aria-label="Back to home" title="Back to home">
          <svg viewBox="0 0 16 16" width="18" height="18" aria-hidden="true">
            <path
              d="M10 3L5 8l5 5"
              fill="none"
              stroke="currentColor"
              stroke-width="1.6"
              stroke-linecap="round"
              stroke-linejoin="round"
            />
          </svg>
        </a>
        <ProjectTitle :project="projectSlug" />
      </template>
      <NewItemButton :project="currentProject" />
    </header>

    <main>
      <div v-if="page.isNotFound" class="not-found">
        <h1>Not found</h1>
        <p><a :href="withBase('/')">Back home</a></p>
      </div>
      <Home v-else-if="frontmatter.layout === 'home'" />
      <ArchivePage v-else-if="frontmatter.layout === 'archive'" />
      <ItemPage v-else-if="item" :item="item" />
      <template v-else>
        <ProjectRepos v-if="projectSlug" />
        <article class="prose">
          <Content />
        </article>
        <ProjectItems v-if="projectSlug" :project="projectSlug" />
      </template>
    </main>

    <footer class="site-footer">
      <a v-if="!isHome" :href="withBase('/')">Home</a>
      <a :href="withBase('/archive')">Archive</a>
      <button type="button" class="link-button" @click="openToken">Token</button>
    </footer>

    <ItemDialog />
    <TokenDialog />
    <Toast />
  </div>
</template>
