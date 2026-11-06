<script setup>
import { computed } from 'vue'
import { useData, withBase } from 'vitepress'
import { data } from './content.data.js'
import Home from './Home.vue'
import ProjectItems from './ProjectItems.vue'
import ItemPage from './ItemPage.vue'
import ArchivePage from './ArchivePage.vue'
import NewItemButton from './NewItemButton.vue'
import NewItemPage from './NewItemPage.vue'
import TokenPage from './TokenPage.vue'

const { site, frontmatter, page } = useData()

const projectSlug = computed(() => page.value.relativePath.match(/^projects\/([^/]+)\.md$/)?.[1])

const item = computed(() => {
  const url = '/' + page.value.relativePath.replace(/\.md$/, '')
  return data.items.find((item) => item.url === url)
})

const currentProject = computed(() => projectSlug.value ?? item.value?.project ?? '')
</script>

<template>
  <div class="site">
    <header class="site-header">
      <a class="site-title" :href="withBase('/')">
        <img class="avatar" :src="withBase('/avatar.jpg')" alt="" />
        {{ site.title }}
      </a>
      <NewItemButton v-if="frontmatter.layout !== 'new'" :project="currentProject" />
    </header>

    <main>
      <div v-if="page.isNotFound" class="not-found">
        <h1>Not found</h1>
        <p><a :href="withBase('/')">Back home</a></p>
      </div>
      <Home v-else-if="frontmatter.layout === 'home'" />
      <ArchivePage v-else-if="frontmatter.layout === 'archive'" />
      <NewItemPage v-else-if="frontmatter.layout === 'new'" />
      <TokenPage v-else-if="frontmatter.layout === 'token'" />
      <ItemPage v-else-if="item" :item="item" />
      <template v-else>
        <article class="prose">
          <Content />
        </article>
        <ProjectItems v-if="projectSlug" :project="projectSlug" />
      </template>
    </main>

    <footer class="site-footer">
      <a :href="withBase('/archive')">Archive</a>
      <a :href="withBase('/token')">Token</a>
    </footer>
  </div>
</template>
