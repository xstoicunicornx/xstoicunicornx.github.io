<script setup>
import { computed, onMounted } from 'vue'
import { useData, withBase } from 'vitepress'
import { data } from './content.data.js'
import { useItems } from './localChanges.js'
import { loadToken } from './github.js'
import { openToken } from './dialogs.js'
import Home from './Home.vue'
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

const currentProject = computed(() => projectSlug.value ?? item.value?.project ?? '')
</script>

<template>
  <div class="site">
    <header class="site-header">
      <a class="site-title" :href="withBase('/')">
        <img class="avatar" :src="withBase('/avatar.jpg')" alt="" />
        {{ site.title }}
      </a>
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
        <article class="prose">
          <Content />
        </article>
        <ProjectItems v-if="projectSlug" :project="projectSlug" />
      </template>
    </main>

    <footer class="site-footer">
      <a :href="withBase('/archive')">Archive</a>
      <button type="button" class="link-button" @click="openToken">Token</button>
    </footer>

    <ItemDialog />
    <TokenDialog />
    <Toast />
  </div>
</template>
