<script setup>
import { useData, withBase } from 'vitepress'
import Home from './Home.vue'

const { site, frontmatter, page } = useData()
</script>

<template>
  <div class="site">
    <header class="site-header">
      <a class="site-title" :href="withBase('/')">
        <span class="moon" aria-hidden="true"></span>
        {{ site.title }}
      </a>
    </header>

    <main>
      <div v-if="page.isNotFound" class="not-found">
        <h1>Not found</h1>
        <p><a :href="withBase('/')">Back home</a></p>
      </div>
      <Home v-else-if="frontmatter.layout === 'home'" />
      <article v-else class="prose">
        <Content />
      </article>
    </main>
  </div>
</template>
