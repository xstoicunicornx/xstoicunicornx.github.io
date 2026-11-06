<script setup>
import { withBase } from 'vitepress'
import { data } from './content.data.js'
import ItemList from './ItemList.vue'

const active = data.items.filter((item) => !item.archived && item.status === 'active')
</script>

<template>
  <section class="home">
    <section v-if="active.length" class="item-section">
      <h2 class="section-label">Active</h2>
      <ItemList :items="active" show-project />
    </section>

    <ul class="projects">
      <li v-for="project in data.projects" :key="project.slug">
        <a class="project" :href="withBase(`/projects/${project.slug}`)">
          <span class="project-name">{{ project.name }}</span>
        </a>
      </li>
    </ul>
  </section>
</template>
