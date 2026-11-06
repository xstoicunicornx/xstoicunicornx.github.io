<script setup>
import { withBase } from 'vitepress'
import { data as projects } from './projects.data.js'
</script>

<template>
  <section class="home">
    <ul class="projects">
      <li v-for="project in projects" :key="project.slug">
        <a class="project" :href="withBase(`/projects/${project.slug}`)">
          <span class="project-name">{{ project.name }}</span>
        </a>
      </li>
    </ul>
  </section>
</template>
