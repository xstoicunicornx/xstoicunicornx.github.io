<script setup>
import { withBase } from 'vitepress'
import { data as projects } from './projects.data.js'

function nextTodo(project) {
  return project.todos.find((todo) => !todo.done)
}

function doneCount(project) {
  return project.todos.filter((todo) => todo.done).length
}
</script>

<template>
  <section class="home">
    <p class="intro">Things I want to work on, follow up on, or get done.</p>

    <ul class="projects">
      <li v-for="project in projects" :key="project.slug">
        <a class="project" :href="withBase(`/projects/${project.slug}`)">
          <div class="project-head">
            <span class="project-name">{{ project.name }}</span>
            <span class="status" :class="`status-${project.status}`">{{ project.status }}</span>
          </div>
          <p v-if="project.description" class="project-desc">{{ project.description }}</p>
          <p v-if="nextTodo(project)" class="project-next">
            <span class="label">next</span> {{ nextTodo(project).text }}
          </p>
          <p v-if="project.todos.length" class="project-count">
            {{ doneCount(project) }}/{{ project.todos.length }} done
          </p>
        </a>
      </li>
    </ul>
  </section>
</template>
