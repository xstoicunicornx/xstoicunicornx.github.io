import fs from 'node:fs'
import { defineConfig } from 'vitepress'
import { validateProjectPage } from './theme/validate.js'

export default defineConfig({
  title: 'xstoicunicornx',
  description: 'Projects I want to work on and follow up on.',
  cleanUrls: true,
  appearance: false,
  srcExclude: ['README.md'],
  head: [['meta', { name: 'theme-color', content: '#0a1530' }]],
  // Fail the build on a project page with bad repo frontmatter, and use the
  // project's name from data/projects/<name>.json as the page title.
  transformPageData(pageData, { siteConfig }) {
    const slug = pageData.relativePath.match(/^projects\/([^/]+)\.md$/)?.[1]
    if (!slug) return
    const errors = validateProjectPage(pageData.frontmatter)
    if (errors.length) throw new Error(`${pageData.relativePath}: ${errors.join('; ')}`)
    const project = JSON.parse(fs.readFileSync(`${siteConfig.srcDir}/data/projects/${slug}.json`, 'utf-8'))
    pageData.title = project.name
  },
  themeConfig: {
    repo: 'xstoicunicornx/xstoicunicornx.github.io',
    branch: 'main'
  }
})
